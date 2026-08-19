"""Fetch a public URL to disk without letting it reach inside the network.

The dangerous part of fetching a caller-supplied URL is not the file, it is the
*request*. A URL is an instruction to open a connection from inside the trusted
runtime, so ``http://169.254.169.254/`` reads cloud instance credentials and
``http://127.0.0.1:8000/`` reaches services that assume only local callers can
see them. Neither looks like an attack at the call site: both are ordinary
fetches that return 200.

So every hop is validated before it is followed. A redirect is a *new* request
to a *new* host, which is the usual way a public-looking URL becomes a private
one, and validating only the first URL would check the one hop an attacker does
not control.

Content is a separate concern and already handled elsewhere: parsing happens on
the dispatched fleet, off the assistant's process, so a hostile file never
executes where the agent lives. What is enforced here is that the request goes
somewhere public, does not run forever, and does not write without bound.
"""

from __future__ import annotations

import ipaddress
import logging
import re
import socket
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse, unquote

import httpx

logger = logging.getLogger(__name__)

ALLOWED_SCHEMES = frozenset({"http", "https"})
MAX_REDIRECTS = 5
DEFAULT_MAX_BYTES = 512 * 1024 * 1024
DEFAULT_TIMEOUT_S = 120.0

_UNSAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


class FetchRejected(Exception):
    """A URL was refused before any request was made, or mid-redirect."""


def _addresses_for(host: str) -> list[ipaddress._BaseAddress]:
    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise FetchRejected(f"Could not resolve host {host!r}: {exc}") from exc
    found = []
    for info in infos:
        try:
            found.append(ipaddress.ip_address(info[4][0]))
        except ValueError:
            continue
    if not found:
        raise FetchRejected(f"Host {host!r} resolved to no usable address.")
    return found


def _reject_non_public(host: str, addresses: Iterable[ipaddress._BaseAddress]) -> None:
    """Refuse any host that resolves to an address outside the public internet.

    ``is_global`` is the check rather than ``is_private``: the space that must
    not be reachable includes loopback, link-local (which is where cloud
    metadata lives), multicast, and reserved ranges, and enumerating those
    individually leaves whichever one is forgotten reachable. Requiring the
    address to be positively global fails closed instead.

    Every resolved address is checked, not just the first -- a host answering
    with one public and one private address would otherwise pass while
    connecting to either.
    """
    for address in addresses:
        if not address.is_global:
            raise FetchRejected(
                f"{host!r} resolves to {address}, which is not a public address. "
                "Only the public internet can be fetched this way.",
            )


def assert_fetchable(url: str) -> None:
    """Refuse *url* unless it is a public http(s) address.

    Raises :class:`FetchRejected` with a reason the caller can relay.
    """
    parsed = urlparse(url)
    if parsed.scheme.lower() not in ALLOWED_SCHEMES:
        raise FetchRejected(
            f"Only http and https can be fetched; got {parsed.scheme or 'no'} scheme.",
        )
    host = parsed.hostname
    if not host:
        raise FetchRejected("The URL names no host.")
    if parsed.username or parsed.password:
        raise FetchRejected(
            "Credentials embedded in a URL are not forwarded; remove them.",
        )
    _reject_non_public(host, _addresses_for(host))


def filename_for(url: str, content_disposition: str = "") -> str:
    """A safe, recognisable filename for the fetched bytes.

    Derived from the URL, never from a path the server suggests: a
    ``Content-Disposition`` naming ``../../etc/passwd`` is a write outside the
    destination if it is trusted verbatim.
    """
    match = re.search(r'filename="?([^";]+)"?', content_disposition or "")
    raw = match.group(1) if match else Path(unquote(urlparse(url).path)).name
    cleaned = _UNSAFE_NAME.sub("_", Path(raw or "download").name).strip("._-")
    return cleaned or "download"


async def fetch_to_directory(
    url: str,
    dest_dir: str | Path,
    *,
    max_bytes: int = DEFAULT_MAX_BYTES,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> Path:
    """Download *url* into *dest_dir* and return the path written.

    Redirects are followed manually so each hop can be validated; the client is
    told not to follow them itself, since a client-followed redirect is a
    request that was never checked.
    """
    destination = Path(dest_dir)
    destination.mkdir(parents=True, exist_ok=True)

    current = url
    async with httpx.AsyncClient(
        timeout=timeout_s,
        follow_redirects=False,
    ) as client:
        for _hop in range(MAX_REDIRECTS + 1):
            assert_fetchable(current)
            async with client.stream("GET", current) as response:
                if response.is_redirect:
                    location = response.headers.get("location")
                    if not location:
                        raise FetchRejected(
                            "The server redirected without saying where to.",
                        )
                    current = str(httpx.URL(current).join(location))
                    continue
                response.raise_for_status()

                declared = response.headers.get("content-length")
                if declared and int(declared) > max_bytes:
                    raise FetchRejected(
                        f"The file is {int(declared)} bytes, over the "
                        f"{max_bytes}-byte limit for a direct fetch.",
                    )

                target = destination / filename_for(
                    current,
                    response.headers.get("content-disposition", ""),
                )
                written = 0
                with target.open("wb") as sink:
                    async for chunk in response.aiter_bytes():
                        written += len(chunk)
                        if written > max_bytes:
                            sink.close()
                            target.unlink(missing_ok=True)
                            # A server can under-declare or omit its length, so
                            # the ceiling is enforced against what actually
                            # arrives rather than what was announced.
                            raise FetchRejected(
                                f"The download exceeded the {max_bytes}-byte "
                                "limit and was discarded.",
                            )
                        sink.write(chunk)
                logger.info("[url-fetch] %s -> %s (%d bytes)", url, target, written)
                return target

    raise FetchRejected(f"Gave up after {MAX_REDIRECTS} redirects.")
