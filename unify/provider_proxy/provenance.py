"""What a reference points at, and which route can actually reach it.

A user names content in whatever form they have it: a sharing link, a canonical
library URL, a bare https address, a path. Each of those needs a different
mechanism, and the mechanisms fail in ways that look alike -- an empty listing,
a 404, a page of HTML where a file was expected -- so choosing between them by
trying them in turn produces a sequence of indistinguishable failures and no
diagnosis. That is how a folder that was correctly shared read as revoked for a
week.

So the reference is classified first, and the route follows from what it *is*
rather than from what has not failed yet. Classification is deliberately cheap
and offline: the kinds below are decided from the shape of the reference and
from provenance already learned during discovery, never by probing.

The contract is provider-agnostic on purpose. Microsoft resolves ownership by
tenant; Google Drive is per-file ACL with no tenant-scoped credential at all,
so it must answer the same questions its own way rather than inheriting
semantics that do not apply to it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

# A sharing link, e.g. .../:f:/g/personal/<user>/<token>?e=<code>
_MS_SHARE_PATH = re.compile(r"/:[a-z]:/[a-z]/", re.IGNORECASE)
_GOOGLE_HOSTS = ("drive.google.com", "docs.google.com")

KIND_PUBLIC_URL = "public_url"
KIND_LINK_ONLY = "link_only"
KIND_OWNED = "owned"
KIND_SHARED_SAME_ORG = "shared_same_org"
KIND_SHARED_CROSS_ORG = "shared_cross_org"

# How a kind can actually be reached. A lane named here is one we have evidence
# for, not one that ought to work.
LANE_API = "api"
LANE_FETCH = "fetch"
LANE_BROWSER = "browser"


@dataclass(frozen=True)
class Provenance:
    """Where content lives and how it can be reached."""

    kind: str
    provider: str = ""
    owner_org: str = ""
    reachable_by: tuple[str, ...] = field(default_factory=tuple)
    reason: str = ""

    @property
    def needs_authorisation(self) -> bool:
        """Whether reaching this requires someone outside the product to act."""
        return self.kind == KIND_SHARED_CROSS_ORG


def _is_provider_host(host: str) -> Optional[str]:
    host = (host or "").lower()
    if host.endswith("sharepoint.com") or host.endswith("onedrive.live.com"):
        return "microsoft"
    if host in _GOOGLE_HOSTS:
        return "google"
    return ""


def classify_url(url: str) -> Provenance:
    """Classify a URL without touching the network.

    A provider *sharing* link is not a public URL even though it is an https
    address: whether it can be fetched depends on how it was scoped, which the
    link itself does not say. An anonymous link is readable by anyone and a
    plain fetch works; an identity-scoped one returns a sign-in page, so
    fetching it yields HTML that looks like success. Naming the ambiguity is
    more useful than guessing at it.
    """
    parsed = urlparse(url)
    provider = _is_provider_host(parsed.hostname or "")

    if not provider:
        return Provenance(
            kind=KIND_PUBLIC_URL,
            reachable_by=(LANE_FETCH,),
            reason="An ordinary address with no provider identity behind it.",
        )

    if provider == "microsoft" and _MS_SHARE_PATH.search(parsed.path or ""):
        return Provenance(
            kind=KIND_LINK_ONLY,
            provider=provider,
            # Fetch first: an anonymous link is just a public URL, and that is
            # the common case. The browser is the fallback for a link scoped to
            # named people, where an unauthenticated fetch returns a sign-in
            # page rather than the file.
            reachable_by=(LANE_FETCH, LANE_BROWSER),
            reason=(
                "A sharing link. Readable directly if it was shared with "
                "anyone who has the link; otherwise it needs a signed-in "
                "session."
            ),
        )

    return Provenance(
        kind=KIND_OWNED,
        provider=provider,
        reachable_by=(LANE_API,),
        reason="A canonical provider address; the workspace API addresses it.",
    )


def classify_item(
    provider: str,
    drive_id: str,
    home_org: str,
    owner_org: str = "",
) -> Provenance:
    """Classify an item already discovered through a workspace listing.

    ``owner_org`` is the tenant (Microsoft) or domain (Google) that owns the
    drive, as learned during discovery. An empty value means nothing has said
    otherwise, which is treated as our own -- the ordinary credential is
    correct and no approval is implied.
    """
    if owner_org and home_org and owner_org != home_org:
        return Provenance(
            kind=KIND_SHARED_CROSS_ORG,
            provider=provider,
            owner_org=owner_org,
            # The API reaches it, but only with a credential issued by the
            # owning organisation, which exists only once they have approved
            # the application.
            reachable_by=(LANE_API,),
            reason=(
                "Owned by another organisation. Reachable with a credential "
                "issued by them, which requires their administrator's approval "
                "once."
            ),
        )
    if owner_org and home_org and owner_org == home_org and drive_id:
        return Provenance(
            kind=KIND_SHARED_SAME_ORG,
            provider=provider,
            owner_org=owner_org,
            reachable_by=(LANE_API,),
            reason="Shared from inside this organisation; the ordinary credential reaches it.",
        )
    return Provenance(
        kind=KIND_OWNED,
        provider=provider,
        owner_org=home_org,
        reachable_by=(LANE_API,),
        reason="This account's own content.",
    )
