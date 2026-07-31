"""Canvas token registration.

A canvas row lives in a Unify context, but a short URL has to resolve to its
owner before anything can be read. Orchestra keeps that mapping in a dedicated
table, and this registers and revokes entries in it.

Registration runs *before* the canvas row is written, so its outcome can still
change what happens: a token that genuinely belongs to another canvas gets a
fresh token rather than a dead URL, and an unreachable backend degrades to a
stored-but-unrouted canvas that the result says so about. Deletion is
idempotent for the same reason.
"""

from __future__ import annotations

import logging
import secrets
from typing import Literal, Optional

import httpx

from unify.session_details import SESSION_DETAILS
from unify.settings import SETTINGS

logger = logging.getLogger(__name__)


def generate_token() -> str:
    """Generate a cryptographically secure 12-character URL-safe token."""
    return secrets.token_urlsafe(9)[:12]


def _auth_headers() -> dict[str, str]:
    unify_key = SESSION_DETAILS.unify_key
    if not unify_key:
        logger.warning("UNIFY_KEY not set - canvas token registration may fail")
    return {"Authorization": f"Bearer {unify_key}", "Content-Type": "application/json"}


RegistrationOutcome = Literal["registered", "collision", "unreachable"]


def register_token(
    token: str,
    *,
    context_name: str,
    project_name: str,
    visibility: str = "private",
    status: str = "published",
) -> RegistrationOutcome:
    """Map a canvas token to the context and owner that can serve it.

    ``status`` defaults to published because a canvas only reaches this point
    after every authoring gate has passed; there is no state in which the row
    exists and the canvas is not meant to be servable.

    The three outcomes ask for different responses, which is why this is not a
    boolean: ``collision`` means mint a fresh token and try again,
    ``unreachable`` means the canvas can be stored but its URL will not resolve
    until re-registered -- worth telling the author, not worth losing the work.
    """
    url = f"{SETTINGS.ORCHESTRA_URL}/canvas/tokens"
    payload = {
        "token": token,
        "context_name": context_name,
        "project_name": project_name,
        "visibility": visibility,
        "status": status,
    }

    try:
        response = httpx.post(url, json=payload, headers=_auth_headers(), timeout=30.0)
    except httpx.HTTPError as error:
        logger.warning("Canvas token registration failed for %s: %s", token, error)
        return "unreachable"

    if response.status_code in (200, 201):
        return "registered"

    # A conflict is normally our own retry, and treating that as success is the
    # point of it. But the same status covers a token that genuinely belongs to
    # another canvas, and accepting that would leave the URL resolving to
    # somebody else's view. Vanishingly unlikely at 72 bits of entropy, and
    # cheap to rule out, so confirm the existing row is the one we meant to write.
    if response.status_code == 409:
        existing = _token_context(token)
        if existing == context_name:
            return "registered"
        logger.error(
            "Canvas token %s is already registered to %r, not %r; refusing to "
            "treat the collision as success.",
            token,
            existing,
            context_name,
        )
        return "collision"

    logger.warning(
        "Canvas token registration failed for %s: %s %s",
        token,
        response.status_code,
        response.text[:200],
    )
    return "unreachable"


def _token_context(token: str) -> Optional[str]:
    """The context a token currently resolves to, or None if unreadable.

    Uses the owner-scoped read rather than the admin resolver: an assistant sees
    its own registrations and nothing else, which is all this check needs.
    """
    url = f"{SETTINGS.ORCHESTRA_URL}/canvas/tokens/{token}"
    try:
        response = httpx.get(url, headers=_auth_headers(), timeout=30.0)
    except httpx.HTTPError:
        return None
    if response.status_code != 200:
        return None
    return response.json().get("context_name")


def set_token_state(
    token: str,
    *,
    visibility: str | None = None,
    status: str | None = None,
) -> bool:
    """Change a canvas's visibility or lifecycle status in place.

    Separate from registration because both are operational: quarantining a
    canvas already in front of viewers has to work on the live token rather than
    by reissuing it.
    """
    url = f"{SETTINGS.ORCHESTRA_URL}/canvas/tokens/{token}"
    payload = {
        key: value
        for key, value in (("visibility", visibility), ("status", status))
        if value is not None
    }
    if not payload:
        return True

    try:
        response = httpx.patch(
            url,
            json=payload,
            headers=_auth_headers(),
            timeout=30.0,
        )
    except httpx.HTTPError as error:
        logger.warning("Canvas token update failed for %s: %s", token, error)
        return False

    if response.status_code == 200:
        return True

    logger.warning(
        "Canvas token update failed for %s: %s %s",
        token,
        response.status_code,
        response.text[:200],
    )
    return False


def delete_token(token: str) -> bool:
    """Revoke a canvas token so its URL stops resolving."""
    url = f"{SETTINGS.ORCHESTRA_URL}/canvas/tokens/{token}"

    try:
        response = httpx.delete(url, headers=_auth_headers(), timeout=30.0)
    except httpx.HTTPError as error:
        logger.warning("Canvas token deletion failed for %s: %s", token, error)
        return False

    return response.status_code in (200, 204, 404)


def build_canvas_url(token: str) -> str:
    """Shareable console URL for a canvas."""
    return f"{SETTINGS.CONSOLE_URL.rstrip('/')}/canvas/view/{token}"


def active_project() -> str:
    """Unify project owning the current assistant's contexts."""
    import unisdk

    return unisdk.active_project() or ""
