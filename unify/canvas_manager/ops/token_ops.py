"""Canvas token registration.

A canvas row lives in a Unify context, but a short URL has to resolve to its
owner before anything can be read. Orchestra keeps that mapping in a dedicated
table, and this registers and revokes entries in it.

Registration failures are logged rather than raised: the canvas row is already
written by the time this runs, and losing the URL mapping is a degraded canvas
rather than a lost one. Deletion is idempotent for the same reason.
"""

from __future__ import annotations

import logging
import secrets

import httpx

from unify.session_details import SESSION_DETAILS
from unify.settings import SETTINGS

logger = logging.getLogger(__name__)

CANVAS_ENTITY_TYPE = "canvas"


def generate_token() -> str:
    """Generate a cryptographically secure 12-character URL-safe token."""
    return secrets.token_urlsafe(9)[:12]


def _auth_headers() -> dict[str, str]:
    unify_key = SESSION_DETAILS.unify_key
    if not unify_key:
        logger.warning("UNIFY_KEY not set - canvas token registration may fail")
    return {"Authorization": f"Bearer {unify_key}", "Content-Type": "application/json"}


def register_token(
    token: str,
    *,
    context_name: str,
    project_name: str,
    visibility: str = "private",
) -> bool:
    """Map a canvas token to the context and owner that can serve it.

    Returns ``True`` on success or when the mapping already exists.
    """
    url = f"{SETTINGS.ORCHESTRA_URL}/canvas/tokens"
    payload = {
        "token": token,
        "entity_type": CANVAS_ENTITY_TYPE,
        "context_name": context_name,
        "project_name": project_name,
        "visibility": visibility,
    }

    try:
        response = httpx.post(url, json=payload, headers=_auth_headers(), timeout=30.0)
    except httpx.HTTPError as error:
        logger.warning("Canvas token registration failed for %s: %s", token, error)
        return False

    if response.status_code in (200, 201, 409):
        return True

    logger.warning(
        "Canvas token registration failed for %s: %s %s",
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
