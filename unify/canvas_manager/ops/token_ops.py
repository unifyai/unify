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


def active_project() -> str:
    """Unify project owning the current assistant's contexts."""
    import unisdk

    return unisdk.active_project() or ""


def _bundle_object_path(token: str, sha256: str) -> str:
    """Object path for one compiled bundle.

    Content-addressed under the canvas token, so republishing the same source is
    idempotent and two canvases never collide.
    """
    return f"canvas/{token}/{sha256}.mjs"


def upload_bundle(token: str, bundle: str, *, sha256: str) -> str:
    """Store a compiled bundle and return the URI recorded on the canvas row.

    The bucket is private. Console fetches the bytes server-side and verifies
    this sha256 before handing them to the frame, which is a stronger integrity
    guarantee than subresource integrity because we enforce it rather than
    asking the browser to. It also keeps compiled code -- which encodes column
    names and query shapes -- off any publicly addressable path.

    Falls back to an inline URI when no bucket is configured, so self-host and
    tests work without object storage.
    """
    from unify.canvas_manager.settings import CanvasSettings

    bucket = CanvasSettings().BUNDLE_BUCKET.strip()
    if not bucket:
        return f"inline://{sha256}"

    path = _bundle_object_path(token, sha256)
    _BUNDLE_CACHE[f"gs://{bucket}/{path}"] = bundle
    return f"gs://{bucket}/{path}"


def fetch_bundle(bundle_uri: str) -> str:
    """Read a compiled bundle back, for re-rendering an existing canvas."""
    return _BUNDLE_CACHE.get(bundle_uri, "")


# Process-local bundle cache. Object-storage upload is wired in P3 alongside the
# Orchestra bundle proxy; until then a bundle is re-derivable from the stored
# source, so nothing is lost by not persisting it here.
_BUNDLE_CACHE: dict[str, str] = {}
