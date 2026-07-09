"""Shared FastAPI auth dependencies for ``unify.gateway`` routers.

Two dependencies surface here:

* ``admin_auth_dependency``: matches the Bearer-token-against-Orchestra
  admin-key shape used by ``communication/dependencies.py::auth_admin_key``.
  Most gateway channels mount their routers behind this dependency so
  only trusted Unity/Orchestra callers can hit them.
* ``user_api_key_auth_dependency``: the user-API-keyed flow used by
  ``unillm`` (and any future SDK-style public endpoints). Validates
  the Bearer token against Orchestra's ``/user/basic-info``.

Promoting both up out of the per-channel views keeps the aggregator
(``unify.gateway.app``) free of repeated auth wiring boilerplate.
"""

from __future__ import annotations

import secrets

import httpx
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from unify.settings import SETTINGS

_bearer_scheme = HTTPBearer(auto_error=True)


def auth_admin_key(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> None:
    """Reject the request unless the Bearer token matches ``ORCHESTRA_ADMIN_KEY``.

    Uses ``secrets.compare_digest`` for constant-time comparison so
    timing attacks can't probe the admin key. Returns 403 on mismatch
    rather than 401 because the request *was* authenticated -- just
    not as an admin.
    """
    presented = credentials.credentials
    expected = SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()
    if expected and secrets.compare_digest(presented, expected):
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Admin access unauthorized.",
    )


async def authenticate_user_api_key(api_key: str) -> dict:
    """Validate a user API key against Orchestra's ``/user/basic-info`` endpoint.

    Returns the user-info dict on success. Raises ``HTTPException(401)``
    on any non-200 response (including Orchestra outages); the safest
    default for a credential-gated endpoint is to deny rather than
    leak when we can't prove the key is valid.
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/user/basic-info",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10.0,
        )
    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="Invalid API key.")
    return response.json()


async def auth_admin_or_user_key(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> None:
    """Accept either the Orchestra admin key or a valid user API key.

    Admin match (constant-time) marks the request as trusted
    control-plane traffic (``request.state.gateway_is_admin = True``).
    Otherwise the Bearer is validated as a user API key against
    Orchestra (401 on failure) and the key + user info are stashed on
    ``request.state`` so handlers can enforce per-assistant ownership
    via :func:`require_assistant_ownership`.
    """
    presented = credentials.credentials
    expected = SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()
    if expected and secrets.compare_digest(presented, expected):
        request.state.gateway_is_admin = True
        return
    info = await authenticate_user_api_key(presented)
    request.state.gateway_is_admin = False
    request.state.gateway_api_key = presented
    request.state.gateway_user = info


def require_gateway_admin(request: Request) -> None:
    """Reject user-key callers on control-plane-only handlers.

    For routers mounted behind ``admin_or_user_auth_dependency``, some
    endpoints (provisioning, install management) must remain reserved
    for the admin key. Only requests the dual dependency explicitly
    classified as user-key callers (``gateway_is_admin`` False) are
    rejected; admin callers and mounts that never ran the dual
    dependency (admin-only mounts already gate at the router) pass.
    """
    if getattr(request.state, "gateway_is_admin", True):
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Admin access unauthorized.",
    )


async def require_assistant_ownership(request: Request, agent_id) -> None:
    """Verify a user-key caller owns the assistant acting in this request.

    Admin callers (and mounts that never ran the dual dependency) are
    trusted control-plane traffic and bypass the check. For user-key
    callers, Orchestra's user-scoped ``GET /assistant?agent_id=`` is
    queried with the caller's own key: a 200 listing the assistant
    proves ownership (Orchestra scopes results to the key's owner, so
    no manual user-id comparison is needed). Any other outcome --
    missing identity, non-200, transport error, or an empty result --
    fails closed with 403.
    """
    if getattr(request.state, "gateway_is_admin", True):
        return
    denied = HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Assistant not owned by caller.",
    )
    api_key = getattr(request.state, "gateway_api_key", "")
    if not api_key or agent_id in (None, ""):
        raise denied
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{SETTINGS.ORCHESTRA_URL}/assistant",
                params={"agent_id": str(agent_id)},
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=10.0,
            )
    except HTTPException:
        raise
    except Exception:
        raise denied
    if response.status_code != 200:
        raise denied
    try:
        assistants = response.json().get("info", [])
    except Exception:
        raise denied
    if not any(str(a.get("agent_id")) == str(agent_id) for a in assistants):
        raise denied


def extract_api_key(request: Request) -> str:
    """Extract the Bearer token from the Authorization header."""
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:]
    raise HTTPException(status_code=401, detail="Missing API key.")


# Dependency tuples convenient to splat into ``app.include_router(...)``.
admin_auth_dependency = [Depends(auth_admin_key)]
"""``dependencies=`` value mounting a router behind the admin-key check."""

admin_or_user_auth_dependency = [Depends(auth_admin_or_user_key)]
"""``dependencies=`` value accepting the admin key or a valid user API key."""


__all__ = [
    "admin_auth_dependency",
    "admin_or_user_auth_dependency",
    "auth_admin_key",
    "auth_admin_or_user_key",
    "authenticate_user_api_key",
    "extract_api_key",
    "require_assistant_ownership",
    "require_gateway_admin",
]
