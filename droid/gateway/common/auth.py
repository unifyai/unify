"""Shared FastAPI auth dependencies for ``droid.gateway`` routers.

Two dependencies surface here:

* ``admin_auth_dependency``: matches the Bearer-token-against-Orchestra
  admin-key shape used by ``communication/dependencies.py::auth_admin_key``.
  Most gateway channels mount their routers behind this dependency so
  only trusted Droid/Orchestra callers can hit them.
* ``user_api_key_auth_dependency``: the user-API-keyed flow used by
  ``unillm`` (and any future SDK-style public endpoints). Validates
  the Bearer token against Orchestra's ``/user/basic-info``.

Promoting both up out of the per-channel views keeps the aggregator
(``droid.gateway.app``) free of repeated auth wiring boilerplate.
"""

from __future__ import annotations

import secrets

import httpx
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from droid.settings import SETTINGS

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


def extract_api_key(request: Request) -> str:
    """Extract the Bearer token from the Authorization header."""
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:]
    raise HTTPException(status_code=401, detail="Missing API key.")


# Dependency tuples convenient to splat into ``app.include_router(...)``.
admin_auth_dependency = [Depends(auth_admin_key)]
"""``dependencies=`` value mounting a router behind the admin-key check."""


__all__ = [
    "admin_auth_dependency",
    "auth_admin_key",
    "authenticate_user_api_key",
    "extract_api_key",
]
