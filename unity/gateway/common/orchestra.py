"""Shared Orchestra HTTP lookup helpers for gateway channels.

Promoted from the channel-local copy in
``unity.gateway.channels.gmail.views`` once ``outlook/`` became the
second channel needing the same surface. Future channels (teams,
discord, whatsapp) all reach for an assistant lookup at some point
in their auth/credential resolution path.
"""

from __future__ import annotations

import httpx
from fastapi import HTTPException

from unity.gateway.credentials import CredentialStore
from unity.settings import SETTINGS


async def lookup_assistant(
    user_email: str,
    credentials: CredentialStore,
) -> dict:
    """Fetch the assistant record matching ``user_email`` from Orchestra.

    Raises ``HTTPException(500)`` if ``ORCHESTRA_ADMIN_KEY`` is not
    configured (misconfiguration) or ``HTTPException(404)`` when the
    email doesn't map to any known assistant. Other HTTP errors are
    surfaced as 404 too -- matching the original
    ``communication.helpers._lookup_assistant`` behaviour bit-for-bit
    so consumers can distinguish "infra problem" (500) from "no such
    assistant" (404).
    """
    admin_key = credentials.get_optional("ORCHESTRA_ADMIN_KEY", "")
    if not admin_key:
        raise HTTPException(
            status_code=500,
            detail="ORCHESTRA_ADMIN_KEY not configured",
        )
    orchestra_url = SETTINGS.ORCHESTRA_URL
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{orchestra_url}/admin/assistant",
            params={"email": user_email},
            headers={"Authorization": f"Bearer {admin_key}"},
            timeout=30.0,
        )
    if response.status_code != 200:
        raise HTTPException(
            status_code=404,
            detail=f"Assistant not found: {user_email}",
        )
    assistants = response.json().get("info", [])
    if not assistants:
        raise HTTPException(
            status_code=404,
            detail=f"Assistant not found: {user_email}",
        )
    return assistants[0]


__all__ = ["lookup_assistant"]
