"""Shared Orchestra HTTP lookup helpers for gateway channels.

Promoted from the channel-local copy in
``droid.gateway.channels.gmail.views`` once ``outlook/`` became the
second channel needing the same surface. Future channels (teams,
discord, whatsapp) all reach for an assistant lookup at some point
in their auth/credential resolution path.
"""

from __future__ import annotations

import httpx
from fastapi import HTTPException

from droid.gateway.credentials import CredentialStore
from droid.settings import SETTINGS


async def _lookup_assistant(
    params: dict,
    credentials: CredentialStore,
    not_found_detail: str,
) -> dict:
    """Fetch the first assistant matching ``params`` from Orchestra's admin list.

    Raises ``HTTPException(500)`` if ``ORCHESTRA_ADMIN_KEY`` is not
    configured (misconfiguration) or ``HTTPException(404)`` when nothing
    matches. Other HTTP errors are surfaced as 404 too -- matching the
    original ``communication.helpers._lookup_assistant`` behaviour
    bit-for-bit so consumers can distinguish "infra problem" (500) from
    "no such assistant" (404).
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
            params=params,
            headers={"Authorization": f"Bearer {admin_key}"},
            timeout=30.0,
        )
    if response.status_code != 200:
        raise HTTPException(status_code=404, detail=not_found_detail)
    assistants = response.json().get("info", [])
    if not assistants:
        raise HTTPException(status_code=404, detail=not_found_detail)
    return assistants[0]


async def lookup_assistant(
    user_email: str,
    credentials: CredentialStore,
) -> dict:
    """Fetch the assistant record matching ``user_email`` from Orchestra."""
    return await _lookup_assistant(
        {"email": user_email},
        credentials,
        f"Assistant not found: {user_email}",
    )


async def lookup_assistant_by_id(
    assistant_id: str | int,
    credentials: CredentialStore,
) -> dict:
    """Fetch the assistant record matching ``assistant_id`` from Orchestra.

    Resolves the connected account by assistant id rather than email. This
    is the identity the workspace-file picker uses, since a Coordinator's
    connected workspace account is not registered as its email contact (the
    Coordinator's contacts are the platform-managed universal pools).
    """
    return await _lookup_assistant(
        {"agent_id": str(assistant_id)},
        credentials,
        f"Assistant not found: id={assistant_id}",
    )


__all__ = ["lookup_assistant", "lookup_assistant_by_id"]
