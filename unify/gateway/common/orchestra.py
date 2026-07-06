"""Shared Orchestra HTTP lookup helpers for gateway channels.

Promoted from the channel-local copy in
``unify.gateway.channels.gmail.views`` once ``outlook/`` became the
second channel needing the same surface. Future channels (teams,
discord, whatsapp) all reach for an assistant lookup at some point
in their auth/credential resolution path.
"""

from __future__ import annotations

import httpx
from fastapi import HTTPException

from unify.gateway.credentials import CredentialStore
from unify.settings import SETTINGS

ADMIN_CONTACT_LOOKUP_FROM_FIELDS = (
    "agent_id,api_key,secrets,email,email_provider,phone,user_id,user_email,"
    "user_first_name,user_last_name,user_phone,user_whatsapp_number,"
    "assistant_whatsapp_number,self_contact_id,boss_contact_id,team_ids,"
    "is_coordinator,organization_id,voice_id,voice_provider,first_name,"
    "surname,deploy_env,desktop_mode,user_desktops,demo_id,is_local,"
    "assistant_discord_bot_id,assistant_slack_bot_user_id,assistant_slack_team_id,"
    "age,nationality,"
    "about,job_title,timezone"
)


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
    *,
    from_fields: str | None = ADMIN_CONTACT_LOOKUP_FROM_FIELDS,
) -> dict:
    """Fetch the assistant record matching ``user_email`` from Orchestra."""
    params: dict[str, str] = {"email": user_email}
    if from_fields:
        params["from_fields"] = from_fields
    return await _lookup_assistant(
        params,
        credentials,
        f"Assistant not found: {user_email}",
    )


async def lookup_assistant_by_id(
    assistant_id: str | int,
    credentials: CredentialStore,
    *,
    from_fields: str | None = None,
) -> dict:
    """Fetch the assistant record matching ``assistant_id`` from Orchestra.

    Resolves the connected account by assistant id rather than email. This
    is the identity the workspace-file picker uses, since a Coordinator's
    connected workspace account is not registered as its email contact (the
    Coordinator's contacts are the platform-managed universal pools).
    """
    params: dict[str, str] = {"agent_id": str(assistant_id)}
    if from_fields:
        params["from_fields"] = from_fields
    return await _lookup_assistant(
        params,
        credentials,
        f"Assistant not found: id={assistant_id}",
    )


__all__ = [
    "ADMIN_CONTACT_LOOKUP_FROM_FIELDS",
    "lookup_assistant",
    "lookup_assistant_by_id",
]
