"""Google OAuth and Gmail notification adapter routes."""

from __future__ import annotations

import base64
import json
from typing import Any

import httpx
from fastapi import APIRouter, Body, Depends, Request
from fastapi.responses import RedirectResponse, Response

from droid.gateway.adapters.common import default_contacts, get_assistant
from droid.gateway.adapters.common import publish_runtime_event
from droid.gateway.adapters.oauth import (
    OAuthStateError,
    exchange_google_code_for_tokens,
    get_google_user_info,
    upsert_assistant_secrets,
    verify_oauth_state,
)
from droid.gateway.common.auth import auth_admin_key
from droid.gateway.context import GatewayContext, get_gateway_context

router = APIRouter()


@router.post("/google/revoke", dependencies=[Depends(auth_admin_key)])
async def google_revoke(request: Request) -> Response:
    data = await request.json()
    token = data.get("token")
    assistant_id = data.get("assistant_id")
    if not token:
        return Response(content="Missing token", status_code=400)

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            "https://oauth2.googleapis.com/revoke",
            params={"token": token},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    revoked = response.status_code == 200
    if assistant_id:
        assistant = await get_assistant(assistant_id=str(assistant_id))
        api_key = assistant.get("api_key", "")
        if api_key:
            await upsert_assistant_secrets(
                assistant_id=str(assistant_id),
                api_key=api_key,
                secrets={
                    "GOOGLE_ACCESS_TOKEN": "",
                    "GOOGLE_REFRESH_TOKEN": "",
                    "GOOGLE_TOKEN_EXPIRES_AT": "",
                    "GOOGLE_GRANTED_SCOPES": "",
                },
            )
    return Response(
        content=json.dumps({"revoked": revoked}),
        media_type="application/json",
    )


@router.get("/google/auth/callback")
async def google_oauth_callback(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    error = request.query_params.get("error")
    error_description = request.query_params.get("error_description")
    if error:
        return Response(
            content=f"OAuth error: {error}: {error_description}",
            status_code=400,
        )
    if not code:
        return Response(content="Missing authorization code", status_code=400)
    if not state:
        return Response(content="Missing state parameter", status_code=400)

    signing_key = context.credentials.get_optional("OAUTH_STATE_SIGNING_KEY", "")
    if not signing_key:
        return Response(
            content="OAUTH_STATE_SIGNING_KEY not configured",
            status_code=500,
        )
    try:
        state_data = verify_oauth_state(state, signing_key)
    except OAuthStateError:
        return Response(content="Invalid OAuth state", status_code=400)

    assistant_id = str(state_data.get("assistant_id") or "")
    if not assistant_id:
        return Response(content="Missing assistant_id in state", status_code=400)
    assistant = await get_assistant(assistant_id=assistant_id)
    api_key = assistant.get("api_key", "")
    if not api_key:
        return Response(content="Assistant API key unavailable", status_code=400)

    client_id = context.credentials.get("GOOGLE_OAUTH_CLIENT_ID")
    client_secret = context.credentials.get("GOOGLE_OAUTH_CLIENT_SECRET")
    redirect_uri = context.public_url_provider.url_for(
        "/google/auth/callback",
        surface="adapters",
    )
    tokens = await exchange_google_code_for_tokens(
        client_id,
        client_secret,
        code,
        redirect_uri,
    )
    user_info = await get_google_user_info(tokens["access_token"])
    granted_scopes = tokens.get("scope", "")
    stored = await upsert_assistant_secrets(
        assistant_id=assistant_id,
        api_key=api_key,
        secrets={
            "GOOGLE_ACCESS_TOKEN": tokens["access_token"],
            "GOOGLE_REFRESH_TOKEN": tokens.get("refresh_token", ""),
            "GOOGLE_TOKEN_EXPIRES_AT": tokens.get("expires_at", ""),
            "GOOGLE_GRANTED_SCOPES": granted_scopes,
            "GOOGLE_ACCOUNT_EMAIL": user_info.get("email", ""),
        },
    )
    if not stored:
        return Response(content="Failed to store Google tokens", status_code=500)

    redirect_after = state_data.get("redirect_after")
    if redirect_after:
        return RedirectResponse(str(redirect_after))
    return Response(content="Google account connected", status_code=200)


@router.post("/email/gmail")
async def gmail_notification_processor(
    envelope: dict[str, Any] = Body(...),
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    if not envelope or "message" not in envelope:
        return Response(content="Bad Request: no Pub/Sub message", status_code=400)
    pubsub_message = envelope.get("message", {})
    data = base64.b64decode(pubsub_message.get("data", "")).decode("utf-8")
    notification = json.loads(data)
    assistant_email = notification["emailAddress"]
    history_id = str(notification.get("historyId", ""))
    assistant = await get_assistant(email_address=assistant_email)
    assistant_id = assistant.get("assistant_id")
    if not assistant_id:
        return Response(content="OK", status_code=200)
    await context.runtime_activator.activate(
        str(assistant_id),
        reason="gmail_notification",
        medium="email",
        metadata={
            "email": assistant_email,
            "history_id": history_id,
            "assistant": assistant,
        },
    )
    await publish_runtime_event(
        context,
        assistant_id=str(assistant_id),
        thread="email",
        event={
            "contacts": default_contacts(assistant),
            "assistant_id": assistant_id,
            "from": "",
            "subject": "",
            "body": "",
            "email_id": history_id,
            "provider": "gmail",
            "assistant_email_address": assistant_email,
            "history_id": history_id,
            "to": [assistant_email],
            "attachments": [],
        },
    )
    return Response(content="OK", status_code=200)


__all__ = ["router"]
