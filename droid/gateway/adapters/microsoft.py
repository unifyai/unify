"""Microsoft OAuth, Outlook, and Teams notification adapter routes."""

from __future__ import annotations

import json
import re
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse, Response

from droid.gateway.adapters.common import default_contacts, get_assistant
from droid.gateway.adapters.common import publish_runtime_event
from droid.gateway.adapters.oauth import (
    OAuthStateError,
    exchange_microsoft_code_for_tokens,
    get_microsoft_user_info,
    upsert_assistant_secrets,
    verify_oauth_state,
)
from droid.gateway.context import GatewayContext, get_gateway_context

router = APIRouter()


def parse_client_state(client_state: str, expected_secret: str) -> str | None:
    """Validate ``secret::email`` client state and return the email."""

    if not expected_secret:
        return None
    parts = client_state.split("::")
    if len(parts) < 2 or parts[0] != expected_secret:
        return None
    return parts[1]


def parse_graph_resource_id(resource: str, collection: str) -> str:
    """Extract an id from Graph resource paths."""

    patterns = [
        rf"{re.escape(collection)}\('([^']+)'\)",
        rf"/{re.escape(collection)}/([^/]+)",
        rf"/{re.escape(collection.capitalize())}/([^/]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, resource)
        if match:
            return match.group(1)
    return ""


@router.get("/microsoft/auth/callback")
async def microsoft_oauth_callback(
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

    byod = bool(state_data.get("byod"))
    tenant_id = (
        "common"
        if byod
        else str(
            state_data.get("tenant_id")
            or assistant.get("secrets", {}).get("AZURE_TENANT_ID")
            or "",
        )
    )
    client_id = (
        context.credentials.get("MS365_BYOD_CLIENT_ID")
        if byod
        else str(
            state_data.get("client_id")
            or assistant.get("secrets", {}).get("AZURE_CLIENT_ID")
            or "",
        )
    )
    client_secret = (
        context.credentials.get("MS365_BYOD_CLIENT_SECRET")
        if byod
        else str(assistant.get("secrets", {}).get("AZURE_CLIENT_SECRET") or "")
    )
    if not tenant_id or not client_id or not client_secret:
        return Response(
            content="Microsoft OAuth credentials unavailable",
            status_code=400,
        )

    redirect_uri = context.public_url_provider.url_for(
        "/microsoft/auth/callback",
        surface="adapters",
    )
    tokens = await exchange_microsoft_code_for_tokens(
        tenant_id,
        client_id,
        client_secret,
        code,
        redirect_uri,
    )
    user_info = await get_microsoft_user_info(tokens["access_token"])
    stored = await upsert_assistant_secrets(
        assistant_id=assistant_id,
        api_key=api_key,
        secrets={
            "MICROSOFT_ACCESS_TOKEN": tokens["access_token"],
            "MICROSOFT_REFRESH_TOKEN": tokens.get("refresh_token", ""),
            "MICROSOFT_TOKEN_EXPIRES_AT": tokens.get("expires_at", ""),
            "MICROSOFT_GRANTED_SCOPES": tokens.get("scope", ""),
            "MICROSOFT_TOKEN_SOURCE": "byod" if byod else "enterprise",
            "MICROSOFT_ACCOUNT_EMAIL": (
                user_info.get("mail")
                or user_info.get("userPrincipalName")
                or user_info.get("email")
                or ""
            ),
        },
    )
    if not stored:
        return Response(content="Failed to store Microsoft tokens", status_code=500)

    redirect_after = state_data.get("redirect_after")
    if redirect_after:
        return RedirectResponse(str(redirect_after))
    return Response(content="Microsoft account connected", status_code=200)


@router.post("/email/outlook")
async def outlook_notification_processor(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    notification = await request.json()
    expected_secret = context.credentials.get_optional("OUTLOOK_WEBHOOK_SECRET", "")
    assistant_email = parse_client_state(
        str(notification.get("clientState") or ""),
        expected_secret,
    )
    if not assistant_email:
        return Response(status_code=200)

    resource = str(notification.get("resource") or "")
    email_id = parse_graph_resource_id(resource, "messages")
    if not email_id:
        return Response(status_code=200)

    assistant = await get_assistant(email_address=assistant_email)
    assistant_id = assistant.get("assistant_id")
    if not assistant_id:
        return Response(status_code=200)
    await context.runtime_activator.activate(
        str(assistant_id),
        reason="outlook_notification",
        medium="email",
        metadata={
            "email": assistant_email,
            "email_id": email_id,
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
            "email_id": email_id,
            "provider": "outlook",
            "assistant_email_address": assistant_email,
            "to": [assistant_email],
            "attachments": [],
        },
    )
    return Response(content="OK", status_code=200)


@router.post("/chat/teams")
async def teams_notification_processor(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    notification = await request.json()
    expected_secret = context.credentials.get_optional("TEAMS_WEBHOOK_SECRET", "")
    assistant_email = parse_client_state(
        str(notification.get("clientState") or ""),
        expected_secret,
    )
    if not assistant_email:
        return Response(status_code=200)

    resource = str(notification.get("resource") or "")
    resource_lower = resource.lower()
    is_channel_message = (
        "channels(" in resource_lower or "/channels/" in resource_lower
    ) and ("teams(" in resource_lower or "/teams/" in resource_lower)
    thread = "teams_channel" if is_channel_message else "teams_chat"
    assistant = await get_assistant(email_address=assistant_email)
    assistant_id = assistant.get("assistant_id")
    if not assistant_id:
        return Response(status_code=200)
    await context.runtime_activator.activate(
        str(assistant_id),
        reason=f"{thread}_notification",
        medium="teams",
        metadata={"resource": resource, "assistant": assistant},
    )
    await publish_runtime_event(
        context,
        assistant_id=str(assistant_id),
        thread=thread,
        event={
            "contacts": default_contacts(assistant),
            "assistant_id": assistant_id,
            "assistant_email": assistant_email,
            "resource": resource,
            "message_id": parse_graph_resource_id(resource, "messages"),
            "chat_id": parse_graph_resource_id(resource, "chats"),
            "team_id": parse_graph_resource_id(resource, "teams"),
            "channel_id": parse_graph_resource_id(resource, "channels"),
            "body": "",
            "sender_email": "",
            "sender_name": "",
            "raw_notification": notification,
        },
    )
    return Response(status_code=200)


@router.post("/microsoft/router")
async def microsoft_router(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    validation_token = request.query_params.get("validationToken")
    if validation_token:
        return Response(content=validation_token, media_type="text/plain")

    body = await request.body()
    notifications = json.loads(body or b"{}").get("value", [])
    for notification in notifications:
        resource = str(notification.get("resource") or "")
        if "/Messages/" in resource and "/chats/" not in resource.lower():
            expected_secret = context.credentials.get_optional(
                "OUTLOOK_WEBHOOK_SECRET",
                "",
            )
            assistant_email = parse_client_state(
                str(notification.get("clientState") or ""),
                expected_secret,
            )
            if assistant_email:
                await outlook_notification_processor_from_payload(
                    notification,
                    context=context,
                )
        else:
            await teams_notification_processor_from_payload(
                notification,
                context=context,
            )
    return Response(status_code=202)


async def outlook_notification_processor_from_payload(
    notification: dict[str, Any],
    *,
    context: GatewayContext,
) -> None:
    expected_secret = context.credentials.get_optional("OUTLOOK_WEBHOOK_SECRET", "")
    assistant_email = parse_client_state(
        str(notification.get("clientState") or ""),
        expected_secret,
    )
    if not assistant_email:
        return
    resource = str(notification.get("resource") or "")
    email_id = parse_graph_resource_id(resource, "messages")
    if not email_id:
        return
    assistant = await get_assistant(email_address=assistant_email)
    assistant_id = assistant.get("assistant_id")
    if not assistant_id:
        return
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
            "email_id": email_id,
            "provider": "outlook",
            "assistant_email_address": assistant_email,
            "to": [assistant_email],
            "attachments": [],
        },
    )


async def teams_notification_processor_from_payload(
    notification: dict[str, Any],
    *,
    context: GatewayContext,
) -> None:
    expected_secret = context.credentials.get_optional("TEAMS_WEBHOOK_SECRET", "")
    assistant_email = parse_client_state(
        str(notification.get("clientState") or ""),
        expected_secret,
    )
    if not assistant_email:
        return
    resource = str(notification.get("resource") or "")
    thread = "teams_channel" if "channels" in resource.lower() else "teams_chat"
    assistant = await get_assistant(email_address=assistant_email)
    assistant_id = assistant.get("assistant_id")
    if not assistant_id:
        return
    await publish_runtime_event(
        context,
        assistant_id=str(assistant_id),
        thread=thread,
        event={
            "contacts": default_contacts(assistant),
            "assistant_id": assistant_id,
            "assistant_email": assistant_email,
            "resource": resource,
            "message_id": parse_graph_resource_id(resource, "messages"),
            "chat_id": parse_graph_resource_id(resource, "chats"),
            "team_id": parse_graph_resource_id(resource, "teams"),
            "channel_id": parse_graph_resource_id(resource, "channels"),
            "body": "",
            "sender_email": "",
            "sender_name": "",
            "raw_notification": notification,
        },
    )


__all__ = ["router"]
