"""Microsoft Teams (Bot Framework) inbound adapter routes.

Local/OSS mirror of the production ``POST /ms-teams-bot/messages`` handler
in ``unity-deploy/adapters``. Every inbound Teams activity the Bot
Connector delivers lands here: we verify the Bot Framework JWT, normalize
the activity, ask Orchestra to route it (coordinator vs. per-assistant),
and publish a runtime event for the resolved assistant.

Unlike Slack (where the bot can see every channel message), Teams only
delivers channel/group-chat activities when the bot is @mentioned, and
1:1 (personal) activities whenever the user messages the bot. Because of
that, routing never "drops" — an unroutable mention falls back to the
org coordinator (see ``ms_teams_bot_dispatcher``).
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

from unify.gateway.adapters.common import (
    default_contacts,
    get_assistant,
    publish_runtime_event,
)
from unify.gateway.adapters.ms_teams_bot_auth import (
    BotFrameworkAuthError,
    verify_bot_framework_token,
)
from unify.gateway.channels.ms_teams_bot.views import _mint_connector_token
from unify.gateway.context import GatewayContext, get_gateway_context
from unify.settings import SETTINGS

logger = logging.getLogger("unify.gateway.adapters.ms_teams_bot")

router = APIRouter()


def _orchestra_headers() -> dict[str, str]:
    return {
        "Authorization": (f"Bearer {SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()}"),
    }


async def _post_orchestra(path: str, body: dict[str, Any]) -> dict[str, Any] | None:
    """POST to an Orchestra admin endpoint; ``None`` on a missing install."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"{SETTINGS.ORCHESTRA_URL}{path}",
            json=body,
            headers=_orchestra_headers(),
        )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def _strip_bot_mention(
    text: str,
    entities: list[dict[str, Any]],
    recipient_id: str,
) -> tuple[bool, str]:
    """Return ``(bot_mentioned, text_without_bot_mention)``.

    Teams marks @mentions with ``mention`` entities carrying the exact
    ``<at>Name</at>`` substring; the bot's own mention is the one whose
    ``mentioned.id`` equals the activity ``recipient.id`` (the bot).
    """
    bot_mentioned = False
    cleaned = text or ""
    for entity in entities or []:
        if entity.get("type") != "mention":
            continue
        mentioned = entity.get("mentioned") or {}
        if mentioned.get("id") == recipient_id:
            bot_mentioned = True
            mention_text = entity.get("text") or ""
            if mention_text:
                cleaned = cleaned.replace(mention_text, "")
    return bot_mentioned, cleaned.strip()


def _normalize_attachments(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []
    for item in raw or []:
        content_url = item.get("contentUrl")
        name = item.get("name")
        if not content_url or not name:
            continue
        attachments.append(
            {
                "id": content_url,
                "filename": name,
                "url": content_url,
                "content_type": item.get("contentType") or "",
            },
        )
    return attachments


async def _ensure_pending_install(activity: dict[str, Any]) -> dict[str, Any] | None:
    """Record a pending (unbound) install when the bot is added to a tenant.

    Captures the ``serviceUrl`` (needed for every future outbound call) so
    the tenant-to-org bind handshake can complete later without another
    inbound round-trip. Returns Orchestra's install response (carrying
    ``bind_nonce`` + a one-click ``connect_url``) so the caller can DM the
    installer the connect link.
    """
    channel_data = activity.get("channelData") or {}
    tenant_id = (channel_data.get("tenant") or {}).get("id") or ""
    if not tenant_id:
        return None
    installer = activity.get("from") or {}
    return await _post_orchestra(
        "/admin/ms-teams-bot/pending-install",
        {
            "tenant_id": tenant_id,
            "bot_app_id": SETTINGS.MS_TEAMS_BOT_APP_ID,
            "service_url": activity.get("serviceUrl") or "",
            "installer_aad_object_id": installer.get("aadObjectId") or "",
        },
    )


# Plain-text welcome sent alongside the connect card. Teams Store certification
# requires a visible welcome on add, and some surfaces render only the top-level
# text (not the card), so the greeting must live in ``text`` too.
_WELCOME_TEXT = (
    "Hi! Thanks for adding Unify. I'm your AI teammate here in Teams. "
    "One quick step to finish setup: connect this workspace to your Unify "
    "account or organization using the button below. You only do this once. "
    "You can also say **Hi**, **Hello**, or **Help** anytime."
)

# Canned reply when a message lands on an install not yet bound to a Unify owner.
_PENDING_REPLY_TEXT = (
    "Thanks for the message! This Teams workspace isn't connected to a Unify "
    "account yet, so I can't act on requests here just yet. Tap **Connect to "
    "Unify** below to finish setup, then I'll be able to help."
)


def _connect_card(text: str, connect_url: str) -> dict[str, Any]:
    """Adaptive Card attachment: a message plus a one-tap "Connect to Unify"."""
    return {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "type": "AdaptiveCard",
            "version": "1.4",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "body": [
                {
                    "type": "TextBlock",
                    "size": "Medium",
                    "weight": "Bolder",
                    "text": "Connect Unify to Teams",
                },
                {
                    "type": "TextBlock",
                    "wrap": True,
                    "text": text,
                },
            ],
            "actions": [
                {
                    "type": "Action.OpenUrl",
                    "title": "Connect to Unify",
                    "url": connect_url,
                },
            ],
        },
    }


async def _send_ms_teams_bot_message(
    activity: dict[str, Any],
    install: dict[str, Any] | None,
    message: dict[str, Any],
) -> None:
    """POST a proactive activity into the inbound conversation (best-effort).

    Mints a Bot Connector token via the shared ``/send`` machinery. Any missing
    piece (no ``service_url``, no ``conversation_id``, no token, or a send
    failure) is logged and swallowed so it never breaks the webhook.
    """
    conversation = activity.get("conversation") or {}
    conversation_id = conversation.get("id") or ""
    service_url = (
        activity.get("serviceUrl") or (install or {}).get("service_url") or ""
    ).rstrip("/")
    if not conversation_id or not service_url:
        logger.warning(
            "ms_teams_bot: proactive send skipped — missing conversation_id or "
            "service_url",
        )
        return
    try:
        token = await _mint_connector_token()
    except Exception:
        logger.exception("ms_teams_bot: connector token mint failed")
        return
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{service_url}/v3/conversations/{conversation_id}/activities",
                json=message,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            )
    except Exception:
        logger.exception("ms_teams_bot: proactive send transport error")
        return
    if resp.status_code >= 400:
        logger.error(
            "ms_teams_bot: proactive send failed: %s %s",
            resp.status_code,
            resp.text,
        )


async def _send_install_welcome(
    activity: dict[str, Any],
    install: dict[str, Any] | None,
) -> None:
    """Proactively DM the installer a welcome + one-click connect link.

    The caller gates this on the install's ``created`` flag so it fires exactly
    once per install. The plain-text greeting is always sent; the connect card
    is attached when Orchestra returned a ``connect_url`` (install still
    pending). Best-effort — any failure is logged and swallowed.
    """
    if not install:
        return
    connect_url = install.get("connect_url") or ""
    message: dict[str, Any] = {"type": "message", "text": _WELCOME_TEXT}
    if connect_url:
        message["attachments"] = [_connect_card(_WELCOME_TEXT, connect_url)]
    await _send_ms_teams_bot_message(activity, install, message)


async def _send_pending_reply(
    activity: dict[str, Any],
    connect_url: str | None,
) -> None:
    """Reply to an inbound message on an unbound (pending) install.

    Keeps the bot responsive rather than silent when someone messages it before
    the tenant is connected. Includes the connect card when available.
    """
    message: dict[str, Any] = {"type": "message", "text": _PENDING_REPLY_TEXT}
    if connect_url:
        message["attachments"] = [_connect_card(_PENDING_REPLY_TEXT, connect_url)]
    await _send_ms_teams_bot_message(activity, None, message)


async def resolve_ms_teams_bot_inbound(
    activity: dict[str, Any],
) -> dict[str, Any] | None:
    """Route a Teams message activity via Orchestra.

    The sender's display name is present inline on the activity, so we
    resolve identity in a single dispatch pass (``sender_identity_provided``)
    — no ``users.info``-style second pass is required for name matching.
    Email-based matching (via the Teams roster) is deferred.
    """
    channel_data = activity.get("channelData") or {}
    tenant_id = (channel_data.get("tenant") or {}).get("id") or ""
    conversation = activity.get("conversation") or {}
    conversation_id = conversation.get("id") or ""
    conversation_type = conversation.get("conversationType") or "personal"
    channel_id = (channel_data.get("channel") or {}).get("id")
    sender = activity.get("from") or {}
    recipient = activity.get("recipient") or {}

    bot_mentioned, addressed_text = _strip_bot_mention(
        activity.get("text", "") or "",
        activity.get("entities") or [],
        recipient.get("id") or "",
    )

    conversation_reference = {
        "bot": recipient,
        "user": sender,
        "conversation": conversation,
        "channelId": activity.get("channelId") or "msteams",
        "serviceUrl": activity.get("serviceUrl") or "",
        "tenantId": tenant_id,
    }

    dispatch_body = {
        "tenant_id": tenant_id,
        "conversation_id": conversation_id,
        "conversation_type": conversation_type,
        "channel_id": channel_id,
        "sender_aad_object_id": sender.get("aadObjectId") or "",
        "sender_display_name": sender.get("name") or "",
        "bot_mentioned": bot_mentioned,
        "addressed_text": addressed_text,
        "conversation_reference": json.dumps(conversation_reference),
        "sender_identity_provided": True,
    }
    return await _post_orchestra("/admin/ms-teams-bot/dispatch", dispatch_body)


@router.post("/ms-teams-bot/messages")
async def ms_teams_bot_messages_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> dict[str, Any]:
    token = ""
    auth_header = request.headers.get("Authorization", "")
    if auth_header.lower().startswith("bearer "):
        token = auth_header[7:].strip()
    try:
        await asyncio.to_thread(
            verify_bot_framework_token,
            token,
            app_id=SETTINGS.MS_TEAMS_BOT_APP_ID,
        )
    except BotFrameworkAuthError as exc:
        logger.warning("ms_teams_bot inbound auth failed: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid token") from exc

    activity = await request.json()
    activity_type = activity.get("type") or ""

    if activity_type == "conversationUpdate":
        members_added = activity.get("membersAdded") or []
        recipient_id = (activity.get("recipient") or {}).get("id") or ""
        if any(member.get("id") == recipient_id for member in members_added):
            # Bot added to a conversation (personal 1:1 or a team channel).
            # Fires once per conversation, so it is the right place to welcome:
            # personal chat and each team channel are distinct conversations and
            # each must get its own welcome. ``_ensure_pending_install`` records
            # the tenant and returns the connect link. Keyed on the bot-add
            # event, not the tenant, so a second-scope add on an already-known
            # tenant still greets.
            install = await _ensure_pending_install(activity)
            if install:
                await _send_install_welcome(activity, install)
        return {"status": 200}

    # ``installationUpdate`` fires for app install/uninstall across scopes.
    # ``add`` only registers the tenant so the owner can bind it — the welcome
    # is NOT sent here. Any scope that can receive a proactive welcome
    # (personal, team) also emits a ``conversationUpdate`` bot-add, which is the
    # single place we greet; welcoming here too would double-send.
    # ``remove``/``remove-upgrade`` are teardown signals the local mirror does
    # not act on (no per-tenant token to revoke at Microsoft).
    if activity_type == "installationUpdate":
        if (activity.get("action") or "") == "add":
            await _ensure_pending_install(activity)
        return {"status": 200}

    if activity_type != "message":
        return {"status": 200}

    data = await resolve_ms_teams_bot_inbound(activity)
    if data is None:
        return {"status": 200}
    if not data.get("handled"):
        # A message on a still-pending (unbound) install must not be dropped
        # silently — reply with a connect nudge instead of going dark.
        if data.get("install_state") == "pending":
            await _send_pending_reply(activity, data.get("connect_url"))
        return {"status": 200}

    assistant_id = data.get("assistant_id")
    if not assistant_id:
        return {"status": 200}
    assistant_id = str(assistant_id)
    assistant = await get_assistant(assistant_id=assistant_id)
    if not assistant.get("assistant_id"):
        return {"status": 200}

    conversation = activity.get("conversation") or {}
    conversation_type = conversation.get("conversationType") or "personal"
    channel_data = activity.get("channelData") or {}
    sender = activity.get("from") or {}
    recipient = activity.get("recipient") or {}
    _, addressed_text = _strip_bot_mention(
        activity.get("text", "") or "",
        activity.get("entities") or [],
        recipient.get("id") or "",
    )
    contacts = default_contacts(assistant)

    # Channel identity: the team owns the channel, and a channel
    # conversation id encodes the root thread as ``…;messageid=<rootId>``.
    conversation_id = conversation.get("id", "") or ""
    team_id = (channel_data.get("team") or {}).get("id", "") or ""
    thread_id = ""
    if ";messageid=" in conversation_id:
        thread_id = conversation_id.split(";messageid=", 1)[1]

    await context.runtime_activator.activate(
        assistant_id,
        reason="ms_teams_bot_event",
        medium="ms_teams_bot",
        metadata={**data, "assistant": assistant},
    )
    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="ms_teams_bot",
        event={
            "event_id": activity.get("id", "") or "",
            "message_id": activity.get("id", "") or "",
            "tenant_id": (channel_data.get("tenant") or {}).get("id", "") or "",
            "conversation_id": conversation_id,
            "conversation_type": conversation_type,
            "channel_id": (channel_data.get("channel") or {}).get("id", "") or "",
            "team_id": team_id,
            "thread_id": thread_id,
            "service_url": activity.get("serviceUrl", "") or "",
            "bot_app_id": SETTINGS.MS_TEAMS_BOT_APP_ID,
            "sender_aad_object_id": sender.get("aadObjectId", "") or "",
            "sender_display_name": sender.get("name", "") or "",
            "body": addressed_text or activity.get("text", "") or "",
            "is_channel": conversation_type != "personal",
            "attachments": _normalize_attachments(activity.get("attachments") or []),
            "routing_metadata": data.get("routing_metadata") or {},
            "sender_is_owner": bool(data.get("sender_is_owner")),
            "contacts": contacts,
        },
    )
    return {"status": 200}


__all__ = ["resolve_ms_teams_bot_inbound", "router"]
