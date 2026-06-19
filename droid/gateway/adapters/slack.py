"""Slack Events API adapter routes."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

from droid.gateway.adapters.common import default_contacts, get_assistant
from droid.gateway.adapters.common import publish_runtime_event
from droid.gateway.channels.slack.views import fetch_slack_user_profile
from droid.gateway.context import GatewayContext, get_gateway_context
from droid.settings import SETTINGS

logger = logging.getLogger("droid.gateway.adapters.slack")

router = APIRouter()

_SLACK_MAX_SKEW_SECONDS = 60 * 5
_SLACK_SEEN: dict[str, float] = {}
_SLACK_SEEN_TTL = 300.0


def verify_slack_signature(
    *,
    body: bytes,
    timestamp: str,
    signature: str,
    signing_secret: str,
) -> bool:
    """Validate a Slack Events API webhook signature."""

    if not signing_secret or not timestamp or not signature:
        return False
    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        return False
    if abs(time.time() - ts) > _SLACK_MAX_SKEW_SECONDS:
        return False
    basestring = f"v0:{timestamp}:".encode() + body
    expected = (
        "v0="
        + hmac.new(
            signing_secret.encode(),
            basestring,
            hashlib.sha256,
        ).hexdigest()
    )
    return hmac.compare_digest(expected, signature)


def slack_message_already_seen(message_key: str) -> bool:
    """Best-effort in-process dedup for inbound Slack messages."""

    if not message_key:
        return False
    now = time.time()
    cutoff = now - _SLACK_SEEN_TTL
    for key in [key for key, seen_at in _SLACK_SEEN.items() if seen_at < cutoff]:
        del _SLACK_SEEN[key]
    if message_key in _SLACK_SEEN:
        return True
    _SLACK_SEEN[message_key] = now
    return False


async def _post_dispatch(dispatch_body: dict[str, Any]) -> dict[str, Any] | None:
    """POST to Orchestra's dispatch endpoint; ``None`` on a missing install."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"{SETTINGS.ORCHESTRA_URL}/admin/slack/dispatch",
            json=dispatch_body,
            headers={
                "Authorization": (
                    f"Bearer {SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()}"
                ),
            },
        )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


async def resolve_slack_inbound(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Route a Slack Events API event via Orchestra.

    Coordinator routing in an org workspace is *personal to the sender*:
    each member owns their own workspace Coordinator. The first dispatch
    pass returns a provisional coordinator route plus
    ``needs_sender_identity`` when it needs the sender's email/name to pick
    the right one. We then resolve the sender's profile (``users.info``)
    and re-dispatch so the message is pinned to the sender's own
    Coordinator. The resolution from pass one is already valid, so any
    failure resolving identity degrades to that provisional route.
    """

    event = payload.get("event") or {}
    channel_id = event.get("channel", "") or ""
    channel_type = event.get("channel_type") or (
        "im" if channel_id.startswith("D") else "channel"
    )
    team_id = payload.get("team_id", "") or event.get("team", "")
    sender_slack_user_id = event.get("user", "") or ""
    dispatch_body = {
        "slack_team_id": team_id,
        "channel_id": channel_id,
        "channel_type": channel_type,
        "sender_slack_user_id": sender_slack_user_id,
        "text": event.get("text", "") or "",
        "event_ts": event.get("event_ts", "") or event.get("ts", ""),
        "thread_ts": event.get("thread_ts"),
    }
    data = await _post_dispatch(dispatch_body)
    if data is None:
        return None

    if data.get("needs_sender_identity") and team_id and sender_slack_user_id:
        profile: dict[str, Any] = {}
        try:
            profile = await fetch_slack_user_profile(team_id, sender_slack_user_id)
        except Exception:  # noqa: BLE001 - identity is best-effort
            logger.warning(
                "slack sender profile lookup failed; using provisional "
                "coordinator route",
                exc_info=True,
            )
        # Re-dispatch with whatever identity we resolved. ``provided=True``
        # is sent regardless so Orchestra does not ask again (loop-safe).
        second = await _post_dispatch(
            {
                **dispatch_body,
                "sender_email": profile.get("email"),
                "sender_real_name": profile.get("real_name"),
                "sender_display_name": profile.get("display_name"),
                "sender_identity_provided": True,
            },
        )
        if second is not None:
            data = second

    if not data.get("handled"):
        return {"drop": True}
    return {
        "assistant_id": data.get("assistant_id"),
        "is_channel": channel_type != "im",
        "bot_user_id": data.get("bot_user_id", "") or "",
        "routing_metadata": data.get("routing_metadata") or {},
    }


@router.post("/slack/events")
async def slack_events_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> dict[str, Any]:
    raw_body = await request.body()
    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    signature = request.headers.get("X-Slack-Signature", "")
    signing_secret = context.credentials.get_optional("SLACK_SIGNING_SECRET", "")
    if not verify_slack_signature(
        body=raw_body,
        timestamp=timestamp,
        signature=signature,
        signing_secret=signing_secret,
    ):
        raise HTTPException(status_code=401, detail="Invalid signature")

    payload = json.loads(raw_body or b"{}")
    event_type = payload.get("type")
    if event_type == "url_verification":
        return {"challenge": payload.get("challenge", "")}
    if event_type != "event_callback":
        return {"ok": True}
    if request.headers.get("X-Slack-Retry-Num"):
        return {"ok": True}

    inner = payload.get("event") or {}
    inner_type = inner.get("type") or ""
    if inner_type not in ("message", "app_mention"):
        return {"ok": True}
    if inner.get("bot_id") or inner.get("subtype") == "bot_message":
        return {"ok": True}

    team_id = payload.get("team_id", "")
    dedup_key = inner.get("client_msg_id") or inner.get("ts") or ""
    if dedup_key and slack_message_already_seen(f"{team_id}:{dedup_key}"):
        return {"ok": True}

    resolution = await resolve_slack_inbound(payload)
    if resolution is None or resolution.get("drop"):
        return {"ok": True}

    assistant_id = resolution.get("assistant_id")
    if not assistant_id:
        return {"ok": True}
    assistant_id = str(assistant_id)
    assistant = await get_assistant(assistant_id=assistant_id)
    if not assistant.get("assistant_id"):
        return {"ok": True}

    contacts = default_contacts(assistant)
    await context.runtime_activator.activate(
        assistant_id,
        reason="slack_event",
        medium="slack",
        metadata={**resolution, "assistant": assistant},
    )
    files = [
        {
            "id": file.get("id"),
            "filename": file.get("name") or file.get("title"),
            "url": file.get("url_private") or file.get("permalink"),
            "mimetype": file.get("mimetype"),
            "size": file.get("size"),
        }
        for file in (inner.get("files") or [])
    ]
    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="slack",
        event={
            "event_id": payload.get("event_id", ""),
            "message_id": inner.get("client_msg_id", "") or inner.get("ts", ""),
            "team_id": payload.get("team_id", ""),
            "channel_id": inner.get("channel", ""),
            "bot_user_id": resolution.get("bot_user_id", "") or "",
            "sender_slack_user_id": inner.get("user", ""),
            "body": inner.get("text", ""),
            "event_ts": inner.get("event_ts", ""),
            "thread_ts": inner.get("thread_ts", ""),
            "is_channel": bool(resolution.get("is_channel")),
            "attachments": files,
            "routing_metadata": resolution.get("routing_metadata") or {},
            "contacts": contacts,
        },
    )
    return {"ok": True}


__all__ = [
    "resolve_slack_inbound",
    "router",
    "slack_message_already_seen",
    "verify_slack_signature",
]
