"""Telegram Bot API webhook adapter."""

from __future__ import annotations

import hmac
import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from unify.gateway.adapters.common import (
    default_contacts,
    get_assistant,
    publish_runtime_event,
)
from unify.gateway.context import GatewayContext, get_gateway_context

logger = logging.getLogger("unify.gateway.adapters.telegram")

router = APIRouter()

_SEEN: dict[str, float] = {}
_SEEN_TTL = 300.0


def _already_seen(update_id: str) -> bool:
    if not update_id:
        return False
    now = time.time()
    cutoff = now - _SEEN_TTL
    for key in [k for k, t in _SEEN.items() if t < cutoff]:
        del _SEEN[key]
    if update_id in _SEEN:
        return True
    _SEEN[update_id] = now
    return False


def _verify_secret_token(request: Request, context: GatewayContext) -> None:
    """Verify the ``X-Telegram-Bot-Api-Secret-Token`` header if configured."""
    expected = context.credentials.get_optional("TELEGRAM_WEBHOOK_SECRET", "")
    if not expected:
        return
    actual = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not hmac.compare_digest(expected, actual):
        raise HTTPException(status_code=401, detail="Invalid secret token")


def _extract_message(update: dict[str, Any]) -> dict[str, Any] | None:
    """Return the message dict from an update.

    Handles ``message``, ``edited_message``, and ``channel_post``.
    ``channel_post`` lacks a ``from`` field (no user sender), so the
    caller must tolerate an empty ``from`` dict.
    """
    return (
        update.get("message")
        or update.get("edited_message")
        or update.get("channel_post")
    )


@router.post("/telegram/webhook")
async def telegram_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> dict[str, Any]:
    _verify_secret_token(request, context)

    update = await request.json()
    update_id = str(update.get("update_id", ""))
    if _already_seen(update_id):
        return {"ok": True}

    message = _extract_message(update)
    if not message:
        return {"ok": True}

    text = message.get("text") or message.get("caption") or ""
    if not text:
        return {"ok": True}

    from_user = message.get("from") or {}
    if not from_user:
        return {"ok": True}
    if from_user.get("is_bot"):
        return {"ok": True}

    chat = message.get("chat", {})
    chat_id = str(chat.get("id", ""))
    chat_type = chat.get("type", "private")
    sender_id = str(from_user.get("id", ""))
    message_id = str(message.get("message_id", ""))
    is_group = chat_type in ("group", "supergroup")

    assistant_id = context.credentials.get_optional("TELEGRAM_ASSISTANT_ID", "")
    if not assistant_id:
        logger.warning("TELEGRAM_ASSISTANT_ID not configured; dropping update")
        return {"ok": True}

    assistant = await get_assistant(assistant_id=assistant_id)
    if not assistant.get("assistant_id"):
        return {"ok": True}

    contacts = default_contacts(assistant)
    await context.runtime_activator.activate(
        assistant_id,
        reason="telegram_message",
        medium="telegram",
        metadata={"assistant": assistant},
    )

    attachments: list[dict[str, Any]] = []
    for doc in message.get("document") and [message["document"]] or []:
        attachments.append(
            {
                "id": doc.get("file_id", ""),
                "filename": doc.get("file_name", ""),
                "mimetype": doc.get("mime_type", ""),
                "size": doc.get("file_size", 0),
            },
        )
    for photo_sizes in [message.get("photo")] if message.get("photo") else []:
        if photo_sizes:
            largest = photo_sizes[-1]
            attachments.append(
                {
                    "id": largest.get("file_id", ""),
                    "filename": "photo.jpg",
                    "mimetype": "image/jpeg",
                    "size": largest.get("file_size", 0),
                },
            )

    sender_first = from_user.get("first_name", "")
    sender_last = from_user.get("last_name", "")
    sender_username = from_user.get("username", "")

    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="telegram",
        event={
            "update_id": update_id,
            "message_id": message_id,
            "chat_id": chat_id,
            "chat_type": chat_type,
            "sender_telegram_id": sender_id,
            "sender_first_name": sender_first,
            "sender_last_name": sender_last,
            "sender_username": sender_username,
            "body": text,
            "is_group": is_group,
            "attachments": attachments,
            "contacts": contacts,
        },
    )
    return {"ok": True}


__all__ = ["router"]
