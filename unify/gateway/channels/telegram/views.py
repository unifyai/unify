"""Telegram admin endpoints (tenant-facing).

Mounted under ``/telegram`` by ``unify.gateway.app``:

* ``POST /send``   -- outbound message via ``sendMessage``. Resolves
  the bot token from the gateway credential store.
* ``GET  /status`` -- basic health check (``getMe``).
"""

from __future__ import annotations

import logging

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

from unify.gateway.common.auth import require_assistant_ownership
from unify.gateway.context import GatewayContext, get_gateway_context

logger = logging.getLogger("unify.gateway.channels.telegram.views")

auth_router = APIRouter()

TELEGRAM_API_BASE = "https://api.telegram.org"


def _bot_url(token: str, method: str) -> str:
    return f"{TELEGRAM_API_BASE}/bot{token}/{method}"


def _get_bot_token(context: GatewayContext) -> str:
    token = context.credentials.get_optional("TELEGRAM_BOT_TOKEN", "")
    if not token:
        raise HTTPException(
            status_code=503,
            detail="TELEGRAM_BOT_TOKEN is not configured",
        )
    return token


@auth_router.post("/send")
async def send_telegram_message(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
):
    """Send a Telegram message via ``sendMessage``.

    Body::

        {
            "chat_id": str | int,
            "body": str,
            "assistant_id": str | None,
            "reply_to_message_id": int | None,
        }
    """
    data = await request.json()
    chat_id = data.get("chat_id")
    body = data.get("body", "")
    await require_assistant_ownership(request, data.get("assistant_id"))

    if not chat_id:
        raise HTTPException(status_code=400, detail="'chat_id' is required")
    if not body:
        raise HTTPException(status_code=400, detail="'body' is required")

    token = _get_bot_token(context)
    payload: dict = {"chat_id": chat_id, "text": body}
    reply_to = data.get("reply_to_message_id")
    if reply_to:
        payload["reply_parameters"] = {"message_id": int(reply_to)}

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            _bot_url(token, "sendMessage"),
            json=payload,
            timeout=10.0,
        )
    resp_data = resp.json()
    if not resp_data.get("ok"):
        raise HTTPException(
            status_code=502,
            detail=f"Telegram sendMessage failed: {resp_data.get('description')}",
        )
    sent = resp_data.get("result", {})
    logger.info(
        "sent Telegram message to chat_id=%s (message_id=%s)",
        chat_id,
        sent.get("message_id"),
    )
    return {
        "success": True,
        "message_id": sent.get("message_id"),
        "chat_id": chat_id,
    }


@auth_router.get("/status")
async def status(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
):
    """Health check via ``getMe``."""
    token = _get_bot_token(context)
    async with httpx.AsyncClient() as client:
        resp = await client.get(_bot_url(token, "getMe"), timeout=10.0)
    resp_data = resp.json()
    if not resp_data.get("ok"):
        raise HTTPException(
            status_code=502,
            detail=f"Telegram getMe failed: {resp_data.get('description')}",
        )
    return resp_data.get("result", {})


__all__ = ["auth_router"]
