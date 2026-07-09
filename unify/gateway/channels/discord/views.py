"""Discord Comms API endpoints.

Ports ``communication/discord/views.py`` into ``unify.gateway``.
Five admin endpoints (Bearer-token via Orchestra admin key):

* ``POST /send``   -- outbound DM or channel message
* ``POST /create`` -- register and connect a pool bot
* ``POST /sync``   -- re-sync the pool from Orchestra
* ``DEL  /delete`` -- disconnect a pool bot
* ``GET  /status`` -- per-bot connection status

Translation applied:

* ``from common.settings import SETTINGS`` -> ``from unify.settings
  import SETTINGS``;
  ``SETTINGS.orchestra_admin_key`` ->
  ``SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()``;
  ``SETTINGS.orchestra_url`` -> ``SETTINGS.ORCHESTRA_URL``.
* ``from communication.discord import bot_manager`` ->
  ``from unify.gateway.channels.discord import bot_manager``.
* ``from communication.discord.gateway import DISCORD_API_BASE`` ->
  ``from unify.gateway.channels.discord.gateway import
  DISCORD_API_BASE``.
"""

from __future__ import annotations

import logging

import httpx
from fastapi import APIRouter, HTTPException, Request

from unify.gateway.channels.discord import bot_manager
from unify.gateway.channels.discord.gateway import DISCORD_API_BASE
from unify.gateway.common.auth import (
    require_assistant_ownership,
    require_gateway_admin,
)
from unify.settings import SETTINGS

logger = logging.getLogger("unify.gateway.channels.discord.views")

router = APIRouter()


def _admin_headers() -> dict:
    """Bearer headers for Orchestra admin API calls."""
    return {
        "Authorization": (f"Bearer {SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()}"),
    }


def _bot_headers(bot_token: str) -> dict:
    """Bot token headers for Discord REST API calls."""
    return {
        "Authorization": f"Bot {bot_token}",
        "Content-Type": "application/json",
    }


async def _resolve_route(assistant_id: int, contact_discord_id: str) -> dict:
    """Get or create a route for an outbound Discord message.

    Returns the full Orchestra response including ``pool_bot_id``.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{SETTINGS.ORCHESTRA_URL}/admin/discord/route",
            json={
                "assistant_id": assistant_id,
                "contact_number": contact_discord_id,
            },
            headers=_admin_headers(),
            timeout=10.0,
        )
    if resp.status_code >= 400:
        try:
            detail = resp.json().get("detail", resp.text)
        except Exception:
            detail = resp.text
        raise HTTPException(status_code=resp.status_code, detail=detail)
    return resp.json()


# ---------------------------------------------------------------------------
# POST /send
# ---------------------------------------------------------------------------


@router.post("/send")
async def send_discord_message(request: Request):
    """Send a message to a Discord user (DM) or channel.

    DM body:      ``{"to": "<user_id>", "body": "...", "assistant_id": <int>}``
    Channel body: ``{"channel_id": "...", "body": "...", "assistant_id":
                  <int>, "bot_id": "<pool_bot_id>"}``

    Optional ``media_url`` for image embeds.
    """
    data = await request.json()
    body = data["body"]
    assistant_id = data["assistant_id"]
    media_url = data.get("media_url")
    channel_id = data.get("channel_id")
    to = data.get("to")
    await require_assistant_ownership(request, assistant_id)

    if channel_id:
        pool_bot_id = data.get("bot_id")
        if not pool_bot_id:
            raise HTTPException(
                status_code=400,
                detail="bot_id is required for channel messages",
            )
    elif to:
        route = await _resolve_route(assistant_id, to)
        pool_bot_id = route["pool_bot_id"]
    else:
        raise HTTPException(
            status_code=400,
            detail="Either 'to' (DM) or 'channel_id' (channel) is required",
        )

    bot_token = bot_manager.get_bot_token(pool_bot_id)
    if not bot_token:
        raise HTTPException(
            status_code=503,
            detail=f"Bot {pool_bot_id} is not connected",
        )

    headers = _bot_headers(bot_token)

    if not channel_id:
        async with httpx.AsyncClient() as client:
            ch_resp = await client.post(
                f"{DISCORD_API_BASE}/users/@me/channels",
                json={"recipient_id": to},
                headers=headers,
                timeout=10.0,
            )
            if ch_resp.status_code >= 400:
                raise HTTPException(
                    status_code=ch_resp.status_code,
                    detail=f"Failed to open DM channel: {ch_resp.text}",
                )
            channel_id = ch_resp.json()["id"]

    msg_payload: dict = {"content": body}
    if media_url:
        msg_payload["embeds"] = [{"image": {"url": media_url}}]

    async with httpx.AsyncClient() as client:
        msg_resp = await client.post(
            f"{DISCORD_API_BASE}/channels/{channel_id}/messages",
            json=msg_payload,
            headers=headers,
            timeout=10.0,
        )
        if msg_resp.status_code >= 400:
            raise HTTPException(
                status_code=msg_resp.status_code,
                detail=f"Failed to send message: {msg_resp.text}",
            )

    message_id = msg_resp.json()["id"]
    logger.info(
        "sent Discord message to %s via bot %s (msg=%s)",
        channel_id,
        pool_bot_id,
        message_id,
    )
    return {"success": True, "message_id": message_id, "channel_id": channel_id}


# ---------------------------------------------------------------------------
# POST /create
# ---------------------------------------------------------------------------


@router.post("/create")
async def register_bot(request: Request):
    """Register a bot and connect it to the Discord Gateway.

    Called by Orchestra during assistant contact provisioning.
    Orchestra assigns the pool bot via its DAO first, then calls
    this endpoint to ensure the bot has an active Gateway connection.

    Body: ``{"bot_id": str, "assistant_id": int, "bot_token": str}``
    """
    require_gateway_admin(request)
    data = await request.json()
    bot_id = data["bot_id"]
    bot_token = data.get("bot_token")
    if not bot_token:
        raise HTTPException(status_code=400, detail="bot_token is required")

    await bot_manager.connect_bot(bot_id, bot_token)
    logger.info(
        "bot %s registered for assistant %s",
        bot_id,
        data.get("assistant_id"),
    )
    return {"success": True, "bot_id": bot_id}


# ---------------------------------------------------------------------------
# POST /sync
# ---------------------------------------------------------------------------


@router.post("/sync")
async def sync_pool(request: Request):
    """Re-sync bot pool state from Orchestra."""
    require_gateway_admin(request)
    count = await bot_manager.sync_from_orchestra()
    return {"synced": count, "pool": bot_manager.get_all_status()}


# ---------------------------------------------------------------------------
# DELETE /delete
# ---------------------------------------------------------------------------


@router.delete("/delete")
async def deregister_bot(request: Request):
    """Deregister a bot and disconnect from the Gateway.

    Body: ``{"bot_id": "<discord bot user ID>"}``.
    """
    require_gateway_admin(request)
    data = await request.json()
    bot_id = data["bot_id"]
    await bot_manager.disconnect_bot(bot_id)
    return {"success": True}


# ---------------------------------------------------------------------------
# GET /status
# ---------------------------------------------------------------------------


@router.get("/status")
async def bot_status(request: Request):
    """Health check -- return connection status for all pool bots."""
    require_gateway_admin(request)
    return bot_manager.get_all_status()


__all__ = ["router"]
