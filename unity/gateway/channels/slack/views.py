"""Slack admin endpoints (tenant-facing).

Three admin endpoints, all bearer-authed via the Orchestra admin
key. Mounted under ``/slack`` by ``unity.gateway.app``:

* ``POST /install`` -- record a workspace OAuth install in Orchestra
  (bot token, ``bot_user_id``, ``team_id``, scopes). Called by Console
  after the Slack OAuth callback completes.
* ``POST /send``    -- outbound DM, channel post, or threaded reply
  via ``chat.postMessage``. Resolves the workspace bot token from
  Orchestra by ``team_id``.
* ``GET  /status``  -- list installs known to Orchestra (debug /
  health check).

The inbound ``/slack/events`` webhook does NOT live here -- it is in
``communication/adapters/main.py:slack_events_webhook`` alongside the
other third-party webhooks (Twilio, Microsoft, Google). Splitting
that way matches the existing channel topology: high-volume external
webhooks land in adapters; admin-authed tenant-side endpoints land
in the gateway.
"""

from __future__ import annotations

import logging

import httpx
from fastapi import APIRouter, HTTPException, Request

from unity.settings import SETTINGS

logger = logging.getLogger("unity.gateway.channels.slack.views")

auth_router = APIRouter()

SLACK_API_BASE = "https://slack.com/api"


def _admin_headers() -> dict:
    """Bearer headers for Orchestra admin API calls."""
    return {
        "Authorization": f"Bearer {SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()}",
    }


async def _resolve_bot_token(team_id: str) -> str:
    """Return the workspace bot token for ``team_id`` (admin auth)."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/admin/slack/install",
            params={"team_id": team_id},
            headers=_admin_headers(),
            timeout=10.0,
        )
    if resp.status_code == 404:
        raise HTTPException(
            status_code=404,
            detail=f"No Slack install for team {team_id}",
        )
    if resp.status_code >= 400:
        raise HTTPException(
            status_code=resp.status_code,
            detail=resp.text,
        )
    install = resp.json()
    token = install.get("bot_access_token") or ""
    if not token:
        raise HTTPException(
            status_code=503,
            detail=f"Slack install for team {team_id} has no bot token",
        )
    return token


# ---------------------------------------------------------------------------
# POST /install
# ---------------------------------------------------------------------------


@auth_router.post("/install")
async def upsert_install(request: Request):
    """Register or refresh a workspace OAuth install in Orchestra.

    Body::

        {
            "organization_id": int,
            "team_id": str,
            "team_name": str,
            "bot_user_id": str,
            "bot_access_token": str,
            "scopes": [str, ...],
            "installer_slack_user_id": str | None,
        }

    Idempotent: re-installing the same workspace updates the stored
    token and scopes (Slack rotates bot tokens on reinstall).
    """
    data = await request.json()
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{SETTINGS.ORCHESTRA_URL}/admin/slack/install",
            json=data,
            headers=_admin_headers(),
            timeout=10.0,
        )
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


# ---------------------------------------------------------------------------
# POST /send
# ---------------------------------------------------------------------------


@auth_router.post("/send")
async def send_slack_message(request: Request):
    """Send a Slack message via ``chat.postMessage``.

    Modes (mutually exclusive):

    * **DM**:                ``{"user_id": "...", "team_id": "..."}``.
      Communication opens (or reuses) a DM channel via
      ``conversations.open`` and posts there.
    * **Channel post / reply**: ``{"channel_id": "...", "team_id": "..."}``
      (optional ``thread_ts``).
    """
    data = await request.json()
    team_id = data["team_id"]
    body = data["body"]
    thread_ts = data.get("thread_ts")
    user_id = data.get("user_id")
    channel_id = data.get("channel_id")

    if not channel_id and not user_id:
        raise HTTPException(
            status_code=400,
            detail="Either 'channel_id' or 'user_id' is required",
        )

    bot_token = await _resolve_bot_token(team_id)
    headers = {
        "Authorization": f"Bearer {bot_token}",
        "Content-Type": "application/json; charset=utf-8",
    }

    if not channel_id:
        async with httpx.AsyncClient() as client:
            open_resp = await client.post(
                f"{SLACK_API_BASE}/conversations.open",
                json={"users": user_id},
                headers=headers,
                timeout=10.0,
            )
        open_payload = open_resp.json()
        if not open_payload.get("ok"):
            raise HTTPException(
                status_code=502,
                detail=f"conversations.open failed: {open_payload.get('error')}",
            )
        channel_id = open_payload["channel"]["id"]

    msg_payload: dict = {"channel": channel_id, "text": body}
    if thread_ts:
        msg_payload["thread_ts"] = thread_ts

    async with httpx.AsyncClient() as client:
        msg_resp = await client.post(
            f"{SLACK_API_BASE}/chat.postMessage",
            json=msg_payload,
            headers=headers,
            timeout=10.0,
        )
    msg_data = msg_resp.json()
    if not msg_data.get("ok"):
        raise HTTPException(
            status_code=502,
            detail=f"chat.postMessage failed: {msg_data.get('error')}",
        )
    logger.info(
        "sent Slack message to %s on team %s (ts=%s)",
        channel_id,
        team_id,
        msg_data.get("ts"),
    )
    return {
        "success": True,
        "message_ts": msg_data.get("ts"),
        "channel_id": channel_id,
    }


# ---------------------------------------------------------------------------
# GET /status
# ---------------------------------------------------------------------------


@auth_router.get("/status")
async def status():
    """List workspace installs known to Orchestra."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/admin/slack/installs",
            headers=_admin_headers(),
            timeout=10.0,
        )
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


__all__ = ["auth_router"]
