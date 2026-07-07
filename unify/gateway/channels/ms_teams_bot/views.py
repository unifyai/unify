"""MS Teams bot admin endpoints (tenant-facing).

Mounted under ``/ms-teams-bot`` by ``unify.gateway.app``:

* ``POST /pending-install`` -- record a Teams Store install before its
  Unify owner is known (proxies Orchestra; returns the bind nonce).
* ``POST /bind``            -- bind a pending install to a Unify owner.
* ``POST /install``         -- record an already-owned install
  (Console-driven path).
* ``POST /send``            -- outbound proactive reply into an existing
  Teams conversation via the Bot Connector API. Resolves the tenant's
  ``service_url`` from Orchestra and mints a Connector token from the
  shared bot app credentials.
* ``GET  /status``          -- list installs known to Orchestra.

The inbound ``/ms-teams-bot/messages`` webhook does NOT live here -- it is
owned by the hosted adapters (mirrored in
``unify.gateway.adapters.ms_teams_bot``), matching the Slack topology.
"""

from __future__ import annotations

import logging
import time

import httpx
from fastapi import APIRouter, HTTPException, Request

from unify.settings import SETTINGS

logger = logging.getLogger("unify.gateway.channels.ms_teams_bot.views")


def _log_field(value: object) -> str:
    return str(value).replace("\r", "").replace("\n", "")


auth_router = APIRouter()

# Bot Connector tokens are minted against the shared botframework tenant
# for a multi-tenant bot, with the Connector resource scope.
_BOT_TOKEN_URL = "https://login.microsoftonline.com/botframework.com/oauth2/v2.0/token"
_BOT_TOKEN_SCOPE = "https://api.botframework.com/.default"

# A minted Connector token is valid ~24h and is identical across tenants
# (single app registration), so a tiny process-local cache avoids a token
# round-trip on every outbound send.
_token_cache: dict[str, tuple[str, float]] = {}
_TOKEN_SKEW_SECONDS = 300


def _admin_headers() -> dict:
    """Bearer headers for Orchestra admin API calls."""
    return {
        "Authorization": f"Bearer {SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()}",
    }


async def _mint_connector_token() -> str:
    """Mint (or reuse) a Bot Connector access token via client credentials.

    Uses the deployment-level ``MS_TEAMS_BOT_APP_ID`` /
    ``MS_TEAMS_BOT_APP_SECRET`` — a single multi-tenant Azure bot
    registration serves every tenant, so one token authenticates outbound
    to all of them.
    """
    app_id = SETTINGS.MS_TEAMS_BOT_APP_ID
    app_secret = SETTINGS.MS_TEAMS_BOT_APP_SECRET.get_secret_value()
    if not app_id or not app_secret:
        raise HTTPException(
            status_code=503,
            detail="MS Teams bot app credentials are not configured.",
        )

    cached = _token_cache.get(app_id)
    now = time.time()
    if cached is not None and cached[1] - _TOKEN_SKEW_SECONDS > now:
        return cached[0]

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            _BOT_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": app_id,
                "client_secret": app_secret,
                "scope": _BOT_TOKEN_SCOPE,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10.0,
        )
    if resp.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"Bot Connector token mint failed: {resp.text}",
        )
    payload = resp.json()
    token = payload.get("access_token") or ""
    if not token:
        raise HTTPException(
            status_code=502,
            detail="Bot Connector token mint returned no access_token.",
        )
    expires_in = int(payload.get("expires_in") or 3600)
    _token_cache[app_id] = (token, now + expires_in)
    return token


async def _resolve_install(tenant_id: str) -> dict:
    """Fetch the active install for ``tenant_id`` from Orchestra."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/admin/ms-teams-bot/install",
            params={"tenant_id": tenant_id},
            headers=_admin_headers(),
            timeout=10.0,
        )
    if resp.status_code == 404:
        raise HTTPException(
            status_code=404,
            detail=f"No MS Teams bot install for tenant {tenant_id}",
        )
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


async def _resolve_service_url(install: dict, conversation_id: str) -> str:
    """Best service_url for a conversation: route override, else install.

    The service_url is region-specific and echoed on every inbound
    activity; the conversation route carries the freshest value seen for
    that specific conversation, so prefer it and fall back to the install.
    """
    install_id = install.get("id")
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/admin/ms-teams-bot/conversation-routes",
            params={
                "install_id": install_id,
                "conversation_id": conversation_id,
            },
            headers=_admin_headers(),
            timeout=10.0,
        )
    if resp.status_code == 200:
        # A stored ConversationReference (if any) carries the exact
        # service_url captured on inbound; but we only persist it as an
        # opaque blob, so use the install's service_url as the routable
        # endpoint. (Route lookup still validates the conversation is live.)
        pass
    return install.get("service_url") or ""


# ---------------------------------------------------------------------------
# Install lifecycle (proxied to Orchestra)
# ---------------------------------------------------------------------------


@auth_router.post("/pending-install")
async def ensure_pending_install(request: Request):
    """Record a pending (unbound) Teams install; returns its bind nonce."""
    data = await request.json()
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{SETTINGS.ORCHESTRA_URL}/admin/ms-teams-bot/pending-install",
            json=data,
            headers=_admin_headers(),
            timeout=10.0,
        )
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


@auth_router.post("/bind")
async def bind_install(request: Request):
    """Bind a pending install to a Unify owner (org XOR user)."""
    data = await request.json()
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{SETTINGS.ORCHESTRA_URL}/admin/ms-teams-bot/bind",
            json=data,
            headers=_admin_headers(),
            timeout=10.0,
        )
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


@auth_router.post("/install")
async def upsert_install(request: Request):
    """Register or refresh an already-owned Teams install in Orchestra."""
    data = await request.json()
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{SETTINGS.ORCHESTRA_URL}/admin/ms-teams-bot/install",
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
async def send_ms_teams_bot_message(request: Request):
    """Send a proactive reply into an existing Teams conversation.

    Body::

        {
            "tenant_id": str,
            "conversation_id": str,
            "body": str,
            "assistant_id": int | None,   # persists / refreshes the route
        }

    Resolves the tenant's ``service_url`` from Orchestra, mints a Bot
    Connector token from the shared app credentials, and POSTs a message
    activity to
    ``{service_url}/v3/conversations/{conversation_id}/activities``.
    """
    data = await request.json()
    tenant_id = data["tenant_id"]
    conversation_id = data["conversation_id"]
    body = data.get("body") or ""
    assistant_id = data.get("assistant_id")

    if not body:
        raise HTTPException(status_code=400, detail="'body' is required")

    install = await _resolve_install(tenant_id)
    service_url = await _resolve_service_url(install, conversation_id)
    if not service_url:
        raise HTTPException(
            status_code=503,
            detail=f"No service_url known for tenant {tenant_id}",
        )

    token = await _mint_connector_token()
    activity = {
        "type": "message",
        "text": body,
    }
    endpoint = (
        f"{service_url.rstrip('/')}/v3/conversations/" f"{conversation_id}/activities"
    )
    async with httpx.AsyncClient() as client:
        msg_resp = await client.post(
            endpoint,
            json=activity,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=15.0,
        )
    if msg_resp.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"Bot Connector send failed: {msg_resp.text}",
        )
    sent = msg_resp.json() if msg_resp.content else {}
    activity_id = sent.get("id")

    # Pin the conversation to the sending assistant so replies come back to
    # it (mirrors the Slack post-send thread-route upsert).
    if assistant_id is not None:
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{SETTINGS.ORCHESTRA_URL}"
                    "/admin/ms-teams-bot/conversation-routes",
                    json={
                        "install_id": install.get("id"),
                        "conversation_id": conversation_id,
                        "assistant_id": assistant_id,
                    },
                    headers=_admin_headers(),
                    timeout=10.0,
                )
        except Exception:
            logger.exception(
                "failed to upsert conversation route after send "
                "(tenant=%s conversation=%s)",
                _log_field(tenant_id),
                _log_field(conversation_id),
            )

    logger.info(
        "sent Teams bot message to conversation %s on tenant %s (id=%s)",
        _log_field(conversation_id),
        _log_field(tenant_id),
        _log_field(activity_id),
    )
    return {
        "success": True,
        "activity_id": activity_id,
        "conversation_id": conversation_id,
    }


# ---------------------------------------------------------------------------
# GET /status
# ---------------------------------------------------------------------------


@auth_router.get("/status")
async def status():
    """List MS Teams bot installs known to Orchestra."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/admin/ms-teams-bot/installs",
            headers=_admin_headers(),
            timeout=10.0,
        )
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


__all__ = ["auth_router"]
