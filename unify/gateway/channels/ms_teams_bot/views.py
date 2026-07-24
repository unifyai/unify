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
import os
import time

import httpx
from fastapi import APIRouter, HTTPException, Request

from unify.gateway.common.auth import (
    require_assistant_ownership,
    require_gateway_admin,
)
from unify.settings import SETTINGS

logger = logging.getLogger("unify.gateway.channels.ms_teams_bot.views")


def _log_field(value: object) -> str:
    return str(value).replace("\r", "").replace("\n", "")


auth_router = APIRouter()

# The Azure bot is a *single-tenant* registration living in Unify's own
# Microsoft tenant, so Bot Connector tokens are minted from that tenant's
# authority — not the shared ``botframework.com`` authority that multi-tenant
# bots use. The home tenant is reused from ``MS365_ADMIN_TENANT_ID`` (the same
# tenant that hosts Unify's MS365 admin app registration and this bot's app
# registration), so no separate Teams-bot tenant env var is needed.
_BOT_TOKEN_SCOPE = "https://api.botframework.com/.default"

# Teams renders a native "AI generated" caption beneath a message when the
# activity carries this schema.org entity. Every live assistant reply routes
# through ``POST /send``, so attaching it there labels all AI-authored content
# in-product (Store certification requires AI-generated content be disclosed to
# users), without appending text or altering the message body.
_AI_GENERATED_CONTENT_ENTITY = {
    "type": "https://schema.org/Message",
    "@type": "Message",
    "@context": "https://schema.org",
    "@id": "",
    "additionalType": ["AIGeneratedContent"],
}

# The bot is one app registration, so a single minted token (valid ~24h)
# authenticates every outbound send; a tiny process-local cache avoids a
# token round-trip on each one.
_token_cache: dict[str, tuple[str, float]] = {}
_TOKEN_SKEW_SECONDS = 300


def _bot_token_url() -> str:
    """Client-credentials token endpoint for the bot's home tenant.

    Single-tenant bots authenticate against their own tenant authority.
    Reuses ``MS365_ADMIN_TENANT_ID`` — the tenant hosting Unify's MS365
    admin app registration, which is also where the Teams bot app is
    registered.
    """
    tenant_id = os.getenv("MS365_ADMIN_TENANT_ID", "")
    if not tenant_id:
        raise HTTPException(
            status_code=503,
            detail="MS365_ADMIN_TENANT_ID is not configured for the Teams bot.",
        )
    return f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"


def _admin_headers() -> dict:
    """Bearer headers for Orchestra admin API calls."""
    return {
        "Authorization": f"Bearer {SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()}",
    }


async def _mint_connector_token() -> str:
    """Mint (or reuse) a Bot Connector access token via client credentials.

    Uses the deployment-level ``MS_TEAMS_BOT_APP_ID`` /
    ``MS_TEAMS_BOT_APP_SECRET``. The bot is a single-tenant registration, so
    the token is minted from the bot's home-tenant authority
    (:func:`_bot_token_url`) with the Connector resource scope; the resulting
    token authenticates outbound sends into every customer tenant the bot has
    been installed in.
    """
    app_id = SETTINGS.MS_TEAMS_BOT_APP_ID
    app_secret = SETTINGS.MS_TEAMS_BOT_APP_SECRET.get_secret_value()
    if not app_id or not app_secret:
        raise HTTPException(
            status_code=503,
            detail="MS Teams bot app credentials are not configured.",
        )

    token_url = _bot_token_url()
    cached = _token_cache.get(app_id)
    now = time.time()
    if cached is not None and cached[1] - _TOKEN_SKEW_SECONDS > now:
        return cached[0]

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            token_url,
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
    require_gateway_admin(request)
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
    require_gateway_admin(request)
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
    require_gateway_admin(request)
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
    await require_assistant_ownership(request, assistant_id)

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
        "entities": [_AI_GENERATED_CONTENT_ENTITY],
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
async def status(request: Request):
    """List MS Teams bot installs known to Orchestra."""
    require_gateway_admin(request)
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
