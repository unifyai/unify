"""Which organisation owns a drive, and a token valid for it.

Content shared from another organisation lives in that organisation's tenant.
A delegated token is issued *for one tenant*, so a token stamped with the
assistant's own tenant cannot address a foreign drive: Graph answers 404 for
the item, which is indistinguishable from the item not existing. That is why a
correctly shared folder read as missing for a week.

A refresh token is bound to (user, client) and **not** to a tenant, so the one
already stored redeems against the owning tenant. Redeeming needs the
application's client secret, which deliberately does not exist in this process
-- the runtime holds provider tokens but never the credential that mints them
-- so the exchange is delegated to the deploy layer and only the resulting
short-lived access token is cached here.

Provenance is learned rather than probed. Every ``sharedWithMe`` response names
the owning tenant of each item in ``sharepointIds.tenantId``, so the discovery
call the assistant already makes teaches this module the drive-to-tenant map at
no extra cost.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import threading
import time
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

# drive_id -> tenant_id, learned from listings that name both.
_DRIVE_TENANT: dict[str, str] = {}
# tenant_id -> (access_token, expires_at_monotonic)
_TENANT_TOKENS: dict[str, tuple[str, float]] = {}
_LOCK = threading.Lock()

# Refresh a little before expiry so a call never starts with a token that dies
# mid-flight.
_EXPIRY_MARGIN_S = 120.0


def _jwt_claims(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(payload))
    except (ValueError, TypeError):
        return {}


def home_tenant(access_token: str) -> str:
    """The tenant the assistant's own token is issued for."""
    return str(_jwt_claims(access_token).get("tid") or "")


def remember_drive_tenant(drive_id: str, tenant_id: str) -> None:
    """Record which organisation owns a drive."""
    if not drive_id or not tenant_id:
        return
    with _LOCK:
        _DRIVE_TENANT[drive_id] = tenant_id


def learn_from_listing(payload: Any) -> None:
    """Harvest drive-to-tenant pairs from a Graph listing.

    ``sharedWithMe`` returns each item's real location under ``remoteItem``,
    with the owning tenant in ``sharepointIds.tenantId`` and the owning drive in
    ``parentReference.driveId``. Reading them here means the mapping is already
    known by the time the assistant traverses into the folder, so routing never
    costs an extra round trip and never has to guess.
    """
    if not isinstance(payload, dict):
        return
    for row in payload.get("value") or []:
        if not isinstance(row, dict):
            continue
        item = row.get("remoteItem") if isinstance(row.get("remoteItem"), dict) else row
        tenant = (item.get("sharepointIds") or {}).get("tenantId")
        drive = (item.get("parentReference") or {}).get("driveId")
        remember_drive_tenant(str(drive or ""), str(tenant or ""))


def tenant_for_drive(drive_id: str) -> Optional[str]:
    with _LOCK:
        return _DRIVE_TENANT.get(drive_id)


def _cached_token(tenant_id: str) -> Optional[str]:
    with _LOCK:
        entry = _TENANT_TOKENS.get(tenant_id)
    if entry is None:
        return None
    token, expires_at = entry
    if time.monotonic() >= expires_at:
        return None
    return token


async def token_for_tenant(tenant_id: str) -> Optional[str]:
    """Return an access token valid for *tenant_id*, minting one if needed.

    ``None`` means no token is available -- most often because that
    organisation has not consented to the app, which is a normal first-contact
    state rather than an error, and is reported to the caller so it can ask for
    approval instead of retrying.
    """
    from unify.session_details import SESSION_DETAILS

    cached = _cached_token(tenant_id)
    if cached:
        return cached

    base = (
        os.environ.get("UNIFY_ADAPTERS_URL") or os.environ.get("ADAPTERS_URL") or ""
    ).rstrip("/")
    agent_id = SESSION_DETAILS.assistant.agent_id
    unify_key = SESSION_DETAILS.unify_key
    if not base or agent_id is None or not unify_key:
        return None

    try:
        async with httpx.AsyncClient(timeout=45) as client:
            resp = await client.post(
                f"{base}/microsoft/token-for-tenant",
                json={"assistant_id": str(agent_id), "tenant_id": tenant_id},
                headers={"Authorization": f"Bearer {unify_key}"},
            )
    except httpx.HTTPError as exc:
        logger.warning("[tenant-token] %s unreachable: %s", tenant_id, exc)
        return None

    if resp.status_code != 200:
        logger.warning(
            "[tenant-token] %s http=%s",
            tenant_id,
            resp.status_code,
        )
        return None

    body = resp.json()
    status = body.get("status")
    if status != "ok":
        # consent_required is the expected first-contact answer; log it plainly
        # so a missing approval is visible rather than looking like an empty
        # folder further down.
        logger.info(
            "[tenant-token] %s status=%s",
            tenant_id,
            status,
        )
        return None

    token = body.get("access_token") or ""
    if not token:
        return None
    ttl = float(body.get("expires_in") or 3600)
    with _LOCK:
        _TENANT_TOKENS[tenant_id] = (
            token,
            time.monotonic() + max(ttl - _EXPIRY_MARGIN_S, 60.0),
        )
    logger.info("[tenant-token] %s minted ttl=%ss", tenant_id, int(ttl))
    return token


def clear() -> None:
    """Drop all learned provenance and cached tokens."""
    with _LOCK:
        _DRIVE_TENANT.clear()
        _TENANT_TOKENS.clear()
