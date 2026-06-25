"""FastAPI routes for the WhatsApp channel.

Ports ``communication/whatsapp/views.py`` into ``unity.gateway``.
Translation applied:

* ``from common.settings import SETTINGS`` -> ``from unity.settings
  import SETTINGS``; ``SETTINGS.{comms_url, adapters_url,
  orchestra_url}`` -> ``SETTINGS.conversation.COMMS_URL`` /
  ``SETTINGS.conversation.ADAPTERS_URL`` / ``SETTINGS.ORCHESTRA_URL``;
  ``SETTINGS.orchestra_admin_key`` -> ``SETTINGS.ORCHESTRA_ADMIN_KEY
  .get_secret_value()``.
* ``from communication.helpers import get_twilio_wa_client`` ->
  ``from unity.gateway.common.twilio import build_twilio_wa_client``.
* ``from common.livekit import ensure_phone_dispatch_rule,
  make_sip_uri`` -> ``from unity.gateway.common.livekit import ...``.
* ``os.getenv("TWILIO_WA_*" | "GCP_SA_KEY" | "LIVEKIT_*" |
  "DEPLOY_ENV")`` -> ``credentials.get(...)`` via
  ``EnvCredentialStore``.
* The raw Twilio Senders API + Orchestra admin endpoints stay as
  inline ``httpx.AsyncClient`` calls (channel-specific surface, not
  worth promoting until a second channel needs the same shape).

Wire behaviour preserved bit-for-bit so the gateway aggregator can
mount the routers at ``/whatsapp`` and external callers (Twilio
webhooks, Unity admin clients) see no change.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi import APIRouter, Form, HTTPException, Query, Request
from google.cloud import storage
from google.oauth2.service_account import Credentials
from livekit.api import (
    CreateSIPInboundTrunkRequest,
    LiveKitAPI,
    SIPInboundTrunkInfo,
)

from unity.gateway.common.livekit import ensure_phone_dispatch_rule, make_sip_uri
from unity.gateway.common.twilio import build_twilio_wa_client
from unity.gateway.credentials import (
    CredentialNotFoundError,
    CredentialStore,
    EnvCredentialStore,
)
from unity.settings import SETTINGS

logger = logging.getLogger("unity.gateway.channels.whatsapp")

auth_router = APIRouter()
unauth_router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _admin_headers() -> dict:
    """Bearer headers for Orchestra admin API calls."""
    return {
        "Authorization": (f"Bearer {SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()}"),
    }


def _twilio_whatsapp_auth_headers(credentials: CredentialStore) -> dict:
    """Basic auth header pair for the raw Twilio Senders v2 HTTP API.

    The Senders API isn't surfaced through the official Twilio Python
    SDK, so we hit it directly with httpx. Credentials are the
    WhatsApp-specific sub-account (separate from the SMS account).
    """
    try:
        account_sid = credentials.get("TWILIO_WA_ACCOUNT_SID")
        auth_token = credentials.get("TWILIO_WA_AUTH_TOKEN")
    except CredentialNotFoundError as exc:
        raise RuntimeError(
            "TWILIO_WA_ACCOUNT_SID and TWILIO_WA_AUTH_TOKEN must be set",
        ) from exc
    b64_auth = base64.b64encode(f"{account_sid}:{auth_token}".encode()).decode()
    return {
        "Authorization": f"Basic {b64_auth}",
        "Content-Type": "application/json",
    }


async def _resolve_route(assistant_id: int, contact_number: str) -> dict:
    """Get or create a route for an outbound message via Orchestra.

    Returns ``{"pool_number": str, "window_open": bool}``. Orchestra
    owns the routing decision (which pool number to send from + whether
    the 24-hour freeform window is open) so the channel only has to
    consume the answer.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/route",
            json={"assistant_id": assistant_id, "contact_number": contact_number},
            headers=_admin_headers(),
            timeout=10.0,
        )
    if resp.status_code >= 400:
        try:
            detail = resp.json().get("detail", resp.text)
        except Exception:
            detail = resp.text
        raise HTTPException(status_code=resp.status_code, detail=detail)
    data = resp.json()
    return {
        "pool_number": data["pool_number"],
        "window_open": data.get("window_open", True),
    }


# ---------------------------------------------------------------------------
# POST /status -- unauthenticated Twilio status callback
# ---------------------------------------------------------------------------


@unauth_router.post("/status")
async def check_whatsapp_status(
    MessageStatus: str = Form(...),
    To: str = Form(...),
    From: str = Form(...),
    MessageSid: str | None = Form(None),
    callback_id: str | None = Query(None),
):
    """Twilio delivery-receipt webhook for outbound WhatsApp messages."""
    logger.info(
        "[WhatsApp Status Callback] MessageStatus=%s To=%s From=%s "
        "MessageSid=%s callback_id=%s",
        MessageStatus,
        To,
        From,
        MessageSid,
        callback_id,
    )
    if callback_id:
        await _forward_notification_status(callback_id, To, MessageSid, MessageStatus)
    return {"status": True, "message_status": MessageStatus}


async def _forward_notification_status(
    callback_id: str,
    to: str,
    message_sid: str | None,
    status: str,
) -> None:
    """Forward a delivery receipt to Orchestra's notification-status endpoint."""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/notification-status",
                headers=_admin_headers(),
                json={
                    "callback_id": callback_id,
                    "to": to.replace("whatsapp:", ""),
                    "message_sid": message_sid,
                    "status": status,
                },
                timeout=10.0,
            )
            if resp.status_code >= 400:
                logger.error(
                    "failed to forward notification status: %s %s",
                    resp.status_code,
                    resp.text,
                )
    except Exception:
        logger.exception("error forwarding notification status to Orchestra")


# ---------------------------------------------------------------------------
# Media URL resolution + template SIDs
# ---------------------------------------------------------------------------


_GS_URL_RE = re.compile(r"^gs://([^/]+)/(.+)$")

GREETING_TEMPLATE_SID = "HX002f6aeb3b4e5a79b693fa7190196612"
NUMBER_CHANGE_TEMPLATE_SID = "HXd9c362371aefe97f10526f1c0974f7a2"
VOICE_CALL_TEMPLATE_SID = "HX885d46e6ccb82e4313ef1a42181c142d"
VOICE_CALL_REQUEST_TEMPLATE_SID = "HX67bc29b24fb597e6fad501ea68d2566e"
CALL_PERMISSION_PENDING_SUPPRESSION = timedelta(minutes=10)
CALL_PERMISSION_PROBE_STATUSES = {"pending", "unknown_interaction"}


def render_greeting_template_text(user_name: str, agent_name: str) -> str:
    """Render the approved greeting template for local history/reporting."""
    user = (user_name or "there").strip() or "there"
    agent = (agent_name or "your assistant").strip() or "your assistant"
    return (
        f"Hello {user}, this is {agent} from Unify. "
        "I have a message for you. Reply here and I'll share the details!"
    )


def _resolve_media_url(url: str, credentials: CredentialStore) -> str:
    """If *url* is a ``gs://`` URI, return a signed download URL.

    Twilio's WhatsApp media URL must be HTTP(S); ``gs://`` URIs that
    callers pass in are signed inline using the configured GCS service
    account. Non-``gs://`` URLs pass through unchanged.
    """
    match = _GS_URL_RE.match(url)
    if not match:
        return url
    bucket_name, blob_path = match.group(1), match.group(2)
    creds_json_raw = credentials.get_optional("GCP_SA_KEY", "")
    if not creds_json_raw:
        raise HTTPException(status_code=500, detail="GCP_SA_KEY not configured")
    creds_json = json.loads(creds_json_raw)
    creds = Credentials.from_service_account_info(creds_json)
    client = storage.Client(credentials=creds)
    blob = client.bucket(bucket_name).blob(blob_path)
    return blob.generate_signed_url(
        version="v4",
        expiration=timedelta(hours=1),
        method="GET",
    )


# ---------------------------------------------------------------------------
# Authenticated endpoints
# ---------------------------------------------------------------------------


@auth_router.post("/notify")
async def notify(request: Request):
    """Send number-change template notifications to affected users."""
    credentials = EnvCredentialStore()
    data = await request.json()
    from_number = data["from_number"]
    recipients = data["recipients"]
    old_contact = data["old_contact"]
    new_contact = data["new_contact"]
    callback_id = data.get("callback_id")

    status_callback = f"{SETTINGS.conversation.COMMS_URL}/whatsapp/status"
    if callback_id:
        status_callback += f"?callback_id={callback_id}"

    twilio_client = build_twilio_wa_client(credentials)
    results: dict = {}
    for recipient in recipients:
        to = recipient["to"]
        if not to:
            continue
        msg = twilio_client.messages.create(
            content_sid=NUMBER_CHANGE_TEMPLATE_SID,
            to=f"whatsapp:{to}",
            from_=f"whatsapp:{from_number}",
            content_variables=json.dumps(
                {
                    "user_name": recipient["user_name"],
                    "agent_name": recipient["agent_name"],
                    "old_contact": old_contact,
                    "new_contact": new_contact,
                },
            ),
            status_callback=status_callback,
        )
        results[to] = {"sid": msg.sid, "status": "sent"}

    return {"results": results}


@auth_router.post("/send")
async def send(request: Request):
    """Send a WhatsApp message via the routed pool number.

    Two paths based on whether the 24-hour freeform window is open:
    open -> ``messages.create`` with body + optional media, closed
    -> ``messages.create`` with the GREETING_TEMPLATE_SID template
    (and ignores media_url, since templates don't support media).
    """
    credentials = EnvCredentialStore()
    data = await request.json()
    to = data["to"]
    body = data["body"]
    assistant_id = data["assistant_id"]
    user_name = data.get("user_name", "")
    agent_name = data.get("agent_name", "")
    media_url = data.get("media_url")

    route = await _resolve_route(assistant_id, to)
    pool_number = route["pool_number"]
    window_open = route["window_open"]

    twilio_client = build_twilio_wa_client(credentials)
    if window_open:
        delivered_body = body
        create_kwargs: dict = {
            "to": f"whatsapp:{to}",
            "from_": f"whatsapp:{pool_number}",
            "body": body,
            "status_callback": (f"{SETTINGS.conversation.COMMS_URL}/whatsapp/status"),
        }
        if media_url:
            create_kwargs["media_url"] = [_resolve_media_url(media_url, credentials)]
        twilio_client.messages.create(**create_kwargs)
        method = "freeform"
    else:
        if media_url:
            logger.warning(
                "media_url ignored for out-of-window template message "
                "(to=%s assistant_id=%s)",
                to,
                assistant_id,
            )
        twilio_client.messages.create(
            content_sid=GREETING_TEMPLATE_SID,
            to=f"whatsapp:{to}",
            from_=f"whatsapp:{pool_number}",
            content_variables=json.dumps(
                {"user_name": user_name, "agent_name": agent_name},
            ),
            status_callback=f"{SETTINGS.conversation.COMMS_URL}/whatsapp/status",
        )
        method = "template"
        delivered_body = render_greeting_template_text(user_name, agent_name)

    return {"success": True, "method": method, "delivered_body": delivered_body}


async def _check_call_permission(pool_number: str, contact_number: str) -> dict:
    """Check with Orchestra whether outbound WhatsApp calling is permitted."""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/call-permission",
                params={
                    "pool_number": pool_number,
                    "contact_number": contact_number,
                },
                headers=_admin_headers(),
                timeout=10.0,
            )
        if resp.status_code >= 400:
            return {"permitted": False, "status": "unknown"}
        data = resp.json()
        return {
            "permitted": bool(data.get("permitted")),
            "status": data.get("status") or "unknown",
            "requested_at": data.get("requested_at"),
            "expires_at": data.get("expires_at"),
        }
    except Exception:
        logger.exception("error checking WhatsApp call permission")
        return {"permitted": False, "status": "unknown"}


async def _record_call_permission_pending(
    pool_number: str,
    contact_number: str,
) -> None:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/call-permission",
            headers=_admin_headers(),
            json={
                "pool_number": pool_number,
                "contact_number": contact_number,
                "status": "pending",
                "source": "send_call",
            },
            timeout=10.0,
        )
    response.raise_for_status()


async def _record_call_permission_accepted(
    pool_number: str,
    contact_number: str,
    *,
    source: str,
) -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/call-permission",
            headers=_admin_headers(),
            json={
                "pool_number": pool_number,
                "contact_number": contact_number,
                "status": "accepted",
                "source": source,
            },
            timeout=10.0,
        )
    response.raise_for_status()
    return response.json()


async def _has_pending_call_intent(pool_number: str, contact_number: str) -> bool:
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/pending-call-intent",
            params={
                "pool_number": pool_number,
                "contact_number": contact_number,
            },
            headers=_admin_headers(),
            timeout=10.0,
        )
    if response.status_code == 404:
        return False
    response.raise_for_status()
    return True


def _recent_pending_invite(permission: dict) -> bool:
    if permission.get("status") != "pending":
        return False
    return _recent_permission_request(permission)


def _recent_permission_request(permission: dict) -> bool:
    requested_at = _parse_dt(permission.get("requested_at"))
    if requested_at is None:
        return False
    now = datetime.now(requested_at.tzinfo)
    return now - requested_at < CALL_PERMISSION_PENDING_SUPPRESSION


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _is_local_permission_probe_enabled() -> bool:
    if _truthy_env("WHATSAPP_CALL_PERMISSION_PROBE_ENABLED"):
        return True
    if _truthy_env("SELF_HOST") or _truthy_env("NEXT_PUBLIC_SELF_HOST"):
        return True
    local_urls = (SETTINGS.ORCHESTRA_URL, SETTINGS.conversation.COMMS_URL)
    return any("127.0.0.1" in url or "localhost" in url for url in local_urls)


def _permission_cache_path() -> Path:
    configured = os.environ.get("COMMS_BRIDGE_PERMISSION_CACHE", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".unity" / "whatsapp_call_permissions.json"


def _cache_call_permission(
    *,
    pool_number: str,
    contact_number: str,
    response_payload: dict | None,
) -> None:
    path = _permission_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        cache = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        cache = {}
    cache[f"{pool_number}|{contact_number}"] = {
        "pool_number": pool_number,
        "contact_number": contact_number,
        "status": "accepted",
        "expires_at": (response_payload or {}).get("expires_at"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    tmp = path.with_suffix(f"{path.suffix}.tmp")
    tmp.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _is_permission_probe_failure(exc: Exception) -> bool:
    code = str(getattr(exc, "code", "") or "")
    status = str(getattr(exc, "status", "") or "")
    text = str(exc).lower()
    return (
        code in {"21216", "21217", "21218", "63016", "63018", "63024"}
        or status in {"400", "403"}
        or "permission" in text
        or "not approved" in text
        or "not allowed" in text
    )


async def _place_direct_whatsapp_call(
    *,
    credentials: CredentialStore,
    pool_number: str,
    to: str,
    room_name: str,
    wa_client,
) -> dict:
    sip_uri = make_sip_uri(pool_number, credentials)
    await ensure_phone_dispatch_rule(pool_number, room_name, credentials)

    date_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    conference_name = f"Unity_WA_{pool_number[1:]}_{date_time}"

    user_call = wa_client.calls.create(
        to=f"whatsapp:{to}",
        from_=f"whatsapp:{pool_number}",
        twiml=_conference_twiml(conference_name),
        status_callback=(
            SETTINGS.conversation.ADAPTERS_URL + "/twilio/whatsapp-call-status"
        ),
        status_callback_event=["initiated", "ringing", "answered", "completed"],
    )
    wa_client.calls.create(
        to=sip_uri,
        from_=pool_number,
        twiml=_conference_twiml(conference_name),
    )
    logger.info(
        "outbound WhatsApp call placed to %s call_sid=%s conf=%s",
        to,
        user_call.sid,
        conference_name,
    )
    return {
        "success": True,
        "method": "direct",
        "pool_number": pool_number,
        "conference_name": conference_name,
    }


async def _can_probe_permission(
    *,
    permission: dict,
    allow_permission_probe: bool,
    pool_number: str,
    contact_number: str,
) -> bool:
    if not allow_permission_probe or not _is_local_permission_probe_enabled():
        return False
    if (permission.get("status") or "unknown") not in CALL_PERMISSION_PROBE_STATUSES:
        return False
    if not _recent_permission_request(permission):
        return False
    try:
        return await _has_pending_call_intent(pool_number, contact_number)
    except Exception:
        logger.exception("error checking pending WhatsApp call intent")
        return False


def _conference_twiml(conference_name: str) -> str:
    """TwiML for joining a participant into a named Twilio conference."""
    from twilio.twiml.voice_response import VoiceResponse

    resp = VoiceResponse()
    dial = resp.dial()
    dial.conference(
        conference_name,
        startConferenceOnEnter=True,
        endConferenceOnExit=True,
        muted=False,
        wait_url="https://auburn-eagle-6359.twil.io/assets/ring-tone-68676.mp3",
    )
    return str(resp)


@auth_router.post("/send-call")
async def send_call(request: Request):
    """Place an outbound WhatsApp call or fall back to a call invite template.

    If the contact has granted call permission, places a direct
    outbound call via a Twilio Conference bridged to LiveKit.
    Otherwise sends a VOICE_CALL_REQUEST template so the user can tap
    "Call now" to initiate an inbound call.
    """
    credentials = EnvCredentialStore()
    data = await request.json()
    to = data["to"]
    assistant_id = data["assistant_id"]
    room_name = data["room_name"]
    allow_permission_probe = bool(data.get("allow_permission_probe"))

    route = await _resolve_route(assistant_id, to)
    pool_number = route["pool_number"]

    permission = await _check_call_permission(pool_number, to)
    wa_client = build_twilio_wa_client(credentials)

    if permission["permitted"]:
        return await _place_direct_whatsapp_call(
            credentials=credentials,
            pool_number=pool_number,
            to=to,
            room_name=room_name,
            wa_client=wa_client,
        )

    permission_status = permission.get("status") or "unknown"
    if permission_status == "rejected":
        logger.info("WhatsApp call permission rejected for %s", to)
        return {"success": True, "method": "rejected", "pool_number": pool_number}

    if await _can_probe_permission(
        permission=permission,
        allow_permission_probe=allow_permission_probe,
        pool_number=pool_number,
        contact_number=to,
    ):
        try:
            direct_response = await _place_direct_whatsapp_call(
                credentials=credentials,
                pool_number=pool_number,
                to=to,
                room_name=room_name,
                wa_client=wa_client,
            )
        except Exception as exc:
            if _is_permission_probe_failure(exc):
                logger.info(
                    "WhatsApp permission probe still requires approval for %s: %s",
                    to,
                    exc,
                )
                return {
                    "success": True,
                    "method": "needs_permission",
                    "pool_number": pool_number,
                }
            logger.exception("WhatsApp permission probe failed for %s", to)
            return {
                "success": False,
                "method": "probe_failed",
                "pool_number": pool_number,
                "error": f"Failed to probe WhatsApp call permission for {to}",
            }

        accepted = await _record_call_permission_accepted(
            pool_number,
            to,
            source="local_permission_probe",
        )
        _cache_call_permission(
            pool_number=pool_number,
            contact_number=to,
            response_payload=accepted,
        )
        direct_response["permission_probe"] = True
        return direct_response

    if _recent_pending_invite(permission):
        logger.info("WhatsApp call permission invite already pending for %s", to)
        return {"success": True, "method": "invite_pending", "pool_number": pool_number}

    if permission_status == "unknown_interaction":
        logger.info(
            "WhatsApp call permission state is unknown after interaction for %s",
            to,
        )
        return {
            "success": True,
            "method": "needs_reconciliation",
            "pool_number": pool_number,
        }

    wa_client.messages.create(
        content_sid=VOICE_CALL_REQUEST_TEMPLATE_SID,
        to=f"whatsapp:{to}",
        from_=f"whatsapp:{pool_number}",
        status_callback=f"{SETTINGS.conversation.COMMS_URL}/whatsapp/status",
    )
    await _record_call_permission_pending(pool_number, to)
    logger.info("WhatsApp call permission request sent to %s", to)
    return {"success": True, "method": "invite", "pool_number": pool_number}


# ---------------------------------------------------------------------------
# Provisioning helpers (Twilio Senders v2 API)
# ---------------------------------------------------------------------------


def _whatsapp_voice_app_sid(credentials: CredentialStore) -> str:
    """SID of the TwiML Voice App that powers WhatsApp Business Calling.

    Differs by environment because staging and production each have
    their own Voice App registered. Preserves the legacy
    ``DEPLOY_ENV``-based switch bit-for-bit.
    """
    if credentials.get_optional("DEPLOY_ENV", "") == "staging":
        return "APbf0903608f1a02e93bebcc90e2ea17db"
    return "AP5e48f55135a987a482661a37db8ac68f"


WHATSAPP_GB_BUNDLE_SID = "BUd85f47e01a9d85003c364f400105a8da"
_SENDER_BASE = "https://messaging.twilio.com/v2/Channels/Senders"


async def _attach_voice_app(
    sender_sid: str,
    credentials: CredentialStore,
    timeout: float = 60.0,
) -> bool:
    """Poll until the Sender is ONLINE, then attach the TwiML Voice App.

    Returns True if the voice app was successfully attached; False on
    timeout or error. Failures are non-fatal -- the sender is still
    usable for messaging, just without WhatsApp Business Calling.
    """
    voice_app_sid = _whatsapp_voice_app_sid(credentials)
    if not voice_app_sid:
        return False

    headers = _twilio_whatsapp_auth_headers(credentials)
    sender_url = f"{_SENDER_BASE}/{sender_sid}"
    deadline = time.monotonic() + timeout

    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            try:
                resp = await client.get(sender_url, headers=headers, timeout=10.0)
                if resp.status_code < 400:
                    status = resp.json().get("status", "")
                    if status == "ONLINE":
                        break
                    logger.info(
                        "sender %s status: %s, waiting for ONLINE",
                        sender_sid,
                        status,
                    )
            except Exception:
                logger.exception("error polling sender %s status", sender_sid)
            await asyncio.sleep(5)
        else:
            logger.warning(
                "sender %s did not reach ONLINE within %ss",
                sender_sid,
                timeout,
            )
            return False

        try:
            update_resp = await client.post(
                sender_url,
                json={
                    "configuration": {
                        "voice_application_sid": voice_app_sid,
                    },
                },
                headers=headers,
                timeout=10.0,
            )
            if update_resp.status_code >= 400:
                logger.error(
                    "failed to attach voice app to %s: %s %s",
                    sender_sid,
                    update_resp.status_code,
                    update_resp.text,
                )
                return False
            logger.info(
                "attached voice app %s to sender %s",
                voice_app_sid,
                sender_sid,
            )
            return True
        except Exception:
            logger.exception("error attaching voice app to sender %s", sender_sid)
            return False


async def _provision_gb_phone_number(credentials: CredentialStore) -> str:
    """Buy a GB mobile number with voice + SMS and create a LiveKit SIP trunk.

    Returns the purchased E.164 number. Skips the local-vs-mobile
    fallback policy from the original (whatsapp specifically wants GB
    mobile capability for WhatsApp Business compatibility).
    """
    twilio_client = build_twilio_wa_client(credentials)

    numbers: list = []
    try:
        numbers += twilio_client.available_phone_numbers("GB").mobile.list(
            limit=1,
            sms_enabled=True,
            voice_enabled=True,
            beta=False,
        )
    except Exception:
        pass
    try:
        numbers += twilio_client.available_phone_numbers("GB").local.list(
            limit=1,
            sms_enabled=True,
            voice_enabled=True,
            beta=False,
        )
    except Exception:
        pass
    if not numbers:
        raise HTTPException(
            status_code=404,
            detail="No suitable GB phone numbers available",
        )

    record = numbers[0]
    incoming = twilio_client.incoming_phone_numbers.create(
        phone_number=record.phone_number,
        voice_url=SETTINGS.conversation.ADAPTERS_URL + "/twilio/call",
        voice_method="POST",
        sms_url=SETTINGS.conversation.ADAPTERS_URL + "/twilio/sms",
        sms_method="POST",
        status_callback=SETTINGS.conversation.ADAPTERS_URL + "/twilio/call-status",
        status_callback_method="POST",
        bundle_sid=WHATSAPP_GB_BUNDLE_SID,
    )

    for service in twilio_client.messaging.v1.services.list():
        if service.friendly_name == "Unity":
            service.phone_numbers.create(phone_number_sid=incoming.sid)
            break

    lkapi = LiveKitAPI(
        url=credentials.get("LIVEKIT_URL"),
        api_key=credentials.get("LIVEKIT_API_KEY"),
        api_secret=credentials.get("LIVEKIT_API_SECRET"),
    )
    trunk = SIPInboundTrunkInfo(
        name=f"Unity_WA_{record.phone_number[1:]}",
        numbers=[record.phone_number],
        krisp_enabled=True,
    )
    try:
        await lkapi.sip.create_sip_inbound_trunk(
            CreateSIPInboundTrunkRequest(trunk=trunk),
        )
    finally:
        await lkapi.aclose()

    logger.info("provisioned GB number %s for WhatsApp sender", record.phone_number)
    return record.phone_number


@auth_router.post("/create")
async def create_whatsapp_sender(request: Request):
    """Provision (optionally) and register a new WhatsApp Sender."""
    credentials = EnvCredentialStore()
    data = await request.json()
    phone_number = data.get("phone_number")

    if not phone_number:
        phone_number = await _provision_gb_phone_number(credentials)

    payload: dict = {
        "sender_id": f"whatsapp:{phone_number}",
        "profile": {
            "name": data.get("name", "Unify Assistant"),
            "logo_url": "https://console.unify.ai/icon.png",
        },
        "webhook": {
            "callback_method": "POST",
            "callback_url": data.get(
                "callback_url",
                SETTINGS.conversation.ADAPTERS_URL + "/twilio/whatsapp",
            ),
            "status_callback_url": (
                SETTINGS.conversation.COMMS_URL + "/whatsapp/status"
            ),
            "status_callback_method": "POST",
        },
    }

    headers = _twilio_whatsapp_auth_headers(credentials)
    async with httpx.AsyncClient() as client:
        resp = await client.post(_SENDER_BASE, json=payload, headers=headers)
    if resp.status_code >= 400:
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Failed to create WhatsApp sender: {resp.text}",
        )

    sid = resp.json().get("sid")
    calling_enabled = await _attach_voice_app(sid, credentials)

    return {
        "sid": sid,
        "phone_number": phone_number,
        "calling_enabled": calling_enabled,
    }


@auth_router.delete("/delete")
async def delete_whatsapp_sender(request: Request):
    """Delete a WhatsApp Sender from Twilio."""
    credentials = EnvCredentialStore()
    data = await request.json()
    sid = data["sid"]
    url = f"{_SENDER_BASE}/{sid}"
    headers = _twilio_whatsapp_auth_headers(credentials)
    async with httpx.AsyncClient() as client:
        resp = await client.delete(url, headers=headers)
    if resp.status_code >= 400:
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Failed to delete WhatsApp sender: {resp.text}",
        )
    return {"success": True}


@auth_router.post("/assign")
async def assign_whatsapp_sender(request: Request):
    """Assign a pool number to an assistant via Orchestra."""
    data = await request.json()
    assistant_id = data["assistant_id"]

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/assign",
            json={"assistant_id": assistant_id},
            headers=_admin_headers(),
            timeout=15.0,
        )
    if resp.status_code >= 400:
        try:
            detail = resp.json().get("detail", resp.text)
        except Exception:
            detail = resp.text
        raise HTTPException(status_code=resp.status_code, detail=detail)

    return resp.json()


__all__ = ["auth_router", "unauth_router"]
