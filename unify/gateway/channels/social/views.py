"""FastAPI routes for the social-verification channel.

Ports ``communication/social/views.py`` into ``unify.gateway``,
applying the translation rules from
``unify/gateway/channels/README.md``:

* Twilio credentials resolved through ``EnvCredentialStore`` rather
  than ad-hoc ``os.getenv`` calls. Missing credentials fail loud at
  the boundary instead of returning a generic 500 from deep inside
  the Twilio SDK.
* Twilio client construction goes through
  ``unify.gateway.common.twilio.build_twilio_client`` /
  ``build_twilio_wa_client`` (promoted from the original
  module-local helpers when ``phone/`` became the second channel
  needing the same surface).
* No envelope-schema changes -- this channel doesn't publish to the
  per-assistant Pub/Sub topic; it's a synchronous Twilio SMS /
  WhatsApp send.

The route paths, status codes, and error semantics match the original
communication service. The request model also accepts an optional
WhatsApp sender override for callers that know which approved sender
should deliver the code.
"""

from __future__ import annotations

import json
import logging
import random
import string
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from unify.gateway.common.twilio import build_twilio_client, build_twilio_wa_client
from unify.gateway.credentials import EnvCredentialStore

logger = logging.getLogger("unify.gateway.channels.social")

router = APIRouter()

MESSAGING_SERVICE_NAME = "Unity"
DEFAULT_CODE_LENGTH = 6
DEFAULT_WHATSAPP_VERIFICATION_FROM_NUMBER = "+16626772032"


# ---------------------------------------------------------------------------
# Request schema
# ---------------------------------------------------------------------------


class VerificationRequest(BaseModel):
    platform: str = Field(
        ...,
        description="The platform to verify ('whatsapp' or 'phone').",
    )
    account_identifier: str = Field(
        ...,
        description="The user's account identifier (e.g., phone number).",
    )
    from_number: str | None = Field(
        None,
        description="Optional WhatsApp sender number for verification delivery.",
    )


# ---------------------------------------------------------------------------
# Helpers (module-local until a second channel needs them)
# ---------------------------------------------------------------------------


def _generate_verification_code(length: int = DEFAULT_CODE_LENGTH) -> str:
    """Generate a random numeric verification code."""
    return "".join(random.choices(string.digits, k=length))


def _normalize_whatsapp_number(number: str) -> str:
    return number.replace("whatsapp:", "").strip()


_messaging_service_sid: str | None = None


def _get_messaging_service_sid(credentials: EnvCredentialStore) -> str:
    """Look up the SID of the configured Twilio Messaging Service.

    A Messaging Service holds a pool of phone numbers across
    countries; Twilio picks a valid sender for each destination
    automatically. Cached after first lookup.
    """
    global _messaging_service_sid
    if _messaging_service_sid is not None:
        return _messaging_service_sid
    twilio_client = build_twilio_client(credentials)
    for service in twilio_client.messaging.v1.services.list():
        if service.friendly_name == MESSAGING_SERVICE_NAME:
            _messaging_service_sid = service.sid
            return _messaging_service_sid
    raise RuntimeError(
        f"Twilio Messaging Service '{MESSAGING_SERVICE_NAME}' not found",
    )


def _reset_messaging_service_sid_cache() -> None:
    """Test hook: clear the module-level Messaging Service SID cache."""
    global _messaging_service_sid
    _messaging_service_sid = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/available-platforms", tags=["Verification"])
async def get_social_platforms():
    """List supported verification platforms and per-account cost."""
    platforms = {"whatsapp": 10.0}
    return {"success": True, "platforms": platforms}


@router.post("/verify", tags=["Verification"])
async def send_verification_message(request: VerificationRequest):
    """Send a verification code to ``account_identifier`` via ``platform``.

    Supported platforms are ``whatsapp`` and ``phone``. On success
    returns the generated code and an ISO-8601 ``sent_at`` timestamp.
    Unknown platforms 400. Twilio failures bubble up as 500 with the
    detail preserved for ops.
    """
    credentials = EnvCredentialStore()
    platform = request.platform.lower()
    identifier = request.account_identifier
    code = _generate_verification_code()

    if platform == "whatsapp":
        try:
            twilio_client = build_twilio_wa_client(credentials)
            from_number = _normalize_whatsapp_number(
                request.from_number or DEFAULT_WHATSAPP_VERIFICATION_FROM_NUMBER,
            )
            message = twilio_client.messages.create(
                content_sid="HX66a14c4ec2f4e8a9d1d14ac2fa439a29",
                content_variables=json.dumps({"1": code}),
                to=f"whatsapp:{identifier}",
                from_=f"whatsapp:{from_number}",
            )
            # Twilio returns 201/queued here even when the message later fails to
            # deliver (closed 24h window, unapproved template/sender, etc.). Log
            # the SID + sender so a "code never arrived" report can be resolved
            # against the Twilio message logs without guessing which number sent
            # it (default verification sender vs an overriding coordinator number).
            logger.info(
                "WhatsApp verification queued: sid=%s status=%s error_code=%s "
                "from=%s to=%s",
                getattr(message, "sid", None),
                getattr(message, "status", None),
                getattr(message, "error_code", None),
                from_number,
                identifier,
            )
        except Exception as exc:
            logger.error("WhatsApp verification send failed: %s", exc)
            raise HTTPException(
                status_code=500,
                detail="Failed to send WhatsApp verification message.",
            )

    elif platform == "phone":
        message_body = f"Your Unify verification code is: {code}"
        try:
            twilio_client = build_twilio_client(credentials)
            messaging_sid = _get_messaging_service_sid(credentials)
            message = twilio_client.messages.create(
                to=identifier,
                messaging_service_sid=messaging_sid,
                body=message_body,
            )
            logger.info(
                "Phone verification queued: sid=%s status=%s error_code=%s to=%s",
                getattr(message, "sid", None),
                getattr(message, "status", None),
                getattr(message, "error_code", None),
                identifier,
            )
        except Exception as exc:
            logger.error("Phone verification send failed: %s", exc)
            raise HTTPException(
                status_code=500,
                detail="Failed to send phone verification sms.",
            )

    else:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Platform '{platform}' is not supported. "
                "Supported platforms are: 'whatsapp', 'phone'."
            ),
        )

    return {
        "verification_code": code,
        "sent_at": datetime.now(timezone.utc).isoformat(),
    }


__all__ = [
    "router",
    "VerificationRequest",
    "MESSAGING_SERVICE_NAME",
    "DEFAULT_WHATSAPP_VERIFICATION_FROM_NUMBER",
]
