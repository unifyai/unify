"""FastAPI routes for the phone channel.

Ports ``communication/phone/views.py`` into ``unity.gateway``,
applying the translation rules from
``unity/gateway/channels/README.md``:

* Twilio client construction via
  ``unity.gateway.common.twilio.build_twilio_client``.
* LiveKit SDK access via ``unity.gateway.common.livekit``.
* Credentials read through ``EnvCredentialStore`` (gateway operator
  infra credentials, distinct from ``unity.secret_manager``).
* Settings (COMMS_URL, ADAPTERS_URL) read from
  ``SETTINGS.conversation`` rather than the old
  ``common.settings.SETTINGS`` shape.
* Module-local ``print()`` debug calls replaced with structured
  logger calls.

Wire behaviour (route paths, request/response shapes, status codes,
TwiML output, Twilio call args) is preserved bit-for-bit so the
gateway aggregator can mount the two routers at ``/phone`` and
external callers (Twilio webhooks, Unity admin clients) see no
change.

The endpoint set matches the original 1:1::

  auth_router:
    POST /dispatch-livekit-agent  -- creates LiveKit room + dispatches agent
    POST /send-call               -- creates outbound Twilio call -> SIP -> LiveKit
    POST /send-text               -- sends SMS via Twilio
    GET  /available-countries     -- static list of supported countries
    POST /create                  -- purchases Twilio number + sets up webhooks + LK trunk
    DEL  /delete                  -- deletes phone + LK trunk (idempotent)
    POST /hang-up                 -- removes a participant from a Twilio conference
    POST /end-conference          -- terminates a Twilio conference

  unauth_router:
    POST /conference-status       -- Twilio webhook for conference lifecycle events
    POST /twiml                   -- TwiML response for outbound call leg
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request, Response
from livekit.api import (
    CreateSIPInboundTrunkRequest,
    SIPInboundTrunkInfo,
)
from livekit.protocol.sip import (
    DeleteSIPTrunkRequest,
    ListSIPInboundTrunkRequest,
)
from twilio.base.exceptions import TwilioRestException
from twilio.twiml.voice_response import VoiceResponse

from unity.gateway.common.livekit import (
    create_room_and_dispatch_agent,
    ensure_phone_dispatch_rule,
    get_livekit_api,
    make_sip_uri,
)
from unity.gateway.common.twilio import build_twilio_client
from unity.gateway.credentials import EnvCredentialStore
from unity.settings import SETTINGS

logger = logging.getLogger("unity.gateway.channels.phone")

auth_router = APIRouter()
unauth_router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers (module-local; promote to unity/gateway/common/ if reused)
# ---------------------------------------------------------------------------


# Twilio regulatory bundle / address overrides keyed by ISO country code.
# Country availability is tied to the bundle SIDs registered against the
# hosted Twilio account; mirrors the data in
# communication/phone/views.py:_phone_country_purchase_options.
_PHONE_COUNTRY_PURCHASE_OPTIONS: dict[str, dict[str, str]] = {
    "GB": {"bundle_sid": "BU92b4971def01df8ce390153e23645323"},
    "NL": {"address_sid": "AD742b83eb0aab7a249e7a3f2f5fb615c0"},
    "FI": {"address_sid": "AD742b83eb0aab7a249e7a3f2f5fb615c0"},
    "AU": {
        "bundle_sid": "BUd8f2d4e2fe905d85653f738d7323c88b",
        "address_sid": "AD828c09f385dea4f977464da90006bfd7",
    },
    "TH": {
        "bundle_sid": "BUadbcfca4db22f76c6840ced254c10a11",
        "address_sid": "ADdf839edff37d001d2634edc9b0c4a304",
    },
    "PL": {
        "bundle_sid": "BU0864466d980ebd9df91768d9123110b2",
        "address_sid": "ADdf839edff37d001d2634edc9b0c4a304",
    },
}


def _phone_country_purchase_options(phone_country: str) -> dict[str, str]:
    """Return Twilio regulatory bundle / address overrides for a country."""
    return dict(_PHONE_COUNTRY_PURCHASE_OPTIONS.get(phone_country, {}))


def _sip_trunk_name(phone_number: str) -> str:
    """Return the LiveKit SIP trunk name for a provisioned phone number."""
    return f"Unity_{phone_number.lstrip('+')}"


async def _delete_sip_trunk_for_phone_number(
    phone_number: str,
    credentials: EnvCredentialStore,
) -> bool:
    """Delete the matching LiveKit inbound SIP trunk if one exists.

    Returns True when a trunk was deleted, False otherwise.
    """
    livekit_api = get_livekit_api(credentials)
    try:
        sip_trunks = await livekit_api.sip.list_sip_inbound_trunk(
            ListSIPInboundTrunkRequest(),
        )
        trunk_name = _sip_trunk_name(phone_number)
        for item in sip_trunks.items:
            if item.name == trunk_name:
                await livekit_api.sip.delete_sip_trunk(
                    DeleteSIPTrunkRequest(sip_trunk_id=item.sip_trunk_id),
                )
                return True
        return False
    finally:
        await livekit_api.aclose()


def _create_conference_response(sip_uri: str) -> VoiceResponse:
    """Build the TwiML that bridges a Twilio leg into the LiveKit SIP URI."""
    resp = VoiceResponse()
    dial = resp.dial()
    dial.sip(
        sip_uri,
        status_callback=f"{SETTINGS.conversation.COMMS_URL}/phone/sip-status",
        status_callback_event="initiated ringing answered completed",
    )
    return resp


# ---------------------------------------------------------------------------
# Authenticated endpoints (admin-side; called from Unity)
# ---------------------------------------------------------------------------


@auth_router.post("/dispatch-livekit-agent")
async def dispatch_livekit_agent(request: Request):
    """Create a LiveKit room and dispatch the LiveKit agent into it."""
    credentials = EnvCredentialStore()
    data = await request.json()
    room_name = data.get("room_name") or data.get("livekit_agent_name", "")
    await create_room_and_dispatch_agent(
        room_name,
        room_name,
        credentials,
    )
    return {"success": True}


@auth_router.post("/send-call")
async def send_call(request: Request):
    """Initiate an outbound Twilio call bridged into a LiveKit room.

    Workflow:
    1. Build the SIP URI for the assistant's Twilio number.
    2. Ensure a LiveKit dispatch rule routes inbound SIP into the
       caller-supplied room.
    3. Ask Twilio to create the outbound call; Twilio fetches TwiML
       from /phone/twiml which builds the user-facing call leg.
    """
    credentials = EnvCredentialStore()
    data = await request.json()
    phone_number = data.get("To")
    twilio_number = data.get("From")
    room_name = data.get("room_name")

    sip_uri = make_sip_uri(twilio_number, credentials)
    await ensure_phone_dispatch_rule(twilio_number, room_name, credentials)

    twilio_client = build_twilio_client(credentials)
    call = twilio_client.calls.create(
        to=sip_uri,
        from_=twilio_number,
        url=(
            f"{SETTINGS.conversation.COMMS_URL}/phone/twiml"
            f"?phone_number={phone_number}"
        ),
    )
    return {"success": True, "call_sid": call.sid}


@auth_router.post("/send-text")
async def send_text(request: Request):
    """Send an SMS via Twilio."""
    credentials = EnvCredentialStore()
    data = await request.json()
    to = data.get("To")
    sender = data.get("From")
    body = data.get("Body")

    twilio_client = build_twilio_client(credentials)
    twilio_client.messages.create(to=to, from_=sender, body=body)
    return {"success": True}


@auth_router.get("/available-countries")
async def available_countries():
    """Static list of countries supported for phone number provisioning."""
    return {"success": True, "countries": "US,GB,AU,CA,FI,NL,PR,TH,PL"}


@auth_router.post("/create")
async def create_phone_number(request: Request):
    """Provision a new Twilio phone number and wire it up to LiveKit.

    Steps:
    1. Look up available numbers in the requested country.
    2. Purchase the number with the configured webhook URLs.
    3. Add it to the "Unity" Twilio Messaging Service.
    4. Create a matching LiveKit inbound SIP trunk.
    """
    credentials = EnvCredentialStore()
    data = await request.json()

    voice_url = data.get(
        "voice_url",
        SETTINGS.conversation.ADAPTERS_URL + "/twilio/call",
    )
    sms_url = data.get(
        "sms_url",
        SETTINGS.conversation.ADAPTERS_URL + "/twilio/sms",
    )
    status_callback = data.get(
        "status_callback",
        SETTINGS.conversation.ADAPTERS_URL + "/twilio/call-status",
    )
    phone_country = data.get("phone_country", "US")
    additional_args = _phone_country_purchase_options(phone_country)

    twilio_client = build_twilio_client(credentials)

    # Search for available mobile / local numbers. Twilio raises for
    # countries where the requested capability isn't available; we
    # try both then fall back to a 404 if neither yielded a number.
    numbers = []
    try:
        numbers += twilio_client.available_phone_numbers(phone_country).local.list(
            limit=1,
            sms_enabled=True,
            voice_enabled=True,
            beta=False,
        )
    except Exception as exc:
        logger.debug("local number search failed for %s: %s", phone_country, exc)
    try:
        numbers += twilio_client.available_phone_numbers(phone_country).mobile.list(
            limit=1,
            sms_enabled=True,
            voice_enabled=True,
            beta=False,
        )
    except Exception as exc:
        logger.debug("mobile number search failed for %s: %s", phone_country, exc)

    if not numbers:
        raise HTTPException(status_code=404, detail="No suitable phone numbers found.")
    record = numbers[0]

    incoming = twilio_client.incoming_phone_numbers.create(
        phone_number=record.phone_number,
        voice_url=voice_url,
        voice_method="POST",
        sms_url=sms_url,
        sms_method="POST",
        status_callback=status_callback,
        status_callback_method="POST",
        **additional_args,
    )

    services = twilio_client.messaging.v1.services.list()
    for service in services:
        if service.friendly_name == "Unity":
            service.phone_numbers.create(phone_number_sid=incoming.sid)
            break

    lkapi = get_livekit_api(credentials)
    try:
        trunk_name = _sip_trunk_name(record.phone_number)
        sip_trunk = SIPInboundTrunkInfo(
            name=trunk_name,
            numbers=[record.phone_number],
            krisp_enabled=True,
        )
        await lkapi.sip.create_sip_inbound_trunk(
            CreateSIPInboundTrunkRequest(trunk=sip_trunk),
        )
    finally:
        await lkapi.aclose()
    return {"success": True, "phoneNumber": incoming.phone_number}


@auth_router.delete("/delete")
async def delete_phone_number(request: Request):
    """Delete a provisioned phone number, treating already-missing as success.

    Cleans up the matching LiveKit SIP trunk even if the Twilio
    number was already deleted in a prior attempt, so retries
    converge on a consistent state.
    """
    credentials = EnvCredentialStore()
    data = await request.json()
    phone_number = data.get("PhoneNumber")
    if not phone_number:
        raise HTTPException(status_code=400, detail="Missing PhoneNumber")

    twilio_client = build_twilio_client(credentials)
    incoming_list = twilio_client.incoming_phone_numbers.list(
        phone_number=phone_number,
        limit=1,
    )
    phone_deleted = False
    phone_sid = incoming_list[0].sid if incoming_list else None

    if incoming_list:
        try:
            twilio_client.incoming_phone_numbers(phone_sid).delete()
            phone_deleted = True
        except TwilioRestException as exc:
            if exc.status != 404:
                raise

    sip_trunk_deleted = await _delete_sip_trunk_for_phone_number(
        phone_number,
        credentials,
    )

    return {
        "success": True,
        "sid": phone_sid,
        "deleted": phone_deleted or sip_trunk_deleted,
        "already_absent": not phone_deleted,
        "sip_trunk_deleted": sip_trunk_deleted,
    }


@auth_router.post("/hang-up")
async def hang_up(request: Request):
    """Remove a participant from an active Twilio conference."""
    credentials = EnvCredentialStore()
    data = await request.json()
    call_sid = data.get("CallSid")
    conference_name = data.get("ConferenceName")

    twilio_client = build_twilio_client(credentials)
    conferences = twilio_client.conferences.list(
        friendly_name=conference_name,
        status="in-progress",
    )
    twilio_client.conferences(conferences[0].sid).participants(call_sid).delete()
    return Response(status_code=200)


@auth_router.post("/end-conference")
async def end_conference(request: Request):
    """Terminate an active Twilio conference."""
    credentials = EnvCredentialStore()
    data = await request.json()
    conference_name = data.get("ConferenceName")

    twilio_client = build_twilio_client(credentials)
    conferences = twilio_client.conferences.list(
        friendly_name=conference_name,
        status="in-progress",
    )
    conference = twilio_client.conferences(conferences[0].sid).update(
        status="completed",
    )
    return {"success": True, "status": conference.status}


# ---------------------------------------------------------------------------
# Unauthenticated endpoints (Twilio webhooks)
# ---------------------------------------------------------------------------


@unauth_router.post("/conference-status")
async def conference_status(request: Request):
    """Twilio conference lifecycle webhook.

    On conference ``end``, unmutes every remaining participant so the
    LiveKit agent (which was muted earlier during a three-way bridge)
    can be heard for the trailing leg.
    """
    credentials = EnvCredentialStore()
    data = await request.form()
    event = data.get("StatusCallbackEvent")
    conference_sid = data.get("ConferenceSid")

    twilio_client = build_twilio_client(credentials)
    if event == "end":
        participants = twilio_client.conferences(conference_sid).participants.list()
        for participant in participants:
            twilio_client.conferences(conference_sid).participants(
                participant.sid,
            ).update(muted=False)
    return Response(status_code=200)


@unauth_router.post("/twiml")
async def twiml(request: Request):
    """TwiML response for the outbound call leg.

    Twilio fetches this URL while building the call dispatched by
    /send-call. The TwiML dials the recipient's number with status
    callbacks pointed at the adapters service so call lifecycle
    events feed back through the inbound webhook path.
    """
    data = await request.form()
    twilio_number = data.get("From")
    raw_phone_number = request.query_params.get("phone_number")
    if raw_phone_number is None:
        raise HTTPException(status_code=400, detail="Missing phone_number")
    phone_number = "+" + raw_phone_number.replace(" ", "")
    call_status_url = SETTINGS.conversation.ADAPTERS_URL + "/twilio/call-status"

    resp = VoiceResponse()
    dial = resp.dial(caller_id=twilio_number, timeout=15)
    dial.number(
        phone_number,
        status_callback_event="initiated ringing answered completed",
        status_callback=call_status_url,
    )
    return Response(status_code=200, content=str(resp), media_type="text/xml")


__all__ = ["auth_router", "unauth_router"]
