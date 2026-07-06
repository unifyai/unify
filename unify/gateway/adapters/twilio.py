"""Twilio webhook adapter routes."""

from __future__ import annotations

import time
import uuid
from datetime import datetime
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response
from twilio.request_validator import RequestValidator
from twilio.twiml.messaging_response import MessagingResponse
from twilio.twiml.voice_response import VoiceResponse

from unify.gateway.adapters.common import (
    default_contacts,
    get_assistant,
    publish_runtime_event,
)
from unify.gateway.common.livekit import (
    delete_sip_dispatch_rule,
    ensure_call_scoped_dispatch_rule,
    ensure_phone_dispatch_rule,
    make_call_scoped_sip_uri,
    make_sip_uri,
)
from unify.gateway.common.twilio import build_twilio_client, build_twilio_wa_client
from unify.gateway.context import GatewayContext, get_gateway_context
from unify.gateway.credentials import CredentialNotFoundError
from unify.settings import SETTINGS

router = APIRouter()

CALL_STATUS_THREADS = {
    "in-progress": "call_answered",
    "no-answer": "call_not_answered",
    "busy": "call_not_answered",
    "canceled": "call_not_answered",
    "failed": "call_not_answered",
}
WHATSAPP_CALL_STATUS_THREADS = {
    "in-progress": "whatsapp_call_answered",
    "no-answer": "whatsapp_call_not_answered",
    "busy": "whatsapp_call_not_answered",
    "canceled": "whatsapp_call_not_answered",
    "failed": "whatsapp_call_not_answered",
}
_LOCAL_WHATSAPP_CALL_SESSIONS: dict[str, dict[str, Any]] = {}


def _admin_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()}",
    }


def _whatsapp_call_permission_status(button_payload: str) -> tuple[str, str]:
    payload = (button_payload or "").strip()
    if payload == "ACCEPTED":
        return "accepted", "ACCEPTED"
    if payload == "REJECTED":
        return "rejected", "REJECTED"
    return "unknown_interaction", "UNKNOWN"


def _room_name(assistant_id: str, channel: str) -> str:
    suffix = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    safe_assistant_id = assistant_id.replace(":", "_").replace("/", "_")
    return f"{safe_assistant_id}_{channel}_{suffix}"


def _conference_response(
    conference_name: str,
    *,
    ringback: bool = True,
) -> VoiceResponse:
    """TwiML joining a participant to a named conference.

    The first participant into a Twilio conference hears the ``wait_url``
    audio until a second joins. ``ringback`` stays True only for an inbound
    caller's leg (a human waiting for us to answer); SIP/agent legs wait in
    silence so ring audio never plays into the LiveKit room.

    ``beep`` defaults to true on Twilio, playing a join tone into the
    conference the moment a participant enters — heard by the callee right as
    they pick up (an artificial "call answered" sound) and by the agent's STT.
    Disabled on every leg.
    """
    from unify.gateway.common.callbacks import CONFERENCE_WAIT_URL

    resp = VoiceResponse()
    dial = resp.dial()
    dial.conference(
        conference_name,
        startConferenceOnEnter=True,
        endConferenceOnExit=True,
        muted=False,
        beep=False,
        wait_url=CONFERENCE_WAIT_URL if ringback else "",
    )
    return resp


def _inactive_voice_response() -> Response:
    resp = VoiceResponse()
    resp.say(
        "This number is no longer active. Please visit "
        "console.unify.ai to view your assistant details.",
    )
    resp.hangup()
    return Response(content=str(resp), media_type="text/xml")


def _inactive_message_response(message: str | None = None) -> Response:
    resp = MessagingResponse()
    resp.message(
        message
        or "This number is no longer active. Please visit "
        "console.unify.ai to view your assistant details.",
    )
    return Response(content=str(resp), media_type="text/xml")


async def _validate_twilio_request(request: Request, token_name: str) -> None:
    context = get_gateway_context(request)
    token = context.credentials.get_optional(token_name, "")
    if not token:
        return
    form = await request.form()
    signature = request.headers.get("X-Twilio-Signature", "")
    validator = RequestValidator(token)
    if not validator.validate(str(request.url), dict(form), signature):
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")


async def validate_sms_twilio_signature(request: Request) -> None:
    await _validate_twilio_request(request, "TWILIO_AUTH_TOKEN")


async def validate_whatsapp_twilio_signature(request: Request) -> None:
    await _validate_twilio_request(request, "TWILIO_WA_AUTH_TOKEN")


async def _assistant_for_phone(
    *,
    phone_number: str,
    context: GatewayContext,
    reason: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    assistant = await get_assistant(phone_number=phone_number)
    assistant_id = assistant.get("assistant_id")
    if not assistant_id:
        raise HTTPException(status_code=404, detail="Assistant not found")
    await context.runtime_activator.activate(
        str(assistant_id),
        reason=reason,
        medium="twilio",
        metadata={"phone_number": phone_number, "assistant": assistant},
    )
    return assistant, default_contacts(assistant)


async def resolve_whatsapp_route(
    pool_number: str,
    sender: str,
) -> dict[str, Any] | None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/resolve",
            params={"pool_number": pool_number, "sender": sender},
            headers=_admin_headers(),
        )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


async def resolve_phone_route(
    pool_number: str,
    sender: str,
) -> dict[str, Any] | None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/admin/phone/resolve",
            params={"pool_number": pool_number, "sender": sender},
            headers=_admin_headers(),
        )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


async def upsert_whatsapp_call_session(payload: dict[str, Any]) -> dict[str, Any]:
    _LOCAL_WHATSAPP_CALL_SESSIONS[payload["provider_call_sid"]] = payload
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/call-session",
                headers=_admin_headers(),
                json=payload,
            )
        response.raise_for_status()
        data = response.json()
        _LOCAL_WHATSAPP_CALL_SESSIONS[payload["provider_call_sid"]] = data
        return data
    except httpx.TransportError:
        return payload


async def get_whatsapp_call_session(provider_call_sid: str) -> dict[str, Any] | None:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/call-session/{provider_call_sid}",
                headers=_admin_headers(),
                params={"provider": "twilio"},
            )
        if response.status_code == 404:
            return _LOCAL_WHATSAPP_CALL_SESSIONS.get(provider_call_sid)
        response.raise_for_status()
        return response.json()
    except httpx.TransportError:
        return _LOCAL_WHATSAPP_CALL_SESSIONS.get(provider_call_sid)


async def update_whatsapp_call_session(
    payload: dict[str, Any],
) -> dict[str, Any] | None:
    provider_call_sid = payload["provider_call_sid"]
    existing = _LOCAL_WHATSAPP_CALL_SESSIONS.get(provider_call_sid)
    if existing:
        metadata = dict(existing.get("metadata") or {})
        metadata.update(payload.get("metadata") or {})
        existing.update({k: v for k, v in payload.items() if v is not None})
        existing["metadata"] = metadata
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.patch(
                f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/call-session",
                headers=_admin_headers(),
                json=payload,
            )
        if response.status_code == 404:
            return existing
        response.raise_for_status()
        data = response.json()
        _LOCAL_WHATSAPP_CALL_SESSIONS[provider_call_sid] = data
        return data
    except httpx.TransportError:
        return existing


async def _assistant_for_whatsapp_route(
    *,
    route: dict[str, Any],
    context: GatewayContext,
    reason: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    assistant_id = str(route["assistant_id"])
    assistant = await get_assistant(assistant_id=assistant_id)
    resolved_assistant_id = str(assistant["assistant_id"])
    await context.runtime_activator.activate(
        resolved_assistant_id,
        reason=reason,
        medium="whatsapp",
        metadata={"route": route, "assistant": assistant},
    )
    return assistant, default_contacts(assistant)


async def _assistant_for_phone_route(
    *,
    route: dict[str, Any],
    context: GatewayContext,
    reason: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    assistant_id = str(route["assistant_id"])
    assistant = await get_assistant(assistant_id=assistant_id)
    resolved_assistant_id = str(assistant["assistant_id"])
    await context.runtime_activator.activate(
        resolved_assistant_id,
        reason=reason,
        medium="phone",
        metadata={"route": route, "assistant": assistant},
    )
    return assistant, default_contacts(assistant)


async def _forward_whatsapp_call_permission(
    *,
    pool_number: str,
    sender: str,
    button_payload: str,
) -> str:
    status, event_payload = _whatsapp_call_permission_status(button_payload)
    async with httpx.AsyncClient(timeout=10.0) as client:
        await client.post(
            f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/call-permission",
            headers=_admin_headers(),
            json={
                "pool_number": pool_number,
                "contact_number": sender,
                "status": status,
                "source": "twilio_webhook",
            },
        )
    return event_payload


async def _whatsapp_attachments(
    form_data: Any,
    *,
    assistant_id: str,
    message_sid: str | None,
    context: GatewayContext,
) -> list[dict[str, Any]]:
    num_media = int(form_data.get("NumMedia", "0") or "0")
    if num_media == 0:
        return []
    try:
        account_sid = context.credentials.get("TWILIO_WA_ACCOUNT_SID")
        auth_token = context.credentials.get("TWILIO_WA_AUTH_TOKEN")
    except CredentialNotFoundError:
        return []

    attachments: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        for index in range(num_media):
            media_url = form_data.get(f"MediaUrl{index}")
            if not media_url:
                continue
            content_type = form_data.get(
                f"MediaContentType{index}",
                "application/octet-stream",
            )
            response = await client.get(media_url, auth=(account_sid, auth_token))
            response.raise_for_status()
            attachment_id = str(uuid.uuid4())
            key = f"attachments/{assistant_id}/whatsapp/{message_sid or attachment_id}_{index}"
            stored = await context.storage.write_bytes(
                key,
                response.content,
                content_type=content_type,
            )
            attachments.append(
                {
                    "id": attachment_id,
                    "filename": key.rsplit("/", 1)[-1],
                    "url": await context.storage.signed_url(key),
                    "storage_key": stored.key,
                    "content_type": stored.content_type,
                    "size_bytes": stored.size_bytes,
                },
            )
    return attachments


@router.post("/twilio/sms", dependencies=[Depends(validate_sms_twilio_signature)])
async def twilio_sms_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    form_data = await request.form()
    to_number = str(form_data.get("To") or "")
    from_number = str(form_data.get("From") or "")
    body = str(form_data.get("Body") or "")
    route = await resolve_phone_route(to_number, from_number)
    action = route.get("action") if route else None
    if route is not None and action == "auto_reply":
        return _inactive_message_response()
    if action in {"reject_cold", "reject_ambiguous"}:
        return _inactive_message_response("This number is not accepting new messages.")
    if route and "assistant_id" in route:
        assistant, contacts = await _assistant_for_phone_route(
            route=route,
            context=context,
            reason="twilio_sms",
        )
    else:
        try:
            assistant, contacts = await _assistant_for_phone(
                phone_number=to_number,
                context=context,
                reason="twilio_sms",
            )
        except HTTPException:
            return _inactive_message_response()

    await publish_runtime_event(
        context,
        assistant_id=str(assistant["assistant_id"]),
        thread="msg",
        event={
            "contacts": contacts,
            "to_number": to_number,
            "from_number": from_number,
            "body": body,
            "role": route.get("role", "contact") if route else "contact",
        },
    )
    return Response(content=str(MessagingResponse()), media_type="text/xml")


@router.post("/twilio/call", dependencies=[Depends(validate_sms_twilio_signature)])
async def twilio_call_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    form_data = await request.form()
    to_number = str(form_data.get("To") or "")
    from_number = str(form_data.get("From") or "")
    provider_call_sid = str(form_data.get("CallSid") or f"missing-{uuid.uuid4()}")
    route = await resolve_phone_route(to_number, from_number)
    if route is not None and route.get("action") in {
        "auto_reply",
        "reject_cold",
        "reject_ambiguous",
    }:
        return _inactive_voice_response()
    if route and "assistant_id" in route:
        assistant, contacts = await _assistant_for_phone_route(
            route=route,
            context=context,
            reason="twilio_call",
        )
    else:
        try:
            assistant, contacts = await _assistant_for_phone(
                phone_number=to_number,
                context=context,
                reason="twilio_call",
            )
        except HTTPException:
            return _inactive_voice_response()

    assistant_id = str(assistant["assistant_id"])
    if route and "assistant_id" in route:
        call_id = provider_call_sid.replace(":", "-")
        conference_name = f"unity_phone_conf_{call_id}"
        room_name = f"unity_phone_room_{assistant_id}_{call_id}"
        sip_uri, sip_target = make_call_scoped_sip_uri(
            to_number,
            call_id,
            context.credentials,
            headers={
                "X-Unity-Call-Session": call_id,
                "X-Unity-Provider-Call-Sid": provider_call_sid,
                "X-Unity-Room": room_name,
            },
        )
        sip_dispatch_rule_id = await ensure_call_scoped_dispatch_rule(
            base_phone_number=to_number,
            sip_target=sip_target,
            room_name=room_name,
            call_id=call_id,
            assistant_id=assistant_id,
            credentials=context.credentials,
        )
        if not sip_dispatch_rule_id:
            resp = VoiceResponse()
            resp.say(
                "This number cannot accept calls right now. Please try again later.",
            )
            resp.hangup()
            return Response(content=str(resp), media_type="text/xml")
    else:
        date_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        conference_name = f"Unity_{to_number.lstrip('+')}_{date_time}"
        room_name = _room_name(assistant_id, "phone")
        sip_uri = make_sip_uri(to_number, context.credentials)
        sip_dispatch_rule_id = None
        await ensure_phone_dispatch_rule(to_number, room_name, context.credentials)
    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="call",
        event={
            "contacts": contacts,
            "conference_name": conference_name,
            "call_session_id": provider_call_sid if route else "",
            "provider_call_sid": provider_call_sid if route else "",
            "caller_number": from_number,
            "sip_uri": sip_uri,
            "livekit_room": room_name,
            "assistant_id": assistant_id,
            "action": "start_worker",
            "timestamp": int(time.time() * 1000),
            "call_metadata": {
                "twilio_number": to_number,
                "call_type": "inbound",
                "room_created": True,
                "bridge_established": True,
                "sip_dispatch_rule_id": sip_dispatch_rule_id,
            },
        },
    )

    resp_user = _conference_response(conference_name)
    twilio_client = build_twilio_client(context.credentials)
    twilio_client.calls.create(
        to=sip_uri,
        from_=to_number,
        twiml=str(_conference_response(conference_name, ringback=False)),
    )
    return Response(content=str(resp_user), media_type="text/xml")


@router.post(
    "/twilio/call-status",
    dependencies=[Depends(validate_sms_twilio_signature)],
)
async def twilio_call_status_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    form_data = await request.form()
    call_status = str(form_data.get("CallStatus") or "")
    thread = CALL_STATUS_THREADS.get(call_status)
    if thread is None:
        return Response(status_code=200)

    assistant_number = str(form_data.get("From") or "")
    user_number = str(form_data.get("To") or "")
    try:
        assistant, contacts = await _assistant_for_phone(
            phone_number=assistant_number,
            context=context,
            reason=f"twilio_call_status:{call_status}",
        )
    except HTTPException:
        return Response(status_code=200)

    await publish_runtime_event(
        context,
        assistant_id=str(assistant["assistant_id"]),
        thread=thread,
        event={
            "contacts": contacts,
            "assistant_id": assistant["assistant_id"],
            "user_number": user_number,
            "assistant_number": assistant_number,
            "call_status": call_status,
            "timestamp": int(time.time() * 1000),
        },
    )
    return Response(status_code=200)


@router.post(
    "/twilio/whatsapp",
    dependencies=[Depends(validate_whatsapp_twilio_signature)],
)
async def twilio_whatsapp_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    form_data = await request.form()
    to_number = str(form_data.get("To") or "")
    from_number = str(form_data.get("From") or "")
    body = str(form_data.get("Body") or "")
    message_sid = form_data.get("MessageSid")
    pool_number = to_number.replace("whatsapp:", "").strip()
    sender = from_number.replace("whatsapp:", "").strip()

    if body == "VOICE_CALL_REQUEST":
        button_payload = str(form_data.get("ButtonPayload") or "")
        event_payload = await _forward_whatsapp_call_permission(
            pool_number=pool_number,
            sender=sender,
            button_payload=button_payload,
        )
        route = await resolve_whatsapp_route(pool_number, sender)
        if route and "assistant_id" in route:
            assistant, contacts = await _assistant_for_whatsapp_route(
                route=route,
                context=context,
                reason="whatsapp_call_permission",
            )
            await publish_runtime_event(
                context,
                assistant_id=str(assistant["assistant_id"]),
                thread="whatsapp",
                event={
                    "contacts": contacts,
                    "to_number": to_number,
                    "from_number": from_number,
                    "contact_number": sender,
                    "body": body,
                    "role": route.get("role", "contact"),
                    "type": "call_permission_response",
                    "payload": event_payload,
                },
            )
        return Response(content=str(MessagingResponse()), media_type="text/xml")

    route = await resolve_whatsapp_route(pool_number, sender)
    action = route.get("action") if route else None
    if route is None or action == "auto_reply":
        return _inactive_message_response()
    if action in {"reject_cold", "reject_ambiguous"}:
        return _inactive_message_response("This number is not accepting new messages.")

    assistant, contacts = await _assistant_for_whatsapp_route(
        route=route,
        context=context,
        reason="twilio_whatsapp",
    )
    assistant_id = str(assistant["assistant_id"])
    attachments = await _whatsapp_attachments(
        form_data,
        assistant_id=assistant_id,
        message_sid=str(message_sid) if message_sid else None,
        context=context,
    )
    event_data: dict[str, Any] = {
        "contacts": contacts,
        "to_number": to_number,
        "from_number": from_number,
        "body": body,
        "role": route["role"],
    }
    if message_sid:
        event_data["message_sid"] = str(message_sid)
    if attachments:
        event_data["attachments"] = attachments

    reaction_type = str(
        form_data.get("MessageType") or form_data.get("type") or "",
    ).lower()
    reaction_emoji = form_data.get("Reaction") or form_data.get("reaction_emoji")
    reacted_to_sid = (
        form_data.get("OriginalRepliedMessageSid")
        or form_data.get("reaction_message_id")
        or form_data.get("RepliedMessageSid")
    )
    if reaction_type == "reaction" or reacted_to_sid:
        reaction_event = {
            "contacts": contacts,
            "to_number": to_number,
            "from_number": from_number,
            "provider_message_sid": str(reacted_to_sid or message_sid or ""),
            "message_sid": str(reacted_to_sid or message_sid or ""),
            "emoji": str(reaction_emoji) if reaction_emoji else None,
        }
        await publish_runtime_event(
            context,
            assistant_id=assistant_id,
            thread="whatsapp_reaction",
            event=reaction_event,
        )
        return Response(content=str(MessagingResponse()), media_type="text/xml")

    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="whatsapp",
        event=event_data,
    )
    return Response(content=str(MessagingResponse()), media_type="text/xml")


@router.post(
    "/twilio/whatsapp-call",
    dependencies=[Depends(validate_whatsapp_twilio_signature)],
)
async def twilio_whatsapp_call_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    form_data = await request.form()
    to_raw = str(form_data.get("To") or "")
    from_raw = str(form_data.get("From") or "")
    provider_call_sid = str(form_data.get("CallSid") or f"missing-{uuid.uuid4()}")
    pool_number = to_raw.replace("whatsapp:", "").strip()
    caller_number = from_raw.replace("whatsapp:", "").strip()
    route = await resolve_whatsapp_route(pool_number, caller_number)
    if route is None or route.get("action") in {
        "auto_reply",
        "reject_cold",
        "reject_ambiguous",
    }:
        return _inactive_voice_response()

    assistant, contacts = await _assistant_for_whatsapp_route(
        route=route,
        context=context,
        reason="twilio_whatsapp_call",
    )
    assistant_id = str(assistant["assistant_id"])
    call_id = provider_call_sid.replace(":", "-")
    conference_name = f"unity_wa_conf_{call_id}"
    room_name = f"unity_wa_room_{assistant_id}_{call_id}"
    sip_uri, sip_target = make_call_scoped_sip_uri(
        pool_number,
        call_id,
        context.credentials,
        headers={
            "X-Unity-Call-Session": call_id,
            "X-Unity-Provider-Call-Sid": provider_call_sid,
            "X-Unity-Room": room_name,
        },
    )
    sip_dispatch_rule_id = await ensure_call_scoped_dispatch_rule(
        base_phone_number=pool_number,
        sip_target=sip_target,
        room_name=room_name,
        call_id=call_id,
        assistant_id=assistant_id,
        credentials=context.credentials,
    )
    if not sip_dispatch_rule_id:
        resp = VoiceResponse()
        resp.say("This number cannot accept calls right now. Please try again later.")
        resp.hangup()
        return Response(content=str(resp), media_type="text/xml")

    await upsert_whatsapp_call_session(
        {
            "provider": "twilio",
            "provider_call_sid": provider_call_sid,
            "channel": "whatsapp_call",
            "assistant_id": int(assistant_id),
            "from_number": caller_number,
            "to_number": pool_number,
            "pool_number": pool_number,
            "conference_name": conference_name,
            "livekit_room": room_name,
            "status": "created",
            "metadata": {
                "sip_uri": sip_uri,
                "sip_target": sip_target,
                "sip_dispatch_rule_id": sip_dispatch_rule_id,
            },
        },
    )
    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="whatsapp_call",
        event={
            "contacts": contacts,
            "conference_name": conference_name,
            "call_session_id": provider_call_sid,
            "provider_call_sid": provider_call_sid,
            "caller_number": caller_number,
            "sip_uri": sip_uri,
            "livekit_room": room_name,
            "assistant_id": assistant_id,
            "action": "start_worker",
            "timestamp": int(time.time() * 1000),
            "call_metadata": {
                "whatsapp_number": pool_number,
                "call_type": "inbound",
                "room_created": True,
                "bridge_established": True,
                "sip_dispatch_rule_id": sip_dispatch_rule_id,
            },
        },
    )
    wa_client = build_twilio_wa_client(context.credentials)
    wa_client.calls.create(
        to=sip_uri,
        from_=pool_number,
        twiml=str(_conference_response(conference_name, ringback=False)),
    )
    return Response(
        content=str(_conference_response(conference_name)),
        media_type="text/xml",
    )


@router.post(
    "/twilio/whatsapp-call-status",
    dependencies=[Depends(validate_whatsapp_twilio_signature)],
)
async def twilio_whatsapp_call_status_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    form_data = await request.form()
    call_status = str(form_data.get("CallStatus") or "")
    provider_call_sid = str(form_data.get("CallSid") or "")
    if call_status == "completed":
        thread = None
    else:
        thread = WHATSAPP_CALL_STATUS_THREADS.get(call_status)
    if thread is None and call_status != "completed":
        return Response(status_code=200)

    if not provider_call_sid:
        return Response(status_code=200)

    call_session = await get_whatsapp_call_session(provider_call_sid)
    if not call_session:
        return Response(status_code=200)

    await update_whatsapp_call_session(
        {
            "provider": "twilio",
            "provider_call_sid": provider_call_sid,
            "status": call_status,
        },
    )
    metadata = call_session.get("metadata") or {}
    if call_status in {"no-answer", "busy", "canceled", "failed", "completed"}:
        await delete_sip_dispatch_rule(
            metadata.get("sip_dispatch_rule_id"),
            context.credentials,
        )
    if call_status == "completed":
        return Response(status_code=200)

    pool_number = call_session["to_number"]
    user_number = call_session["from_number"]
    route = {"assistant_id": call_session["assistant_id"]}
    assistant, contacts = await _assistant_for_whatsapp_route(
        route=route,
        context=context,
        reason=f"twilio_whatsapp_call_status:{call_status}",
    )
    await publish_runtime_event(
        context,
        assistant_id=str(assistant["assistant_id"]),
        thread=thread,
        event={
            "contacts": contacts,
            "assistant_id": assistant["assistant_id"],
            "user_number": user_number,
            "assistant_number": pool_number,
            "call_status": call_status,
            "call_session_id": provider_call_sid,
            "provider_call_sid": provider_call_sid,
            "conference_name": call_session["conference_name"],
            "livekit_room": call_session["livekit_room"],
            "timestamp": int(time.time() * 1000),
        },
    )
    return Response(status_code=200)


__all__ = ["router"]
