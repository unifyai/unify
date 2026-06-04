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

from unity.gateway.adapters.common import (
    default_contacts,
    get_assistant,
    publish_runtime_event,
)
from unity.gateway.common.livekit import ensure_phone_dispatch_rule, make_sip_uri
from unity.gateway.common.twilio import build_twilio_client, build_twilio_wa_client
from unity.gateway.context import GatewayContext, get_gateway_context
from unity.gateway.credentials import CredentialNotFoundError
from unity.settings import SETTINGS

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


def _admin_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()}",
    }


def _room_name(assistant_id: str, channel: str) -> str:
    suffix = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    safe_assistant_id = assistant_id.replace(":", "_").replace("/", "_")
    return f"{safe_assistant_id}_{channel}_{suffix}"


def _conference_response(conference_name: str) -> VoiceResponse:
    resp = VoiceResponse()
    dial = resp.dial()
    dial.conference(
        conference_name,
        startConferenceOnEnter=True,
        endConferenceOnExit=True,
        muted=False,
        wait_url="https://auburn-eagle-6359.twil.io/assets/ring-tone-68676.mp3",
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


async def _forward_whatsapp_call_permission(
    *,
    pool_number: str,
    sender: str,
    button_payload: str,
) -> None:
    status = "accepted" if button_payload == "ACCEPTED" else "rejected"
    async with httpx.AsyncClient(timeout=10.0) as client:
        await client.post(
            f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/call-permission",
            headers=_admin_headers(),
            json={
                "pool_number": pool_number,
                "contact_number": sender,
                "status": status,
            },
        )


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
    try:
        assistant, contacts = await _assistant_for_phone(
            phone_number=to_number,
            context=context,
            reason="twilio_call",
        )
    except HTTPException:
        return _inactive_voice_response()

    assistant_id = str(assistant["assistant_id"])
    date_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    conference_name = f"Unity_{to_number.lstrip('+')}_{date_time}"
    room_name = _room_name(assistant_id, "phone")
    sip_uri = make_sip_uri(to_number, context.credentials)
    await ensure_phone_dispatch_rule(to_number, room_name, context.credentials)
    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="call",
        event={
            "contacts": contacts,
            "conference_name": conference_name,
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
            },
        },
    )

    resp_user = _conference_response(conference_name)
    twilio_client = build_twilio_client(context.credentials)
    twilio_client.calls.create(
        to=sip_uri,
        from_=to_number,
        twiml=str(_conference_response(conference_name)),
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
        await _forward_whatsapp_call_permission(
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
                    "body": body,
                    "role": route.get("role", "contact"),
                    "type": "call_permission_response",
                    "payload": button_payload,
                },
            )
        return Response(content=str(MessagingResponse()), media_type="text/xml")

    route = await resolve_whatsapp_route(pool_number, sender)
    action = route.get("action") if route else None
    if route is None or action == "auto_reply":
        return _inactive_message_response()
    if action == "reject_cold":
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
    if attachments:
        event_data["attachments"] = attachments
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
    pool_number = to_raw.replace("whatsapp:", "").strip()
    caller_number = from_raw.replace("whatsapp:", "").strip()
    route = await resolve_whatsapp_route(pool_number, caller_number)
    if route is None or route.get("action") in {"auto_reply", "reject_cold"}:
        return _inactive_voice_response()

    assistant, contacts = await _assistant_for_whatsapp_route(
        route=route,
        context=context,
        reason="twilio_whatsapp_call",
    )
    assistant_id = str(assistant["assistant_id"])
    date_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    conference_name = f"Unity_WA_{pool_number.lstrip('+')}_{date_time}"
    room_name = _room_name(assistant_id, "whatsapp_call")
    sip_uri = make_sip_uri(pool_number, context.credentials)
    await ensure_phone_dispatch_rule(pool_number, room_name, context.credentials)
    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="whatsapp_call",
        event={
            "contacts": contacts,
            "conference_name": conference_name,
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
            },
        },
    )
    wa_client = build_twilio_wa_client(context.credentials)
    wa_client.calls.create(
        to=sip_uri,
        from_=pool_number,
        twiml=str(_conference_response(conference_name)),
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
    thread = WHATSAPP_CALL_STATUS_THREADS.get(call_status)
    if thread is None:
        return Response(status_code=200)

    from_raw = str(form_data.get("From") or "")
    to_raw = str(form_data.get("To") or "")
    pool_number = from_raw.replace("whatsapp:", "").strip()
    user_number = to_raw.replace("whatsapp:", "").strip()
    route = await resolve_whatsapp_route(pool_number, user_number)
    if not route or "assistant_id" not in route:
        return Response(status_code=200)

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
            "timestamp": int(time.time() * 1000),
        },
    )
    return Response(status_code=200)


__all__ = ["router"]
