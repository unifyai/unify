"""Internal adapter routes for Unity-originated runtime events."""

from __future__ import annotations

import json
import uuid
from pathlib import PurePath
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, Response

from unity.gateway.adapters.common import (
    build_internal_context,
    parse_json_field,
    publish_runtime_event,
    request_payload,
    required_contact_id,
    validate_attachments,
)
from unity.gateway.context import GatewayContext, get_gateway_context

router = APIRouter()


def _safe_filename(filename: str) -> str:
    return PurePath(filename.replace("\\", "/")).name or "attachment"


def _json_response(payload: dict[str, Any], status_code: int = 200) -> JSONResponse:
    return JSONResponse(content=payload, status_code=status_code)


def _optional_list(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    parsed = parse_json_field(value)
    if not isinstance(parsed, list):
        raise HTTPException(status_code=400, detail="Expected a list")
    return parsed


def _normalize_space_summaries(value: Any) -> list[dict[str, Any]]:
    summaries = _optional_list(value)
    for summary in summaries:
        if not isinstance(summary, dict):
            raise HTTPException(
                status_code=400,
                detail="space_summaries must be objects",
            )
    return summaries


def _normalize_int_list(value: Any) -> list[int]:
    return [int(item) for item in _optional_list(value)]


@router.post("/unify/attachment")
async def unify_attachment_upload(
    file: UploadFile = File(...),
    assistant_id: str | None = Form(default=None),
    context: GatewayContext = Depends(get_gateway_context),
) -> dict[str, Any]:
    content = await file.read()
    filename = _safe_filename(file.filename or "attachment")
    content_type = file.content_type or "application/octet-stream"
    attachment_id = str(uuid.uuid4())
    key_prefix = assistant_id or "unknown"
    key = f"attachments/{key_prefix}/{attachment_id}_{filename}"
    stored = await context.storage.write_bytes(
        key,
        content,
        content_type=content_type,
    )
    url = await context.storage.signed_url(key)
    return {
        "id": attachment_id,
        "filename": filename,
        "url": url,
        "signed_url": url,
        "storage_key": stored.key,
        "gs_url": stored.key,
        "content_type": stored.content_type,
        "size_bytes": stored.size_bytes,
    }


@router.post("/unify/message")
async def unify_message_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    payload = await request_payload(request)
    assistant_id_input = str(payload.get("assistant_id") or "")
    if not assistant_id_input:
        return Response(status_code=400, content="assistant_id is required")

    contact_id = payload.get("contact_id")
    if contact_id is None:
        return Response(status_code=400, content="contact_id is required")

    raw_attachments = payload.get("attachments") or []
    if isinstance(raw_attachments, str):
        raw_attachments = parse_json_field(raw_attachments)
    assistant_data, contacts = await build_internal_context(
        context,
        assistant_id=assistant_id_input,
        reason="unify_message",
    )
    assistant_id = str(assistant_data["assistant_id"])
    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="unify_message",
        event={
            "contact_id": contact_id,
            "contacts": contacts,
            "assistant_id": assistant_id,
            "body": payload.get("body") or payload.get("Body") or "",
            "attachments": validate_attachments(raw_attachments),
        },
    )
    return Response(status_code=200)


@router.post("/api/message")
async def api_message_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    payload = await request_payload(request)
    assistant_id_input = str(payload.get("assistant_id") or "")
    api_message_id = str(payload.get("api_message_id") or "")
    if not assistant_id_input:
        return Response(status_code=400, content="assistant_id is required")
    if not api_message_id:
        return Response(status_code=400, content="api_message_id is required")

    raw_attachments = payload.get("attachments") or []
    raw_tags = payload.get("tags") or []
    if isinstance(raw_attachments, str):
        raw_attachments = parse_json_field(raw_attachments)
    if isinstance(raw_tags, str):
        raw_tags = parse_json_field(raw_tags)

    assistant_data, _contacts = await build_internal_context(
        context,
        assistant_id=assistant_id_input,
        reason="api_message",
    )
    assistant_id = str(assistant_data["assistant_id"])
    event_data: dict[str, Any] = {
        "api_message_id": api_message_id,
        "body": payload.get("body") or "",
        "contact_id": required_contact_id(assistant_data, "boss_contact_id"),
        "assistant_id": assistant_id,
    }
    attachments = validate_attachments(raw_attachments)
    if attachments:
        event_data["attachments"] = attachments
    if raw_tags:
        event_data["tags"] = raw_tags

    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="api_message",
        event=event_data,
    )
    return Response(status_code=200)


@router.post("/unify/meet")
async def unify_meet_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    payload = await request_payload(request)
    room_name = str(payload.get("room_name") or "")
    if not room_name:
        return Response(status_code=400, content="room_name is required")
    assistant_id_input = str(payload.get("assistant_id") or "")
    if not assistant_id_input:
        return Response(status_code=400, content="assistant_id is required")

    assistant_data, contacts = await build_internal_context(
        context,
        assistant_id=assistant_id_input,
        reason="unify_meet",
    )
    assistant_id = str(assistant_data["assistant_id"])
    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="unify_meet",
        event={
            "contacts": contacts,
            "assistant_id": assistant_id,
            "livekit_room": room_name,
            "livekit_agent_name": payload.get("livekit_agent_name") or room_name,
        },
    )
    return Response(status_code=200)


@router.post("/unity/system-event")
async def unity_system_event_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    payload = await request_payload(request)
    assistant_id_input = str(payload.get("assistant_id") or "")
    event_type = str(payload.get("event_type") or "")
    message = str(payload.get("message") or "")
    if not assistant_id_input:
        return Response(status_code=400, content="assistant_id is required")
    if not event_type:
        return Response(status_code=400, content="event_type is required")
    if not message:
        return Response(status_code=400, content="message is required")

    extra_event_fields_raw = payload.get("extra_event_fields")
    extra_event_fields = None
    if extra_event_fields_raw not in (None, ""):
        extra_event_fields = parse_json_field(extra_event_fields_raw)
        if not isinstance(extra_event_fields, dict):
            extra_event_fields = None

    assistant_data, contacts = await build_internal_context(
        context,
        assistant_id=assistant_id_input,
        reason=f"unity_system_event:{event_type}",
    )
    assistant_id = str(assistant_data["assistant_id"])
    event_payload = {
        "contacts": contacts,
        "assistant_id": assistant_id,
        "event_type": event_type,
        "message": message,
    }
    if extra_event_fields:
        event_payload.update(extra_event_fields)

    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="unity_system_event",
        event=event_payload,
    )
    return Response(status_code=200)


@router.post("/unity/pre-hire")
async def unity_pre_hire_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    payload = await request_payload(request)
    assistant_id_input = str(payload.get("assistant_id") or "")
    if not assistant_id_input:
        return Response(status_code=400, content="assistant_id is required")

    raw_body = payload.get("body")
    if raw_body is None:
        raw_body = payload.get("Body", "") or ""
    try:
        body = parse_json_field(raw_body)
    except json.JSONDecodeError:
        body = None
    if not isinstance(body, list) or not all(
        isinstance(item, dict)
        and isinstance(item.get("role"), str)
        and isinstance(item.get("msg"), str)
        for item in body
    ):
        return _json_response(
            {"error": "body must be a list of {role, msg}"},
            status_code=400,
        )

    assistant_data, contacts = await build_internal_context(
        context,
        assistant_id=assistant_id_input,
        reason="log_pre_hire_chats",
    )
    assistant_id = str(assistant_data["assistant_id"])
    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="log_pre_hire_chats",
        event={"contacts": contacts, "assistant_id": assistant_id, "body": body},
    )
    return Response(status_code=200)


@router.post("/assistant/wakeup")
async def assistant_wakeup_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    payload = await request_payload(request)
    assistant_id = str(payload.get("assistant_id") or "")
    if not assistant_id:
        return Response(status_code=400, content="assistant_id is required")
    await build_internal_context(
        context,
        assistant_id=assistant_id,
        reason="wakeup",
    )
    return Response(status_code=200)


@router.post("/assistant/update")
async def assistant_update_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    payload = await request_payload(request)
    assistant_id_input = str(payload.get("assistant_id") or "")
    update_kind = str(payload.get("update_kind") or "general")
    if not assistant_id_input:
        return Response(status_code=400, content="assistant_id is required")
    if update_kind not in {"general", "membership"}:
        raise HTTPException(
            status_code=400,
            detail="update_kind must be 'general' or 'membership'",
        )

    assistant_data, _contacts = await build_internal_context(
        context,
        assistant_id=assistant_id_input,
        reason=f"assistant_update:{update_kind}",
        ensure_runtime=update_kind != "membership",
    )
    assistant_id = str(assistant_data["assistant_id"])
    assistant_event = {
        **assistant_data,
        "space_ids": _normalize_int_list(assistant_data.get("space_ids") or []),
        "space_summaries": _normalize_space_summaries(
            assistant_data.get("space_summaries") or [],
        ),
        "update_kind": update_kind,
    }
    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="assistant_update",
        event=assistant_event,
    )
    return _json_response(
        {
            "success": True,
            "message": "Assistant update published successfully",
            "assistant_id": assistant_id,
        },
    )


__all__ = ["router"]
