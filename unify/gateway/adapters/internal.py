"""Internal adapter routes for Unity-originated runtime events."""

from __future__ import annotations

import json
import os
import uuid
from pathlib import PurePath
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, Response

from unify.gateway.adapters.common import (
    build_internal_context,
    parse_json_field,
    publish_runtime_event,
    request_payload,
    required_contact_id,
    validate_attachments,
)
from unify.gateway.common.auth import (
    require_assistant_ownership,
    require_gateway_admin,
)
from unify.gateway.context import GatewayContext, get_gateway_context
from unify.settings import SETTINGS

router = APIRouter()


def _attachments_bucket() -> str:
    """Bucket namespace shared with Orchestra for message attachments.

    Orchestra resolves its message-attachments bucket from
    ``ORCHESTRA_GCP_ASSISTANT_MESSAGE_ATTACHMENTS_BUCKET_NAME``, defaulting to
    ``assistant-message-attachments-{staging|production}`` keyed off its
    deploy environment. Mirror both the override and the environment
    resolution (Orchestra treats any URL containing "staging" as staging) so
    the ``gs://`` URIs minted here resolve in Orchestra's bucket allowlist.
    """
    configured = os.environ.get(
        "ORCHESTRA_GCP_ASSISTANT_MESSAGE_ATTACHMENTS_BUCKET_NAME",
        "",
    ).strip()
    if configured:
        return configured
    env = "staging" if "staging" in SETTINGS.ORCHESTRA_URL.lower() else "production"
    return f"assistant-message-attachments-{env}"


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


def _normalize_team_summaries(value: Any) -> list[dict[str, Any]]:
    summaries = _optional_list(value)
    for summary in summaries:
        if not isinstance(summary, dict):
            raise HTTPException(
                status_code=400,
                detail="team_summaries must be objects",
            )
    return summaries


def _normalize_int_list(value: Any) -> list[int]:
    return [int(item) for item in _optional_list(value)]


@router.post("/unify/attachment")
async def unify_attachment_upload(
    request: Request,
    file: UploadFile = File(...),
    assistant_id: str | None = Form(default=None),
    context: GatewayContext = Depends(get_gateway_context),
) -> dict[str, Any]:
    await require_assistant_ownership(request, assistant_id)
    content = await file.read()
    filename = _safe_filename(file.filename or "attachment")
    content_type = file.content_type or "application/octet-stream"
    attachment_id = str(uuid.uuid4())
    bucket = _attachments_bucket()
    object_path = f"{assistant_id or 'unknown'}/{attachment_id}_{filename}"
    key = f"{bucket}/{object_path}"
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
        "gs_url": f"gs://{bucket}/{object_path}",
        "content_type": stored.content_type,
        "size_bytes": stored.size_bytes,
    }


@router.post("/unify/message")
async def unify_message_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    require_gateway_admin(request)
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


_ensured_org_topics: set[str] = set()


def _publish_org_chat_frame(
    *,
    organization_id: Any,
    thread: str,
    event: dict[str, Any],
    attributes: dict[str, str],
) -> None:
    """Publish one Console org-chat frame to ``unity-org-{org_id}``.

    Best-effort: local installs without Pub/Sub (or without the emulator
    running) log and continue — the message is already persisted in
    Orchestra, so Console can still load it as history.
    """
    try:
        from google.cloud import pubsub_v1
    except ImportError:
        return

    import json as _json

    try:
        publisher = pubsub_v1.PublisherClient()
        topic_name = f"unity-org-{organization_id}{SETTINGS.ENV_SUFFIX}"
        topic_path = publisher.topic_path(SETTINGS.GCP_PROJECT_ID, topic_name)
        if topic_path not in _ensured_org_topics:
            try:
                publisher.create_topic(request={"name": topic_path})
            except Exception as exc:
                if "already exists" not in str(exc).lower():
                    raise
            _ensured_org_topics.add(topic_path)
        publisher.publish(
            topic_path,
            _json.dumps(
                {"thread": thread, "event": event},
            ).encode("utf-8"),
            **attributes,
        ).result(timeout=10)
    except Exception as exc:
        import logging

        logging.getLogger("unify").warning(
            "org-chat frame publish failed for org %s: %s",
            organization_id,
            exc,
        )


@router.post("/unify/org-chat")
async def unify_org_chat_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    """Self-host twin of the hosted adapters ``/unify/org-chat`` endpoint.

    Publishes the Console frame to the per-organization topic and fans out
    standard ``unify_message`` envelopes to each listed assistant runtime.
    """
    require_gateway_admin(request)
    payload = await request_payload(request)
    kind = str(payload.get("kind") or "")
    organization_id = payload.get("organization_id")
    message = payload.get("message") or {}
    if isinstance(message, str):
        message = parse_json_field(message)

    if kind not in {"team", "dm", "group"}:
        return Response(
            status_code=400,
            content="kind must be 'team', 'dm', or 'group'",
        )
    if not organization_id:
        return Response(status_code=400, content="organization_id is required")
    if not message:
        return Response(status_code=400, content="message is required")

    if kind == "team":
        thread = "team_message"
    elif kind == "group":
        thread = "group_message"
    else:
        thread = "dm_message"
    attributes = {"thread": thread, "organization_id": str(organization_id)}
    if kind == "team":
        team_id = payload.get("team_id") or message.get("team_id")
        if not team_id:
            return Response(status_code=400, content="team_id is required")
        attributes["team_id"] = str(team_id)
    elif kind == "group":
        group_id = payload.get("group_id") or message.get("group_id")
        if not group_id:
            return Response(status_code=400, content="group_id is required")
        attributes["group_id"] = str(group_id)
    else:
        user_ids = message.get("user_ids") or []
        if len(user_ids) != 2:
            return Response(
                status_code=400,
                content="message.user_ids must be a pair",
            )
        attributes["dm_user_a"] = str(user_ids[0])
        attributes["dm_user_b"] = str(user_ids[1])

    _publish_org_chat_frame(
        organization_id=organization_id,
        thread=thread,
        event=message,
        attributes=attributes,
    )

    fanout_assistant_ids = payload.get("fanout_assistant_ids") or []
    if isinstance(fanout_assistant_ids, str):
        fanout_assistant_ids = parse_json_field(fanout_assistant_ids)
    assistant_event = payload.get("assistant_event") or {}
    if isinstance(assistant_event, str):
        assistant_event = parse_json_field(assistant_event)

    # Team / org-group chat fan-out rides the standard unify_message thread —
    # every listed assistant receives a copy, like a large email CC chain.
    # When the sender is this assistant's owner we can resolve contact_id
    # here; otherwise the runtime resolves the sender by email against its
    # Contacts table. assistant_event may include team_id/team_name or
    # group_id depending on kind.
    fanout_errors: list[str] = []
    if kind in {"team", "group"}:
        for raw_assistant_id in fanout_assistant_ids:
            try:
                assistant_data, contacts = await build_internal_context(
                    context,
                    assistant_id=str(raw_assistant_id),
                    reason="unify_message",
                )
                assistant_id = str(assistant_data["assistant_id"])
                event: dict[str, Any] = {
                    **assistant_event,
                    "assistant_id": assistant_id,
                    "contacts": contacts,
                }
                sender_user_id = str(assistant_event.get("sender_user_id") or "")
                if sender_user_id and sender_user_id == str(
                    assistant_data.get("user_id") or "",
                ):
                    event["contact_id"] = required_contact_id(
                        assistant_data,
                        "boss_contact_id",
                    )
                await publish_runtime_event(
                    context,
                    assistant_id=assistant_id,
                    thread="unify_message",
                    event=event,
                )
            except Exception as exc:
                fanout_errors.append(str(raw_assistant_id))
                import logging

                logging.getLogger("unify").warning(
                    "org chat fan-out failed for assistant %s: %s",
                    raw_assistant_id,
                    exc,
                )

    return _json_response(
        {
            "published": True,
            "fanned_out": (
                len(fanout_assistant_ids) - len(fanout_errors)
                if kind in {"team", "group"}
                else 0
            ),
            "fanout_errors": fanout_errors,
        },
    )


@router.post("/unify/reaction")
async def unify_reaction_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    require_gateway_admin(request)
    payload = await request_payload(request)
    assistant_id_input = str(payload.get("assistant_id") or "")
    if not assistant_id_input:
        return Response(status_code=400, content="assistant_id is required")

    contact_id = payload.get("contact_id")
    target_message_id = payload.get("target_message_id")
    if contact_id is None or target_message_id is None:
        return Response(
            status_code=400,
            content="contact_id and target_message_id are required",
        )

    assistant_data, contacts = await build_internal_context(
        context,
        assistant_id=assistant_id_input,
        reason="unify_reaction",
    )
    assistant_id = str(assistant_data["assistant_id"])
    emoji = payload.get("emoji")
    if emoji == "":
        emoji = None
    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="unify_message_reaction",
        event={
            "contact_id": contact_id,
            "contacts": contacts,
            "assistant_id": assistant_id,
            "target_message_id": target_message_id,
            "emoji": emoji,
        },
    )
    return Response(status_code=200)


@router.post("/api/message")
async def api_message_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    require_gateway_admin(request)
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
    require_gateway_admin(request)
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
    event_data: dict[str, Any] = {
        "contacts": contacts,
        "assistant_id": assistant_id,
        "livekit_room": room_name,
        "livekit_agent_name": payload.get("livekit_agent_name") or room_name,
    }
    call_session_id = payload.get("call_session_id")
    if call_session_id:
        event_data["call_session_id"] = str(call_session_id)
    opening_config = payload.get("opening_config")
    if opening_config not in (None, ""):
        opening_config = parse_json_field(opening_config)
    if isinstance(opening_config, dict):
        event_data["opening_config"] = opening_config

    await publish_runtime_event(
        context,
        assistant_id=assistant_id,
        thread="unify_meet",
        event=event_data,
    )
    return Response(status_code=200)


@router.post("/unity/system-event")
async def unity_system_event_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    require_gateway_admin(request)
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
    require_gateway_admin(request)
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
    require_gateway_admin(request)
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
    require_gateway_admin(request)
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
        "team_ids": _normalize_int_list(assistant_data.get("team_ids") or []),
        "team_summaries": _normalize_team_summaries(
            assistant_data.get("team_summaries") or [],
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
