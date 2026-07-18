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


_ensured_frame_topics: set[str] = set()


def _publish_console_frame(
    *,
    topic_name: str,
    thread: str,
    event: dict[str, Any],
    attributes: dict[str, str],
) -> None:
    """Publish one Console frame to a Pub/Sub topic.

    Best-effort: local installs without Pub/Sub (or without the emulator
    running) log and continue — the message is already persisted in
    Orchestra's unified chat store, so Console can still load it as history.
    """
    try:
        from google.cloud import pubsub_v1
    except ImportError:
        return

    import json as _json

    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(SETTINGS.GCP_PROJECT_ID, topic_name)
        if topic_path not in _ensured_frame_topics:
            try:
                publisher.create_topic(request={"name": topic_path})
            except Exception as exc:
                if "already exists" not in str(exc).lower():
                    raise
            _ensured_frame_topics.add(topic_path)
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
            "Console frame publish failed for topic %s: %s",
            topic_name,
            exc,
        )


def _chat_frame_attributes(
    *,
    thread: str,
    payload: dict[str, Any],
    message: dict[str, Any],
) -> dict[str, str]:
    attributes = {"thread": thread}
    for key in ("organization_id", "thread_id", "team_id", "group_id", "assistant_id"):
        value = payload.get(key) or message.get(key)
        if value is not None:
            attributes[key] = str(value)
    kind = str(payload.get("kind") or message.get("kind") or "")
    if kind:
        attributes["kind"] = kind
    user_ids = message.get("user_ids") or []
    if len(user_ids) == 2:
        attributes["dm_user_a"] = str(user_ids[0])
        attributes["dm_user_b"] = str(user_ids[1])
    return attributes


@router.post("/unify/chat")
async def unify_chat_webhook(
    request: Request,
    context: GatewayContext = Depends(get_gateway_context),
) -> Response:
    """Self-host twin of the hosted adapters ``/unify/chat`` endpoint.

    Orchestra has already persisted the message in the unified chat store;
    this endpoint owns hosted delivery:

    1. Publish the Console frame — org-scoped threads (dm / team / group) go
       to the per-organization topic (``unity-org-{org_id}``); assistant DMs
       go to the per-assistant topic Console's 1-on-1 stream subscribes to.
    2. Fan out one standard ``unify_message`` envelope per listed assistant
       runtime (ensuring each job is started) — chat is ordinary
       unify_message traffic, like a large email CC chain. The author is
       never listed, which prevents AI reply loops.
    3. ``kind="reaction"`` publishes a ``chat_reaction`` frame plus
       ``unify_message_reaction`` envelopes to the listed assistants.
    4. ``kind="call"`` publishes one Console-only ``call_*`` signaling frame,
       routed by call scope: org scopes (dm/team/group) go to the org topic;
       ``assistant_dm`` goes to the assistant topic.
    """
    require_gateway_admin(request)
    payload = await request_payload(request)
    kind = str(payload.get("kind") or "")
    organization_id = payload.get("organization_id")
    message = payload.get("message") or {}
    if isinstance(message, str):
        message = parse_json_field(message)

    if kind == "call":
        action = payload.get("action")
        call = payload.get("call") or {}
        if isinstance(call, str):
            call = parse_json_field(call)
        allowed_actions = {
            "incoming",
            "answered",
            "ended",
            "declined",
            "participant_joined",
            "participant_left",
        }
        if action not in allowed_actions:
            return Response(status_code=400, content="invalid call action")
        if not call.get("call_id") or not call.get("room_name"):
            return Response(
                status_code=400,
                content="call.call_id and call.room_name required",
            )
        scope = str(call.get("scope") or "")
        participants = [str(uid) for uid in (call.get("user_ids") or []) if uid]
        thread = f"call_{action}"
        attributes = {
            "thread": thread,
            "call_id": str(call["call_id"]),
            "user_ids": ",".join(participants),
        }
        if organization_id:
            attributes["organization_id"] = str(organization_id)
        if len(participants) >= 1:
            attributes["dm_user_a"] = participants[0]
        if len(participants) >= 2:
            attributes["dm_user_b"] = participants[1]
        if call.get("team_id") is not None:
            attributes["team_id"] = str(call["team_id"])
        if call.get("group_id") is not None:
            attributes["group_id"] = str(call["group_id"])

        if scope == "assistant_dm":
            # 1:1 assistant calls signal on the assistant topic — the same
            # per-assistant stream the owner's Console chat panel follows.
            assistant_ids = [str(a) for a in (call.get("assistant_ids") or []) if a]
            if not assistant_ids:
                return Response(
                    status_code=400,
                    content="assistant_dm call frames require assistant_ids",
                )
            for frame_assistant_id in assistant_ids:
                _publish_console_frame(
                    topic_name=f"unity-{frame_assistant_id}{SETTINGS.ENV_SUFFIX}",
                    thread=thread,
                    event=call,
                    attributes={**attributes, "assistant_id": frame_assistant_id},
                )
            return _json_response({"published": True, "fanned_out": 0})

        if not organization_id:
            return Response(status_code=400, content="organization_id is required")
        _publish_console_frame(
            topic_name=f"unity-org-{organization_id}{SETTINGS.ENV_SUFFIX}",
            thread=thread,
            event=call,
            attributes=attributes,
        )
        return _json_response({"published": True, "fanned_out": 0})

    if kind not in {"assistant_dm", "dm", "team", "group", "reaction"}:
        return Response(
            status_code=400,
            content=(
                "kind must be 'assistant_dm', 'dm', 'team', 'group', "
                "'reaction', or 'call'"
            ),
        )
    if not message:
        return Response(status_code=400, content="message is required")

    frame_thread = "chat_reaction" if kind == "reaction" else "chat_message"
    thread_kind = str(payload.get("thread_kind") or kind)
    attributes = _chat_frame_attributes(
        thread=frame_thread,
        payload=payload,
        message=message,
    )
    if thread_kind == "assistant_dm":
        frame_assistant_id = payload.get("assistant_id") or message.get("assistant_id")
        if not frame_assistant_id:
            return Response(status_code=400, content="assistant_id is required")
        _publish_console_frame(
            topic_name=f"unity-{frame_assistant_id}{SETTINGS.ENV_SUFFIX}",
            thread=frame_thread,
            event=message,
            attributes=attributes,
        )
    else:
        if not organization_id:
            return Response(status_code=400, content="organization_id is required")
        _publish_console_frame(
            topic_name=f"unity-org-{organization_id}{SETTINGS.ENV_SUFFIX}",
            thread=frame_thread,
            event=message,
            attributes=attributes,
        )

    fanout_assistant_ids = payload.get("fanout_assistant_ids") or []
    if isinstance(fanout_assistant_ids, str):
        fanout_assistant_ids = parse_json_field(fanout_assistant_ids)
    assistant_event = payload.get("assistant_event") or {}
    if isinstance(assistant_event, str):
        assistant_event = parse_json_field(assistant_event)

    # Chat fan-out rides the standard unify_message thread — every listed
    # assistant receives a copy, like a large email CC chain. When the
    # sender is this assistant's owner we can resolve contact_id here;
    # otherwise the runtime resolves the sender by email against its
    # Contacts table. Reactions fan out on unify_message_reaction so the
    # runtime can patch its own Transcripts mirror.
    fanout_errors: list[str] = []
    for raw_assistant_id in fanout_assistant_ids:
        try:
            assistant_data, contacts = await build_internal_context(
                context,
                assistant_id=str(raw_assistant_id),
                reason="unify_message",
            )
            assistant_id = str(assistant_data["assistant_id"])
            if kind == "reaction":
                reactor_user_id = str(payload.get("reactor_user_id") or "")
                event: dict[str, Any] = {
                    "contacts": contacts,
                    "assistant_id": assistant_id,
                    "chat_message_id": message.get("id"),
                    "thread_id": payload.get("thread_id") or message.get("thread_id"),
                    "emoji": payload.get("emoji"),
                }
                if reactor_user_id and reactor_user_id == str(
                    assistant_data.get("user_id") or "",
                ):
                    event["contact_id"] = required_contact_id(
                        assistant_data,
                        "boss_contact_id",
                    )
                await publish_runtime_event(
                    context,
                    assistant_id=assistant_id,
                    thread="unify_message_reaction",
                    event=event,
                )
            else:
                event = {
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
                "chat fan-out failed for assistant %s: %s",
                raw_assistant_id,
                exc,
            )

    return _json_response(
        {
            "published": True,
            "fanned_out": len(fanout_assistant_ids) - len(fanout_errors),
            "fanout_errors": fanout_errors,
        },
    )


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
    # Every meet is a call session with a roster; Orchestra owns dispatch.
    call_session_id = str(payload.get("call_session_id") or "").strip()
    if not call_session_id:
        return Response(status_code=400, content="call_session_id is required")
    raw_participants = payload.get("participants")
    if isinstance(raw_participants, str):
        raw_participants = parse_json_field(raw_participants)
    participants = [
        {
            "kind": raw.get("kind") or "human",
            "user_id": raw.get("user_id"),
            "assistant_id": raw.get("assistant_id"),
            "display_name": raw.get("display_name") or "",
            "contact_id": raw.get("contact_id"),
            "email": raw.get("email"),
        }
        for raw in (raw_participants if isinstance(raw_participants, list) else [])
        if isinstance(raw, dict)
    ]
    if not participants:
        return Response(status_code=400, content="participants roster is required")
    event_data: dict[str, Any] = {
        "contacts": contacts,
        "assistant_id": assistant_id,
        "livekit_room": room_name,
        "call_session_id": call_session_id,
        "participants": participants,
    }
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
