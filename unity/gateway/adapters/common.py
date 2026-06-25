"""Shared helpers for gateway adapter routes."""

from __future__ import annotations

import json
from typing import Any

import httpx
from fastapi import HTTPException, Request

from unity.gateway.context import GatewayContext
from unity.settings import SETTINGS

NO_DESKTOP_MODE = "none"
COORDINATOR_DEFAULT_DESKTOP_MODE = "ubuntu"


async def request_payload(request: Request) -> dict[str, Any]:
    """Read JSON or form payloads into a plain dictionary."""

    content_type = request.headers.get("Content-Type", "")
    if "application/json" in content_type:
        payload = await request.json()
        if isinstance(payload, dict):
            return payload
        raise HTTPException(status_code=400, detail="Request body must be an object")
    form_data = await request.form()
    return dict(form_data)


def parse_json_field(value: Any) -> Any:
    """Parse a JSON-encoded field when callers submit form data."""

    if isinstance(value, str):
        return json.loads(value)
    return value


def validate_attachments(raw_attachments: Any) -> list[dict[str, Any]]:
    """Normalize attachment metadata accepted by runtime-facing adapters."""

    if not isinstance(raw_attachments, list):
        return []
    validated: list[dict[str, Any]] = []
    for attachment in raw_attachments:
        if (
            isinstance(attachment, dict)
            and attachment.get("id")
            and attachment.get("filename")
            and (attachment.get("url") or attachment.get("gs_url"))
        ):
            item = {
                "id": str(attachment["id"]),
                "filename": str(attachment["filename"]),
                "url": str(attachment.get("url", "")),
            }
            if attachment.get("gs_url"):
                item["gs_url"] = str(attachment["gs_url"])
            if attachment.get("content_type"):
                item["content_type"] = str(attachment["content_type"])
            if attachment.get("size_bytes") is not None:
                item["size_bytes"] = int(attachment["size_bytes"])
            validated.append(item)
    return validated


def _resolve_desktop_mode(assistant: dict[str, Any]) -> str:
    desktop_mode = assistant.get("desktop_mode")
    if desktop_mode:
        return str(desktop_mode)
    if assistant.get("is_coordinator", False):
        return COORDINATOR_DEFAULT_DESKTOP_MODE
    return NO_DESKTOP_MODE


def _local_assistant_data(assistant_id: str | None = None) -> dict[str, Any]:
    return {
        "assistant_id": assistant_id or "local-assistant",
        "deploy_env": None,
        "user_id": "local-user",
        "voice_provider": "cartesia",
        "voice_id": None,
        "api_key": "",
        "user_first_name": "",
        "user_surname": "",
        "assistant_first_name": "Local",
        "assistant_surname": "Assistant",
        "assistant_age": "20",
        "assistant_nationality": "United States",
        "assistant_about": "Local Assistant",
        "assistant_job_title": "",
        "assistant_timezone": "UTC",
        "assistant_email": "unity.agent@unify.ai",
        "user_email": "unity.agent@unify.ai",
        "user_number": "",
        "assistant_number": "",
        "user_whatsapp_number": "",
        "assistant_whatsapp_number": "",
        "assistant_discord_bot_id": "",
        "desktop_mode": COORDINATOR_DEFAULT_DESKTOP_MODE,
        "user_desktops": [],
        "is_local": True,
        "team_ids": [],
        "team_summaries": [],
        "self_contact_id": 0,
        "boss_contact_id": 1,
        "is_coordinator": False,
    }


def _assistant_payload(assistant: dict[str, Any]) -> dict[str, Any]:
    return {
        "assistant_id": assistant["agent_id"],
        "deploy_env": assistant.get("deploy_env"),
        "user_id": assistant["user_id"],
        "api_key": assistant["api_key"],
        "user_first_name": assistant["user_first_name"] or "",
        "user_surname": assistant["user_last_name"] or "",
        "assistant_first_name": assistant["first_name"] or "",
        "assistant_surname": assistant["surname"] or "",
        "assistant_age": str(assistant.get("age") or ""),
        "assistant_nationality": assistant["nationality"] or "",
        "assistant_about": assistant["about"] or "",
        "assistant_job_title": assistant.get("job_title") or "",
        "assistant_timezone": assistant.get("timezone", "UTC"),
        "assistant_number": assistant["phone"] or "",
        "assistant_whatsapp_number": assistant.get("assistant_whatsapp_number") or "",
        "assistant_discord_bot_id": assistant.get("assistant_discord_bot_id", ""),
        "assistant_email": assistant["email"] or "",
        "assistant_email_provider": assistant.get("email_provider")
        or "google_workspace",
        "user_number": assistant["user_phone"] or "",
        "user_whatsapp_number": assistant.get("user_whatsapp_number") or "",
        "user_email": assistant["user_email"] or "",
        "voice_provider": assistant["voice_provider"] or "",
        "voice_id": assistant["voice_id"] or "",
        "secrets": assistant.get("secrets", {}),
        "desktop_mode": _resolve_desktop_mode(assistant),
        "user_desktops": assistant.get("user_desktops", []),
        "demo_id": assistant.get("demo_id"),
        "is_local": assistant.get("is_local", False),
        "team_ids": assistant.get("team_ids", []),
        "team_summaries": assistant.get("team_summaries", []),
        "self_contact_id": assistant.get("self_contact_id", 0),
        "boss_contact_id": assistant.get("boss_contact_id", 1),
        "is_coordinator": assistant.get("is_coordinator", False),
        "org_id": assistant.get("organization_id"),
    }


async def get_assistant(
    *,
    assistant_id: str | None = None,
    email_address: str | None = None,
    phone_number: str | None = None,
) -> dict[str, Any]:
    """Return assistant routing metadata from Orchestra."""

    email_check = email_address or ""
    phone_check = phone_number or ""
    if "+17343611691" in phone_check or assistant_id == "local-assistant":
        return _local_assistant_data("local-assistant")
    if (
        "+0123456789" in phone_check
        or "local-test-assistant@unify.ai" in email_check
        or assistant_id == "local-test-assistant"
    ):
        return {
            **_local_assistant_data("local-test-assistant"),
            "user_first_name": "Test",
            "user_surname": "User",
            "user_number": "+9876543210",
            "user_email": "test@unify.ai",
            "assistant_first_name": "Test",
            "assistant_surname": "Assistant",
            "assistant_number": "+0123456789",
            "assistant_email": "local-test-assistant@unify.ai",
            "user_whatsapp_number": "+9876543210",
        }

    params: dict[str, str] = {}
    if email_address:
        params["email"] = email_address
    if phone_number:
        params["phone"] = phone_number
    if assistant_id:
        params["agent_id"] = assistant_id

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/admin/assistant",
            params=params,
            headers={
                "Authorization": (
                    f"Bearer {SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()}"
                ),
            },
        )
    response.raise_for_status()
    body = response.json()
    assistants = body.get("info") or []
    if not assistants:
        return {**_local_assistant_data(assistant_id), "assistant_id": None}
    return _assistant_payload(assistants[0])


def required_contact_id(assistant_data: dict[str, Any], field_name: str) -> int:
    """Return a resolved contact id required by runtime-facing adapters."""

    value = assistant_data.get(field_name)
    if value is None:
        assistant_id = assistant_data.get("assistant_id") or assistant_data.get(
            "agent_id",
        )
        raise ValueError(f"Assistant {assistant_id} is missing required {field_name}")
    return int(value)


def default_contacts(assistant_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the assistant and owner contacts embedded in assistant metadata."""

    self_contact_id = required_contact_id(assistant_data, "self_contact_id")
    boss_contact_id = required_contact_id(assistant_data, "boss_contact_id")
    return [
        {
            "contact_id": self_contact_id,
            "first_name": assistant_data["assistant_first_name"],
            "surname": assistant_data["assistant_surname"],
            "email_address": assistant_data["assistant_email"],
            "phone_number": assistant_data["assistant_number"],
            "whatsapp_number": assistant_data.get("assistant_whatsapp_number", ""),
            "discord_id": assistant_data.get("assistant_discord_bot_id", ""),
            "slack_user_id": assistant_data.get("assistant_slack_bot_user_id", ""),
            "bio": "",
            "rolling_summary": "",
            "should_respond": False,
            "response_policy": "",
        },
        {
            "contact_id": boss_contact_id,
            "first_name": assistant_data["user_first_name"],
            "surname": assistant_data["user_surname"],
            "email_address": assistant_data["user_email"],
            "phone_number": assistant_data["user_number"],
            "whatsapp_number": assistant_data.get("user_whatsapp_number", ""),
            "discord_id": assistant_data.get("user_discord_id", ""),
            "slack_user_id": assistant_data.get("user_slack_user_id", ""),
            "bio": "",
            "rolling_summary": "",
            "should_respond": True,
            "response_policy": "",
        },
    ]


async def build_internal_context(
    context: GatewayContext,
    *,
    assistant_id: str,
    reason: str,
    ensure_runtime: bool = True,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Resolve assistant metadata and prepare runtime delivery."""

    assistant_data = await get_assistant(assistant_id=assistant_id)
    resolved_assistant_id = assistant_data.get("assistant_id")
    if not resolved_assistant_id:
        raise HTTPException(status_code=404, detail="Assistant not found")
    if ensure_runtime:
        await context.runtime_activator.activate(
            str(resolved_assistant_id),
            reason=reason,
            medium="internal",
            metadata={
                "requested_assistant_id": assistant_id,
                "assistant": assistant_data,
            },
        )
    return assistant_data, default_contacts(assistant_data)


async def publish_runtime_event(
    context: GatewayContext,
    *,
    assistant_id: str,
    thread: str,
    event: dict[str, Any],
) -> str:
    """Publish a runtime-facing event through the configured sink."""

    return await context.envelope_sink.publish(
        assistant_id,
        {"thread": thread, "event": event},
        thread="inbound",
    )


__all__ = [
    "build_internal_context",
    "default_contacts",
    "get_assistant",
    "parse_json_field",
    "publish_runtime_event",
    "request_payload",
    "required_contact_id",
    "validate_attachments",
]
