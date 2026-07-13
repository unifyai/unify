"""Thin client for Orchestra's typed assistant Tasks API."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel
from unisdk import BASE_URL
from unisdk.utils import http

from unify.session_details import SESSION_DETAILS

_TYPED_CREATE_KEYS = frozenset(
    {
        "name",
        "description",
        "trigger",
        "schedule",
        "status",
        "enabled",
        "offline",
        "priority",
        "entrypoint",
    },
)
_TYPED_PATCH_KEYS = _TYPED_CREATE_KEYS


def format_task_etag(task_revision: int) -> str:
    """Return the opaque ETag for one task revision."""

    return f'"{task_revision}"'


def _request_headers() -> dict[str, str]:
    return {
        "accept": "application/json",
        "Authorization": f"Bearer {SESSION_DETAILS.unify_key}",
        "Content-Type": "application/json",
    }


def _assistant_id() -> int:
    agent_id = SESSION_DETAILS.assistant.agent_id
    if agent_id is None:
        raise ValueError(
            "assistant agent_id is required for typed Tasks API calls",
        )
    return int(agent_id)


def _json_ready(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


def typed_create_payload(entries: dict[str, Any]) -> dict[str, Any]:
    """Keep only fields accepted by the typed Tasks create endpoint."""

    return {
        key: _json_ready(value)
        for key, value in entries.items()
        if key in _TYPED_CREATE_KEYS and value is not None
    }


def typed_patch_payload(entries: dict[str, Any]) -> dict[str, Any]:
    """Keep only fields accepted by the typed Tasks patch endpoint."""

    return {
        key: _json_ready(value)
        for key, value in entries.items()
        if key in _TYPED_PATCH_KEYS
    }


def create_task(*, payload: dict[str, Any]) -> dict[str, Any]:
    """Create one authored task row via the typed Tasks API."""

    response = http.post(
        f"{BASE_URL}/assistants/{_assistant_id()}/tasks",
        headers=_request_headers(),
        json=typed_create_payload(payload),
    )
    return response.json()["info"]


def patch_task(
    *,
    task_id: int,
    expected_task_revision: int,
    updates: dict[str, Any],
) -> dict[str, Any]:
    """Apply one authored patch under revision CAS."""

    response = http.patch(
        f"{BASE_URL}/assistants/{_assistant_id()}/tasks/{task_id}",
        headers={
            **_request_headers(),
            "If-Match": format_task_etag(expected_task_revision),
        },
        json=typed_patch_payload(updates),
    )
    return response.json()["info"]


def delete_task(*, task_id: int, expected_task_revision: int) -> None:
    """Delete one authored task row under revision CAS."""

    http.delete(
        f"{BASE_URL}/assistants/{_assistant_id()}/tasks/{task_id}",
        headers={
            **_request_headers(),
            "If-Match": format_task_etag(expected_task_revision),
        },
    )
