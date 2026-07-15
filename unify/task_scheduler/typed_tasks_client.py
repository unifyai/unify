"""Thin client for Orchestra's typed assistant Tasks API."""

from __future__ import annotations

import json
from enum import Enum
from typing import Any

from pydantic import BaseModel
from unisdk import BASE_URL
from unisdk.utils import http
from unisdk.utils.http import RequestError

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


class TaskRevisionConflictError(Exception):
    """Raised when an authored mutation loses the revision compare-and-swap."""

    def __init__(self, *, latest_task_revision: int) -> None:
        self.latest_task_revision = latest_task_revision
        super().__init__("task_revision_conflict")


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


def _parse_conflict_response(response: Any) -> TaskRevisionConflictError | None:
    if getattr(response, "status_code", None) != 409:
        return None
    try:
        body = response.json()
    except (AttributeError, json.JSONDecodeError, TypeError, ValueError):
        return None
    detail = body.get("detail", body) if isinstance(body, dict) else body
    if isinstance(detail, dict) and detail.get("code") == "task_revision_conflict":
        return TaskRevisionConflictError(
            latest_task_revision=int(detail["task_revision"]),
        )
    return None


def _raise_typed_tasks_error(exc: RequestError) -> None:
    conflict = _parse_conflict_response(exc.response)
    if conflict is not None:
        raise conflict from exc
    status_code = getattr(exc.response, "status_code", None)
    if status_code == 404:
        raise ValueError("Task not found.") from exc
    if status_code == 400:
        raise ValueError("The request could not be applied.") from exc
    if status_code == 428:
        raise ValueError(
            "task_revision is required; re-read the task before mutating.",
        ) from exc
    raise ValueError("Typed Tasks API request failed.") from exc


def _request(method: str, path: str, **kwargs: Any) -> Any:
    url = f"{BASE_URL}{path}"
    headers = {**_request_headers(), **kwargs.pop("headers", {})}
    try:
        response = getattr(http, method)(url, headers=headers, **kwargs)
    except RequestError as exc:
        _raise_typed_tasks_error(exc)
    return response


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


def _info(response: Any) -> dict[str, Any]:
    return response.json()["info"]


def list_tasks() -> list[dict[str, Any]]:
    """List authored task rows for the current assistant."""

    response = _request("get", f"/assistants/{_assistant_id()}/tasks")
    return list(_info(response).get("tasks") or [])


def get_task(*, task_id: int) -> dict[str, Any]:
    """Read one authored task row."""

    response = _request(
        "get",
        f"/assistants/{_assistant_id()}/tasks/{task_id}",
    )
    return _info(response)


def create_task(*, payload: dict[str, Any]) -> dict[str, Any]:
    """Create one authored task row via the typed Tasks API."""

    response = _request(
        "post",
        f"/assistants/{_assistant_id()}/tasks",
        json=typed_create_payload(payload),
    )
    return _info(response)


def patch_task(
    *,
    task_id: int,
    expected_task_revision: int,
    updates: dict[str, Any],
) -> dict[str, Any]:
    """Apply one authored patch under revision CAS."""

    response = _request(
        "patch",
        f"/assistants/{_assistant_id()}/tasks/{task_id}",
        headers={"If-Match": format_task_etag(expected_task_revision)},
        json=typed_patch_payload(updates),
    )
    return _info(response)


def delete_task(*, task_id: int, expected_task_revision: int) -> None:
    """Delete one authored task row under revision CAS."""

    _request(
        "delete",
        f"/assistants/{_assistant_id()}/tasks/{task_id}",
        headers={"If-Match": format_task_etag(expected_task_revision)},
    )


def pause_trigger(
    *,
    task_id: int,
    expected_task_revision: int,
) -> dict[str, Any]:
    """Pause provider-event automation without disabling manual execution."""

    response = _request(
        "post",
        f"/assistants/{_assistant_id()}/tasks/{task_id}/pause",
        headers={"If-Match": format_task_etag(expected_task_revision)},
    )
    return _info(response)


def resume_trigger(
    *,
    task_id: int,
    expected_task_revision: int,
) -> dict[str, Any]:
    """Resume provider-event automation."""

    response = _request(
        "post",
        f"/assistants/{_assistant_id()}/tasks/{task_id}/resume",
        headers={"If-Match": format_task_etag(expected_task_revision)},
    )
    return _info(response)


def retry_trigger(*, task_id: int) -> dict[str, Any]:
    """Request immediate provider-trigger reconciliation."""

    response = _request(
        "post",
        f"/assistants/{_assistant_id()}/tasks/{task_id}/retry-trigger",
    )
    return _info(response)


def get_trigger_health(*, task_id: int) -> dict[str, Any]:
    """Read composed provider-trigger health for one task."""

    response = _request(
        "get",
        f"/assistants/{_assistant_id()}/tasks/{task_id}/trigger-health",
    )
    return _info(response)


def get_trigger_catalog() -> dict[str, Any]:
    """List curated provider-event trigger catalog entries."""

    response = _request("get", "/task-trigger-catalog")
    return _info(response)


def get_event_context(
    *,
    task_id: int,
    run_id: int,
) -> dict[str, Any]:
    """Inspect one provider-event run context."""

    response = _request(
        "get",
        f"/assistants/{_assistant_id()}/tasks/{task_id}/runs/{run_id}/event-context",
    )
    return _info(response)


def export_event_context(
    *,
    task_id: int,
    run_id: int,
) -> dict[str, Any]:
    """Export one provider-event run context with audit logging."""

    response = _request(
        "post",
        f"/assistants/{_assistant_id()}/tasks/{task_id}/runs/{run_id}/event-context/export",
    )
    return _info(response)


def delete_event_context(*, task_id: int, run_id: int) -> None:
    """Delete one provider-event run context."""

    _request(
        "delete",
        f"/assistants/{_assistant_id()}/tasks/{task_id}/runs/{run_id}/event-context",
    )


def trigger_task(*, task_id: int) -> dict[str, Any]:
    """Request one manual task execution through the compatibility route."""

    response = _request(
        "post",
        f"/tasks/{task_id}/trigger",
        json={"assistant_id": _assistant_id()},
    )
    return _info(response)
