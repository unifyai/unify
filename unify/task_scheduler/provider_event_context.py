"""Typed provider-event context and Orchestra fetch client for live dispatch.

Source content is structured untrusted data. Callers must keep it out of system
and task instruction channels; it cannot grant tools or override authorization.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

import requests
from pydantic import BaseModel, ConfigDict, Field

from unify.session_details import SESSION_DETAILS
from unify.settings import SETTINGS
from unify.task_scheduler.provider_event_dispatch import (
    ProviderEventDispatchRequest,
    ProviderEventDispatchValidationError,
)

PROVIDER_EVENT_CONTEXT_AUDIENCE = "orchestra:event-context"
_EVENT_CONTEXT_PATH = "/provider-event/event-context"
_TASK_RUN_GET_PATH = "/task-run/get"
_ORCHESTRA_TASK_MACHINE_PROJECT = "Assistants"
_HTTP_TIMEOUT_SECONDS = 30


class ProviderEventContext(BaseModel):
    """Matched event context delivered to a live provider-event instance.

    Fields are data only. Never splice ``source_body`` or projection text into
    system prompts or task instructions.
    """

    model_config = ConfigDict(extra="forbid")

    receipt_id: str
    run_id: int
    event_context_ref: str
    envelope: dict[str, Any] = Field(default_factory=dict)
    curated_projection: dict[str, Any] = Field(default_factory=dict)
    source_body: dict[str, Any] | list[Any] | str | None = None


def _orchestra_headers() -> dict[str, str]:
    unify_key = SESSION_DETAILS.unify_key
    if not unify_key:
        raise ProviderEventDispatchValidationError("orchestra_credentials_missing")
    return {
        "accept": "application/json",
        "Authorization": f"Bearer {unify_key}",
        "Content-Type": "application/json",
    }


def _orchestra_url(path: str) -> str:
    base = (SETTINGS.ORCHESTRA_URL or "").rstrip("/")
    if not base:
        raise ProviderEventDispatchValidationError("orchestra_url_missing")
    return f"{base}{path}"


def verify_precreated_provider_event_run(
    request: ProviderEventDispatchRequest,
) -> None:
    """Require the Orchestra run referenced by one provider-event dispatch."""

    response = requests.post(
        _orchestra_url(_TASK_RUN_GET_PATH),
        headers=_orchestra_headers(),
        json={
            "project_name": _ORCHESTRA_TASK_MACHINE_PROJECT,
            "assistant_id": request.assistant_id,
            "run_key": request.run_key,
        },
        timeout=_HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    body = response.json()
    run = body.get("run") if isinstance(body, Mapping) else None
    if not isinstance(run, Mapping):
        raise ProviderEventDispatchValidationError("run_not_found")
    run_row_id = run.get("run_id")
    if run_row_id is None or int(run_row_id) != request.run_id:
        raise ProviderEventDispatchValidationError("run_id_mismatch")
    if str(run.get("run_key") or "") != request.run_key:
        raise ProviderEventDispatchValidationError("run_key_mismatch")
    run_task_id = run.get("task_id")
    if run_task_id is not None and int(run_task_id) != request.task_id:
        raise ProviderEventDispatchValidationError("run_task_id_mismatch")
    source_type = run.get("source_type")
    if source_type is not None and str(source_type) != request.source_type:
        raise ProviderEventDispatchValidationError("run_source_type_mismatch")
    execution_mode = run.get("execution_mode")
    if execution_mode is not None and str(execution_mode) != request.dispatch_mode:
        raise ProviderEventDispatchValidationError("run_execution_mode_mismatch")


def fetch_provider_event_context(
    request: ProviderEventDispatchRequest,
) -> ProviderEventContext:
    """Fetch authorized event context for one live dispatch operation."""

    response = requests.post(
        _orchestra_url(_EVENT_CONTEXT_PATH),
        headers=_orchestra_headers(),
        json={
            "assistant_id": request.assistant_id,
            "task_id": request.task_id,
            "run_id": request.run_id,
            "receipt_id": request.receipt_id,
            "event_context_ref": request.event_context_ref,
            "audience": PROVIDER_EVENT_CONTEXT_AUDIENCE,
        },
        timeout=_HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code in {401, 403, 404}:
        raise ProviderEventDispatchValidationError("event_context_unavailable")
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, Mapping):
        raise ProviderEventDispatchValidationError("event_context_unavailable")

    source_body = payload.get("source_body")
    if isinstance(source_body, str):
        try:
            source_body = json.loads(source_body)
        except json.JSONDecodeError:
            pass

    return ProviderEventContext(
        receipt_id=str(payload.get("receipt_id") or request.receipt_id),
        run_id=int(payload.get("run_id") or request.run_id),
        event_context_ref=str(
            payload.get("event_context_ref") or request.event_context_ref,
        ),
        envelope=dict(payload.get("envelope") or {}),
        curated_projection=dict(payload.get("curated_projection") or {}),
        source_body=source_body,
    )


def provider_event_context_as_untrusted_data(
    context: ProviderEventContext,
) -> dict[str, Any]:
    """Return a data-only container for entrypoint kwargs / actor data channels."""

    return {
        "kind": "provider_event_context",
        "trust": "untrusted_data",
        "receipt_id": context.receipt_id,
        "run_id": context.run_id,
        "event_context_ref": context.event_context_ref,
        "envelope": context.envelope,
        "curated_projection": context.curated_projection,
        "source_body": context.source_body,
    }
