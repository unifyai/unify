"""Helpers for CodeActActor provider-trigger task tools."""

from __future__ import annotations

from typing import Any

from unify.integrations import ops as integration_ops
from unify.session_details import SESSION_DETAILS

from .typed_tasks_client import TaskRevisionConflictError

CONNECTION_SUMMARY_KEYS = frozenset(
    {
        "connection_id",
        "canonical_app_slug",
        "backend_id",
        "status",
        "external_account_label",
        "provider_user_id",
        "assistant_id",
        "owner_scope",
        "reconnect_reason",
        "last_health_check_status",
    },
)


def task_revision_conflict_outcome(
    exc: TaskRevisionConflictError,
) -> dict[str, Any]:
    """Return a stable actor outcome for one revision conflict."""

    return {
        "outcome": "task_revision_conflict",
        "details": {
            "message": (
                "The task changed since it was last read. Re-read the task and "
                "ask the user how to reconcile before retrying."
            ),
            "latest_task_revision": exc.latest_task_revision,
        },
    }


def list_eligible_provider_trigger_connections(
    *,
    event_slug: str,
    schema_version: str = "1",
    backend_id: str | None = None,
    catalog_events: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Return assistant-scoped connections suitable for one curated provider event."""

    event = _find_catalog_event(
        events=catalog_events or [],
        event_slug=event_slug,
        schema_version=schema_version,
    )
    if event is None:
        raise ValueError(
            f"Unsupported provider event {event_slug!r} schema {schema_version!r}.",
        )
    canonical_app_slug = str(event.get("canonical_app_slug") or "")
    eligible_backends = set(event.get("backends") or [])

    agent_id = SESSION_DETAILS.assistant.agent_id
    if agent_id is None:
        raise ValueError(
            "assistant agent_id is required to list provider-trigger connections",
        )
    raw = integration_ops.list_connections(
        owner_scope="assistant",
        assistant_id=int(agent_id),
    )
    if isinstance(raw, dict) and raw.get("status") == "error":
        raise ValueError("Integration connections could not be loaded.")
    connections = raw if isinstance(raw, list) else []
    eligible: list[dict[str, Any]] = []
    for connection in connections:
        if not isinstance(connection, dict):
            continue
        if connection.get("canonical_app_slug") != canonical_app_slug:
            continue
        resolved_backend = str(connection.get("backend_id") or "")
        if backend_id is not None and resolved_backend != backend_id:
            continue
        if backend_id is None and resolved_backend not in eligible_backends:
            continue
        if connection.get("status") not in {"connected", "active"}:
            continue
        eligible.append(summarize_connection(connection))
    return eligible


def summarize_connection(connection: dict[str, Any]) -> dict[str, Any]:
    """Strip secret-bearing fields from one integration connection."""

    return {
        key: connection[key] for key in CONNECTION_SUMMARY_KEYS if key in connection
    }


def describe_provider_trigger_resource_contract(
    *,
    event_slug: str,
    schema_version: str = "1",
    catalog_events: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Explain how resources are selected for one curated provider event."""

    event = _find_catalog_event(
        events=catalog_events or [],
        event_slug=event_slug,
        schema_version=schema_version,
    )
    if event is None:
        raise ValueError(
            f"Unsupported provider event {event_slug!r} schema {schema_version!r}.",
        )
    return {
        "event_slug": event_slug,
        "schema_version": schema_version,
        "resource_kind": event.get("resource_kind"),
        "resource_id_format": event.get("resource_id_format"),
        "resource_filter_field": event.get("resource_filter_field"),
        "resource_filter_operator": event.get("resource_filter_operator"),
        "selection_contract": event.get("selection_contract"),
        "filters": event.get("filters") or [],
        "backends": event.get("backends") or [],
    }


def _find_catalog_event(
    *,
    events: list[dict[str, Any]],
    event_slug: str,
    schema_version: str,
) -> dict[str, Any] | None:
    for event in events:
        if not isinstance(event, dict):
            continue
        if (
            event.get("event_slug") == event_slug
            and str(event.get("schema_version")) == schema_version
        ):
            return event
    return None
