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
    canonical_app_slug: str | None = None,
    backend_id: str | None = None,
) -> list[dict[str, Any]]:
    """Return assistant-scoped connections for provider-trigger setup."""

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
        if canonical_app_slug is not None and (
            connection.get("canonical_app_slug") != canonical_app_slug
        ):
            continue
        resolved_backend = str(connection.get("backend_id") or "")
        if backend_id is not None and resolved_backend != backend_id:
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


def describe_provider_trigger(
    *,
    provider_trigger_slug: str,
    backend_id: str,
    catalog_triggers: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return staged catalog metadata for one provider trigger."""

    for trigger in catalog_triggers or []:
        if not isinstance(trigger, dict):
            continue
        if (
            trigger.get("provider_trigger_slug") == provider_trigger_slug
            and trigger.get("backend_id") == backend_id
        ):
            return trigger
    raise ValueError(
        f"Unsupported provider trigger {provider_trigger_slug!r} on {backend_id!r}.",
    )
