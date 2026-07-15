"""Compose actor-facing provider-trigger health from Orchestra trigger-health.

Foundational actor-layer contract: maps Orchestra trigger-health into composed
lifecycle states and sanitizes event-context payloads.
"""

from __future__ import annotations

from typing import Any, Literal

ComposedProviderTriggerState = Literal[
    "draft",
    "connecting",
    "active",
    "recovering",
    "paused",
    "needs_attention",
    "removing",
]


def compose_provider_trigger_state(
    health: dict[str, Any],
) -> dict[str, Any]:
    """Map trigger-health fields to the actor-facing composed lifecycle state."""

    authored_state = health.get("authored_trigger_state")
    task_enabled = bool(health.get("task_enabled", True))
    runtime_health = str(health.get("runtime_health") or "absent")
    local_acceptance_open = bool(health.get("local_acceptance_open"))
    active_generation_id = health.get("active_generation_id")

    composed_state = _resolve_composed_state(
        authored_state=authored_state,
        task_enabled=task_enabled,
        runtime_health=runtime_health,
        local_acceptance_open=local_acceptance_open,
        active_generation_id=active_generation_id,
    )

    return {
        "task_id": health.get("task_id"),
        "task_revision": health.get("task_revision"),
        "composed_state": composed_state,
        "authored_trigger_state": authored_state,
        "task_enabled": task_enabled,
        "runtime_health": runtime_health,
        "desired_activation_revision": health.get("desired_activation_revision"),
        "observed_activation_revision": health.get("observed_activation_revision"),
        "acceptance_epoch": health.get("acceptance_epoch"),
        "local_acceptance_open": local_acceptance_open,
        "active_generation_id": active_generation_id,
        "coverage_started_at": health.get("coverage_started_at"),
        "coverage_ended_at": health.get("coverage_ended_at"),
        "remediation": health.get("remediation"),
        "event_storage_configured": bool(
            health.get("event_storage_configured", False),
        ),
        "manual_run_available": task_enabled,
    }


def _resolve_composed_state(
    *,
    authored_state: str | None,
    task_enabled: bool,
    runtime_health: str,
    local_acceptance_open: bool,
    active_generation_id: str | None,
) -> ComposedProviderTriggerState:
    if runtime_health == "removing":
        return "removing"
    if runtime_health == "needs_attention":
        return "needs_attention"
    if not task_enabled:
        if authored_state == "paused":
            return "paused"
        return "needs_attention"
    if authored_state == "paused":
        return "paused"
    if authored_state == "draft" or runtime_health == "absent":
        return "draft"
    if runtime_health == "recovering":
        return "recovering"
    if (
        task_enabled
        and authored_state == "enabled"
        and runtime_health == "healthy"
        and local_acceptance_open
        and active_generation_id
    ):
        return "active"
    if runtime_health in {"provisioning", "healthy", "recovering"}:
        return "connecting"
    return "needs_attention"


def sanitize_event_context_for_actor(
    context: dict[str, Any],
    *,
    include_source_body: bool,
) -> dict[str, Any]:
    """Return event context safe for model-visible actor responses."""

    payload = {
        "receipt_id": context.get("receipt_id"),
        "run_id": context.get("run_id"),
        "event_context_ref": context.get("event_context_ref"),
        "envelope": context.get("envelope") or {},
        "curated_projection": context.get("curated_projection") or {},
        "expires_at": context.get("expires_at"),
    }
    if include_source_body:
        payload["source_body"] = context.get("source_body")
    return payload
