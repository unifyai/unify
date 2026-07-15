"""Pure helper tests for provider-trigger actor health composition."""

from __future__ import annotations

from unify.task_scheduler.provider_trigger_health import (
    compose_provider_trigger_state,
    sanitize_event_context_for_actor,
)


def test_compose_provider_trigger_state_reports_active_only_when_fully_healthy() -> (
    None
):
    composed = compose_provider_trigger_state(
        {
            "task_id": 7,
            "task_revision": 3,
            "authored_trigger_state": "enabled",
            "task_enabled": True,
            "runtime_health": "healthy",
            "local_acceptance_open": True,
            "active_generation_id": "gen-1",
            "event_storage_configured": True,
        },
    )
    assert composed["composed_state"] == "active"
    assert composed["manual_run_available"] is True


def test_compose_provider_trigger_state_reports_paused_when_authored_paused() -> None:
    composed = compose_provider_trigger_state(
        {
            "task_id": 7,
            "task_revision": 3,
            "authored_trigger_state": "paused",
            "task_enabled": True,
            "runtime_health": "healthy",
            "local_acceptance_open": True,
            "active_generation_id": "gen-1",
        },
    )
    assert composed["composed_state"] == "paused"


def test_compose_provider_trigger_state_reports_needs_attention_when_task_disabled() -> (
    None
):
    composed = compose_provider_trigger_state(
        {
            "task_id": 7,
            "task_revision": 3,
            "authored_trigger_state": "enabled",
            "task_enabled": False,
            "runtime_health": "healthy",
            "local_acceptance_open": True,
            "active_generation_id": "gen-1",
        },
    )
    assert composed["composed_state"] == "needs_attention"
    assert composed["manual_run_available"] is False


def test_sanitize_event_context_hides_source_body_by_default() -> None:
    sanitized = sanitize_event_context_for_actor(
        {
            "receipt_id": "receipt-1",
            "run_id": 42,
            "event_context_ref": "blob-1",
            "envelope": {"event_slug": "github.issue_created"},
            "curated_projection": {"title": "Fixture"},
            "source_body": {"raw_payload_field": "value"},
            "expires_at": "2026-08-12T12:00:00+00:00",
        },
        include_source_body=False,
    )
    assert "source_body" not in sanitized
    assert sanitized["curated_projection"] == {"title": "Fixture"}


def test_sanitize_event_context_includes_source_body_when_requested() -> None:
    sanitized = sanitize_event_context_for_actor(
        {
            "receipt_id": "receipt-1",
            "run_id": 42,
            "event_context_ref": "blob-1",
            "envelope": {},
            "curated_projection": {},
            "source_body": {"issue": "opened"},
            "expires_at": None,
        },
        include_source_body=True,
    )
    assert sanitized["source_body"] == {"issue": "opened"}
