from __future__ import annotations

from collections import deque
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

import unify.coordinator_manager.activity as activity_module
from unify.coordinator_manager.activity import (
    activity_entity,
    flush_pending_coordinator_activity_publishes,
    publish_coordinator_activity,
    safe_activity_text,
)
from unify.events.stream_filters import is_streaming_noise
from unify.events.types.coordinator_activity import CoordinatorActivityPayload
from unify.session_details import SESSION_DETAILS


def test_activity_schema_rejects_extra_fields_and_shapes_entities():
    entity = activity_entity("colleague", name="Revenue Ops", entity_id=7002)

    payload = CoordinatorActivityPayload(
        activity_id="activity-1",
        phase="completed",
        stage="integration_setup",
        surfaces=["colleagues"],
        title="Created Revenue Ops colleague",
        related_entities=[entity],
        occurred_at=datetime.now(UTC),
    )

    assert payload.related_entities == [entity]
    assert payload.occurred_at.tzinfo is UTC
    with pytest.raises(ValidationError):
        CoordinatorActivityPayload(
            activity_id="activity-2",
            phase="completed",
            stage="implementation",
            title="Created Revenue Ops colleague",
            occurred_at=datetime.now(UTC),
            unexpected="not allowed",
        )


def test_activity_stage_uses_integration_setup_contract():
    payload = CoordinatorActivityPayload(
        activity_id="activity-integration-setup",
        phase="needs_input",
        stage="integration_setup",
        title="Connect Salesforce",
        occurred_at=datetime.now(UTC),
    )

    assert payload.stage == "integration_setup"

    with pytest.raises(ValidationError):
        CoordinatorActivityPayload(
            activity_id="activity-credential-setup",
            phase="needs_input",
            stage="credential_setup",
            title="Connect Salesforce",
            occurred_at=datetime.now(UTC),
        )


def test_activity_text_redacts_secret_like_content_and_truncates():
    assert (
        safe_activity_text(
            "Salesforce API key configured",
            fallback="Credential updated",
        )
        == "Credential updated"
    )
    assert safe_activity_text("a" * 200, fallback="fallback", max_length=20) == (
        "a" * 17 + "..."
    )


def test_activity_publisher_noops_for_non_coordinator_session():
    SESSION_DETAILS.reset()

    assert (
        publish_coordinator_activity(
            phase="progress",
            stage="requirements",
            title="Added setup step",
        )
        is None
    )


def test_activity_publish_queues_until_event_bus_is_available(monkeypatch):
    class _FakeBus:
        def __init__(self) -> None:
            self.ready = False
            self.published: list[tuple[str, bool]] = []

        def __bool__(self) -> bool:
            return self.ready

        async def publish(self, event, *, blocking: bool = False) -> None:
            if not self.ready:
                raise RuntimeError("EVENT_BUS has not been initialised yet")
            self.published.append((event.payload["title"], blocking))

    fake_bus = _FakeBus()
    monkeypatch.setattr(activity_module, "EVENT_BUS", fake_bus)
    monkeypatch.setattr(activity_module, "_PUBLISH_TASKS", set())
    monkeypatch.setattr(activity_module, "_PENDING_PUBLISH_EVENTS", deque())
    monkeypatch.setattr(activity_module, "_PENDING_DRAIN_TASK", None)
    SESSION_DETAILS.reset()
    SESSION_DETAILS.is_coordinator = True

    publish_coordinator_activity(
        phase="progress",
        stage="requirements",
        title="Queued until init",
    )
    assert fake_bus.published == []

    fake_bus.ready = True
    flush_pending_coordinator_activity_publishes()

    assert fake_bus.published == [("Queued until init", True)]


def test_activity_publish_flush_preserves_fifo_order(monkeypatch):
    class _FakeBus:
        def __init__(self) -> None:
            self.ready = False
            self.published: list[str] = []

        def __bool__(self) -> bool:
            return self.ready

        async def publish(self, event, *, blocking: bool = False) -> None:
            if not self.ready:
                raise RuntimeError("EVENT_BUS has not been initialised yet")
            self.published.append(event.payload["title"])

    fake_bus = _FakeBus()
    monkeypatch.setattr(activity_module, "EVENT_BUS", fake_bus)
    monkeypatch.setattr(activity_module, "_PUBLISH_TASKS", set())
    monkeypatch.setattr(activity_module, "_PENDING_PUBLISH_EVENTS", deque())
    monkeypatch.setattr(activity_module, "_PENDING_DRAIN_TASK", None)
    SESSION_DETAILS.reset()
    SESSION_DETAILS.is_coordinator = True

    publish_coordinator_activity(
        phase="progress",
        stage="requirements",
        title="First queued activity",
    )
    publish_coordinator_activity(
        phase="progress",
        stage="requirements",
        title="Second queued activity",
    )

    fake_bus.ready = True
    flush_pending_coordinator_activity_publishes()

    assert fake_bus.published == [
        "First queued activity",
        "Second queued activity",
    ]


def test_coordinator_activity_is_not_streaming_noise():
    payload = CoordinatorActivityPayload(
        activity_id="activity-1",
        phase="progress",
        stage="requirements",
        surfaces=["tasks"],
        title="Added setup step",
        occurred_at=datetime.now(UTC),
    )

    assert (
        is_streaming_noise("CoordinatorActivity", payload.model_dump(mode="json"))
        is False
    )
