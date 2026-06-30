"""Tests for Coordinator delegation wake-reason handling."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from unify.conversation_manager.domains.coordinator_delegate import (
    _SEEN_DEDUPE_KEYS,
    _coordinator_delegate_event_from_payload,
    _coordinator_delegate_event_from_wake_reason,
    _handle_coordinator_delegate_event,
)
from unify.conversation_manager.domains.event_handlers import EventHandler
from unify.conversation_manager.domains.notifications import NotificationBar
from unify.conversation_manager.domains.task_activation import (
    _consume_startup_wake_reasons,
)
from unify.conversation_manager.events import CoordinatorDelegate


def _delegate_payload() -> dict:
    return {
        "type": "coordinator_delegate",
        "requested_by_assistant_id": "2071",
        "instruction": "Schedule the renewal summary tomorrow.",
        "intent": "schedule_task",
        "dedupe_key": "renewal-summary",
        "related_context": {"source": "coordinator"},
    }


@pytest.fixture(autouse=True)
def _reset_dedupe_keys():
    _SEEN_DEDUPE_KEYS.clear()
    yield
    _SEEN_DEDUPE_KEYS.clear()


def test_payload_factory_returns_event_with_structured_fields() -> None:
    event = _coordinator_delegate_event_from_payload(
        _delegate_payload(),
        reason="Coordinator 2071 assigned schedule_task work.",
    )

    assert isinstance(event, CoordinatorDelegate)
    assert event.requested_by_assistant_id == "2071"
    assert event.instruction == "Schedule the renewal summary tomorrow."
    assert event.intent == "schedule_task"
    assert event.dedupe_key == "renewal-summary"
    assert event.related_context == {"source": "coordinator"}
    assert event.reason == "Coordinator 2071 assigned schedule_task work."


def test_wake_reason_factory_accepts_coordinator_delegate_type() -> None:
    event = _coordinator_delegate_event_from_wake_reason(_delegate_payload())

    assert isinstance(event, CoordinatorDelegate)
    assert event.requested_by_assistant_id == "2071"
    assert event.instruction == "Schedule the renewal summary tomorrow."


def test_factories_reject_missing_required_fields() -> None:
    assert _coordinator_delegate_event_from_wake_reason({"type": "task_due"}) is None
    assert (
        _coordinator_delegate_event_from_wake_reason(
            {"type": "coordinator_delegate", "instruction": "missing requester"},
        )
        is None
    )
    assert (
        _coordinator_delegate_event_from_payload(
            {"requested_by_assistant_id": "2071"},
        )
        is None
    )


@pytest.mark.anyio
async def test_handler_pushes_notification_and_logs() -> None:
    cm = MagicMock()
    cm.notifications_bar = NotificationBar()
    cm._session_logger = MagicMock()

    event = CoordinatorDelegate(
        requested_by_assistant_id="2071",
        instruction="Schedule the renewal summary tomorrow.",
        intent="schedule_task",
        related_context={"source": "coordinator"},
    )
    result = await _handle_coordinator_delegate_event(event, cm)

    assert result is True
    assert len(cm.notifications_bar.notifications) == 1
    notification = cm.notifications_bar.notifications[0]
    assert notification.type == "Coordinator"
    assert "Schedule the renewal summary tomorrow." in notification.content
    assert "manager primitives" in notification.content
    assert '"source": "coordinator"' in notification.content
    cm._session_logger.info.assert_called_once()


@pytest.mark.anyio
async def test_handler_skips_duplicate_dedupe_key() -> None:
    cm = MagicMock()
    cm.notifications_bar = NotificationBar()
    cm._session_logger = MagicMock()

    event = CoordinatorDelegate(
        requested_by_assistant_id="2071",
        instruction="Schedule the renewal summary tomorrow.",
        intent="schedule_task",
        dedupe_key="renewal-summary",
    )

    assert await _handle_coordinator_delegate_event(event, cm) is True
    assert await _handle_coordinator_delegate_event(event, cm) is False
    assert len(cm.notifications_bar.notifications) == 1


def test_coordinator_delegate_is_registered() -> None:
    assert CoordinatorDelegate in EventHandler._registry


@pytest.mark.anyio
async def test_registered_handler_requests_llm_run() -> None:
    cm = MagicMock()
    cm.notifications_bar = NotificationBar()
    cm._session_logger = MagicMock()
    cm.request_llm_run = AsyncMock()

    event = CoordinatorDelegate(
        requested_by_assistant_id="2071",
        instruction="Schedule the renewal summary tomorrow.",
        intent="schedule_task",
    )

    await EventHandler._registry[CoordinatorDelegate](event, cm)

    cm.request_llm_run.assert_awaited_once_with(delay=0)


@pytest.mark.anyio
async def test_startup_wake_reason_replay_handles_coordinator_delegate() -> None:
    cm = MagicMock()
    cm.notifications_bar = NotificationBar()
    cm._session_logger = MagicMock()
    cm._startup_wake_reasons = [_delegate_payload()]

    await _consume_startup_wake_reasons(cm)

    assert cm._startup_wake_reasons == []
    assert len(cm.notifications_bar.notifications) == 1
    assert (
        "Schedule the renewal summary tomorrow."
        in cm.notifications_bar.notifications[0].content
    )
