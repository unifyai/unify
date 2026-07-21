"""Focused tests for REST offline task-trigger routing in task_execution."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unify.conversation_manager.domains import task_execution
from unify.conversation_manager.events import TaskTriggerRequested
from unify.task_scheduler.machine_state import TaskExecutionSnapshot


@pytest.mark.asyncio
async def test_rest_offline_task_trigger_dispatches_without_actor():
    mock_cm = MagicMock()
    mock_cm.actor = None
    mock_cm.notifications_bar = MagicMock()
    mock_cm._session_logger = MagicMock()
    activation = TaskExecutionSnapshot(
        run_key="42:401",
        assistant_id="42",
        task_id=401,
        wake="scheduled",
        delivery="offline",
        source_task_log_id=9002,
        revision="rev-offline",
        task_name="Poll stargazers",
        task_description="Poll GitHub stargazers.",
        entrypoint=27,
    )
    event = TaskTriggerRequested(
        task_id=401,
        source_task_log_id=9002,
        source_ref="req-offline",
        task_label="Poll stargazers",
        task_summary="Poll GitHub stargazers.",
    )

    with (
        patch.object(
            task_execution,
            "_current_task_assistant_id",
            return_value="42",
        ),
        patch.object(
            task_execution,
            "get_open_task_execution",
            return_value=activation,
        ),
        patch(
            "unify.settings.SETTINGS.task.LOCAL_SCHEDULER_ENABLED",
            False,
        ),
        patch.object(
            task_execution,
            "_dispatch_offline_explicit_candidate",
            return_value={"success": True, "status": "launched"},
        ) as offline_dispatch,
        patch.object(
            task_execution,
            "_start_live_task_trigger_execution",
            new_callable=AsyncMock,
        ) as live_execute,
        patch.object(
            task_execution,
            "remember_live_task_run_provenance",
        ) as remember_provenance,
    ):
        result = await task_execution._handle_task_trigger_requested_event(
            event,
            mock_cm,
        )

    assert result is False
    offline_dispatch.assert_called_once()
    assert offline_dispatch.call_args.kwargs["candidate"] is activation
    assert offline_dispatch.call_args.kwargs["source_ref"] == "req-offline"
    live_execute.assert_not_awaited()
    remember_provenance.assert_not_called()


@pytest.mark.asyncio
async def test_rest_live_task_trigger_still_uses_live_execute():
    mock_cm = MagicMock()
    mock_cm.actor = object()
    mock_cm.notifications_bar = MagicMock()
    mock_cm._session_logger = MagicMock()
    event = TaskTriggerRequested(
        task_id=301,
        source_task_log_id=9001,
        source_ref="req-abc",
        task_label="Review report",
        task_summary="Review the weekly report.",
    )

    with (
        patch.object(
            task_execution,
            "_current_task_assistant_id",
            return_value="42",
        ),
        patch.object(
            task_execution,
            "get_open_task_execution",
            return_value=None,
        ),
        patch.object(
            task_execution,
            "remember_live_task_run_provenance",
        ) as remember_provenance,
        patch.object(
            task_execution,
            "_start_live_task_trigger_execution",
            new_callable=AsyncMock,
            return_value=77,
        ) as live_execute,
        patch.object(
            task_execution,
            "_queue_fast_brain_task_context",
            new_callable=AsyncMock,
        ),
    ):
        result = await task_execution._handle_task_trigger_requested_event(
            event,
            mock_cm,
        )

    assert result is False
    live_execute.assert_awaited_once()
    provenance = remember_provenance.call_args.args[0]
    from unify.task_scheduler.types.execution import Delivery

    assert provenance.wake.value == "explicit"
    assert provenance.delivery == Delivery.live
    assert provenance.source_ref == "req-abc"


def test_offline_explicit_dispatch_posts_task_execution_payload(monkeypatch):
    captured: dict = {}

    class _Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, str]:
            return {"success": True, "status": "launched"}

    def _fake_post(url: str, *, json: dict, headers: dict, timeout: int) -> _Response:
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        captured["timeout"] = timeout
        return _Response()

    monkeypatch.setattr(
        "unify.conversation_manager.domains.task_execution.requests.post",
        _fake_post,
    )
    monkeypatch.setattr(
        "unify.settings.SETTINGS",
        type(
            "S",
            (),
            {"conversation": type("C", (), {"COMMS_URL": "https://comms.example"})()},
        )(),
    )
    monkeypatch.setattr(
        "unify.session_details.SESSION_DETAILS",
        type("SD", (), {"unify_key": "test-key"})(),
    )
    monkeypatch.setattr(
        "unify.conversation_manager.domains.task_execution._current_task_assistant_id",
        lambda: "42",
    )

    candidate = TaskExecutionSnapshot(
        run_key="42:401",
        assistant_id="42",
        task_id=401,
        wake="scheduled",
        delivery="offline",
        source_task_log_id=9002,
        revision="rev-offline",
        task_name="Poll stargazers",
        task_description="Poll GitHub stargazers.",
        entrypoint=27,
    )

    result = task_execution._dispatch_offline_explicit_candidate(
        candidate=candidate,
        source_ref="req-offline",
    )

    assert result == {"success": True, "status": "launched"}
    assert (
        captured["url"] == "https://comms.example/infra/task-execution/offline-dispatch"
    )
    assert captured["json"] == {
        "assistant_id": "42",
        "task_id": 401,
        "source_task_log_id": 9002,
        "revision": "rev-offline",
        "delivery": "offline",
        "wake": "explicit",
        "source_ref": "req-offline",
        "source_medium": "api",
        "task_name": "Poll stargazers",
        "task_description": "Poll GitHub stargazers.",
        "entrypoint": 27,
        "requires_filesystem": False,
        "requires_computer": False,
    }
    assert captured["headers"] == {"Authorization": "Bearer test-key"}


def test_offline_trigger_dispatch_posts_task_execution_payload(monkeypatch):
    captured: dict = {}

    class _Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, str]:
            return {"success": True, "status": "launched"}

    def _fake_post(url: str, *, json: dict, headers: dict, timeout: int) -> _Response:
        captured["url"] = url
        captured["json"] = json
        return _Response()

    monkeypatch.setattr(
        "unify.conversation_manager.domains.task_execution.requests.post",
        _fake_post,
    )
    monkeypatch.setattr(
        "unify.settings.SETTINGS",
        type(
            "S",
            (),
            {"conversation": type("C", (), {"COMMS_URL": "https://comms.example"})()},
        )(),
    )
    monkeypatch.setattr(
        "unify.session_details.SESSION_DETAILS",
        type("SD", (), {"unify_key": "test-key"})(),
    )
    monkeypatch.setattr(
        "unify.conversation_manager.domains.task_execution._current_task_assistant_id",
        lambda: "42",
    )

    candidate = TaskExecutionSnapshot(
        run_key="42:17",
        assistant_id="42",
        task_id=17,
        wake="triggered",
        delivery="offline",
        source_task_log_id=2017,
        revision="rev-xyz",
    )
    event = type("E", (), {"content": "hello", "timestamp": None})()

    from unify.conversation_manager.cm_types import Medium

    result = task_execution._dispatch_offline_trigger_candidate(
        candidate=candidate,
        event=event,
        medium=Medium.SMS_MESSAGE,
        contact_id=123,
        sender_name="Alice",
    )

    assert result == {"success": True, "status": "launched"}
    assert (
        captured["url"] == "https://comms.example/infra/task-execution/offline-dispatch"
    )
    payload = captured["json"]
    assert payload["revision"] == "rev-xyz"
    assert payload["delivery"] == "offline"
    assert payload["wake"] == "triggered"
    assert payload["source_medium"] == "sms_message"
    assert payload["source_contact_id"] == 123
    assert payload["source_contact_display_name"] == "Alice"
    assert payload["source_ref"]
