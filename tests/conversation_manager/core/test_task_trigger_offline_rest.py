"""Focused tests for REST offline task-trigger routing in task_activation."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unify.conversation_manager.domains import task_activation
from unify.conversation_manager.events import TaskTriggerRequested
from unify.task_scheduler.machine_state import TaskActivationSnapshot


@pytest.mark.asyncio
async def test_rest_offline_task_trigger_dispatches_without_actor():
    mock_cm = MagicMock()
    mock_cm.actor = None
    mock_cm.notifications_bar = MagicMock()
    mock_cm._session_logger = MagicMock()
    activation = TaskActivationSnapshot(
        assistant_id="42",
        activation_key="42:401",
        task_id=401,
        activation_kind="scheduled",
        execution_mode="offline",
        source_task_log_id=9002,
        activation_revision="rev-offline",
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
            task_activation,
            "_current_task_assistant_id",
            return_value="42",
        ),
        patch.object(
            task_activation,
            "get_task_activation",
            return_value=activation,
        ),
        patch(
            "unify.settings.SETTINGS.task.LOCAL_SCHEDULER_ENABLED",
            False,
        ),
        patch.object(
            task_activation,
            "_dispatch_offline_explicit_candidate",
            return_value={"success": True, "status": "launched"},
        ) as offline_dispatch,
        patch.object(
            task_activation,
            "_start_live_task_trigger_execution",
            new_callable=AsyncMock,
        ) as live_execute,
        patch.object(
            task_activation,
            "remember_live_task_run_provenance",
        ) as remember_provenance,
    ):
        result = await task_activation._handle_task_trigger_requested_event(
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
            task_activation,
            "_current_task_assistant_id",
            return_value="42",
        ),
        patch.object(
            task_activation,
            "get_task_activation",
            return_value=None,
        ),
        patch.object(
            task_activation,
            "remember_live_task_run_provenance",
        ) as remember_provenance,
        patch.object(
            task_activation,
            "_start_live_task_trigger_execution",
            new_callable=AsyncMock,
            return_value=77,
        ) as live_execute,
        patch.object(
            task_activation,
            "_queue_fast_brain_task_context",
            new_callable=AsyncMock,
        ),
    ):
        result = await task_activation._handle_task_trigger_requested_event(
            event,
            mock_cm,
        )

    assert result is False
    live_execute.assert_awaited_once()
    provenance = remember_provenance.call_args.args[0]
    assert provenance.source_type == "explicit"
    assert provenance.execution_mode == "live"
    assert provenance.source_ref == "req-abc"
