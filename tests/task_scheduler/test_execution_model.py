"""Unit tests for the minimal Execution identity model."""

from __future__ import annotations

from unify.task_scheduler.types.execution import (
    EXECUTIONS_CONTEXT_NAME,
    Delivery,
    ExecutionState,
    Wake,
)


def test_execution_context_name():
    assert EXECUTIONS_CONTEXT_NAME == "Tasks/Executions"


def test_delivery_from_offline_flag():
    assert Delivery.from_offline_flag(True) is Delivery.offline
    assert Delivery.from_offline_flag(False) is Delivery.live
    assert Delivery.normalize("OFFLINE") is Delivery.offline


def test_wake_normalize():
    assert Wake.normalize("scheduled") is Wake.scheduled
    assert Wake.normalize(None) is Wake.explicit


def test_execution_state_open_and_terminal():
    assert ExecutionState.scheduled.is_open
    assert ExecutionState.triggerable.is_open
    assert ExecutionState.running.is_open
    assert not ExecutionState.completed.is_open
    assert ExecutionState.failed.is_terminal
    assert ExecutionState.cancelled.is_terminal
    assert not ExecutionState.running.is_terminal
