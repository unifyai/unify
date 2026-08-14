from types import SimpleNamespace

import pytest

from unify.common.custom_sync import reconcile_custom_rows
from unify.task_scheduler import task_scheduler as task_scheduler_module
from unify.task_scheduler.task_scheduler import TaskScheduler, _TaskSyncAdapter
from unify.task_scheduler.types.execution import ExecutionState


def _scheduler_with_task(
    task_id: int = 0,
    destination: str | None = None,
) -> TaskScheduler:
    scheduler = TaskScheduler.__new__(TaskScheduler)
    # Both the resolved task's destination and its trigger: the callers here
    # reach a task through `_resolve_task_for_mutation`, which asks whether it
    # carries a provider-event trigger before returning it. A stub with only
    # `destination` raised AttributeError from inside the code under test, so
    # every test in this file failed for a reason none of them were about.
    scheduler._get_task_or_raise = lambda tid: SimpleNamespace(
        destination=destination,
        trigger=None,
    )
    return scheduler


_DEFAULT_OPEN_STATES = (
    ExecutionState.scheduled,
    ExecutionState.triggerable,
    ExecutionState.running,
)


def _fake_find_running_execution(*, running_states):
    """Build a fake ``find_running_execution_for_task`` scoped to which states have an open row.

    ``running_states`` is the set of ``ExecutionState`` values that currently
    have an execution present, so the fake can answer truthfully whatever
    subset of states a caller queries with.
    """

    def _fake(*, task_id, destination=None, states=None):
        queried = states if states is not None else _DEFAULT_OPEN_STATES
        for state in queried:
            if state in running_states:
                # Enough of a real snapshot to reference the row: cancelling
                # one now patches the execution it already found rather than
                # looking it up again, which is what the lookup it replaced
                # could not do without a source_task_log_id it never had.
                return SimpleNamespace(
                    task_id=task_id,
                    state=state.value,
                    run_key=f"live:scheduled:42:{task_id}:rev:once",
                    assistant_id="42",
                    source_task_log_id=555,
                )
        return None

    return _fake


def test_ensure_not_active_task_allows_armed_triggerable_task(monkeypatch):
    """An armed provider-event task (triggerable, not running) must not block mutation."""

    monkeypatch.setattr(
        task_scheduler_module,
        "find_running_execution_for_task",
        _fake_find_running_execution(running_states={ExecutionState.triggerable}),
    )
    scheduler = _scheduler_with_task()

    scheduler._ensure_not_active_task(0)


def test_ensure_not_active_task_blocks_genuinely_running_task(monkeypatch):
    monkeypatch.setattr(
        task_scheduler_module,
        "find_running_execution_for_task",
        _fake_find_running_execution(running_states={ExecutionState.running}),
    )
    scheduler = _scheduler_with_task()

    with pytest.raises(
        RuntimeError,
        match="Operation not permitted on the active task",
    ):
        scheduler._ensure_not_active_task(0)


@pytest.mark.parametrize(
    "open_state",
    [ExecutionState.scheduled, ExecutionState.triggerable],
)
def test_cancel_open_executions_still_cancels_non_running_open_states(
    monkeypatch,
    open_state,
):
    """``_cancel_open_executions`` must keep cancelling scheduled/triggerable rows, unchanged."""

    monkeypatch.setattr(
        task_scheduler_module,
        "find_running_execution_for_task",
        _fake_find_running_execution(running_states={open_state}),
    )
    update_calls: list[dict] = []
    monkeypatch.setattr(
        task_scheduler_module,
        "update_task_run_record",
        lambda reference, updates: update_calls.append(updates),
    )
    scheduler = _scheduler_with_task()

    scheduler._cancel_open_executions(0)

    assert len(update_calls) == 1
    assert update_calls[0]["state"] == ExecutionState.cancelled.value
    # `completed_at`, not `ended_at`. Nothing reads `ended_at`, so a cancelled
    # run showed no finish time anywhere it was displayed.
    assert update_calls[0]["completed_at"]
    assert "ended_at" not in update_calls[0]


def _reconcile_one_update(scheduler) -> list[int]:
    """Run one engine pass over a single changed task and return update calls."""

    update_calls: list[int] = []
    scheduler._update_custom_task = lambda **kwargs: update_calls.append(
        kwargs["task_id"],
    )
    adapter = _TaskSyncAdapter(scheduler, function_name_to_id={})
    adapter.live_rows = lambda: [
        {"custom_key": "k", "task_id": 5, "custom_hash": "old"},
    ]
    reconcile_custom_rows(
        source={"k": {"custom_hash": "new"}},
        adapter=adapter,
    )
    return update_calls


def test_sync_updates_armed_triggerable_custom_task(monkeypatch):
    """An armed (triggerable) custom task must still be synced, not skipped."""

    monkeypatch.setattr(
        task_scheduler_module,
        "find_running_execution_for_task",
        _fake_find_running_execution(running_states={ExecutionState.triggerable}),
    )

    assert _reconcile_one_update(_scheduler_with_task()) == [5]


def test_sync_skips_genuinely_running_custom_task(monkeypatch):
    monkeypatch.setattr(
        task_scheduler_module,
        "find_running_execution_for_task",
        _fake_find_running_execution(running_states={ExecutionState.running}),
    )

    assert _reconcile_one_update(_scheduler_with_task()) == []
