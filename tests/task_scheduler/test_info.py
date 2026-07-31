"""Run summaries belong to the run that produced them.

A summary describes one execution, so it is stored on that execution's
``Tasks/Executions`` row. Writing it to the definition meant concurrent runs
of the same task overwrote each other's summary on a row that outlives them
both, and the last run to finish won.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from unify.task_scheduler.active_task import ActiveTask
from unify.task_scheduler.machine_state import TaskRunReference

MOCK_SUMMARY = "Mock summary: Task completed important steps."


class _Handle:
    def __init__(self, action_log=None):
        # `or` would turn an explicitly empty log back into the default and
        # send the no-log case down the summarizer path.
        self.action_log = ["did a thing"] if action_log is None else action_log

    async def result(self):
        return "done"


def _active_task(
    *,
    run_key: str = "run-1",
    reference: TaskRunReference | None = ...,
) -> ActiveTask:
    """An ActiveTask wired for summary generation with no live Orchestra."""

    task = object.__new__(ActiveTask)
    task._actor_handle = _Handle()
    task._was_stopped = False
    task._scheduler = None
    task._task_id = 10
    task._instance_id = 0
    task._summary_scheduled = False
    task._task_run_reference = (
        TaskRunReference(assistant_id="1406", run_key=run_key)
        if reference is ...
        else reference
    )
    return task


@pytest.fixture
def recorded_updates(monkeypatch):
    """Capture every Tasks/Executions patch the run performs."""

    updates: list[tuple[str, dict]] = []

    def _record(reference, patch):
        updates.append((reference.run_key, dict(patch)))

    monkeypatch.setattr(
        "unify.task_scheduler.active_task.update_task_run_record",
        _record,
    )
    monkeypatch.setattr(
        ActiveTask,
        "_generate_summary_from_log",
        AsyncMock(return_value=MOCK_SUMMARY),
    )
    return updates


def test_summary_lands_on_the_execution_row(recorded_updates):
    asyncio.run(_active_task()._save_final_summary("completed"))

    assert recorded_updates == [("run-1", {"result_summary": MOCK_SUMMARY})]


def test_summary_is_recorded_for_a_cancelled_run(recorded_updates):
    asyncio.run(_active_task()._save_final_summary("cancelled"))

    assert recorded_updates
    assert recorded_updates[0][1]["result_summary"] == MOCK_SUMMARY


def test_concurrent_runs_record_to_their_own_executions(recorded_updates):
    """The race the move fixes: two runs, two summaries, neither clobbered."""

    async def _both():
        await asyncio.gather(
            _active_task(run_key="run-a")._save_final_summary("completed"),
            _active_task(run_key="run-b")._save_final_summary("completed"),
        )

    asyncio.run(_both())

    assert {run_key for run_key, _ in recorded_updates} == {"run-a", "run-b"}
    assert all(patch["result_summary"] == MOCK_SUMMARY for _, patch in recorded_updates)


def test_run_without_an_execution_row_writes_nothing(recorded_updates):
    """Executions are assistant-owned; a session without one has no ledger."""

    asyncio.run(_active_task(reference=None)._save_final_summary("completed"))

    assert recorded_updates == []


def test_summary_falls_back_when_no_action_log(monkeypatch):
    updates: list[dict] = []
    monkeypatch.setattr(
        "unify.task_scheduler.active_task.update_task_run_record",
        lambda reference, patch: updates.append(dict(patch)),
    )

    task = _active_task()
    task._actor_handle = _Handle(action_log=[])
    asyncio.run(task._save_final_summary("failed"))

    assert updates
    assert "failed" in updates[0]["result_summary"]
