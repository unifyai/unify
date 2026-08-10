"""Run outcomes belong to the run that produced them.

An outcome describes one execution, so it is stored on that execution's
``Tasks/Executions`` row. Writing it to the definition meant concurrent runs
of the same task overwrote each other on a row that outlives them both, and
the last run to finish won.

The row is written exactly once, from the actor's own result. A second,
after-the-fact summary pass used to overwrite that value with boilerplate,
which is how a crashed run came to read as a clean ``completed``.
"""

from __future__ import annotations

import asyncio

import pytest

from unify.task_scheduler.active_task import ActiveTask
from unify.task_scheduler.machine_state import TaskRunReference


class _Handle:
    """Minimal actor handle: returns a result, or raises one."""

    def __init__(self, *, result: str | None = "done", raises: Exception | None = None):
        self._result = result
        self._raises = raises

    async def result(self):
        if self._raises is not None:
            raise self._raises
        return self._result


def _active_task(
    *,
    run_key: str = "run-1",
    reference: TaskRunReference | None = ...,
    handle: _Handle | None = None,
) -> ActiveTask:
    """An ActiveTask wired for outcome persistence with no live Orchestra."""

    task = object.__new__(ActiveTask)
    task._actor_handle = handle if handle is not None else _Handle()
    task._was_stopped = False
    task._scheduler = None
    task._task_id = 10
    task._preserve_definition_status = False
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
    return updates


def test_outcome_lands_on_the_execution_row(recorded_updates):
    assert asyncio.run(_active_task().result()) == "done"

    assert len(recorded_updates) == 1
    run_key, patch = recorded_updates[0]
    assert run_key == "run-1"
    assert patch["state"] == "completed"
    assert patch["result_summary"] == "done"


def test_the_row_is_written_once_and_keeps_the_actor_result(recorded_updates):
    """The regression: a second write used to replace the result with boilerplate."""

    asyncio.run(_active_task().result())

    assert len(recorded_updates) == 1
    assert "No detailed log found" not in recorded_updates[0][1]["result_summary"]


def test_a_raising_actor_records_failed_with_the_error(recorded_updates):
    """A crashed run must not read as completed."""

    task = _active_task(handle=_Handle(raises=ValueError("Expecting value: line 1")))

    with pytest.raises(ValueError):
        asyncio.run(task.result())

    assert len(recorded_updates) == 1
    patch = recorded_updates[0][1]
    assert patch["state"] == "failed"
    assert "Expecting value" in patch["error"]
    assert "ValueError" in patch["result_summary"]


def test_a_run_that_returns_nothing_says_so(recorded_updates):
    """An empty result must be recorded, not dropped so the field reads stale."""

    asyncio.run(_active_task(handle=_Handle(result="")).result())

    assert len(recorded_updates) == 1
    assert recorded_updates[0][1]["result_summary"] == "The run returned no result."


def test_concurrent_runs_record_to_their_own_executions(recorded_updates):
    """Two runs, two outcomes, neither clobbered."""

    async def _both():
        await asyncio.gather(
            _active_task(run_key="run-a").result(),
            _active_task(run_key="run-b").result(),
        )

    asyncio.run(_both())

    assert {run_key for run_key, _ in recorded_updates} == {"run-a", "run-b"}
    assert all(patch["result_summary"] == "done" for _, patch in recorded_updates)


def test_run_without_an_execution_row_writes_nothing(recorded_updates):
    """Executions are assistant-owned; a session without one has no ledger."""

    asyncio.run(_active_task(reference=None).result())

    assert recorded_updates == []
