from tests.helpers import _handle_project
from unify.task_scheduler.task_scheduler import TaskScheduler
import pytest
from types import SimpleNamespace


@_handle_project
def test_cancel_single_task():
    """Cancelling a single active task should set its status to 'cancelled'."""
    ts = TaskScheduler()

    # Create an active task (id will be 0)
    ts._create_task(
        name="Follow-up with client",
        description="Send a thank-you email and next-steps proposal.",
    )

    # Cancel the task
    ts._cancel_tasks([0])

    # Verify the task was cancelled
    tasks = ts._filter_tasks()
    assert tasks[0].enabled is False


@_handle_project
def test_cancel_multiple_tasks():
    """Cancelling multiple tasks at once should update all of their statuses."""
    ts = TaskScheduler()

    # Create two tasks (ids 0 and 1)
    ts._create_task(
        name="Prepare quarterly report",
        description="Compile Q1 financials into slide deck.",
    )
    ts._create_task(
        name="Schedule team off-site",
        description="Book venue and send calendar invites.",
    )

    # Cancel both tasks
    ts._cancel_tasks([0, 1])

    # Verify both tasks were cancelled
    tasks = ts._filter_tasks()
    armed_by_id = {t.task_id: t.enabled for t in tasks}
    assert armed_by_id[0] is False
    assert armed_by_id[1] is False


@_handle_project
def test_cancel_completed_task_raises(monkeypatch):
    """Cancelling a one-shot that already ran is rejected.

    "Already ran" is derived from a terminal Tasks/Executions row, so the run
    ledger is stubbed here — a session without an assistant has no ledger of
    its own to write to.
    """
    ts = TaskScheduler()

    task_id = ts._create_task(
        name="Ship version 1.0",
        description="Publish release notes and push tags.",
    )["details"]["task_id"]

    monkeypatch.setattr(
        "unify.task_scheduler.task_scheduler.find_terminal_execution_for_task",
        lambda **kwargs: SimpleNamespace(run_key="run-done", state="completed"),
    )

    with pytest.raises(ValueError, match="completed"):
        ts._cancel_tasks([task_id])


@_handle_project
def test_cancel_disarmed_task_without_a_run_is_allowed():
    """A paused one-shot has no terminal run, so it stays cancellable."""
    ts = TaskScheduler()

    task_id = ts._create_task(
        name="Paused release",
        description="Not started yet.",
    )[
        "details"
    ]["task_id"]
    ts._set_tasks_enabled(task_ids=task_id, enabled=False)

    ts._cancel_tasks([task_id])
    assert ts._filter_tasks(filter=f"task_id == {task_id}")[0].enabled is False
