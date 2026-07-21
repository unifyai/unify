import asyncio
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

from tests.helpers import _handle_project
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.actor.simulated import SimulatedActor, SimulatedActorHandle
from unify.task_scheduler.types.status import Status
from unify.task_scheduler.active_task import ActiveTask
from unify.task_scheduler.types.repetition import Frequency, RepeatPattern
from unify.task_scheduler.types.schedule import Schedule

# Define a predictable summary string for mocked LLM calls
MOCK_SUMMARY = "Mock summary: Task completed important steps."

pytestmark = pytest.mark.llm_call


# Helper to create a scheduler with a controllable actor for tests
def create_test_scheduler(actor):
    return TaskScheduler(actor=actor if actor else SimulatedActor(steps=0))


@pytest.mark.asyncio
@_handle_project
async def test_summary_on_natural_completion(monkeypatch):
    """
    Verify info is populated when a task completes normally via result().
    """
    # steps=0 completes immediately; steps>=1 waits for tool calls that never
    # happen in this mocked path and would hang await handle.result() forever.
    actor = SimulatedActor(steps=0)
    ts = create_test_scheduler(actor)

    monkeypatch.setattr(
        SimulatedActorHandle,
        "action_log",
        ["Simulated action log entry"],
        raising=False,
    )
    monkeypatch.setattr(
        ActiveTask,
        "_generate_summary_from_log",
        AsyncMock(return_value=MOCK_SUMMARY),
    )

    # Signal when the actual write of status+info occurs (avoid racing teardown)
    summary_saved_event = asyncio.Event()
    original_write_entries = ts._write_log_entries

    def write_entries_probe(*args, **kwargs):
        res = original_write_entries(*args, **kwargs)
        entries = kwargs.get("entries", {})
        # New behavior: status and info are written in separate calls.
        # Trigger when the summary ('info') is written, regardless of status in the same write.
        if isinstance(entries, dict) and entries.get("info") == MOCK_SUMMARY:
            summary_saved_event.set()
        return res

    write_entries_spy = MagicMock(side_effect=write_entries_probe)
    monkeypatch.setattr(TaskScheduler, "_write_log_entries", write_entries_spy)

    task_info = ts._create_task(
        name="Test Complete",
        description="Natural completion test",
    )
    task_id = task_info["details"]["task_id"]

    handle = await ts.execute(task_id=task_id)
    result_text = await handle.result()

    await asyncio.wait_for(summary_saved_event.wait(), timeout=5.0)

    # Assertions
    assert handle.done()
    assert "completed" in result_text.lower()

    # Verify the expected write happened
    assert write_entries_spy.call_count >= 1

    # Find the specific call related to saving the summary (info-only write is expected)
    summary_info_call = None
    status_completed_call = None
    for call in write_entries_spy.call_args_list:
        args, kwargs = call
        entries = kwargs.get("entries", {})
        # Info write
        if isinstance(entries, dict) and entries.get("info") == MOCK_SUMMARY:
            summary_info_call = call
        # Status write (may be in a separate call)
        if isinstance(entries, dict) and entries.get("status") == Status.completed:
            status_completed_call = call
    assert summary_info_call is not None, (
        "Did not find the expected call to _write_log_entries with the correct summary. "
        f"Calls: {write_entries_spy.call_args_list}"
    )
    assert status_completed_call is not None, (
        "Did not find the expected call to _write_log_entries setting status='completed'. "
        f"Calls: {write_entries_spy.call_args_list}"
    )

    # Verify the data in the store (definition row only; executions live separately)
    final_rows = ts._filter_tasks(filter=f"task_id == {task_id}")
    assert final_rows, f"Could not find final row for task_id {task_id}"
    final_row = final_rows[0]
    assert final_row.status == Status.completed.value
    assert final_row.info == MOCK_SUMMARY


@pytest.mark.asyncio
@_handle_project
async def test_summary_on_stop_cancel(monkeypatch):
    """
    Verify info is populated when a task is stopped with cancel=True.
    """
    # Stay live until stop(): steps=0 would auto-complete before cancel,
    # and a positive step budget would hang waiting for simulate_step().
    actor = SimulatedActor(steps=None, duration=None)
    ts = create_test_scheduler(actor)

    monkeypatch.setattr(
        SimulatedActorHandle,
        "action_log",
        ["Simulated action log entry"],
        raising=False,
    )
    monkeypatch.setattr(
        ActiveTask,
        "_generate_summary_from_log",
        AsyncMock(return_value=MOCK_SUMMARY),
    )

    # Spy on scheduler log writes
    original_write_entries = ts._write_log_entries
    write_entries_spy = MagicMock(wraps=original_write_entries)
    monkeypatch.setattr(TaskScheduler, "_write_log_entries", write_entries_spy)

    # Signal when background summary save completes
    summary_saved_event = asyncio.Event()
    original_save_summary = ActiveTask._save_final_summary

    async def patched_save_summary(self, final_status: str):
        try:
            await original_save_summary(self, final_status)
        finally:
            summary_saved_event.set()

    monkeypatch.setattr(ActiveTask, "_save_final_summary", patched_save_summary)

    task_info = ts._create_task(name="Test Stop Cancel", description="Stop cancel test")
    task_id = task_info["details"]["task_id"]

    handle = await ts.execute(task_id=task_id)
    await asyncio.sleep(0.1)

    # Stop the task (cancel) – triggers background summary saving
    stop_reason = "Cancelling explicitly"
    await handle.stop(cancel=True, reason=stop_reason)

    # Wait for the handle to finish
    while not handle.done():
        await asyncio.sleep(0.01)
    result_text = await handle.result()

    # Assertions on handle state
    assert handle.done()
    # Check if result_text indicates stoppage
    assert "stopped" in result_text.lower() or stop_reason in result_text

    # Wait for the background summary task to complete
    await asyncio.wait_for(summary_saved_event.wait(), timeout=5.0)

    # Verify the expected write happened
    assert write_entries_spy.call_count >= 1

    # Expect separate writes: one for status=cancelled, one for info=<summary>
    status_cancelled_found = False
    info_summary_found = False
    for call in write_entries_spy.call_args_list:
        args, kwargs = call
        entries = kwargs.get("entries", {})
        if isinstance(entries, dict) and entries.get("status") == Status.cancelled:
            status_cancelled_found = True
        if isinstance(entries, dict) and entries.get("info") == MOCK_SUMMARY:
            info_summary_found = True

    assert status_cancelled_found, (
        "Did not find the expected call to _write_log_entries setting status='cancelled'. "
        f"Calls: {write_entries_spy.call_args_list}"
    )
    assert info_summary_found, (
        "Did not find the expected call to _write_log_entries saving the summary. "
        f"Calls: {write_entries_spy.call_args_list}"
    )

    # Verify the data in the store
    final_rows = ts._filter_tasks(filter=f"task_id == {task_id}")
    assert final_rows, f"Could not find final row for task_id {task_id}"
    final_row = final_rows[0]
    # In the cancel=True case, the final status *should* be 'cancelled'
    assert final_row.status == Status.cancelled.value
    assert final_row.info == MOCK_SUMMARY


@pytest.mark.asyncio
@_handle_project
async def test_summary_on_execution_error(monkeypatch):
    """
    Verify info is populated even if the underlying actor execution fails.
    """

    # Mock the actor's execution to raise an error
    class ErrorActor(SimulatedActor):
        async def act(self, description: str, **kwargs) -> SimulatedActorHandle:
            # Create a handle that will raise an error when result() is awaited
            class ErrorHandle(SimulatedActorHandle):
                action_log = ["Simulated action log entry before error"]

                async def result(self):
                    await asyncio.sleep(0.01)  # Short delay
                    raise ValueError("Simulated Actor Failure")

            mock_llm = MagicMock()
            return ErrorHandle(
                mock_llm,
                description,
                steps=0,
                duration=None,
            )

    actor = ErrorActor()
    ts = create_test_scheduler(actor)

    monkeypatch.setattr(
        ActiveTask,
        "_generate_summary_from_log",
        AsyncMock(return_value=MOCK_SUMMARY),
    )

    # Signal when the write with status=failed + info occurs
    summary_saved_event = asyncio.Event()
    original_write_entries = ts._write_log_entries

    def write_entries_probe_failed(*args, **kwargs):
        res = original_write_entries(*args, **kwargs)
        entries = kwargs.get("entries", {})
        # Trigger when summary info is written; status is written separately
        if isinstance(entries, dict) and entries.get("info") == MOCK_SUMMARY:
            summary_saved_event.set()
        return res

    write_entries_spy = MagicMock(side_effect=write_entries_probe_failed)
    monkeypatch.setattr(TaskScheduler, "_write_log_entries", write_entries_spy)

    task_info = ts._create_task(name="Test Error", description="Execution error test")
    task_id = task_info["details"]["task_id"]

    # Patch ActiveTask.result to return an error string but keep original scheduling of summary
    original_result = ActiveTask.result

    async def patched_result_for_error(self):
        try:
            return await original_result(self)
        except Exception as e:
            return f"ERROR: Task execution failed: {e}"

    monkeypatch.setattr(ActiveTask, "result", patched_result_for_error)

    handle = await ts.execute(task_id=task_id)

    # Await result, expecting the error text returned by patched_result_for_error
    result_text = await handle.result()

    # Assertions
    assert handle.done()
    assert "ERROR" in result_text
    assert "Simulated Actor Failure" in result_text

    # Wait for the background summary task triggered by the exception handler
    await asyncio.wait_for(summary_saved_event.wait(), timeout=5.0)

    # Verify the expected write happened
    assert write_entries_spy.call_count >= 1

    # Expect separate writes: one with status failed, one with info summary
    status_failed_found = False
    info_summary_found = False
    for call in write_entries_spy.call_args_list:
        args, kwargs = call
        entries = kwargs.get("entries", {})
        if isinstance(entries, dict) and entries.get("status") == Status.failed:
            status_failed_found = True
        if isinstance(entries, dict) and entries.get("info") == MOCK_SUMMARY:
            info_summary_found = True

    assert status_failed_found, (
        "Did not find the expected call to _write_log_entries setting status='failed'. "
        f"Calls: {write_entries_spy.call_args_list}"
    )
    assert info_summary_found, (
        "Did not find the expected call to _write_log_entries saving the summary. "
        f"Calls: {write_entries_spy.call_args_list}"
    )

    # Verify the data in the store
    final_rows = ts._filter_tasks(filter=f"task_id == {task_id}")
    assert final_rows, f"Could not find final row for task_id {task_id}"
    final_row = final_rows[0]
    # The final status should be 'failed'
    assert final_row.status == Status.failed.value
    assert final_row.info == MOCK_SUMMARY


@pytest.mark.asyncio
@_handle_project
async def test_summary_targets_definition_row_for_recurring(monkeypatch):
    """
    Verify recurring runs update the same task definition row's info field.
    """
    actor = SimulatedActor(steps=0)
    ts = create_test_scheduler(actor)

    monkeypatch.setattr(
        SimulatedActorHandle,
        "action_log",
        ["Log for run 0"],
        raising=False,
    )
    mock_generate_summary = AsyncMock(return_value=MOCK_SUMMARY)
    monkeypatch.setattr(ActiveTask, "_generate_summary_from_log", mock_generate_summary)

    summary_saved_event_0 = asyncio.Event()
    summary_saved_event_1 = asyncio.Event()
    definition_info_calls = []
    original_update_definition_info = TaskScheduler._update_task_definition_info

    def check_definition_info_write(self, *, task_id: int, info: str):
        definition_info_calls.append({"task_id": task_id, "info": info})
        if info == MOCK_SUMMARY:
            summary_saved_event_0.set()
        elif info == "Mock summary for run 1":
            summary_saved_event_1.set()
        return original_update_definition_info(self, task_id=task_id, info=info)

    monkeypatch.setattr(
        TaskScheduler,
        "_update_task_definition_info",
        check_definition_info_write,
    )

    initial_start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(
        hours=1,
    )
    task_create_outcome = ts._create_task(
        name="Recurring Test Task",
        description="A task that repeats",
        status=Status.scheduled,
        schedule=Schedule(start_at=initial_start.isoformat()),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
    )
    task_id = task_create_outcome["details"]["task_id"]

    handle_0 = await ts.execute(task_id=task_id)
    await handle_0.result()
    await asyncio.wait_for(summary_saved_event_0.wait(), timeout=5.0)

    definition_rows = ts._filter_tasks(filter=f"task_id == {task_id}")
    assert len(definition_rows) == 1
    assert definition_rows[0].info == MOCK_SUMMARY

    monkeypatch.setattr(
        SimulatedActorHandle,
        "action_log",
        ["Log for run 1"],
        raising=False,
    )
    mock_generate_summary.return_value = "Mock summary for run 1"
    EXPECTED_SUMMARY_1 = "Mock summary for run 1"

    handle_1 = await ts.execute(task_id=task_id)
    await handle_1.result()
    await asyncio.wait_for(summary_saved_event_1.wait(), timeout=5.0)

    definition_rows_after_second = ts._filter_tasks(filter=f"task_id == {task_id}")
    assert len(definition_rows_after_second) == 1
    assert definition_rows_after_second[0].info == EXPECTED_SUMMARY_1
    assert all(call["task_id"] == task_id for call in definition_info_calls)
