"""
Tests for `TaskScheduler.execute`.

These go through the full `TaskScheduler` surface to cover the integration
layer that retrieves the task from storage, wraps it in `ActiveTask`, and
wires the actor instance for live execution.
"""

from __future__ import annotations

import asyncio
import functools
from typing import Dict
from datetime import datetime, timezone, timedelta

import pytest

from droid.task_scheduler import task_scheduler as task_scheduler_module
from droid.task_scheduler.machine_state import (
    TaskRunProvenance,
    TaskRunReference,
    remember_live_task_run_provenance,
)
from droid.task_scheduler.task_scheduler import TaskScheduler
from droid.actor.simulated import SimulatedActor
from droid.actor.simulated import SimulatedActorHandle
from droid.task_scheduler.types.schedule import Schedule
from droid.task_scheduler.types.activated_by import ActivatedBy
from droid.task_scheduler.types.repetition import Frequency, RepeatPattern
from droid.task_scheduler.types.status import Status
from droid.common.task_execution_context import (
    current_post_run_review_context,
    current_task_execution_delegate,
)

#  The helper used in the existing test‑suite – applies project‑level monkey‐
#  patches (e.g. env vars, tracers) so we keep behaviour consistent.
from tests.helpers import _handle_project

# --------------------------------------------------------------------------- #
#  Test helpers                                                               #
# --------------------------------------------------------------------------- #


async def _make_scheduler_with_task(description: str, *, steps: int = 1):
    """Return *(scheduler, handle)* where *handle* is the active task."""
    # Always keep the simulated actor alive indefinitely; tests will stop explicitly
    actor = SimulatedActor(steps=None, duration=None)
    scheduler = TaskScheduler(actor=actor)

    task_id = scheduler._create_task(name=description, description=description)[
        "details"
    ]["task_id"]
    handle = await scheduler.execute(task_id=task_id)
    return scheduler, handle


def _source_log_id(scheduler: TaskScheduler, task_id: int, instance_id: int = 0) -> int:
    return scheduler._get_log_by_task_instance(
        task_id=task_id,
        instance_id=instance_id,
    ).id


@pytest.mark.asyncio
@pytest.mark.llm_call
@_handle_project
async def test_execute_uses_trigger_source_type_when_attempt_token_present(monkeypatch):
    """Trigger execution should consume provenance with the explicit attempt token."""

    actor = SimulatedActor(steps=None, duration=None)
    scheduler = TaskScheduler(actor=actor)
    task_id = scheduler._create_task(
        name="Trigger-aware run",
        description="Trigger-aware run",
    )["details"]["task_id"]
    captured: dict[str, object] = {}

    monkeypatch.setattr(task_scheduler_module.SESSION_DETAILS.assistant, "agent_id", 42)

    def _fake_consume(**kwargs):
        captured.update(kwargs)
        return None

    monkeypatch.setattr(
        task_scheduler_module,
        "consume_live_task_run_provenance",
        _fake_consume,
    )

    handle = await scheduler.execute(
        task_id=task_id,
        trigger_attempt_token="trigger-123",
    )
    await handle.stop(cancel=False)
    await handle.result()

    assert captured["task_id"] == task_id
    assert captured["assistant_id"] == 42
    assert captured["source_type"] == "triggered"
    assert captured["trigger_attempt_token"] == "trigger-123"


@pytest.mark.asyncio
@pytest.mark.llm_call
@_handle_project
async def test_execute_materializes_live_run_after_actor_start(monkeypatch):
    """Execution should create/adopt the live run row only after the actor starts."""

    actor = SimulatedActor(steps=None, duration=None)
    scheduler = TaskScheduler(actor=actor)
    task_id = scheduler._create_task(
        name="Persist live run",
        description="Persist live run",
    )["details"]["task_id"]
    captured: dict[str, object] = {}
    events: list[str] = []

    monkeypatch.setattr(task_scheduler_module.SESSION_DETAILS.assistant, "agent_id", 42)
    monkeypatch.setattr(
        task_scheduler_module,
        "consume_live_task_run_provenance",
        lambda **_: TaskRunProvenance(
            assistant_id="42",
            task_id=task_id,
            source_type="explicit",
            execution_mode="live",
        ),
    )

    original_act = SimulatedActor.act

    async def _spy_act(self, *args, **kwargs):
        events.append("act")
        return await original_act(self, *args, **kwargs)

    monkeypatch.setattr(SimulatedActor, "act", _spy_act)

    def _fake_create_or_adopt(provenance: TaskRunProvenance) -> TaskRunReference:
        events.append("materialize")
        captured["provenance"] = provenance
        return TaskRunReference(
            assistant_id=provenance.assistant_id,
            run_key="live:explicit:42:1:rev:once",
        )

    monkeypatch.setattr(
        "droid.task_scheduler.active_task.create_or_adopt_live_task_run",
        _fake_create_or_adopt,
    )
    monkeypatch.setattr(
        "droid.task_scheduler.active_task.update_task_run_record",
        lambda *args, **kwargs: None,
    )

    handle = await scheduler.execute(task_id=task_id)
    await handle.stop(cancel=False)
    await handle.result()

    provenance = captured["provenance"]
    assert isinstance(provenance, TaskRunProvenance)
    assert provenance.task_id == task_id
    assert provenance.assistant_id == "42"
    assert provenance.execution_mode == "live"
    assert events == ["act", "materialize"]


@pytest.mark.asyncio
@pytest.mark.llm_call
@_handle_project
async def test_scheduled_execution_consumes_provenance_and_rearms(monkeypatch):
    actor = SimulatedActor(steps=0)
    scheduler = TaskScheduler(actor=actor)
    task_id = scheduler._create_task(
        name="Scheduled report",
        description="Send the scheduled report.",
        status=Status.scheduled,
        schedule=Schedule(
            start_at=(
                datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=1)
            ).isoformat(),
        ),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
    )["details"]["task_id"]
    scheduled_for = scheduler._get_task_or_raise(task_id).schedule_start_at.isoformat()
    captured: dict[str, object] = {}
    run_updates: list[tuple[TaskRunReference | None, dict]] = []

    monkeypatch.setattr(task_scheduler_module.SESSION_DETAILS.assistant, "agent_id", 42)
    remember_live_task_run_provenance(
        TaskRunProvenance(
            assistant_id="42",
            task_id=task_id,
            source_type="scheduled",
            execution_mode="live",
            source_task_log_id=_source_log_id(scheduler, task_id),
            activation_revision="rev-scheduled",
            scheduled_for=scheduled_for,
            task_name="Scheduled report",
            task_description="Send the scheduled report.",
        ),
    )

    def _fake_create_or_adopt(provenance: TaskRunProvenance) -> TaskRunReference:
        captured["provenance"] = provenance
        return TaskRunReference(
            assistant_id=provenance.assistant_id,
            run_key="live:scheduled:42:0:rev-scheduled:once",
        )

    monkeypatch.setattr(
        "droid.task_scheduler.active_task.create_or_adopt_live_task_run",
        _fake_create_or_adopt,
    )
    monkeypatch.setattr(
        "droid.task_scheduler.active_task.update_task_run_record",
        lambda run_reference, updates: run_updates.append(
            (run_reference, dict(updates)),
        ),
    )

    handle = await scheduler.execute(
        task_id=task_id,
        _activated_by=ActivatedBy.schedule,
    )
    await handle.result()

    provenance = captured["provenance"]
    assert isinstance(provenance, TaskRunProvenance)
    assert provenance.source_type == "scheduled"
    assert provenance.execution_mode == "live"
    assert provenance.scheduled_for == scheduled_for
    assert run_updates
    assert run_updates[-1][0] == TaskRunReference(
        assistant_id="42",
        run_key="live:scheduled:42:0:rev-scheduled:once",
    )
    assert run_updates[-1][1]["state"] == "completed"
    assert run_updates[-1][1]["completed_at"]

    rows = sorted(
        scheduler._filter_tasks(filter=f"task_id == {task_id}"),
        key=lambda task: task.instance_id,
    )
    assert [row.instance_id for row in rows] == [0, 1]
    assert rows[0].status == Status.completed
    assert rows[1].status == Status.scheduled


@pytest.mark.asyncio
@pytest.mark.llm_call
@_handle_project
async def test_scheduled_execution_live_delegate_materializes_run_and_rearms(
    monkeypatch,
):
    scheduler = TaskScheduler()
    task_id = scheduler._create_task(
        name="Delegate live report",
        description="Send the live delegated report.",
        status=Status.scheduled,
        schedule=Schedule(
            start_at=(
                datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=1)
            ).isoformat(),
        ),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
    )["details"]["task_id"]
    scheduled_for = scheduler._get_task_or_raise(task_id).schedule_start_at.isoformat()
    captured: dict[str, object] = {}
    run_updates: list[tuple[TaskRunReference | None, dict]] = []

    class _DelegateHandle:
        async def result(self):
            return "live delegate completed"

    class _LiveDelegate:
        async def start_task_run(self, **kwargs):
            captured["delegate_kwargs"] = kwargs
            return _DelegateHandle()

    def _fake_create_or_adopt(provenance: TaskRunProvenance) -> TaskRunReference:
        captured["provenance"] = provenance
        return TaskRunReference(
            assistant_id=provenance.assistant_id,
            run_key="live:scheduled:42:delegate:once",
        )

    monkeypatch.setattr(task_scheduler_module.SESSION_DETAILS.assistant, "agent_id", 42)
    remember_live_task_run_provenance(
        TaskRunProvenance(
            assistant_id="42",
            task_id=task_id,
            source_type="scheduled",
            execution_mode="live",
            source_task_log_id=_source_log_id(scheduler, task_id),
            activation_revision="rev-live-delegate",
            scheduled_for=scheduled_for,
            task_name="Delegate live report",
            task_description="Send the live delegated report.",
        ),
    )
    monkeypatch.setattr(
        "droid.task_scheduler.active_task.create_or_adopt_live_task_run",
        _fake_create_or_adopt,
    )
    monkeypatch.setattr(
        "droid.task_scheduler.active_task.update_task_run_record",
        lambda run_reference, updates: run_updates.append(
            (run_reference, dict(updates)),
        ),
    )

    token = current_task_execution_delegate.set(_LiveDelegate())
    try:
        handle = await scheduler.execute(
            task_id=task_id,
            _activated_by=ActivatedBy.schedule,
        )
        result = await handle.result()
    finally:
        current_task_execution_delegate.reset(token)

    assert result == "live delegate completed"
    delegate_kwargs = captured["delegate_kwargs"]
    assert delegate_kwargs["entrypoint"] is None
    assert "Send the live delegated report." in delegate_kwargs["task_description"]
    provenance = captured["provenance"]
    assert isinstance(provenance, TaskRunProvenance)
    assert provenance.execution_mode == "live"
    assert run_updates[-1][0] == TaskRunReference(
        assistant_id="42",
        run_key="live:scheduled:42:delegate:once",
    )
    assert run_updates[-1][1]["state"] == "completed"

    rows = sorted(
        scheduler._filter_tasks(filter=f"task_id == {task_id}"),
        key=lambda task: task.instance_id,
    )
    assert [row.instance_id for row in rows] == [0, 1]
    assert rows[0].status == Status.completed
    assert rows[1].status == Status.scheduled


@pytest.mark.asyncio
@pytest.mark.llm_call
@_handle_project
async def test_scheduled_execution_offline_delegate_materializes_run_and_rearms(
    monkeypatch,
):
    from droid.task_scheduler import offline_runner

    scheduler = TaskScheduler()
    task_id = scheduler._create_task(
        name="Offline report",
        description="Send the offline delegated report.",
        status=Status.scheduled,
        schedule=Schedule(
            start_at=(
                datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=1)
            ).isoformat(),
        ),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
        entrypoint=777,
        offline=True,
    )["details"]["task_id"]
    scheduled_for = scheduler._get_task_or_raise(task_id).schedule_start_at.isoformat()
    captured: dict[str, object] = {}
    run_updates: list[tuple[TaskRunReference | None, dict]] = []

    class _FunctionHandle:
        async def result(self):
            return "sent"

    class _FakeOfflineActor:
        def __init__(self):
            captured["actor_created"] = True

        async def act(self, request, **kwargs):
            captured["function_request"] = request
            captured["function_id"] = kwargs["entrypoint"]
            captured["entrypoint_kwargs"] = kwargs["entrypoint_kwargs"]
            captured["clarification_enabled"] = kwargs["clarification_enabled"]
            captured["persist"] = kwargs["persist"]
            return _FunctionHandle()

        async def close(self):
            captured["actor_closed"] = True

    def _fake_create_or_adopt(provenance: TaskRunProvenance) -> TaskRunReference:
        captured["provenance"] = provenance
        return TaskRunReference(
            assistant_id=provenance.assistant_id,
            run_key="offline:scheduled:42:delegate:once",
        )

    monkeypatch.setattr(task_scheduler_module.SESSION_DETAILS.assistant, "agent_id", 42)
    monkeypatch.setattr(
        offline_runner,
        "_build_offline_actor",
        _FakeOfflineActor,
    )
    remember_live_task_run_provenance(
        TaskRunProvenance(
            assistant_id="42",
            task_id=task_id,
            source_type="scheduled",
            execution_mode="offline",
            source_task_log_id=_source_log_id(scheduler, task_id),
            activation_revision="rev-offline-delegate",
            scheduled_for=scheduled_for,
            task_name="Offline report",
            task_description="Send the offline delegated report.",
        ),
    )
    monkeypatch.setattr(
        "droid.task_scheduler.active_task.create_or_adopt_live_task_run",
        _fake_create_or_adopt,
    )
    monkeypatch.setattr(
        "droid.task_scheduler.active_task.update_task_run_record",
        lambda run_reference, updates: run_updates.append(
            (run_reference, dict(updates)),
        ),
    )

    config = offline_runner.OfflineTaskConfig(
        assistant_id="42",
        run_key="offline:scheduled:42:delegate:once",
        task_id=task_id,
        function_id=777,
        request="Send the offline delegated report.",
        source_type="scheduled",
        source_task_log_id=_source_log_id(scheduler, task_id),
        activation_revision="rev-offline-delegate",
        task_name="Offline report",
        task_description="Send the offline delegated report.",
        scheduled_for=scheduled_for,
    )
    delegate = offline_runner._OfflineTaskExecutionDelegate(config)
    token = current_task_execution_delegate.set(delegate)
    try:
        handle = await scheduler.execute(
            task_id=task_id,
            _activated_by=ActivatedBy.schedule,
        )
        result = await handle.result()
    finally:
        current_task_execution_delegate.reset(token)
        await delegate.close()

    assert '"function_id": 777' in result
    assert captured["function_id"] == 777
    assert "Send the offline delegated report." in captured["function_request"]
    assert captured["entrypoint_kwargs"]["scheduled_run_timestamp"] == scheduled_for
    assert captured["clarification_enabled"] is False
    assert captured["persist"] is False
    assert captured["actor_closed"] is True
    provenance = captured["provenance"]
    assert isinstance(provenance, TaskRunProvenance)
    assert provenance.execution_mode == "offline"
    assert run_updates[-1][0] == TaskRunReference(
        assistant_id="42",
        run_key="offline:scheduled:42:delegate:once",
    )
    assert run_updates[-1][1]["state"] == "completed"
    assert '"result": "sent"' in run_updates[-1][1]["result_summary"]

    rows = sorted(
        scheduler._filter_tasks(filter=f"task_id == {task_id}"),
        key=lambda task: task.instance_id,
    )
    assert [row.instance_id for row in rows] == [0, 1]
    assert rows[0].status == Status.completed
    assert rows[1].status == Status.scheduled


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", [None, 777])
@_handle_project
async def test_offline_recurring_execution_uses_physical_source_instance(
    monkeypatch,
    entrypoint,
):
    from droid.task_scheduler import offline_runner

    scheduler = TaskScheduler()
    task_id = scheduler._create_task(
        name=f"Offline recurring {'symbolic' if entrypoint else 'agentic'}",
        description="Run the offline recurring physical instance.",
        status=Status.scheduled,
        schedule=Schedule(
            start_at=(
                datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=1)
            ).isoformat(),
        ),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
        entrypoint=entrypoint,
        offline=True,
    )["details"]["task_id"]
    captured: list[dict[str, object]] = []

    class _Handle:
        async def result(self):
            return "done"

    class _FakeOfflineActor:
        async def act(self, request, **kwargs):
            captured.append({"request": request, "kwargs": kwargs})
            return _Handle()

        async def close(self):
            return None

    monkeypatch.setattr(task_scheduler_module.SESSION_DETAILS.assistant, "agent_id", 42)
    monkeypatch.setattr(offline_runner, "_build_offline_actor", _FakeOfflineActor)
    monkeypatch.setattr(
        "droid.task_scheduler.active_task.create_or_adopt_live_task_run",
        lambda provenance: TaskRunReference(
            assistant_id=provenance.assistant_id,
            run_key=(
                f"{provenance.execution_mode}:{provenance.source_type}:"
                f"{provenance.assistant_id}:{provenance.task_id}:"
                f"{provenance.activation_revision}"
            ),
        ),
    )
    monkeypatch.setattr(
        "droid.task_scheduler.active_task.update_task_run_record",
        lambda run_reference, updates: None,
    )

    for instance_id in range(3):
        row = [
            task
            for task in scheduler._filter_tasks(filter=f"task_id == {task_id}")
            if task.instance_id == instance_id
        ][0]
        config = offline_runner.OfflineTaskConfig(
            assistant_id="42",
            run_key=f"offline:scheduled:42:{task_id}:instance-{instance_id}",
            task_id=task_id,
            function_id=entrypoint,
            request="Run the offline recurring physical instance.",
            source_type="scheduled",
            source_task_log_id=_source_log_id(scheduler, task_id, instance_id),
            activation_revision=f"rev-instance-{instance_id}",
            task_name=row.name,
            task_description=row.description,
            scheduled_for=row.schedule_start_at.isoformat(),
        )
        delegate = offline_runner._OfflineTaskExecutionDelegate(config)
        token = current_task_execution_delegate.set(delegate)
        try:
            handle = await scheduler.execute(
                task_id=task_id,
                _activated_by=ActivatedBy.schedule,
            )
            await handle.result()
        finally:
            current_task_execution_delegate.reset(token)
            await delegate.close()

    rows = sorted(
        scheduler._filter_tasks(filter=f"task_id == {task_id}"),
        key=lambda task: task.instance_id,
    )
    assert [row.instance_id for row in rows] == [0, 1, 2, 3]
    assert [row.status for row in rows[:3]] == [
        Status.completed,
        Status.completed,
        Status.completed,
    ]
    assert rows[3].status == Status.scheduled
    for index, call in enumerate(captured):
        assert f"Instance id: {index}" in call["request"]
        kwargs = call["kwargs"]
        if entrypoint is None:
            assert kwargs["entrypoint"] is None
            assert kwargs["entrypoint_kwargs"] is None
        else:
            assert kwargs["entrypoint"] == entrypoint
            assert kwargs["entrypoint_kwargs"]["instance_id"] == index


@pytest.mark.asyncio
@_handle_project
async def test_offline_scheduled_execution_reconciles_stale_active_blocker(
    monkeypatch,
):
    from droid.task_scheduler import offline_runner

    scheduler = TaskScheduler()
    task_id = scheduler._create_task(
        name="Offline stale active blocker",
        description="Run the current scheduled instance.",
        status=Status.scheduled,
        schedule=Schedule(
            start_at=(
                datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=1)
            ).isoformat(),
        ),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
        offline=True,
    )["details"]["task_id"]
    first = scheduler._filter_tasks(filter=f"task_id == {task_id}")[0]
    scheduler._clone_task_instance(first)
    first_source_log_id = _source_log_id(scheduler, task_id, 0)
    second_source_log_id = _source_log_id(scheduler, task_id, 1)
    scheduler._update_task_status_instance(
        task_id=task_id,
        instance_id=0,
        new_status=Status.active,
        activated_by=ActivatedBy.schedule,
    )

    class _Handle:
        async def result(self):
            return "done"

    class _FakeOfflineActor:
        async def act(self, request, **kwargs):
            return _Handle()

        async def close(self):
            return None

    monkeypatch.setattr(task_scheduler_module.SESSION_DETAILS.assistant, "agent_id", 42)
    monkeypatch.setattr(offline_runner, "_build_offline_actor", _FakeOfflineActor)
    monkeypatch.setattr(
        "droid.task_scheduler.active_task.create_or_adopt_live_task_run",
        lambda provenance: TaskRunReference(
            assistant_id=provenance.assistant_id,
            run_key=(
                f"{provenance.execution_mode}:{provenance.source_type}:"
                f"{provenance.assistant_id}:{provenance.task_id}:"
                f"{provenance.activation_revision}"
            ),
        ),
    )
    monkeypatch.setattr(
        "droid.task_scheduler.active_task.update_task_run_record",
        lambda run_reference, updates: None,
    )
    reconciled_run_updates = []
    expected_task_id = task_id

    def _latest_run_for_source(*, assistant_id, task_id, source_task_log_id):
        assert assistant_id == "42"
        assert task_id == expected_task_id
        assert source_task_log_id == first_source_log_id
        return TaskRunReference(
            assistant_id="42",
            run_key=f"offline:scheduled:42:{expected_task_id}:instance-0",
        )

    monkeypatch.setattr(
        task_scheduler_module,
        "latest_task_run_reference_for_source",
        _latest_run_for_source,
    )
    monkeypatch.setattr(
        task_scheduler_module,
        "update_task_run_record",
        lambda run_reference, updates: reconciled_run_updates.append(
            (run_reference, updates),
        ),
    )

    current = [
        task
        for task in scheduler._filter_tasks(filter=f"task_id == {task_id}")
        if task.instance_id == 1
    ][0]
    config = offline_runner.OfflineTaskConfig(
        assistant_id="42",
        run_key=f"offline:scheduled:42:{task_id}:instance-1",
        task_id=task_id,
        function_id=None,
        request="Run the current scheduled instance.",
        source_type="scheduled",
        source_task_log_id=second_source_log_id,
        activation_revision="rev-instance-1",
        task_name=current.name,
        task_description=current.description,
        scheduled_for=current.schedule_start_at.isoformat(),
    )
    remember_live_task_run_provenance(
        TaskRunProvenance(
            assistant_id="42",
            task_id=task_id,
            source_type="scheduled",
            execution_mode="offline",
            source_task_log_id=second_source_log_id,
            activation_revision="rev-instance-1",
            scheduled_for=current.schedule_start_at.isoformat(),
            task_name=current.name,
            task_description=current.description,
        ),
    )
    delegate = offline_runner._OfflineTaskExecutionDelegate(config)
    token = current_task_execution_delegate.set(delegate)
    try:
        handle = await scheduler.execute(
            task_id=task_id,
            _activated_by=ActivatedBy.schedule,
        )
        await handle.result()
    finally:
        current_task_execution_delegate.reset(token)
        await delegate.close()

    rows = sorted(
        scheduler._filter_tasks(filter=f"task_id == {task_id}"),
        key=lambda task: task.instance_id,
    )
    assert first_source_log_id != second_source_log_id
    assert rows[0].status == Status.failed
    assert "offline lifecycle reconciliation" in (rows[0].info or "")
    assert rows[1].status == Status.completed
    assert rows[2].status == Status.scheduled
    assert len(reconciled_run_updates) == 1
    run_reference, updates = reconciled_run_updates[0]
    assert run_reference == TaskRunReference(
        assistant_id="42",
        run_key=f"offline:scheduled:42:{expected_task_id}:instance-0",
    )
    assert updates["state"] == "failed"
    assert updates["reconciliation_reason"] == "offline_active_row_reconciliation"
    assert "offline lifecycle reconciliation" in updates["error"]


@pytest.mark.asyncio
@pytest.mark.llm_call
@_handle_project
async def test_triggered_execution_offline_delegate_consumes_trigger_provenance(
    monkeypatch,
):
    from droid.task_scheduler import offline_runner

    scheduler = TaskScheduler()
    task_id = scheduler._create_task(
        name="Triggered offline report",
        description="Send the triggered offline delegated report.",
        status=Status.triggerable,
        entrypoint=777,
        offline=True,
    )["details"]["task_id"]
    captured: dict[str, object] = {}
    run_updates: list[tuple[TaskRunReference | None, dict]] = []

    class _FunctionHandle:
        async def result(self):
            return "sent"

    class _FakeOfflineActor:
        def __init__(self):
            pass

        async def act(self, request, **kwargs):
            captured["function_request"] = request
            captured["function_id"] = kwargs["entrypoint"]
            return _FunctionHandle()

        async def close(self):
            captured["actor_closed"] = True

    def _fake_create_or_adopt(provenance: TaskRunProvenance) -> TaskRunReference:
        captured["provenance"] = provenance
        return TaskRunReference(
            assistant_id=provenance.assistant_id,
            run_key="offline:triggered:42:delegate:once",
        )

    monkeypatch.setattr(task_scheduler_module.SESSION_DETAILS.assistant, "agent_id", 42)
    monkeypatch.setattr(
        offline_runner,
        "_build_offline_actor",
        _FakeOfflineActor,
    )
    monkeypatch.setattr(
        "droid.task_scheduler.active_task.create_or_adopt_live_task_run",
        _fake_create_or_adopt,
    )
    monkeypatch.setattr(
        "droid.task_scheduler.active_task.update_task_run_record",
        lambda run_reference, updates: run_updates.append(
            (run_reference, dict(updates)),
        ),
    )

    config = offline_runner.OfflineTaskConfig(
        assistant_id="42",
        run_key="offline:triggered:42:delegate:once",
        task_id=task_id,
        function_id=777,
        request="Send the triggered offline delegated report.",
        source_type="triggered",
        source_task_log_id=_source_log_id(scheduler, task_id),
        activation_revision="rev-triggered-offline-delegate",
        task_name="Triggered offline report",
        task_description="Send the triggered offline delegated report.",
        source_ref="sms-message-123",
        source_medium="sms",
        source_contact_id="123",
    )

    await offline_runner._execute_scheduler_managed_task(config)

    provenance = captured["provenance"]
    assert isinstance(provenance, TaskRunProvenance)
    assert provenance.source_type == "triggered"
    assert provenance.execution_mode == "offline"
    assert provenance.source_ref == "sms-message-123"
    assert provenance.source_medium == "sms"
    assert provenance.source_contact_id == "123"
    assert provenance.attempt_token == "offline:triggered:42:delegate:once"
    assert captured["function_id"] == 777
    assert captured["actor_closed"] is True
    assert run_updates[-1][0] == TaskRunReference(
        assistant_id="42",
        run_key="offline:triggered:42:delegate:once",
    )
    assert run_updates[-1][1]["state"] == "completed"


@pytest.mark.asyncio
@pytest.mark.llm_call
@_handle_project
async def test_symbolic_task_execution_passes_deterministic_entrypoint_kwargs(
    monkeypatch,
):
    scheduler = TaskScheduler()
    task_id = scheduler._create_task(
        name="Symbolic run context",
        description="Run with deterministic context.",
        entrypoint=777,
    )["details"]["task_id"]
    captured: dict[str, object] = {}

    class _Handle:
        async def result(self):
            return "ok"

    class _Delegate:
        async def start_task_run(self, **kwargs):
            captured.update(kwargs)
            return _Handle()

    monkeypatch.setattr(task_scheduler_module.SESSION_DETAILS.assistant, "agent_id", 42)

    token = current_task_execution_delegate.set(_Delegate())
    try:
        handle = await scheduler.execute(task_id=task_id)
        assert await handle.result() == "ok"
    finally:
        current_task_execution_delegate.reset(token)

    entrypoint_kwargs = captured["entrypoint_kwargs"]
    assert isinstance(entrypoint_kwargs, dict)
    assert entrypoint_kwargs["task_id"] == task_id
    assert entrypoint_kwargs["instance_id"] == 0
    assert entrypoint_kwargs["execution_style"] == "symbolic"
    assert entrypoint_kwargs["delivery_mode"] == "live"
    assert entrypoint_kwargs["task_execution_context"]["task_id"] == task_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("offline", "entrypoint", "delivery_mode", "execution_style"),
    [
        (False, None, "live", "agentic"),
        (False, 777, "live", "symbolic"),
        (True, None, "offline", "agentic"),
        (True, 777, "offline", "symbolic"),
    ],
)
@_handle_project
async def test_task_execution_routes_all_delivery_and_style_combinations(
    offline,
    entrypoint,
    delivery_mode,
    execution_style,
):
    scheduler = TaskScheduler()
    task_id = scheduler._create_task(
        name=f"{delivery_mode} {execution_style}",
        description=f"Run the {delivery_mode} {execution_style} combination.",
        entrypoint=entrypoint,
        offline=offline,
    )["details"]["task_id"]
    captured: dict[str, object] = {}

    class _Handle:
        async def result(self):
            return "ok"

    class _Delegate:
        async def start_task_run(self, **kwargs):
            captured.update(kwargs)
            return _Handle()

    token = current_task_execution_delegate.set(_Delegate())
    try:
        handle = await scheduler.execute(task_id=task_id)
        assert await handle.result() == "ok"
    finally:
        current_task_execution_delegate.reset(token)

    assert captured["entrypoint"] == entrypoint
    assert f"Task id: {task_id}" in captured["task_description"]
    assert "Instance id: 0" in captured["task_description"]
    if entrypoint is None:
        assert captured["entrypoint_kwargs"] is None
    else:
        entrypoint_kwargs = captured["entrypoint_kwargs"]
        assert isinstance(entrypoint_kwargs, dict)
        assert entrypoint_kwargs["delivery_mode"] == delivery_mode
        assert entrypoint_kwargs["execution_style"] == execution_style
        assert (
            entrypoint_kwargs["task_execution_context"]["delivery_mode"]
            == delivery_mode
        )


# --------------------------------------------------------------------------- #
#  0. Ask                                                                     #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
@pytest.mark.llm_call
@_handle_project
async def test_execute_ask(monkeypatch):
    """`ActiveTask.ask` should forward to the wrapped plan exactly once."""

    calls: Dict[str, int] = {"ask": 0}

    original_ask = SimulatedActorHandle.ask

    @functools.wraps(original_ask)
    async def spy_ask(self, question: str) -> str:  # type: ignore[override]
        calls["ask"] += 1
        return await original_ask(self, question)

    monkeypatch.setattr(SimulatedActorHandle, "ask", spy_ask, raising=True)

    _scheduler, task = await _make_scheduler_with_task(
        "Analyse new product launch performance.",
    )

    # Perform a read-only ask on the returned handle – should delegate once
    ask_h = await task.ask("Do we have any early metrics?")
    await ask_h.result()

    # Explicitly stop to avoid relying on step-based completion
    await task.stop(cancel=False)
    await task.result()

    assert calls["ask"] == 1, "ask must be called exactly once"


# --------------------------------------------------------------------------- #
#  1. Interjection                                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
@pytest.mark.llm_call
@_handle_project
async def test_execute_interject(monkeypatch):
    """`ActiveTask.interject` should forward to the wrapped plan exactly once."""

    calls: Dict[str, int] = {"interject": 0}

    original_interject = SimulatedActorHandle.interject

    @functools.wraps(original_interject)
    async def spy_interject(self, instruction: str, *, images=None) -> None:  # type: ignore[override]
        calls["interject"] += 1
        await original_interject(self, instruction)

    monkeypatch.setattr(SimulatedActorHandle, "interject", spy_interject, raising=True)

    _scheduler, task = await _make_scheduler_with_task(
        "Investigate competitor pricing.",
    )

    await task.interject("First gather public filings.")
    # Give the background thread one beat to process the step counter.
    await asyncio.sleep(0.2)
    # Gracefully stop to avoid leaking the background thread.
    await task.stop(cancel=False)
    await task.result()

    assert calls["interject"] == 1, "interject must be called exactly once"


# --------------------------------------------------------------------------- #
#  2. Stop                                                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
@pytest.mark.llm_call
@_handle_project
async def test_execute_stop(monkeypatch):
    """Calling `ActiveTask.stop` should proxy to the plan and mark it done."""

    called = {"stop": 0}

    orig_stop = SimulatedActorHandle.stop

    @functools.wraps(orig_stop)
    async def spy_stop(self, reason: str | None = None, **kwargs) -> None:  # type: ignore[override]
        called["stop"] += 1
        await orig_stop(self, reason=reason, **kwargs)

    monkeypatch.setattr(SimulatedActorHandle, "stop", spy_stop, raising=True)

    _scheduler, task = await _make_scheduler_with_task(
        "Extract sentiment from reviews.",
    )

    await task.stop(cancel=False)
    result = await task.result()

    assert called["stop"] == 1, "stop must be invoked exactly once"
    assert task.done(), "`done()` should report True after stopping"


# --------------------------------------------------------------------------- #
#  4. Result & Done Lifecycle                                                 #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
@pytest.mark.llm_call
@_handle_project
async def test_execute_result_and_done():
    """A normal workflow should complete once enough steps have been taken."""

    _scheduler, task = await _make_scheduler_with_task(
        "Compile coverage metrics.",
    )

    # Perform an interjection for activity, then stop explicitly
    await task.interject("Provide initial outline first.")
    await task.stop(cancel=False)
    await task.result()

    assert task.done(), "`done()` must return True after explicit stop"


# --------------------------------------------------------------------------- #
#  A. Activation metadata                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
@pytest.mark.llm_call
@_handle_project
async def test_execute_sets_activated_by_explicit():
    """Starting a task explicitly via execute should set activated_by='explicit'."""

    actor = SimulatedActor(steps=None, duration=None)
    ts = TaskScheduler(actor=actor)

    name = "Simple task"
    task_id = ts._create_task(name=name, description=name)["details"]["task_id"]

    # Start by id (fast-path)
    handle = await ts.execute(task_id=task_id)
    await handle.stop(cancel=False)
    await handle.result()

    # Verify activated_by on the activated instance (may already be completed)
    rows = ts._filter_tasks(filter=f"task_id == {task_id}")
    assert any(r.activated_by == ActivatedBy.explicit for r in rows)


@pytest.mark.asyncio
@_handle_project
async def test_execute_without_delegate_or_actor_fails_before_mutation():
    ts = TaskScheduler()
    task_id = ts._create_task(name="Needs actor", description="Needs actor")["details"][
        "task_id"
    ]
    initial_status = ts._get_task_or_raise(task_id).status

    with pytest.raises(RuntimeError, match="run-scoped actor delegate"):
        await ts.execute(task_id=task_id)

    row = ts._get_task_or_raise(task_id)
    assert row.status == initial_status


@pytest.mark.asyncio
@_handle_project
async def test_direct_description_driven_recurring_execution_passes_entrypoint_review():
    calls = []
    actor = SimulatedActor(steps=0)
    original_act = actor.act

    async def _spy_act(*args, **kwargs):
        calls.append(
            {
                "kwargs": kwargs,
                "post_run_review_context": current_post_run_review_context.get(),
            },
        )
        return await original_act(*args, **kwargs)

    actor.act = _spy_act  # type: ignore[method-assign]
    ts = TaskScheduler(actor=actor)
    task_id = ts._create_task(
        name="Recurring no-entrypoint task",
        description="Run from the natural-language description every day.",
        status=Status.scheduled,
        schedule=Schedule(start_at=datetime.now(timezone.utc)),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
    )["details"]["task_id"]

    handle = await ts.execute(task_id=task_id)
    await handle.result()

    assert "task_entrypoint_review" not in calls[0]["kwargs"]
    post_run_review_context = calls[0]["post_run_review_context"]
    assert post_run_review_context is not None
    assert post_run_review_context.display_label == "Storing reusable workflow"
    review = post_run_review_context.extensions.get("task_entrypoint_review")
    assert review is not None
    assert review["metadata"]["task_id"] == task_id
    assert review["metadata"]["task_name"] == "Recurring no-entrypoint task"
    assert callable(review["attach_entrypoint"])


@pytest.mark.asyncio
@_handle_project
async def test_update_status_cannot_force_active():
    """Direct status updates cannot set 'active' and should not set 'activated_by'."""

    actor = SimulatedActor(steps=None, duration=None)
    ts = TaskScheduler(actor=actor)

    label = "Cannot force active"
    task_id = ts._create_task(name=label, description=label)["details"]["task_id"]

    # Attempt to force 'active' via status update should fail
    with pytest.raises(ValueError):
        ts._update_task(task_id=task_id, status="active")

    # Ensure no activation metadata exists prior to activation
    rows = ts._filter_tasks(filter=f"task_id == {task_id}")
    assert len(rows) == 1
    assert rows[0].activated_by is None

    # Change a non-active status and ensure activated_by remains unset
    ts._update_task(task_id=task_id, status="cancelled")
    rows2 = ts._filter_tasks(filter=f"task_id == {task_id}")
    assert rows2[0].status == Status.cancelled
    assert rows2[0].activated_by is None


@pytest.mark.asyncio
@_handle_project
async def test_tasks_table_has_activated_by_column():
    """The Tasks context should include the activated_by column based on the Task model."""

    actor = SimulatedActor(steps=None, duration=None)
    ts = TaskScheduler(actor=actor)

    # Create any task to ensure context exists
    title = "Column presence check"
    _ = ts._create_task(name=title, description=title)

    cols = ts._list_columns()
    if isinstance(cols, dict):
        assert "activated_by" in cols
    else:
        assert "activated_by" in cols
