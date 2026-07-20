"""Live provider-event handles must be watched so instances reach completed."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.helpers import _handle_project
from tests.task_scheduler.test_provider_event_captured_instance import (
    _seed_provider_event_definition,
)
from unify.actor.simulated import SimulatedActor
from unify.conversation_manager.domains.task_activation import (
    _register_live_task_handle,
)
from unify.task_scheduler.active_task import ActiveTask
from unify.task_scheduler.prompt_builders import build_provider_event_run_guidelines
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.task_scheduler.types.status import Status


@pytest.mark.asyncio
@_handle_project
async def test_registered_provider_event_handle_marks_captured_instance_completed():
    """CM's live-handle watcher is what flips an active PE instance to completed."""

    actor = SimulatedActor(steps=0)
    scheduler = TaskScheduler(actor=actor)
    task_id = _seed_provider_event_definition(scheduler, task_revision=3)
    definition = scheduler._get_task_or_raise(task_id)
    instance = scheduler._create_provider_event_captured_instance(
        definition=definition,
        operation_id="op-complete-1",
        captured_task_revision=3,
        binding_id="binding-1",
    )
    scheduler._update_task_status_instance(
        task_id=instance.task_id,
        instance_id=instance.instance_id,
        new_status=Status.active,
        activated_by="explicit",
    )

    handle = await ActiveTask.create(
        actor,
        task_description=(
            f"Execute provider-event task {instance.task_id} "
            f"instance {instance.instance_id}."
        ),
        task_id=instance.task_id,
        instance_id=instance.instance_id,
        scheduler=scheduler,
        task_guidelines=build_provider_event_run_guidelines(instance),
    )

    cm = SimpleNamespace(
        in_flight_actions={},
        event_broker=SimpleNamespace(publish=AsyncMock()),
        _current_snapshot_state=None,
    )
    await _register_live_task_handle(
        cm,
        handle=handle,
        query=f"Provider event started task {task_id} (operation op-complete-1).",
    )
    assert cm.in_flight_actions, "live PE start must register a watched handle"

    for _ in range(100):
        rows = scheduler._filter_tasks(filter=f"task_id == {task_id}")
        captured = next(row for row in rows if row.instance_id == instance.instance_id)
        if captured.status == Status.completed:
            break
        await asyncio.sleep(0.05)
    else:
        pytest.fail(
            "watched provider-event instance stayed non-completed; "
            f"status={captured.status!r}",
        )

    definition_after = next(row for row in rows if row.instance_id == 0)
    assert definition_after.status == Status.triggerable
