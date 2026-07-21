"""Live provider-event handles must be watched so executions reach completed."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from tests.helpers import _handle_project
from tests.task_scheduler.test_provider_event_captured_instance import (
    _request,
    _seed_provider_event_definition,
)
from unify.actor.simulated import SimulatedActor
from unify.conversation_manager.domains.task_execution import (
    _register_live_task_handle,
)
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.task_scheduler.types.status import Status


@pytest.mark.asyncio
@_handle_project
async def test_registered_provider_event_handle_preserves_definition_status():
    """CM's live-handle watcher completes the run without mutating definition."""

    actor = SimulatedActor(steps=0)
    scheduler = TaskScheduler(actor=actor)
    task_id = _seed_provider_event_definition(scheduler, task_revision=3)
    untrusted = {"kind": "provider_event_context", "trust": "untrusted_data"}

    with patch("unify.task_scheduler.task_scheduler.update_task_run_record"):
        handle = await scheduler.start_provider_event_instance(
            request=_request(task_id=task_id, operation_id="op-complete-1"),
            captured_task_revision=3,
            provider_event_context=untrusted,
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

    await asyncio.wait_for(handle.result(), timeout=5.0)

    rows = scheduler._filter_tasks(filter=f"task_id == {task_id}")
    definition_row = rows[0]
    assert definition_row.instance_id == 0
    assert definition_row.status == Status.triggerable
