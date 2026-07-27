"""ActiveTask run_key resolution for EventBus lineage."""

from __future__ import annotations

import asyncio

import pytest

from unify.common._async_tool.loop_config import TOOL_LOOP_LINEAGE
from unify.events.task_run_lineage import CURRENT_TASK_RUN_LINEAGE
from unify.task_scheduler.active_task import ActiveTask, _resolve_active_task_run_key
from unify.task_scheduler.machine_state import (
    TaskRunProvenance,
    TaskRunReference,
    build_task_run_key,
)
from unify.task_scheduler.types.execution import Delivery, Wake


def test_resolve_run_key_prefers_reference():
    ref = TaskRunReference(
        assistant_id="1",
        run_key="from-ref",
    )
    prov = TaskRunProvenance(
        assistant_id="1",
        task_id=2,
        wake=Wake.scheduled,
        delivery=Delivery.live,
        revision="rev",
    )
    assert (
        _resolve_active_task_run_key(
            task_run_reference=ref,
            task_run_provenance=prov,
        )
        == "from-ref"
    )


def test_resolve_run_key_from_provenance():
    prov = TaskRunProvenance(
        assistant_id="1",
        task_id=42,
        wake=Wake.scheduled,
        delivery=Delivery.live,
        revision="rev-1",
    )
    expected = build_task_run_key(prov)
    assert (
        _resolve_active_task_run_key(
            task_run_reference=None,
            task_run_provenance=prov,
        )
        == expected
    )
    assert expected


def test_resolve_run_key_none_without_inputs():
    assert (
        _resolve_active_task_run_key(
            task_run_reference=None,
            task_run_provenance=None,
        )
        is None
    )


class _CompletedHandle:
    async def result(self) -> str:
        return "task summary"


class _LineageCapturingActor:
    def __init__(self) -> None:
        self.run_lineage = None
        self.tool_loop_lineage = None

    async def act(self, *args, **kwargs) -> _CompletedHandle:
        self.run_lineage = CURRENT_TASK_RUN_LINEAGE.get()
        self.tool_loop_lineage = TOOL_LOOP_LINEAGE.get()
        return _CompletedHandle()


@pytest.mark.asyncio
async def test_active_task_scopes_lineage_before_watcher_result():
    """Actor startup inherits run lineage without retaining reset tokens on its handle."""

    actor = _LineageCapturingActor()
    task = await ActiveTask.create(
        actor,
        task_description="Summarize the inbox.",
        task_id=42,
        instance_id=0,
        task_run_reference=TaskRunReference(
            assistant_id="1",
            run_key="live:scheduled:1:42:revision:once",
        ),
    )

    assert actor.run_lineage is not None
    assert actor.run_lineage.task_id == 42
    assert any("Task.run(task_id=42" in segment for segment in actor.tool_loop_lineage)
    assert CURRENT_TASK_RUN_LINEAGE.get() is None
    assert not TOOL_LOOP_LINEAGE.get()

    # This regression only exercises the handoff to the live watcher. Terminal
    # persistence is covered independently and would require Orchestra here.
    task._task_run_reference = None
    result = await asyncio.create_task(task.result())

    assert result == "task summary"
