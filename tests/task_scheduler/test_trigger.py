"""
Unit-tests for the new **event-triggered** task functionality.
"""

from __future__ import annotations
from datetime import datetime, timezone, timedelta

import pytest

from tests.helpers import _handle_project
from unify.actor.simulated import SimulatedActor
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.task_scheduler.types.status import Status
from unify.task_scheduler.types.schedule import Schedule
from unify.task_scheduler.types.trigger import Trigger, Medium

# --------------------------------------------------------------------------- #
# 1.  Creation – triggerable tasks                                            #
# --------------------------------------------------------------------------- #


@_handle_project
def test_create_triggerable_task():
    ts = TaskScheduler()

    trig = Trigger(
        medium=Medium.EMAIL,
        from_contact_ids=[42],
        recurring=False,
    )

    task_id = ts._create_task(
        name="Wait for client reply",
        description="Begin work once the customer emails us back.",
        trigger=trig,
    )["details"]["task_id"]

    row = ts._filter_tasks(filter=f"task_id == {task_id}", limit=1)[0]
    assert row.status == Status.triggerable
    assert row.schedule is None
    assert row.trigger is not None
    assert row.trigger.medium == Medium.EMAIL
    assert row.trigger.from_contact_ids == [42]


# --------------------------------------------------------------------------- #
# 2.  Mutual exclusion (schedule × trigger)                                   #
# --------------------------------------------------------------------------- #


@_handle_project
def test_schedule_and_trigger_mutually_exclusive():
    ts = TaskScheduler()

    trig = Trigger(medium=Medium.SMS_MESSAGE)
    sched = Schedule(start_at=datetime.now(timezone.utc) + timedelta(hours=1))

    with pytest.raises(ValueError):
        ts._create_task(
            name="Bad combo task",
            description="Should fail – both schedule and trigger.",
            schedule=sched,
            trigger=trig,
        )


# --------------------------------------------------------------------------- #
# 3.  Adding a trigger to a *scheduled* task should fail                      #
# --------------------------------------------------------------------------- #


@_handle_project
def test_update_trigger_on_scheduled_task_raises():
    ts = TaskScheduler()

    future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    tid = ts._create_task(
        name="Morning maintenance window",
        description="Auto-patch servers tomorrow.",
        schedule=Schedule(start_at=future),
    )["details"]["task_id"]

    with pytest.raises(ValueError):
        ts._update_task(
            task_id=tid,
            trigger=Trigger(medium=Medium.SMS_MESSAGE),
        )


# --------------------------------------------------------------------------- #
# 4.  Removing the trigger ⇢ status falls back to *scheduled*                 #
# --------------------------------------------------------------------------- #


@_handle_project
def test_clear_trigger_transitions_status():
    ts = TaskScheduler()
    trig = Trigger(medium=Medium.PHONE_CALL)

    tid = ts._create_task(
        name="Answer support hotline",
        description="Pick up when the phone rings.",
        trigger=trig,
    )["details"]["task_id"]

    ts._update_task(task_id=tid, trigger=None)

    row = ts._filter_tasks(filter=f"task_id == {tid}", limit=1)[0]
    assert row.trigger is None
    assert row.status == Status.scheduled


# --------------------------------------------------------------------------- #
# 5.  Disallow start_at edits on trigger-based tasks                          #
# --------------------------------------------------------------------------- #


@_handle_project
def test_start_at_on_trigger_task_raises():
    ts = TaskScheduler()
    trig = Trigger(medium=Medium.PHONE_CALL)

    tid = ts._create_task(
        name="Respond to phone call",
        description="Jump on the phone call when it comes in.",
        trigger=trig,
    )["details"]["task_id"]

    with pytest.raises(ValueError):
        ts._update_task(
            task_id=tid,
            start_at=datetime.now(timezone.utc) + timedelta(hours=2),
        )


# --------------------------------------------------------------------------- #
# 6.  Instance cloning when a triggerable task is started                     #
# --------------------------------------------------------------------------- #


@_handle_project
@pytest.mark.asyncio
@pytest.mark.llm_call
async def test_triggerable_start_rearms_definition():
    """
    Starting a **triggerable** task should:

    • promote the definition row to **active**
    • re-arm the definition back to **triggerable** for the next wake
    """
    ts = TaskScheduler(actor=SimulatedActor(steps=None, duration=None))

    trig = Trigger(medium=Medium.EMAIL, recurring=False)
    tid = ts._create_task(
        name="Wait for invoice approval",
        description="Start when finance emails us.",
        trigger=trig,
    )["details"]["task_id"]

    rows_before = ts._filter_tasks(filter=f"task_id == {tid}")
    assert len(rows_before) == 1

    handle = await ts.execute(task_id=tid)

    row_after = ts._get_task_or_raise(tid)
    assert row_after.status == Status.active

    result = ts._attach_entrypoint_to_definition(
        task_id=tid,
        function_id=654,
        rationale="The triggered run revealed a stable reusable workflow.",
    )
    assert result["outcome"] == "candidate_recorded"
    assert ts._get_task_or_raise(tid).entrypoint == 654
    assert ts._get_task_or_raise(tid).offline is False

    await handle.stop(cancel=True)
    await handle.result()
