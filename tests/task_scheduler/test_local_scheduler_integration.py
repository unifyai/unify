"""End-to-end integration tests for LocalActivationScheduler.

These tests run against the bundled local Orchestra and exercise the full
chain from TaskScheduler mutation → Orchestra projection of
``Tasks/Activations`` → ``list_scheduled_activations`` read → reconcile →
boot-time timer arming. Unlike the symbolic tests in
``test_local_scheduler.py``, nothing is monkeypatched at the data
boundary — if Orchestra's projection writes the wrong shape, these tests
catch it.

We do NOT actually wait for a timer to fire end-to-end here (the
firing path is fully unit-tested in ``test_local_scheduler.py``); we
verify the read/reconcile half of the pipeline, which is the part that
depends on real Orchestra integration.

These tests require a working local Orchestra with a valid UNIFY_KEY in
the environment. They are skipped automatically when authentication
isn't configured (e.g. CI environments without a key, or sandboxes that
don't have Orchestra running).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
import unify

from tests.helpers import _handle_project
from unity.session_details import SESSION_DETAILS
from unity.task_scheduler.local_scheduler import LocalActivationScheduler
from unity.task_scheduler.machine_state import list_scheduled_activations
from unity.task_scheduler.task_scheduler import TaskScheduler
from unity.task_scheduler.types.repetition import Frequency, RepeatPattern
from unity.task_scheduler.types.schedule import Schedule
from unity.task_scheduler.types.status import Status


def _local_orchestra_authenticated() -> bool:
    """Probe whether local Orchestra accepts the current UNIFY_KEY.

    A 401 from any read here means the test environment doesn't have a
    working key — skip the integration tests rather than fail noisily on
    an unrelated auth problem.
    """

    try:
        unify.get_projects()
        return True
    except Exception:
        return False


_REQUIRES_LIVE_ORCHESTRA = pytest.mark.skipif(
    not _local_orchestra_authenticated(),
    reason=(
        "Local Orchestra is not authenticated for this test environment "
        "(no valid UNIFY_KEY). Integration tests skipped — see "
        "test_local_scheduler.py for the equivalent unit coverage."
    ),
)


class _RecordingBroker:
    """Records every publish call so tests can assert on them."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    async def publish(self, topic: str, payload: str) -> None:
        self.published.append((topic, payload))


@_REQUIRES_LIVE_ORCHESTRA
@_handle_project
def test_local_scheduler_picks_up_real_orchestra_activation():
    """A scheduled task created via TaskScheduler is visible to the local scheduler.

    Validates the full read pipeline:

    1. ``TaskScheduler._create_task(status=scheduled, schedule=...)`` writes
       to Orchestra via Unify.
    2. Orchestra's projection writes a ``Tasks/Activations`` row.
    3. ``list_scheduled_activations`` returns that row.
    4. ``LocalActivationScheduler._reconcile()`` arms a timer for it.

    None of these layers are mocked. If Orchestra's projection field shape
    changes, or ``list_scheduled_activations`` query semantics drift, this
    test fails.
    """

    # SESSION_DETAILS.assistant.agent_id must match the value Orchestra uses
    # for the assistant-scoped Tasks context so projection writes the right
    # value into the activation row.  The _handle_project fixture sets up
    # the context path as ".../default/0/...", so agent_id == 0.
    SESSION_DETAILS.assistant.agent_id = 0
    try:
        asyncio.run(_run_scheduler_integration())
    finally:
        SESSION_DETAILS.assistant.agent_id = None


async def _run_scheduler_integration() -> None:
    assistant_id = SESSION_DETAILS.assistant.agent_id
    assert assistant_id is not None, "Test context did not populate agent_id"

    scheduler = TaskScheduler()
    start_at = datetime.now(timezone.utc) + timedelta(hours=1)
    create_result = scheduler._create_task(
        name="Local scheduler integration probe",
        description="A scheduled probe used by the integration test.",
        status=Status.scheduled,
        schedule=Schedule(start_at=start_at.isoformat()),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
    )
    task_id = create_result["details"]["task_id"]

    # Orchestra projection is synchronous on the log-write path, but give
    # it a tiny window in case of transient races against the local server.
    activations = []
    for _ in range(20):
        activations = list_scheduled_activations(assistant_id=assistant_id)
        if any(a.task_id == task_id for a in activations):
            break
        await asyncio.sleep(0.1)

    matching = [a for a in activations if a.task_id == task_id]
    assert matching, (
        f"Orchestra projection did not produce a scheduled activation row for "
        f"task_id={task_id}. Saw: {[a.task_id for a in activations]}"
    )
    snap = matching[0]
    assert snap.activation_kind == "scheduled"
    assert snap.execution_mode == "live"
    assert snap.next_due_at is not None
    assert snap.activation_revision

    # Boot-time reconcile arms a timer keyed by this activation.
    local_scheduler = LocalActivationScheduler(
        event_broker=_RecordingBroker(),
        poll_interval_seconds=0.0,  # disable poll loop for this test
    )
    await local_scheduler.start()
    try:
        assert snap.activation_key in local_scheduler._timers, (
            "LocalActivationScheduler did not arm a timer for the projected "
            "activation"
        )
        timer = local_scheduler._timers[snap.activation_key]
        # Timer should be scheduled roughly an hour out (we gave start_at = now + 1h).
        assert not timer.cancelled()
    finally:
        await local_scheduler.stop()


@_REQUIRES_LIVE_ORCHESTRA
@_handle_project
def test_recurring_task_rearm_visible_to_local_scheduler():
    """A recurring task's next instance is also visible to the local scheduler.

    Recurring re-arm: when a scheduled instance executes, the scheduler clones
    a new row for the next occurrence via ``_clone_task_instance``. Orchestra
    re-projects, picks the new head-of-queue row, and emits an updated
    activation. The local scheduler should see the new activation on its
    next reconcile and arm a fresh timer.
    """

    SESSION_DETAILS.assistant.agent_id = 0
    try:
        asyncio.run(_run_recurring_integration())
    finally:
        SESSION_DETAILS.assistant.agent_id = None


async def _run_recurring_integration() -> None:
    assistant_id = SESSION_DETAILS.assistant.agent_id
    assert assistant_id is not None

    scheduler = TaskScheduler()
    initial_start = datetime.now(timezone.utc) + timedelta(hours=1)
    create_result = scheduler._create_task(
        name="Local scheduler recurring probe",
        description="Recurring probe used by the integration test.",
        status=Status.scheduled,
        schedule=Schedule(start_at=initial_start.isoformat()),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
    )
    task_id = create_result["details"]["task_id"]

    # Establish the initial activation.
    local_scheduler = LocalActivationScheduler(
        event_broker=_RecordingBroker(),
        poll_interval_seconds=0.0,
    )
    await local_scheduler.start()
    try:
        # Find activation_key for this task.
        activations = list_scheduled_activations(assistant_id=assistant_id)
        initial_snap = next(
            (a for a in activations if a.task_id == task_id),
            None,
        )
        assert initial_snap is not None
        assert initial_snap.activation_key in local_scheduler._timers
        initial_handle = local_scheduler._timers[initial_snap.activation_key]
        initial_revision = local_scheduler._known_revisions[initial_snap.activation_key]

        # Simulate the recurring re-arm: clone the current instance forward.
        current_task = scheduler._get_task_or_raise(task_id)
        scheduler._clone_task_instance(current_task)

        # Reconcile picks up the new instance's activation (or the existing
        # activation now points at the new instance with a fresh revision).
        await local_scheduler._reconcile()

        after_activations = list_scheduled_activations(assistant_id=assistant_id)
        # The activation row for this task should still exist (its head pointer
        # follows _clone_task_instance forward).
        new_snap = next(
            (a for a in after_activations if a.task_id == task_id),
            None,
        )
        assert (
            new_snap is not None
        ), "Recurring rearm: Orchestra dropped the scheduled activation row"
        # Activation key for this task should still be tracked.
        assert new_snap.activation_key in local_scheduler._timers
        # If the activation revision changed (e.g. start_at moved forward),
        # the timer must have been replaced.
        new_revision = local_scheduler._known_revisions[new_snap.activation_key]
        if new_revision != initial_revision:
            assert initial_handle.cancelled() or initial_handle.when() != (
                local_scheduler._timers[new_snap.activation_key].when()
            )
    finally:
        await local_scheduler.stop()
