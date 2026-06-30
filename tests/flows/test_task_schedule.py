"""Task scheduling + execution flow test.

Exercises the real online (scheduled) and offline task-execution machinery
end-to-end against the live local Orchestra, under the user's actual
coordinator identity and the shared ``Assistants`` task-machine project that
projection requires:

    TaskScheduler write (``Assistants/{user}/{agent}/Tasks``)
      -> Orchestra projects ``Tasks/Activations``
      -> ``list_scheduled_activations`` read
      -> ``LocalActivationScheduler`` reconcile + timer
      -> fire (``TaskDue`` publish for live, offline dispatch for offline).

Task creation is driven through the production ``TaskScheduler`` rather than
the LLM so fire timing is deterministic; the brain's natural-language task
selection and persistence are covered by the dedicated task-scheduler suite
under ``tests/task_scheduler`` and the coordinator product-literacy eval.
Binding the real coordinator and the ``Assistants`` project is what lets
Orchestra projection actually fire, so the scheduler has live activation rows
to reconcile.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import unisdk

from unify.common.context_registry import ContextRegistry
from unify.manager_registry import ManagerRegistry
from unify.session_details import SESSION_DETAILS
from unify.task_scheduler.local_scheduler import LocalActivationScheduler
from unify.task_scheduler.machine_state import (
    TASK_MACHINE_STATE_PROJECT,
    list_scheduled_activations,
)
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.task_scheduler.types.schedule import Schedule
from unify.task_scheduler.types.status import Status

# Topic the LocalActivationScheduler publishes a live TaskDue on at fire time;
# the hosted Cloud Tasks ingress path uses the same topic.
TASK_DUE_TOPIC = "app:comms:task_due"


class _RecordingBroker:
    """Event broker that records publishes so a test can assert on a fire.

    The broker is the scheduler's real output boundary — ``_fire`` publishes a
    ``TaskDue`` here — so observing it asserts the genuine output rather than
    replacing any code under test.
    """

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    async def publish(self, topic: str, payload: str) -> None:
        self.published.append((topic, payload))


class _RecordingOfflineDispatcher:
    """Offline dispatcher stub that records dispatches at the subprocess seam.

    ``LocalActivationScheduler`` accepts an injected ``offline_dispatcher`` so
    the offline lane (which otherwise spawns the ``offline_runner`` subprocess —
    a genuine external side effect) can be observed without launching a child
    process. The scheduler's real ``_fire`` -> ``_fire_offline`` routing still
    runs.
    """

    def __init__(self) -> None:
        self.dispatched: list[Any] = []

    async def dispatch(self, snap: Any, *, source_type: str) -> None:
        self.dispatched.append(snap)

    async def stop(self) -> None:
        return None


@dataclass
class CoordinatorTaskEnv:
    """Real TaskScheduler bound to the coordinator's ``Assistants`` Tasks table."""

    scheduler: TaskScheduler
    assistant_id: int
    created_task_ids: list[int] = field(default_factory=list)

    def create_scheduled_task(
        self,
        *,
        name: str,
        description: str,
        start_at: datetime,
        offline: bool = False,
    ) -> int:
        """Create one scheduled task and remember it for teardown cleanup."""

        outcome = self.scheduler._create_task(
            name=name,
            description=description,
            status=Status.scheduled,
            schedule=Schedule(start_at=start_at.isoformat()),
            offline=offline,
        )
        task_id = outcome["details"]["task_id"]
        self.created_task_ids.append(task_id)
        return task_id


def _resolve_coordinator() -> dict[str, Any] | None:
    """Return the caller's coordinator assistant record, or None.

    Resolves identity purely from the active ``UNIFY_KEY`` so the test targets
    whichever user/coordinator the environment seeded (local or CI), never a
    hardcoded id.
    """

    for assistant in unisdk.list_assistants():
        if assistant.get("is_coordinator") and assistant.get("agent_id") is not None:
            return assistant
    return None


@pytest.fixture
def coordinator_task_env():
    """Bind the process to the real coordinator + ``Assistants`` project.

    Projection writes the activation to a context derived from the *real*
    assistant's ``user_id``, and the scheduler reads activations from
    ``{SESSION_DETAILS.user_context}/{assistant_context}/Tasks/Activations`` —
    so both halves must use the coordinator's actual ``(user_id, agent_id)``.
    All mutated process globals are restored on teardown and created tasks are
    deleted (the delete re-projects and removes their activation rows).
    """

    coordinator = _resolve_coordinator()
    if coordinator is None:
        pytest.skip(
            "No coordinator assistant resolvable for the active UNIFY_KEY; this "
            "flow needs a seeded coordinator (scripts/local.sh start provisions "
            "one).",
        )

    agent_id = int(coordinator["agent_id"])
    user_id = str(coordinator["user_id"])
    base_context = f"{user_id}/{agent_id}"

    prev_project = unisdk.active_project()
    prev_ctx = unisdk.get_active_context()
    prev_user_id = SESSION_DETAILS.user.id
    prev_agent_id = SESSION_DETAILS.assistant.agent_id
    prev_base_context = ContextRegistry._base_context

    SESSION_DETAILS.user.id = user_id
    SESSION_DETAILS.assistant.agent_id = agent_id
    unisdk.activate(TASK_MACHINE_STATE_PROJECT)
    unisdk.set_context(base_context, relative=False, skip_create=False)
    ContextRegistry.clear()
    ContextRegistry.set_base_context(base_context)
    # Drop any cached singleton built under a previous flow test's context so
    # the scheduler resolves its Tasks context against the coordinator base.
    ManagerRegistry.clear()

    env = CoordinatorTaskEnv(scheduler=TaskScheduler(), assistant_id=agent_id)
    try:
        yield env
    finally:
        for task_id in env.created_task_ids:
            try:
                env.scheduler._delete_task(task_id=task_id)
            except Exception:
                pass
        SESSION_DETAILS.user.id = prev_user_id
        SESSION_DETAILS.assistant.agent_id = prev_agent_id
        ContextRegistry.clear()
        if prev_base_context:
            ContextRegistry.set_base_context(prev_base_context)
        ManagerRegistry.clear()
        if prev_project:
            unisdk.activate(prev_project)
        prev_read = (prev_ctx or {}).get("read") or ""
        prev_write = (prev_ctx or {}).get("write") or ""
        if prev_read or prev_write:
            # Restore read/write independently so a prior context whose read and
            # write halves differ is not silently collapsed to one value.
            unisdk.set_context(
                prev_write,
                mode="write",
                relative=False,
                skip_create=True,
            )
            unisdk.set_context(prev_read, mode="read", relative=False, skip_create=True)
        else:
            unisdk.unset_context()


async def _wait_for_activation(
    *,
    assistant_id: int,
    task_name: str,
    timeout: float = 20.0,
):
    """Poll the projected activations until the one for ``task_name`` appears.

    Matching is keyed on the unique per-test task name rather than the numeric
    ``task_id`` so a stale activation row left by an earlier run on the shared
    coordinator table can never be mistaken for this test's task.
    """

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        for snap in list_scheduled_activations(assistant_id=assistant_id):
            if snap.task_name == task_name:
                return snap
        await asyncio.sleep(0.25)
    return None


async def _wait_until(predicate, *, timeout: float, interval: float = 0.25):
    """Await until ``predicate()`` is truthy or the timeout elapses."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        result = predicate()
        if result:
            return result
        await asyncio.sleep(interval)
    return None


@pytest.mark.asyncio
async def test_scheduled_task_projects_and_fires(
    coordinator_task_env: CoordinatorTaskEnv,
) -> None:
    """A live scheduled task projects an activation and the scheduler fires it."""

    token = uuid.uuid4().hex[:8]
    task_name = f"FlowSchedule-{token}"
    coordinator_task_env.create_scheduled_task(
        name=task_name,
        description=f"Reply TASK-{token} when executed.",
        # Near-future so the timer fires within the test window. The scheduler
        # uses wall-clock time (not the synthetic test clock) for fire delays.
        start_at=datetime.now(timezone.utc) + timedelta(seconds=2),
    )

    snap = await _wait_for_activation(
        assistant_id=coordinator_task_env.assistant_id,
        task_name=task_name,
        timeout=20.0,
    )
    assert snap is not None, (
        "Orchestra did not project a scheduled activation for the task under the "
        "Assistants project"
    )
    assert snap.activation_kind == "scheduled"
    assert snap.execution_mode == "live"
    assert snap.next_due_at is not None
    assert snap.activation_revision

    broker = _RecordingBroker()
    scheduler = LocalActivationScheduler(
        event_broker=broker,
        poll_interval_seconds=0.5,
    )
    await scheduler.start()
    try:

        def _fired_our_task() -> bool:
            for topic, payload in broker.published:
                if topic != TASK_DUE_TOPIC:
                    continue
                # The TaskDue envelope nests the task fields under "payload".
                event_payload = json.loads(payload).get("payload", {})
                if event_payload.get("task_label") == task_name:
                    return True
            return False

        # Assert at-least-one fire for our task; the scheduler may re-arm and
        # re-fire on later reconciles since nothing consumes the activation here.
        fired = await _wait_until(_fired_our_task, timeout=15.0)
        assert fired, (
            f"LocalActivationScheduler never published a {TASK_DUE_TOPIC} event "
            f"for {task_name!r} (published={broker.published!r})"
        )
    finally:
        await scheduler.stop()


@pytest.mark.asyncio
async def test_offline_task_projects_and_routes_to_offline_dispatcher(
    coordinator_task_env: CoordinatorTaskEnv,
) -> None:
    """An offline scheduled task projects offline and fires down the offline lane."""

    token = uuid.uuid4().hex[:8]
    task_name = f"FlowOffline-{token}"
    coordinator_task_env.create_scheduled_task(
        name=task_name,
        description=f"Run offline job OFF-{token}.",
        start_at=datetime.now(timezone.utc) + timedelta(seconds=2),
        offline=True,
    )

    snap = await _wait_for_activation(
        assistant_id=coordinator_task_env.assistant_id,
        task_name=task_name,
        timeout=20.0,
    )
    assert (
        snap is not None
    ), "Orchestra did not project an activation for the offline task"
    assert snap.activation_kind == "scheduled"
    assert snap.execution_mode == "offline"
    assert snap.next_due_at is not None
    assert snap.activation_revision

    broker = _RecordingBroker()
    dispatcher = _RecordingOfflineDispatcher()
    scheduler = LocalActivationScheduler(
        event_broker=broker,
        poll_interval_seconds=0.5,
        offline_dispatcher=dispatcher,
    )
    await scheduler.start()
    try:

        def _dispatched_our_task() -> bool:
            return any(
                getattr(s, "task_name", None) == task_name
                for s in dispatcher.dispatched
            )

        dispatched = await _wait_until(_dispatched_our_task, timeout=15.0)
        assert dispatched, (
            "LocalActivationScheduler did not route the offline activation to the "
            f"offline dispatcher for {task_name!r}"
        )
        offline_snap = next(
            s
            for s in dispatcher.dispatched
            if getattr(s, "task_name", None) == task_name
        )
        assert offline_snap.execution_mode == "offline"

        # The offline lane must not also publish a live TaskDue for this task;
        # otherwise an offline task would be executed twice.
        live_fires = [
            payload
            for topic, payload in broker.published
            if topic == TASK_DUE_TOPIC
            and json.loads(payload).get("payload", {}).get("task_label") == task_name
        ]
        assert not live_fires, (
            f"Offline task {task_name!r} also published a live {TASK_DUE_TOPIC} "
            f"event (would double-execute): {live_fires!r}"
        )
    finally:
        await scheduler.stop()
