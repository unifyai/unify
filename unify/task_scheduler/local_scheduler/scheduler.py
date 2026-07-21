"""In-process LocalActivationScheduler — Cloud Tasks replacement.

The scheduler maintains one asyncio timer per currently armed
``Tasks/Executions`` row. On boot it scans the Orchestra-projected
executions context and arms a timer for every scheduled row that has a
``scheduled_for``. Periodically it re-scans to pick up changes (new rows,
revised rows, deleted rows). At fire time it publishes a ``TaskDue`` event
directly to the conversation manager's event broker, where the existing
``@EventHandler.register(TaskDue)`` consumer handles validation and
execution.

Subprocess delegation for ``delivery == "offline"`` is handled by
:class:`unify.task_scheduler.local_scheduler.offline_dispatcher.LocalOfflineDispatcher`.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from unify.session_details import SESSION_DETAILS
from unify.task_scheduler.types.execution import Delivery, Wake

if TYPE_CHECKING:
    from unify.task_scheduler.machine_state import TaskExecutionSnapshot

LOGGER = logging.getLogger(__name__)


def _task_due_payload_from_snapshot(
    snap: "TaskExecutionSnapshot",
) -> dict[str, Any]:
    """Project a ``TaskExecutionSnapshot`` into the shared TaskDue payload shape."""

    return {
        "task_id": snap.task_id,
        "source_task_log_id": snap.source_task_log_id,
        "revision": snap.revision,
        "scheduled_for": snap.scheduled_for,
        "wake": snap.wake or Wake.scheduled.value,
        "task_label": snap.task_name or "",
        "task_summary": (snap.task_description or "")[:220],
        "visibility_policy": "silent_by_default",
        "recurrence_hint": "recurring" if snap.repeat else "one_off",
        "requires_filesystem": bool(snap.requires_filesystem),
        "requires_computer": bool(snap.requires_computer),
    }


def _task_due_from_snapshot(
    snap: "TaskExecutionSnapshot",
):
    """Build a ``TaskDue`` event from a projected execution row."""

    from unify.conversation_manager.events import TaskDue

    return TaskDue.from_dict(_task_due_payload_from_snapshot(snap))


class LocalActivationScheduler:
    """Asyncio-timer supervisor that fires scheduled tasks in-process.

    One scheduler per ConversationManager process. Owns:

    - ``self._timers``: ``{run_key: asyncio.TimerHandle}`` for the
      currently armed scheduled rows.
    - ``self._known_revisions``: last revision we armed a timer
      for, so we can detect when a row's identity has changed and re-arm.
    - ``self._poll_task``: a background task that periodically re-reads
      executions and reconciles the timer set with what's projected.
    """

    def __init__(
        self,
        *,
        event_broker: Any,
        poll_interval_seconds: float = 60.0,
        offline_dispatcher: Any | None = None,
    ) -> None:
        from .offline_dispatcher import LocalOfflineDispatcher

        self._broker = event_broker
        self._poll_interval_seconds = poll_interval_seconds
        self._timers: dict[str, asyncio.TimerHandle] = {}
        self._known_revisions: dict[str, str | None] = {}
        self._poll_task: asyncio.Task | None = None
        self._started = False
        self._stopping = False
        self._offline = offline_dispatcher or LocalOfflineDispatcher()

    async def start(self) -> None:
        """Start the scheduler. Boot-time reconcile + periodic poll loop."""

        if self._started:
            return
        self._started = True
        self._stopping = False
        LOGGER.info(
            "LocalActivationScheduler starting (poll_interval=%.1fs)",
            self._poll_interval_seconds,
        )
        try:
            await self._reconcile()
        except Exception as exc:
            LOGGER.warning(
                "LocalActivationScheduler boot reconcile failed (degraded): %s",
                exc,
            )
        if self._poll_interval_seconds > 0:
            self._poll_task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        """Cancel all pending timers and stop the poll loop. Idempotent."""

        if not self._started or self._stopping:
            return
        self._stopping = True
        for handle in list(self._timers.values()):
            handle.cancel()
        self._timers.clear()
        self._known_revisions.clear()
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):
                pass
            self._poll_task = None
        try:
            await self._offline.stop()
        except Exception as exc:
            LOGGER.warning(
                "LocalActivationScheduler offline dispatcher stop failed: %s",
                exc,
            )
        self._started = False
        LOGGER.info("LocalActivationScheduler stopped")

    async def _poll_loop(self) -> None:
        """Re-reconcile periodically to pick up execution rows added later."""

        while not self._stopping:
            try:
                await asyncio.sleep(self._poll_interval_seconds)
            except asyncio.CancelledError:
                break
            if self._stopping:
                break
            try:
                await self._reconcile()
            except Exception as exc:
                LOGGER.warning(
                    "LocalActivationScheduler poll iteration failed: %s",
                    exc,
                )

    async def _reconcile(self) -> None:
        """Sync the in-process timer set with the projected execution rows."""

        if self._stopping:
            return
        assistant_id = self._assistant_id()
        if assistant_id is None:
            self._drop_all_timers()
            return

        from unify.task_scheduler.machine_state import list_scheduled_executions

        try:
            executions = await asyncio.to_thread(
                list_scheduled_executions,
                assistant_id=assistant_id,
            )
        except Exception as exc:
            LOGGER.warning(
                "LocalActivationScheduler reconcile read failed: %s",
                exc,
            )
            return

        seen_keys: set[str] = set()
        for snap in executions:
            key = snap.run_key
            seen_keys.add(key)
            signature = self._snapshot_signature(snap)
            if self._known_revisions.get(key) == signature and key in self._timers:
                continue
            self._arm(snap)

        for stale_key in list(self._timers.keys() - seen_keys):
            self._cancel(stale_key)

    def _drop_all_timers(self) -> None:
        """Cancel every armed timer and forget all known revisions."""

        for handle in list(self._timers.values()):
            handle.cancel()
        self._timers.clear()
        self._known_revisions.clear()

    def _cancel(self, run_key: str) -> None:
        """Cancel one armed timer and forget its bookkeeping."""

        handle = self._timers.pop(run_key, None)
        if handle is not None:
            handle.cancel()
        self._known_revisions.pop(run_key, None)

    def _arm(self, snap: "TaskExecutionSnapshot") -> None:
        """Arm or re-arm a timer for one scheduled execution."""

        key = snap.run_key
        prev = self._timers.pop(key, None)
        if prev is not None:
            prev.cancel()

        delay = self._seconds_until(snap.scheduled_for)
        loop = asyncio.get_running_loop()
        handle = loop.call_later(delay, self._make_fire_callback(snap))
        self._timers[key] = handle
        self._known_revisions[key] = self._snapshot_signature(snap)
        LOGGER.debug(
            "LocalActivationScheduler armed execution %s for %.1fs (revision=%s)",
            key,
            delay,
            snap.revision,
        )

    def _make_fire_callback(self, snap: "TaskExecutionSnapshot"):
        """Return the synchronous timer callback bound to one execution."""

        key = snap.run_key

        def _on_timer() -> None:
            self._timers.pop(key, None)
            asyncio.create_task(self._fire(snap))

        return _on_timer

    async def _fire(self, snap: "TaskExecutionSnapshot") -> None:
        """Convert an execution snapshot into a TaskDue event and publish."""

        if self._stopping:
            return

        if snap.delivery == Delivery.offline.value:
            await self._fire_offline(snap)
            return

        event = _task_due_from_snapshot(snap)
        if event is None:
            LOGGER.warning(
                "LocalActivationScheduler refused to publish TaskDue for %s "
                "(missing required fields on snapshot)",
                snap.run_key,
            )
            return

        try:
            await self._broker.publish("app:comms:task_due", event.to_json())
        except Exception as exc:
            LOGGER.error(
                "LocalActivationScheduler failed to publish TaskDue for %s: %s",
                snap.run_key,
                exc,
            )
            return

        LOGGER.info(
            "LocalActivationScheduler fired TaskDue for execution %s "
            "(task_id=%s, scheduled_for=%s, revision=%s)",
            snap.run_key,
            snap.task_id,
            snap.scheduled_for,
            snap.revision,
        )

    async def _fire_offline(self, snap: "TaskExecutionSnapshot") -> None:
        """Hand an offline execution off to the subprocess lane."""

        try:
            await self._offline.dispatch(snap, wake=Wake.scheduled.value)
        except Exception as exc:
            LOGGER.error(
                "LocalActivationScheduler offline dispatch failed for %s: %s",
                snap.run_key,
                exc,
            )

    def _assistant_id(self) -> str | int | None:
        """Return the assistant id the scheduler is responsible for."""

        return SESSION_DETAILS.assistant.agent_id

    @staticmethod
    def _seconds_until(due_iso: str | None) -> float:
        """Return seconds until ``due_iso`` (wall clock), clamped to >= 0."""

        if not due_iso:
            return 0.0
        try:
            parsed = datetime.fromisoformat(due_iso.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return 0.0
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        delta = parsed - datetime.now(timezone.utc)
        return max(0.0, delta.total_seconds())

    def _snapshot_signature(
        self,
        snap: "TaskExecutionSnapshot",
    ) -> str | None:
        """Return the value we compare against to detect execution changes."""

        return snap.revision
