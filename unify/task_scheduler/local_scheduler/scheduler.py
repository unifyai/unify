"""In-process LocalActivationScheduler — Cloud Tasks replacement.

The scheduler maintains one asyncio timer per currently armed
``Tasks/Activations`` row. On boot it scans the Orchestra-projected
activations context and arms a timer for every scheduled row that has a
``next_due_at``. Periodically it re-scans to pick up changes (new rows,
revised rows, deleted rows). At fire time it publishes a ``TaskDue`` event
directly to the conversation manager's event broker, where the existing
``@EventHandler.register(TaskDue)`` consumer handles validation and
execution.

Subprocess delegation for ``execution_mode == "offline"`` is handled by
:class:`unify.task_scheduler.local_scheduler.offline_dispatcher.LocalOfflineDispatcher`.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from unify.session_details import SESSION_DETAILS

if TYPE_CHECKING:
    from unify.task_scheduler.machine_state import TaskActivationSnapshot

LOGGER = logging.getLogger(__name__)


def _task_due_payload_from_snapshot(
    snap: "TaskActivationSnapshot",
) -> dict[str, Any]:
    """Project a `TaskActivationSnapshot` into the shared TaskDue payload shape.

    The hosted ingress path receives this same shape via Cloud Tasks /
    Pub/Sub (Communication's ``ScheduledTaskDuePayload`` body). The local
    path produces the same shape directly from the projected activation
    row so :meth:`TaskDue.from_dict` is the single builder both paths use.

    Activation rows carry rich fields that the Cloud Tasks payload does
    not (e.g. ``repeat`` list, full ``task_description``) — those are
    used to derive payload-level hints (``recurrence_hint``,
    ``task_summary``) here, while the hosted path receives those hints
    pre-computed by Orchestra projection.
    """

    return {
        "task_id": snap.task_id,
        "source_task_log_id": snap.source_task_log_id,
        "activation_revision": snap.activation_revision,
        "scheduled_for": snap.next_due_at,
        "source_type": "scheduled",
        "task_label": snap.task_name or "",
        "task_summary": (snap.task_description or "")[:220],
        "visibility_policy": "silent_by_default",
        "recurrence_hint": "recurring" if snap.repeat else "one_off",
        "requires_filesystem": bool(snap.requires_filesystem),
        "requires_computer": bool(snap.requires_computer),
    }


def _task_due_from_snapshot(
    snap: "TaskActivationSnapshot",
):
    """Build a ``TaskDue`` event from a projected activation row.

    Delegates to :meth:`TaskDue.from_dict` so the local and hosted
    producers share a single field-extraction implementation. Returns
    ``None`` when any required identity field is missing — the consumer
    side logs / drops as it would for any other invalid payload.
    """

    # Local import keeps this module importable from settings/derivation
    # paths that run before the conversation_manager package is loaded.
    from unify.conversation_manager.events import TaskDue

    return TaskDue.from_dict(_task_due_payload_from_snapshot(snap))


class LocalActivationScheduler:
    """Asyncio-timer supervisor that fires scheduled tasks in-process.

    One scheduler per ConversationManager process. Owns:

    - ``self._timers``: ``{activation_key: asyncio.TimerHandle}`` for the
      currently armed scheduled rows.
    - ``self._known_revisions``: last activation_revision we armed a timer
      for, so we can detect when a row's identity has changed and re-arm.
    - ``self._poll_task``: a background task that periodically re-reads
      activations and reconciles the timer set with what's projected.

    The scheduler is stateless across restarts: on boot it scans all
    scheduled activations and re-arms everything from wall-clock
    ``next_due_at`` values. There is no in-memory persistence to lose.
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
        # Offline dispatcher is injectable so tests can substitute a fake.
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

    # ----- Periodic poll ----------------------------------------------------

    async def _poll_loop(self) -> None:
        """Re-reconcile periodically to pick up activation rows added later.

        New scheduled tasks created mid-session (e.g. the user just asked
        the agent to remind them in an hour) appear as fresh rows in the
        Orchestra-projected ``Tasks/Activations`` context. Boot-time
        reconcile won't see them, so the scheduler re-scans on a slow
        cadence to catch up. The default 60s interval is the same cadence
        Hermes Agent uses for its in-process cron ticker.

        The loop is structured so that cancellation during ``stop()`` is
        clean: ``asyncio.sleep`` raises ``CancelledError`` which propagates
        out of the loop without doing another reconcile.
        """

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
                # ``_reconcile`` already logs its own failures, but a
                # programming bug above the read could still surface here.
                LOGGER.warning(
                    "LocalActivationScheduler poll iteration failed: %s",
                    exc,
                )

    # ----- Reconciliation ---------------------------------------------------

    async def _reconcile(self) -> None:
        """Sync the in-process timer set with the projected activation rows.

        On every reconcile pass:

        - Activations newly visible (or with a changed ``activation_revision``)
          get a fresh timer; any prior timer for the same activation key is
          cancelled first.
        - Activations no longer projected (deleted, cancelled, became
          non-scheduled) have their timer cancelled and their bookkeeping
          dropped.
        - Activations whose snapshot signature is unchanged are left alone.

        Reconcile runs entirely in the scheduler's event loop. The Unify
        read is performed via ``asyncio.to_thread`` so the loop is not
        blocked while Orchestra responds.
        """

        if self._stopping:
            return
        assistant_id = self._assistant_id()
        if assistant_id is None:
            # Without an assistant id we can't filter activations; treat this
            # as "nothing to schedule" and clear any state from a previous
            # assistant.
            self._drop_all_timers()
            return

        from unify.task_scheduler.machine_state import list_scheduled_activations

        try:
            activations = await asyncio.to_thread(
                list_scheduled_activations,
                assistant_id=assistant_id,
            )
        except Exception as exc:
            LOGGER.warning(
                "LocalActivationScheduler reconcile read failed: %s",
                exc,
            )
            return

        seen_keys: set[str] = set()
        for snap in activations:
            key = snap.activation_key
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

    def _cancel(self, activation_key: str) -> None:
        """Cancel one armed timer and forget its bookkeeping."""

        handle = self._timers.pop(activation_key, None)
        if handle is not None:
            handle.cancel()
        self._known_revisions.pop(activation_key, None)

    def _arm(self, snap: "TaskActivationSnapshot") -> None:
        """Arm or re-arm a timer for one scheduled activation.

        Phase 2 stops short of actually firing — only the timer is scheduled
        and tracked, with a placeholder callback. Phase 3 swaps the
        placeholder for the real ``_fire`` coroutine that builds and
        publishes a ``TaskDue`` event.
        """

        key = snap.activation_key
        # Always cancel the previous timer for this key first so a re-arm
        # never leaks a pending callback.
        prev = self._timers.pop(key, None)
        if prev is not None:
            prev.cancel()

        delay = self._seconds_until(snap.next_due_at)
        loop = asyncio.get_running_loop()
        handle = loop.call_later(delay, self._make_fire_callback(snap))
        self._timers[key] = handle
        self._known_revisions[key] = self._snapshot_signature(snap)
        LOGGER.debug(
            "LocalActivationScheduler armed activation %s for %.1fs (revision=%s)",
            key,
            delay,
            snap.activation_revision,
        )

    def _make_fire_callback(self, snap: "TaskActivationSnapshot"):
        """Return the synchronous timer callback bound to one activation.

        ``loop.call_later`` invokes a synchronous callable, so the callback
        spawns an asyncio task that runs the actual ``_fire`` coroutine.
        The activation key is dropped from ``self._timers`` first so a
        reconcile pass that happens concurrently with the fire doesn't
        leak a cancelled handle.
        """

        key = snap.activation_key

        def _on_timer() -> None:
            self._timers.pop(key, None)
            # _known_revisions is kept until the next reconcile pass so a
            # rapid second reconcile doesn't re-arm the same fire while the
            # coroutine is still executing.
            asyncio.create_task(self._fire(snap))

        return _on_timer

    async def _fire(self, snap: "TaskActivationSnapshot") -> None:
        """Convert an activation snapshot into a TaskDue event and publish.

        For ``execution_mode == "live"`` the event flows through the same
        ``app:comms:task_due`` topic the hosted ingress path uses, so the
        existing ``@EventHandler.register(TaskDue)`` consumer takes over
        unchanged. Validation (activation_revision freshness, scheduled_for
        equality) happens inside that consumer via
        ``validate_task_due_activation``; if the user edited the task
        between arming and firing, the stale-rejection path catches it.

        Phase 5 adds the ``execution_mode == "offline"`` branch that
        delegates to ``LocalOfflineDispatcher``.
        """

        if self._stopping:
            return

        if snap.execution_mode == "offline":
            await self._fire_offline(snap)
            return

        event = _task_due_from_snapshot(snap)
        if event is None:
            LOGGER.warning(
                "LocalActivationScheduler refused to publish TaskDue for %s "
                "(missing required fields on snapshot)",
                snap.activation_key,
            )
            return

        try:
            await self._broker.publish("app:comms:task_due", event.to_json())
        except Exception as exc:
            LOGGER.error(
                "LocalActivationScheduler failed to publish TaskDue for %s: %s",
                snap.activation_key,
                exc,
            )
            return

        LOGGER.info(
            "LocalActivationScheduler fired TaskDue for activation %s "
            "(task_id=%s, scheduled_for=%s, revision=%s)",
            snap.activation_key,
            snap.task_id,
            snap.next_due_at,
            snap.activation_revision,
        )

    async def _fire_offline(self, snap: "TaskActivationSnapshot") -> None:
        """Hand an offline-execution activation off to the subprocess lane.

        Spawns ``unify.task_scheduler.offline_runner`` as a child process
        with the activation context wired via env vars. The dispatcher
        watches the subprocess in the background and logs its exit code,
        so this coroutine returns as soon as the subprocess has been
        launched.
        """

        try:
            await self._offline.dispatch(snap, source_type="scheduled")
        except Exception as exc:
            LOGGER.error(
                "LocalActivationScheduler offline dispatch failed for %s: %s",
                snap.activation_key,
                exc,
            )

    def _assistant_id(self) -> str | int | None:
        """Return the assistant id the scheduler is responsible for.

        Reads from ``SESSION_DETAILS`` so the value tracks any in-flight
        assistant reassignment. Returns None when no assistant is bound;
        in that case the scheduler simply has no activations to manage.
        """

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
        snap: "TaskActivationSnapshot",
    ) -> str | None:
        """Return the value we compare against to detect activation changes.

        Today we use ``activation_revision`` because Orchestra recomputes it
        on every projection write that affects an activation. Two snapshots
        with the same revision describe the same scheduled fire; differing
        revisions mean the timer needs to be replaced.
        """

        return snap.activation_revision
