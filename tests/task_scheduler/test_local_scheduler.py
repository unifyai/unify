"""Tests for the in-process LocalActivationScheduler (local Unity installs).

These are symbolic infrastructure tests — no LLM involvement, no Orchestra
network calls. Activation storage is monkeypatched so each test runs in
milliseconds.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from unity.task_scheduler.local_scheduler import (
    ActivationMaterializer,
    LocalActivationScheduler,
    NoopMaterializer,
    build_materializer,
)
from unity.task_scheduler.local_scheduler import scheduler as scheduler_module
from unity.task_scheduler.local_scheduler.scheduler import (
    LocalActivationScheduler as _LAS,
)
from unity.task_scheduler.machine_state import TaskActivationSnapshot


def _make_snapshot(
    *,
    task_id: int,
    activation_key: str | None = None,
    next_due_at: str | None = None,
    activation_revision: str = "rev-1",
    execution_mode: str = "live",
    assistant_id: str = "42",
) -> TaskActivationSnapshot:
    """Construct a scheduled TaskActivationSnapshot with sane defaults."""

    return TaskActivationSnapshot(
        assistant_id=assistant_id,
        activation_key=activation_key or f"{assistant_id}:{task_id}",
        task_id=task_id,
        source_task_log_id=1000 + task_id,
        activation_kind="scheduled",
        execution_mode=execution_mode,
        next_due_at=next_due_at or _iso_future(seconds=600),
        activation_revision=activation_revision,
    )


def _iso_future(*, seconds: float) -> str:
    """Return an ISO-8601 UTC timestamp ``seconds`` in the future."""

    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


# --------------------------------------------------------------------------- #
# Settings derivation                                                         #
# --------------------------------------------------------------------------- #


class TestLocalSchedulerSettings:
    """SETTINGS.task.LOCAL_SCHEDULER_ENABLED derivation rules."""

    @staticmethod
    def _derive(monkeypatch, **env_overrides: str | None) -> bool:
        """Re-import the derivation helper with a controlled environment."""

        # Clear all signal env vars first, then apply overrides.
        for name in (
            "UNITY_LOCAL_SCHEDULER",
            "UNITY_COMMS_URL",
            "UNITY_CONVERSATION_LOCAL_COMMS_MODE",
            "UNITY_CONVERSATION_LOCAL_COMMS_ENABLED",
        ):
            monkeypatch.delenv(name, raising=False)
        for name, value in env_overrides.items():
            if value is None:
                continue
            monkeypatch.setenv(name, value)

        from unity.task_scheduler.settings import _derive_local_scheduler_default

        return _derive_local_scheduler_default()

    def test_explicit_true_wins_over_everything(self, monkeypatch):
        assert (
            self._derive(
                monkeypatch,
                UNITY_LOCAL_SCHEDULER="true",
                UNITY_COMMS_URL="https://comms.example.com",
                UNITY_CONVERSATION_LOCAL_COMMS_MODE="hosted",
            )
            is True
        )

    def test_explicit_false_wins_over_everything(self, monkeypatch):
        assert (
            self._derive(
                monkeypatch,
                UNITY_LOCAL_SCHEDULER="false",
                UNITY_COMMS_URL="",
                UNITY_CONVERSATION_LOCAL_COMMS_MODE="local",
            )
            is False
        )

    @pytest.mark.parametrize("truthy", ["1", "true", "TRUE", "yes", "on"])
    def test_explicit_truthy_variants_recognised(self, monkeypatch, truthy):
        assert self._derive(monkeypatch, UNITY_LOCAL_SCHEDULER=truthy) is True

    def test_empty_comms_url_implies_local(self, monkeypatch):
        assert self._derive(monkeypatch, UNITY_COMMS_URL="") is True

    def test_missing_comms_url_implies_local(self, monkeypatch):
        # No UNITY_COMMS_URL set at all.
        assert self._derive(monkeypatch) is True

    def test_comms_url_set_with_hosted_comms_implies_hosted(self, monkeypatch):
        assert (
            self._derive(
                monkeypatch,
                UNITY_COMMS_URL="https://comms.example.com",
                UNITY_CONVERSATION_LOCAL_COMMS_MODE="hosted",
            )
            is False
        )

    def test_comms_url_set_with_local_mode_implies_local(self, monkeypatch):
        assert (
            self._derive(
                monkeypatch,
                UNITY_COMMS_URL="https://comms.example.com",
                UNITY_CONVERSATION_LOCAL_COMMS_MODE="local",
            )
            is True
        )

    def test_comms_url_set_with_local_enabled_flag_implies_local(self, monkeypatch):
        assert (
            self._derive(
                monkeypatch,
                UNITY_COMMS_URL="https://comms.example.com",
                UNITY_CONVERSATION_LOCAL_COMMS_ENABLED="true",
            )
            is True
        )


# --------------------------------------------------------------------------- #
# Materializer factory selection                                              #
# --------------------------------------------------------------------------- #


class _FakeBroker:
    """Minimal stand-in for the conversation event broker."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    async def publish(self, topic: str, payload: str) -> None:
        self.published.append((topic, payload))


class _FakeCM:
    """Minimal stand-in for ConversationManager used by build_materializer."""

    def __init__(self, broker: _FakeBroker) -> None:
        self.event_broker = broker


class TestBuildMaterializer:
    """build_materializer routes by SETTINGS.task.LOCAL_SCHEDULER_ENABLED."""

    def test_local_returns_local_scheduler(self, monkeypatch):
        from unity.settings import SETTINGS

        monkeypatch.setattr(SETTINGS.task, "LOCAL_SCHEDULER_ENABLED", True)
        monkeypatch.setattr(
            SETTINGS.task,
            "LOCAL_SCHEDULER_POLL_INTERVAL_SECONDS",
            42.0,
        )

        cm = _FakeCM(_FakeBroker())
        materializer = build_materializer(cm)

        assert isinstance(materializer, _LAS)
        assert materializer._poll_interval_seconds == 42.0
        assert materializer._broker is cm.event_broker

    def test_hosted_returns_noop(self, monkeypatch):
        from unity.settings import SETTINGS

        monkeypatch.setattr(SETTINGS.task, "LOCAL_SCHEDULER_ENABLED", False)

        cm = _FakeCM(_FakeBroker())
        materializer = build_materializer(cm)

        assert isinstance(materializer, NoopMaterializer)

    def test_both_satisfy_protocol(self):
        local = LocalActivationScheduler(event_broker=_FakeBroker())
        noop = NoopMaterializer()

        assert isinstance(local, ActivationMaterializer)
        assert isinstance(noop, ActivationMaterializer)


# --------------------------------------------------------------------------- #
# NoopMaterializer lifecycle                                                  #
# --------------------------------------------------------------------------- #


class TestNoopMaterializer:
    @pytest.mark.asyncio
    async def test_start_stop_returns_none_and_is_idempotent(self):
        noop = NoopMaterializer()
        assert await noop.start() is None
        assert await noop.start() is None
        assert await noop.stop() is None
        assert await noop.stop() is None


# --------------------------------------------------------------------------- #
# LocalActivationScheduler lifecycle (Phase 1: plumbing only)                 #
# --------------------------------------------------------------------------- #


class TestLocalActivationSchedulerLifecycle:
    """Phase-1 lifecycle: start/stop are idempotent and clean."""

    @pytest.mark.asyncio
    async def test_start_is_idempotent(self):
        scheduler = LocalActivationScheduler(event_broker=_FakeBroker())
        await scheduler.start()
        assert scheduler._started is True
        # Second start does nothing.
        await scheduler.start()
        assert scheduler._started is True
        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_stop_without_start_is_safe(self):
        scheduler = LocalActivationScheduler(event_broker=_FakeBroker())
        # Must not raise.
        await scheduler.stop()
        assert scheduler._started is False

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(self):
        scheduler = LocalActivationScheduler(event_broker=_FakeBroker())
        await scheduler.start()
        await scheduler.stop()
        await scheduler.stop()
        assert scheduler._started is False

    @pytest.mark.asyncio
    async def test_stop_cancels_armed_timers(self):
        scheduler = LocalActivationScheduler(event_broker=_FakeBroker())
        await scheduler.start()

        loop = asyncio.get_running_loop()
        handle = loop.call_later(60.0, lambda: None)
        scheduler._timers["dummy:1"] = handle
        scheduler._known_revisions["dummy:1"] = "rev-1"

        await scheduler.stop()
        # The handle's underscore-prefixed attribute name is implementation
        # detail, but cancelled() is part of the public TimerHandle API.
        assert handle.cancelled()
        assert scheduler._timers == {}
        assert scheduler._known_revisions == {}


# --------------------------------------------------------------------------- #
# _seconds_until helper — wall-clock arithmetic                               #
# --------------------------------------------------------------------------- #


class TestSecondsUntil:
    def test_past_returns_zero(self):
        assert (
            LocalActivationScheduler._seconds_until("2020-01-01T00:00:00+00:00") == 0.0
        )

    def test_none_returns_zero(self):
        assert LocalActivationScheduler._seconds_until(None) == 0.0

    def test_empty_string_returns_zero(self):
        assert LocalActivationScheduler._seconds_until("") == 0.0

    def test_malformed_returns_zero(self):
        assert LocalActivationScheduler._seconds_until("not-a-date") == 0.0

    def test_future_returns_positive(self):
        from datetime import datetime, timedelta, timezone

        future = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        secs = LocalActivationScheduler._seconds_until(future)
        assert 290.0 <= secs <= 305.0

    def test_naive_datetime_treated_as_utc(self):
        from datetime import datetime, timedelta, timezone

        future = (datetime.now(timezone.utc) + timedelta(minutes=5)).replace(
            tzinfo=None,
        )
        secs = LocalActivationScheduler._seconds_until(future.isoformat())
        assert 290.0 <= secs <= 305.0

    def test_z_suffix_supported(self):
        future = datetime.now(timezone.utc) + timedelta(minutes=5)
        formatted = future.strftime("%Y-%m-%dT%H:%M:%SZ")
        secs = LocalActivationScheduler._seconds_until(formatted)
        assert 290.0 <= secs <= 305.0


# --------------------------------------------------------------------------- #
# Reconciliation (Phase 2)                                                    #
# --------------------------------------------------------------------------- #


def _patch_list_scheduled(
    monkeypatch,
    activations: list[TaskActivationSnapshot],
) -> list[str | int | None]:
    """Patch the module's `list_scheduled_activations` to return a fixed list.

    Returns a list that records the assistant_id of each call so tests can
    assert how reconciliation was invoked.
    """

    calls: list[str | int | None] = []

    def _fake(*, assistant_id):
        calls.append(assistant_id)
        return list(activations)

    from unity.task_scheduler import machine_state

    monkeypatch.setattr(
        machine_state,
        "list_scheduled_activations",
        _fake,
    )
    return calls


class TestReconcile:
    """LocalActivationScheduler._reconcile() arms / cancels timers."""

    @pytest.mark.asyncio
    async def test_no_assistant_id_drops_all_timers(self, monkeypatch):
        monkeypatch.setattr(
            scheduler_module.SESSION_DETAILS.assistant,
            "agent_id",
            None,
        )
        scheduler = LocalActivationScheduler(event_broker=_FakeBroker())

        # Pre-populate with a leftover timer to verify it's dropped.
        loop = asyncio.get_running_loop()
        scheduler._timers["stale:1"] = loop.call_later(60.0, lambda: None)
        scheduler._known_revisions["stale:1"] = "rev-x"

        await scheduler._reconcile()
        assert scheduler._timers == {}
        assert scheduler._known_revisions == {}

    @pytest.mark.asyncio
    async def test_arms_one_timer_per_activation(self, monkeypatch):
        monkeypatch.setattr(
            scheduler_module.SESSION_DETAILS.assistant,
            "agent_id",
            "42",
        )
        snaps = [
            _make_snapshot(task_id=1),
            _make_snapshot(task_id=2),
            _make_snapshot(task_id=3),
        ]
        _patch_list_scheduled(monkeypatch, snaps)

        scheduler = LocalActivationScheduler(event_broker=_FakeBroker())
        await scheduler._reconcile()

        try:
            assert set(scheduler._timers.keys()) == {"42:1", "42:2", "42:3"}
            assert all(not t.cancelled() for t in scheduler._timers.values())
            assert scheduler._known_revisions == {
                "42:1": "rev-1",
                "42:2": "rev-1",
                "42:3": "rev-1",
            }
        finally:
            for t in scheduler._timers.values():
                t.cancel()

    @pytest.mark.asyncio
    async def test_cancels_disappeared_activations(self, monkeypatch):
        monkeypatch.setattr(
            scheduler_module.SESSION_DETAILS.assistant,
            "agent_id",
            "42",
        )
        # First pass: two activations.
        first = [_make_snapshot(task_id=1), _make_snapshot(task_id=2)]
        _patch_list_scheduled(monkeypatch, first)

        scheduler = LocalActivationScheduler(event_broker=_FakeBroker())
        await scheduler._reconcile()
        assert set(scheduler._timers.keys()) == {"42:1", "42:2"}
        prior_handle_for_2 = scheduler._timers["42:2"]

        # Second pass: only one activation remains. The other should be cancelled.
        _patch_list_scheduled(monkeypatch, [_make_snapshot(task_id=1)])
        await scheduler._reconcile()
        try:
            assert set(scheduler._timers.keys()) == {"42:1"}
            assert prior_handle_for_2.cancelled()
            assert "42:2" not in scheduler._known_revisions
        finally:
            for t in scheduler._timers.values():
                t.cancel()

    @pytest.mark.asyncio
    async def test_revision_change_replaces_timer(self, monkeypatch):
        monkeypatch.setattr(
            scheduler_module.SESSION_DETAILS.assistant,
            "agent_id",
            "42",
        )

        # Pass 1: snapshot at rev-1.
        _patch_list_scheduled(
            monkeypatch,
            [_make_snapshot(task_id=7, activation_revision="rev-1")],
        )
        scheduler = LocalActivationScheduler(event_broker=_FakeBroker())
        await scheduler._reconcile()
        old_handle = scheduler._timers["42:7"]
        assert scheduler._known_revisions["42:7"] == "rev-1"

        # Pass 2: snapshot at rev-2 (e.g. user edited the schedule).
        _patch_list_scheduled(
            monkeypatch,
            [_make_snapshot(task_id=7, activation_revision="rev-2")],
        )
        await scheduler._reconcile()
        try:
            assert old_handle.cancelled()
            assert scheduler._timers["42:7"] is not old_handle
            assert scheduler._known_revisions["42:7"] == "rev-2"
        finally:
            for t in scheduler._timers.values():
                t.cancel()

    @pytest.mark.asyncio
    async def test_unchanged_revision_keeps_existing_timer(self, monkeypatch):
        monkeypatch.setattr(
            scheduler_module.SESSION_DETAILS.assistant,
            "agent_id",
            "42",
        )
        _patch_list_scheduled(
            monkeypatch,
            [_make_snapshot(task_id=7, activation_revision="rev-1")],
        )

        scheduler = LocalActivationScheduler(event_broker=_FakeBroker())
        await scheduler._reconcile()
        old_handle = scheduler._timers["42:7"]

        # Same revision, same activation — the existing timer must not be
        # replaced (avoid drift from repeated reconciliation).
        await scheduler._reconcile()
        try:
            assert scheduler._timers["42:7"] is old_handle
        finally:
            for t in scheduler._timers.values():
                t.cancel()

    @pytest.mark.asyncio
    async def test_storage_failure_is_swallowed(self, monkeypatch):
        monkeypatch.setattr(
            scheduler_module.SESSION_DETAILS.assistant,
            "agent_id",
            "42",
        )

        from unity.task_scheduler import machine_state

        def _boom(*, assistant_id):
            raise RuntimeError("simulated Unify outage")

        monkeypatch.setattr(machine_state, "list_scheduled_activations", _boom)

        scheduler = LocalActivationScheduler(event_broker=_FakeBroker())
        # Existing timers should be preserved when the read fails.
        loop = asyncio.get_running_loop()
        scheduler._timers["42:7"] = loop.call_later(60.0, lambda: None)
        scheduler._known_revisions["42:7"] = "rev-1"

        await scheduler._reconcile()
        try:
            assert "42:7" in scheduler._timers
        finally:
            for t in scheduler._timers.values():
                t.cancel()

    @pytest.mark.asyncio
    async def test_start_runs_boot_reconcile(self, monkeypatch):
        monkeypatch.setattr(
            scheduler_module.SESSION_DETAILS.assistant,
            "agent_id",
            "42",
        )
        _patch_list_scheduled(monkeypatch, [_make_snapshot(task_id=99)])

        scheduler = LocalActivationScheduler(event_broker=_FakeBroker())
        await scheduler.start()
        try:
            assert "42:99" in scheduler._timers
        finally:
            await scheduler.stop()


# --------------------------------------------------------------------------- #
# TaskDue payload construction                                                #
# --------------------------------------------------------------------------- #


class TestTaskDueFromSnapshot:
    """`_task_due_from_snapshot` matches the field contract Communication uses."""

    def test_returns_none_when_required_fields_missing(self):
        from unity.task_scheduler.local_scheduler.scheduler import (
            _task_due_from_snapshot,
        )

        # Missing source_task_log_id.
        bad = TaskActivationSnapshot(
            assistant_id="42",
            activation_key="42:1",
            task_id=1,
            source_task_log_id=None,
            activation_kind="scheduled",
            execution_mode="live",
            next_due_at="2030-01-01T00:00:00+00:00",
            activation_revision="rev",
        )
        assert _task_due_from_snapshot(bad) is None

    def test_returns_none_when_revision_missing(self):
        from unity.task_scheduler.local_scheduler.scheduler import (
            _task_due_from_snapshot,
        )

        bad = TaskActivationSnapshot(
            assistant_id="42",
            activation_key="42:1",
            task_id=1,
            source_task_log_id=99,
            activation_kind="scheduled",
            execution_mode="live",
            next_due_at="2030-01-01T00:00:00+00:00",
            activation_revision=None,
        )
        assert _task_due_from_snapshot(bad) is None

    def test_returns_none_when_due_missing(self):
        from unity.task_scheduler.local_scheduler.scheduler import (
            _task_due_from_snapshot,
        )

        bad = TaskActivationSnapshot(
            assistant_id="42",
            activation_key="42:1",
            task_id=1,
            source_task_log_id=99,
            activation_kind="scheduled",
            execution_mode="live",
            next_due_at=None,
            activation_revision="rev",
        )
        assert _task_due_from_snapshot(bad) is None

    def test_returns_taskdue_with_expected_fields(self):
        from unity.task_scheduler.local_scheduler.scheduler import (
            _task_due_from_snapshot,
        )

        snap = TaskActivationSnapshot(
            assistant_id="42",
            activation_key="42:7",
            task_id=7,
            source_task_log_id=1007,
            activation_kind="scheduled",
            execution_mode="live",
            task_name="Weekly Status",
            task_description=(
                "Send Monday morning status report to the team — "
                "summarise Friday's progress."
            ),
            next_due_at="2030-04-10T09:00:00+00:00",
            activation_revision="rev-abc",
        )

        event = _task_due_from_snapshot(snap)

        assert event is not None
        assert event.task_id == 7
        assert event.source_task_log_id == 1007
        assert event.activation_revision == "rev-abc"
        assert event.scheduled_for == "2030-04-10T09:00:00+00:00"
        assert event.execution_mode == "live"
        assert event.source_type == "scheduled"
        assert event.task_label == "Weekly Status"
        assert event.task_summary.startswith("Send Monday morning")
        assert len(event.task_summary) <= 220
        assert event.visibility_policy == "silent_by_default"
        assert event.recurrence_hint == "one_off"
        assert "Weekly Status" in event.reason

    def test_recurring_hint_set_when_repeat_present(self):
        from unity.task_scheduler.local_scheduler.scheduler import (
            _task_due_from_snapshot,
        )

        snap = _make_snapshot(task_id=1)
        snap = TaskActivationSnapshot(
            **{
                **{k: getattr(snap, k) for k in snap.__dataclass_fields__.keys()},
                "repeat": [{"frequency": "weekly", "interval": 1}],
            },
        )

        event = _task_due_from_snapshot(snap)
        assert event is not None
        assert event.recurrence_hint == "recurring"


# --------------------------------------------------------------------------- #
# Firing (Phase 3)                                                            #
# --------------------------------------------------------------------------- #


class TestFire:
    """The timer callback publishes a real TaskDue to the event broker."""

    @pytest.mark.asyncio
    async def test_fire_publishes_to_task_due_topic(self, monkeypatch):
        monkeypatch.setattr(
            scheduler_module.SESSION_DETAILS.assistant,
            "agent_id",
            "42",
        )
        broker = _FakeBroker()
        scheduler = LocalActivationScheduler(event_broker=broker)
        await scheduler._fire(_make_snapshot(task_id=7))

        assert len(broker.published) == 1
        topic, payload = broker.published[0]
        assert topic == "app:comms:task_due"

        import json

        decoded = json.loads(payload)
        assert decoded["event_name"] == "TaskDue"
        assert decoded["payload"]["task_id"] == 7
        assert decoded["payload"]["execution_mode"] == "live"

    @pytest.mark.asyncio
    async def test_fire_does_nothing_when_stopping(self, monkeypatch):
        broker = _FakeBroker()
        scheduler = LocalActivationScheduler(event_broker=broker)
        scheduler._stopping = True
        await scheduler._fire(_make_snapshot(task_id=1))
        assert broker.published == []

    @pytest.mark.asyncio
    async def test_fire_swallows_publish_failure(self):
        class _BoomBroker:
            async def publish(self, *_args, **_kwargs):
                raise RuntimeError("simulated broker outage")

        scheduler = LocalActivationScheduler(event_broker=_BoomBroker())
        # Must not raise.
        await scheduler._fire(_make_snapshot(task_id=1))

    @pytest.mark.asyncio
    async def test_fire_offline_delegates_to_dispatcher(self):
        """Offline fires hit LocalOfflineDispatcher.dispatch, not the broker."""

        class _FakeDispatcher:
            def __init__(self) -> None:
                self.calls: list[tuple] = []

            async def dispatch(self, snap, *, source_type):
                self.calls.append((snap.activation_key, source_type))

            async def stop(self):
                return None

        broker = _FakeBroker()
        fake = _FakeDispatcher()
        scheduler = LocalActivationScheduler(
            event_broker=broker,
            offline_dispatcher=fake,
        )
        await scheduler._fire(
            _make_snapshot(task_id=9, execution_mode="offline"),
        )
        assert broker.published == []
        assert fake.calls == [("42:9", "scheduled")]

    @pytest.mark.asyncio
    async def test_fire_offline_swallows_dispatcher_failure(self):
        class _BoomDispatcher:
            async def dispatch(self, snap, *, source_type):
                raise RuntimeError("subprocess spawn failed")

            async def stop(self):
                return None

        scheduler = LocalActivationScheduler(
            event_broker=_FakeBroker(),
            offline_dispatcher=_BoomDispatcher(),
        )
        # Must not raise.
        await scheduler._fire(
            _make_snapshot(task_id=9, execution_mode="offline"),
        )

    @pytest.mark.asyncio
    async def test_short_delay_actually_fires_via_callback(self, monkeypatch):
        """End-to-end: arm a timer with ~0 delay, await, verify TaskDue published."""

        monkeypatch.setattr(
            scheduler_module.SESSION_DETAILS.assistant,
            "agent_id",
            "42",
        )
        broker = _FakeBroker()
        scheduler = LocalActivationScheduler(event_broker=broker)
        snap = _make_snapshot(
            task_id=11,
            next_due_at=_iso_future(seconds=0.05),
        )
        scheduler._arm(snap)

        # Wait for the timer to fire and the spawned task to publish.
        # The callback fires after ~50ms, and asyncio.create_task scheduling
        # then runs _fire(), which publishes synchronously to our fake broker.
        for _ in range(30):
            if broker.published:
                break
            await asyncio.sleep(0.05)

        try:
            assert broker.published, "Timer should have fired and published TaskDue"
            topic, _payload = broker.published[0]
            assert topic == "app:comms:task_due"
            # The timer key is consumed when the timer fires.
            assert "42:11" not in scheduler._timers
        finally:
            for t in scheduler._timers.values():
                t.cancel()


# --------------------------------------------------------------------------- #
# Periodic poll loop (Phase 4)                                                #
# --------------------------------------------------------------------------- #


class TestPollLoop:
    """`_poll_loop` re-reconciles on a cadence and stops cleanly."""

    @pytest.mark.asyncio
    async def test_start_spawns_poll_task(self, monkeypatch):
        monkeypatch.setattr(
            scheduler_module.SESSION_DETAILS.assistant,
            "agent_id",
            "42",
        )
        _patch_list_scheduled(monkeypatch, [])

        scheduler = LocalActivationScheduler(
            event_broker=_FakeBroker(),
            poll_interval_seconds=10.0,
        )
        await scheduler.start()
        try:
            assert scheduler._poll_task is not None
            assert not scheduler._poll_task.done()
        finally:
            await scheduler.stop()

    @pytest.mark.asyncio
    async def test_zero_poll_interval_disables_poll_task(self, monkeypatch):
        monkeypatch.setattr(
            scheduler_module.SESSION_DETAILS.assistant,
            "agent_id",
            "42",
        )
        _patch_list_scheduled(monkeypatch, [])

        scheduler = LocalActivationScheduler(
            event_broker=_FakeBroker(),
            poll_interval_seconds=0.0,
        )
        await scheduler.start()
        try:
            assert scheduler._poll_task is None
        finally:
            await scheduler.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_poll_task(self, monkeypatch):
        monkeypatch.setattr(
            scheduler_module.SESSION_DETAILS.assistant,
            "agent_id",
            "42",
        )
        _patch_list_scheduled(monkeypatch, [])

        scheduler = LocalActivationScheduler(
            event_broker=_FakeBroker(),
            poll_interval_seconds=10.0,
        )
        await scheduler.start()
        task = scheduler._poll_task
        assert task is not None
        await scheduler.stop()
        assert task.cancelled() or task.done()
        assert scheduler._poll_task is None

    @pytest.mark.asyncio
    async def test_poll_picks_up_new_activation(self, monkeypatch):
        """An activation added after boot is armed within one poll cycle."""

        monkeypatch.setattr(
            scheduler_module.SESSION_DETAILS.assistant,
            "agent_id",
            "42",
        )

        # Mutable list the patched list_scheduled_activations reads from.
        live: list[TaskActivationSnapshot] = []

        from unity.task_scheduler import machine_state

        def _fake(*, assistant_id):
            return list(live)

        monkeypatch.setattr(
            machine_state,
            "list_scheduled_activations",
            _fake,
        )

        # Very short interval to keep the test fast.
        scheduler = LocalActivationScheduler(
            event_broker=_FakeBroker(),
            poll_interval_seconds=0.05,
        )
        await scheduler.start()
        try:
            assert scheduler._timers == {}

            # Add an activation after boot.
            live.append(_make_snapshot(task_id=55))

            # Wait long enough for at least one poll iteration.
            for _ in range(40):
                if "42:55" in scheduler._timers:
                    break
                await asyncio.sleep(0.05)

            assert "42:55" in scheduler._timers
        finally:
            await scheduler.stop()
