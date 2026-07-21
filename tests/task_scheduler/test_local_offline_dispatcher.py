"""Tests for LocalOfflineDispatcher — subprocess offline_runner spawner.

These tests mock ``asyncio.create_subprocess_exec`` so no real subprocess
is launched and they run in milliseconds.
"""

from __future__ import annotations

import asyncio

import pytest

from unify.task_scheduler.local_scheduler import LocalOfflineDispatcher
from unify.task_scheduler.local_scheduler import offline_dispatcher as od
from unify.task_scheduler.machine_state import TaskExecutionSnapshot
from unify.task_scheduler.types.execution import Delivery, Wake

_DEFAULT_DUE = "2030-04-10T09:00:00+00:00"


def _make_snapshot(
    *,
    task_id: int = 7,
    delivery: str = Delivery.offline.value,
    assistant_id: str = "42",
    revision: str = "rev-1",
    next_due_at: str | None = _DEFAULT_DUE,
    task_name: str = "Run weekly report",
    task_description: str = "Generate the weekly report and email finance.",
    entrypoint: int | None = None,
    trigger_medium: str | None = None,
    source_task_log_id: int | None = 999,
) -> TaskExecutionSnapshot:
    wake = Wake.scheduled.value if not trigger_medium else Wake.triggered.value
    return TaskExecutionSnapshot(
        run_key=f"offline:{wake}:{assistant_id}:{task_id}:{revision}:once",
        assistant_id=assistant_id,
        task_id=task_id,
        source_task_log_id=source_task_log_id,
        wake=wake,
        delivery=delivery,
        task_name=task_name,
        task_description=task_description,
        scheduled_for=next_due_at,
        trigger_medium=trigger_medium,
        entrypoint=entrypoint,
        revision=revision,
    )


class _FakeProcess:
    """Minimal stand-in for asyncio.subprocess.Process used in tests."""

    def __init__(
        self,
        *,
        returncode: int = 0,
        stdout: bytes = b"",
        stderr: bytes = b"",
    ):
        self.returncode = returncode
        self._stdout = stdout
        self._stderr = stderr

    async def communicate(self) -> tuple[bytes, bytes]:
        return self._stdout, self._stderr


# --------------------------------------------------------------------------- #
# Env builder                                                                 #
# --------------------------------------------------------------------------- #


class TestBuildLocalOfflineRunnerEnv:
    """Env shape matches Communication's _build_offline_runner_env contract."""

    def test_minimum_required_fields_present(self):
        snap = _make_snapshot()
        env = od._build_local_offline_runner_env(snap, wake=Wake.scheduled.value)

        required = {
            "UNITY_OFFLINE_TASK_MODE",
            "UNITY_OFFLINE_TASK_RUN_KEY",
            "UNITY_OFFLINE_TASK_ID",
            "UNITY_OFFLINE_TASK_SOURCE_TASK_LOG_ID",
            "UNITY_OFFLINE_TASK_REVISION",
            "UNITY_OFFLINE_TASK_REQUEST",
            "UNITY_OFFLINE_TASK_WAKE",
            "ASSISTANT_ID",
        }
        missing = required - set(env.keys())
        assert not missing, f"Missing env vars: {missing}"

    def test_task_request_falls_back_through_description_then_name(self):
        snap = _make_snapshot(
            task_name="The Name",
            task_description="The Description",
        )
        env = od._build_local_offline_runner_env(snap, wake=Wake.scheduled.value)
        assert env["UNITY_OFFLINE_TASK_REQUEST"] == "The Description"

        snap = _make_snapshot(task_name="The Name", task_description="")
        env = od._build_local_offline_runner_env(snap, wake=Wake.scheduled.value)
        assert env["UNITY_OFFLINE_TASK_REQUEST"] == "The Name"

        snap = _make_snapshot(task_name="", task_description="")
        env = od._build_local_offline_runner_env(snap, wake=Wake.scheduled.value)
        assert env["UNITY_OFFLINE_TASK_REQUEST"] == f"Execute task {snap.task_id}"

    def test_mode_is_actor(self):
        env = od._build_local_offline_runner_env(
            _make_snapshot(),
            wake=Wake.scheduled.value,
        )
        assert env["UNITY_OFFLINE_TASK_MODE"] == "actor"

    def test_function_id_omitted_when_no_entrypoint(self):
        env = od._build_local_offline_runner_env(
            _make_snapshot(entrypoint=None),
            wake=Wake.scheduled.value,
        )
        assert env["UNITY_OFFLINE_TASK_FUNCTION_ID"] == ""

    def test_function_id_serialised_when_entrypoint(self):
        env = od._build_local_offline_runner_env(
            _make_snapshot(entrypoint=42),
            wake=Wake.scheduled.value,
        )
        assert env["UNITY_OFFLINE_TASK_FUNCTION_ID"] == "42"

    def test_eventbus_not_forced_off(self):
        env = od._build_local_offline_runner_env(
            _make_snapshot(),
            wake=Wake.scheduled.value,
        )
        assert "EVENTBUS_PUBLISHING_ENABLED" not in env
        assert "EVENTBUS_PUBSUB_STREAMING" not in env

    def test_trigger_medium_propagates_for_triggered_dispatch(self):
        snap = _make_snapshot(trigger_medium="email", delivery=Delivery.offline.value)
        env = od._build_local_offline_runner_env(snap, wake=Wake.triggered.value)
        assert env["UNITY_OFFLINE_TASK_SOURCE_MEDIUM"] == "email"

    def test_explicit_trigger_override_wins_over_snapshot_medium(self):
        snap = _make_snapshot(trigger_medium="email")
        env = od._build_local_offline_runner_env(
            snap,
            wake=Wake.triggered.value,
            source_medium="sms",
        )
        assert env["UNITY_OFFLINE_TASK_SOURCE_MEDIUM"] == "sms"

    def test_explicit_contact_id_set(self):
        env = od._build_local_offline_runner_env(
            _make_snapshot(),
            wake=Wake.triggered.value,
            source_contact_id=123,
        )
        assert env["UNITY_OFFLINE_TASK_SOURCE_CONTACT_ID"] == "123"

    def test_explicit_display_name_emitted(self):
        env = od._build_local_offline_runner_env(
            _make_snapshot(),
            wake=Wake.triggered.value,
            source_contact_display_name="Alice Example",
        )
        assert env["UNITY_OFFLINE_TASK_SOURCE_CONTACT_DISPLAY_NAME"] == "Alice Example"

    def test_scheduled_for_normalised_to_utc(self):
        snap = _make_snapshot(next_due_at="2030-04-10T11:00:00+02:00")
        env = od._build_local_offline_runner_env(snap, wake=Wake.scheduled.value)
        assert env["UNITY_OFFLINE_TASK_SCHEDULED_FOR"] == "2030-04-10T09:00:00+00:00"

    def test_resource_flags_propagate_from_snapshot(self):
        snap = _make_snapshot()
        env = od._build_local_offline_runner_env(snap, wake=Wake.scheduled.value)
        assert env["UNITY_OFFLINE_TASK_REQUIRES_FILESYSTEM"] == "0"
        assert env["UNITY_OFFLINE_TASK_REQUIRES_COMPUTER"] == "0"

        snap = TaskExecutionSnapshot(
            run_key="offline:scheduled:42:7:rev-1:once",
            assistant_id="42",
            task_id=7,
            source_task_log_id=999,
            wake=Wake.scheduled.value,
            delivery=Delivery.offline.value,
            task_name="Run weekly report",
            task_description="Generate the weekly report and email finance.",
            scheduled_for=_DEFAULT_DUE,
            revision="rev-1",
            requires_filesystem=True,
            requires_computer=True,
        )
        env = od._build_local_offline_runner_env(snap, wake=Wake.scheduled.value)
        assert env["UNITY_OFFLINE_TASK_REQUIRES_FILESYSTEM"] == "1"
        assert env["UNITY_OFFLINE_TASK_REQUIRES_COMPUTER"] == "1"


class TestBuildLocalOfflineRunKey:
    """run_key shape matches Communication's _build_offline_run_key field-for-field."""

    def test_contains_wake_and_assistant_and_task(self):
        snap = _make_snapshot(assistant_id="42", task_id=7)
        key = od._build_local_offline_run_key(snap, wake=Wake.scheduled.value)
        assert key.startswith("offline:scheduled:42:7:")

    def test_revision_digest_is_stable(self):
        snap = _make_snapshot(revision="rev-abc")
        first = od._build_local_offline_run_key(snap, wake=Wake.scheduled.value)
        second = od._build_local_offline_run_key(snap, wake=Wake.scheduled.value)
        assert first == second

    def test_revision_change_produces_different_key(self):
        a = od._build_local_offline_run_key(
            _make_snapshot(revision="rev-1"),
            wake=Wake.scheduled.value,
        )
        b = od._build_local_offline_run_key(
            _make_snapshot(revision="rev-2"),
            wake=Wake.scheduled.value,
        )
        assert a != b

    def test_default_tail_when_no_due(self):
        snap = _make_snapshot(next_due_at=None)
        key = od._build_local_offline_run_key(snap, wake=Wake.scheduled.value)
        assert key.endswith(":once")


# --------------------------------------------------------------------------- #
# Subprocess dispatch                                                          #
# --------------------------------------------------------------------------- #


class TestDispatch:
    """LocalOfflineDispatcher.dispatch spawns the subprocess correctly."""

    @pytest.mark.asyncio
    async def test_dispatch_invokes_offline_runner_module(self, monkeypatch):
        captured: dict = {}

        async def _fake_subprocess(*args, **kwargs):
            captured["args"] = args
            captured["env"] = kwargs.get("env")
            return _FakeProcess(returncode=0, stdout=b"ok")

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_subprocess)

        dispatcher = LocalOfflineDispatcher()
        try:
            await dispatcher.dispatch(_make_snapshot(), wake=Wake.scheduled.value)

            assert captured["args"][-2:] == (
                "-m",
                "unify.task_scheduler.offline_runner",
            )
            assert captured["env"]["UNITY_OFFLINE_TASK_MODE"] == "actor"
            assert captured["env"]["UNITY_OFFLINE_TASK_WAKE"] == Wake.scheduled.value
            assert "PATH" in captured["env"]
        finally:
            await dispatcher.stop()

    @pytest.mark.asyncio
    async def test_dispatch_uses_current_python_interpreter(self, monkeypatch):
        captured: dict = {}

        async def _fake_subprocess(*args, **kwargs):
            captured["argv0"] = args[0]
            return _FakeProcess(returncode=0)

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_subprocess)

        import sys as _sys

        dispatcher = LocalOfflineDispatcher()
        try:
            await dispatcher.dispatch(_make_snapshot(), wake=Wake.scheduled.value)
            assert captured["argv0"] == _sys.executable
        finally:
            await dispatcher.stop()

    @pytest.mark.asyncio
    async def test_dispatch_returns_process_handle(self, monkeypatch):
        async def _fake_subprocess(*args, **kwargs):
            return _FakeProcess(returncode=0)

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_subprocess)

        dispatcher = LocalOfflineDispatcher()
        try:
            process = await dispatcher.dispatch(
                _make_snapshot(),
                wake=Wake.scheduled.value,
            )
            assert isinstance(process, _FakeProcess)
        finally:
            await dispatcher.stop()

    @pytest.mark.asyncio
    async def test_triggered_wake_propagates(self, monkeypatch):
        captured: dict = {}

        async def _fake_subprocess(*args, **kwargs):
            captured["env"] = kwargs.get("env")
            return _FakeProcess(returncode=0)

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_subprocess)

        dispatcher = LocalOfflineDispatcher()
        try:
            await dispatcher.dispatch(
                _make_snapshot(trigger_medium="sms"),
                wake=Wake.triggered.value,
            )
            assert captured["env"]["UNITY_OFFLINE_TASK_WAKE"] == Wake.triggered.value
        finally:
            await dispatcher.stop()


class TestDispatcherLifecycle:
    @pytest.mark.asyncio
    async def test_stop_without_dispatch_is_safe(self):
        dispatcher = LocalOfflineDispatcher()
        await dispatcher.stop()
        assert dispatcher._inflight == set()

    @pytest.mark.asyncio
    async def test_stop_cancels_inflight_watchers(self, monkeypatch):
        async def _hang(self):
            await asyncio.sleep(60.0)
            return b"", b""

        async def _fake_subprocess(*args, **kwargs):
            proc = _FakeProcess(returncode=0)
            proc.communicate = lambda: _hang(proc)
            return proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_subprocess)

        dispatcher = LocalOfflineDispatcher()
        await dispatcher.dispatch(_make_snapshot(), wake=Wake.scheduled.value)
        await asyncio.sleep(0)
        assert len(dispatcher._inflight) == 1
        await dispatcher.stop()
        assert dispatcher._inflight == set()
