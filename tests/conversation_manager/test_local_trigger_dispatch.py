"""Tests for the local-mode branch of offline trigger dispatch.

When ``SETTINGS.task.LOCAL_SCHEDULER_ENABLED`` is True, inbound-event
trigger matches that need offline (background) execution should spawn
``unify.task_scheduler.offline_runner`` as a child subprocess instead of
POSTing to Communication for a K8s job.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from unify.conversation_manager.cm_types import Medium
from unify.conversation_manager.domains import task_execution
from unify.task_scheduler.machine_state import TaskExecutionSnapshot


def _make_offline_trigger_snapshot(
    *,
    task_id: int = 17,
    assistant_id: str = "42",
    trigger_medium: str = "sms",
) -> TaskExecutionSnapshot:
    return TaskExecutionSnapshot(
        run_key=f"{assistant_id}:{task_id}",
        assistant_id=assistant_id,
        task_id=task_id,
        source_task_log_id=2000 + task_id,
        wake="triggered",
        delivery="offline",
        task_name="Reply to Alice",
        task_description="Send a templated reply to Alice when she emails.",
        trigger_medium=trigger_medium,
        revision="rev-xyz",
    )


class _FakeProcess:
    def __init__(self, returncode: int = 0) -> None:
        self.returncode = returncode

    async def communicate(self):
        return b"", b""


class _CapturingDispatcher:
    """Stand-in for LocalOfflineDispatcher that records dispatch calls."""

    def __init__(self) -> None:
        self._inflight: set[asyncio.Task] = set()
        self.watch_calls: list[tuple] = []

    async def _watch(self, process, snap, wake):
        self.watch_calls.append((snap.run_key, wake))
        await process.communicate()


def _make_fake_cm(dispatcher) -> SimpleNamespace:
    """Construct the minimum CM-shaped object the trigger helper expects."""

    materializer = SimpleNamespace(_offline=dispatcher)
    return SimpleNamespace(_activation_materializer=materializer)


class TestLocalOfflineTriggerDispatch:
    @pytest.mark.asyncio
    async def test_spawns_offline_runner_subprocess(self, monkeypatch):
        captured: dict = {}

        async def _fake_subprocess(*args, **kwargs):
            captured["args"] = args
            captured["env"] = kwargs.get("env")
            return _FakeProcess(returncode=0)

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_subprocess)

        dispatcher = _CapturingDispatcher()
        cm = _make_fake_cm(dispatcher)
        event = SimpleNamespace(content="Hi from Alice", timestamp=None)

        result = await task_execution._dispatch_offline_trigger_candidate_local(
            cm=cm,
            candidate=_make_offline_trigger_snapshot(),
            event=event,
            medium=Medium.SMS_MESSAGE,
            contact_id=123,
            sender_name="Alice",
        )

        assert captured["args"][-2:] == ("-m", "unify.task_scheduler.offline_runner")
        env = captured["env"]
        assert env["UNITY_OFFLINE_TASK_WAKE"] == "triggered"
        assert env["UNITY_OFFLINE_TASK_SOURCE_MEDIUM"] == "sms_message"
        assert env["UNITY_OFFLINE_TASK_SOURCE_CONTACT_ID"] == "123"
        assert env["UNITY_OFFLINE_TASK_SOURCE_REF"]
        assert result["success"] is True
        assert result["status"] == "spawned_local"
        assert result["wake"] == "triggered"

    @pytest.mark.asyncio
    async def test_adopts_watcher_onto_dispatcher_inflight(self, monkeypatch):
        async def _fake_subprocess(*args, **kwargs):
            return _FakeProcess(returncode=0)

        monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_subprocess)

        dispatcher = _CapturingDispatcher()
        cm = _make_fake_cm(dispatcher)
        event = SimpleNamespace(content="msg", timestamp=None)

        await task_execution._dispatch_offline_trigger_candidate_local(
            cm=cm,
            candidate=_make_offline_trigger_snapshot(),
            event=event,
            medium=Medium.SMS_MESSAGE,
            contact_id=42,
            sender_name="Bob",
        )

        for _ in range(5):
            if not dispatcher._inflight:
                break
            await asyncio.sleep(0.01)

        assert dispatcher.watch_calls == [("42:17", "triggered")]

    @pytest.mark.asyncio
    async def test_raises_when_materializer_not_initialised(self):
        cm = SimpleNamespace(_activation_materializer=None)
        event = SimpleNamespace(content="msg", timestamp=None)

        with pytest.raises(RuntimeError, match="Local activation scheduler"):
            await task_execution._dispatch_offline_trigger_candidate_local(
                cm=cm,
                candidate=_make_offline_trigger_snapshot(),
                event=event,
                medium=Medium.SMS_MESSAGE,
                contact_id=1,
                sender_name="Test",
            )

    @pytest.mark.asyncio
    async def test_raises_when_no_offline_dispatcher_on_materializer(self):
        materializer = SimpleNamespace()
        cm = SimpleNamespace(_activation_materializer=materializer)
        event = SimpleNamespace(content="msg", timestamp=None)

        with pytest.raises(RuntimeError, match="Local activation scheduler"):
            await task_execution._dispatch_offline_trigger_candidate_local(
                cm=cm,
                candidate=_make_offline_trigger_snapshot(),
                event=event,
                medium=Medium.SMS_MESSAGE,
                contact_id=1,
                sender_name="Test",
            )
