"""Tests for the "safe to start a new call" readiness gating.

Covers the real resource the brain must await between back-to-back voice
sessions (e.g. the Meet -> WhatsApp-call handoff): a freshly prewarmed idle
worker process, a drained prior session, and no in-flight dispatch.

- ``worker.mark_worker_busy`` clears the idle-ready marker on job consume.
- ``LivekitCallManager.is_ready_for_new_call`` reflects the real signals.
- ``LivekitCallManager.await_ready_for_new_call`` awaits the marker (no sleep
  heuristic) and times out cleanly when the worker never re-warms.

No LLM or LiveKit calls are involved.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from unify.conversation_manager.domains.call_manager import (
    CallConfig,
    LivekitCallManager,
)
from unify.conversation_manager.medium_scripts import worker as worker_mod


def _build_call_manager() -> LivekitCallManager:
    cfg = CallConfig(
        assistant_id="42",
        user_id="user-1",
        assistant_bio="bio",
        assistant_number="+15555550000",
        voice_provider="elevenlabs",
        voice_id="voice-1",
        assistant_name="Assistant",
        job_name="job-1",
    )
    return LivekitCallManager(cfg)


def _alive_worker() -> MagicMock:
    proc = MagicMock()
    proc.poll.return_value = None  # still running
    return proc


@pytest.fixture
def ready_path(tmp_path, monkeypatch):
    """Point WORKER_READY_PATH at a temp file and return its Path."""
    path = tmp_path / "unity_worker_ready"
    monkeypatch.setattr(worker_mod, "WORKER_READY_PATH", str(path))
    return path


def test_mark_worker_busy_clears_ready_marker(ready_path):
    ready_path.write_text("")
    assert ready_path.exists()
    worker_mod.mark_worker_busy()
    assert not ready_path.exists()
    # Idempotent when already absent.
    worker_mod.mark_worker_busy()
    assert not ready_path.exists()


def test_ready_when_no_livekit_configured(monkeypatch):
    monkeypatch.delenv("LIVEKIT_URL", raising=False)
    cm = _build_call_manager()
    # Subprocess path spawns a fresh process per call -> always safe.
    assert cm.is_ready_for_new_call is True


def test_ready_requires_warm_idle_process(monkeypatch, ready_path):
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    cm = _build_call_manager()
    cm._worker_proc = _alive_worker()

    # No marker yet -> worker has not re-warmed -> not ready.
    assert not ready_path.exists()
    assert cm.is_ready_for_new_call is False

    # Marker present -> a fresh idle process is available -> ready.
    ready_path.write_text("")
    assert cm.is_ready_for_new_call is True


def test_not_ready_when_worker_dead(monkeypatch, ready_path):
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    ready_path.write_text("")
    cm = _build_call_manager()
    dead = MagicMock()
    dead.poll.return_value = -9  # exited
    cm._worker_proc = dead
    assert cm.is_ready_for_new_call is False


def test_not_ready_during_active_session(monkeypatch, ready_path):
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    ready_path.write_text("")
    cm = _build_call_manager()
    cm._worker_proc = _alive_worker()

    cm._active_job = True
    assert cm.is_ready_for_new_call is False
    cm._active_job = False
    assert cm.is_ready_for_new_call is True

    cm._whatsapp_call_joining = True
    assert cm.is_ready_for_new_call is False
    cm._whatsapp_call_joining = False

    cm._meet_session_id = "gmeet-1"
    assert cm.is_ready_for_new_call is False


def test_idle_pending_rewarm_true_when_alive_idle_no_marker(monkeypatch, ready_path):
    """Alive worker, fully idle, but no ready marker -> the wedged-pool state the
    watchdog must recover from."""
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    cm = _build_call_manager()
    cm._worker_proc = _alive_worker()
    assert not ready_path.exists()
    assert cm._is_idle_pending_rewarm() is True


def test_idle_pending_rewarm_false_when_marker_present(monkeypatch, ready_path):
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    cm = _build_call_manager()
    cm._worker_proc = _alive_worker()
    ready_path.write_text("")
    assert cm._is_idle_pending_rewarm() is False


def test_idle_pending_rewarm_false_when_no_livekit(monkeypatch, ready_path):
    monkeypatch.delenv("LIVEKIT_URL", raising=False)
    cm = _build_call_manager()
    cm._worker_proc = _alive_worker()
    # Subprocess path needs no warm pool, so there is nothing to recover.
    assert cm._is_idle_pending_rewarm() is False


def test_idle_pending_rewarm_false_during_active_session(monkeypatch, ready_path):
    """A missing marker during a live session / setup is normal (the idle process
    became the job), not a wedged pool, so it must NOT trigger a restart."""
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    cm = _build_call_manager()
    cm._worker_proc = _alive_worker()
    assert not ready_path.exists()

    cm._active_job = True
    assert cm._is_idle_pending_rewarm() is False
    cm._active_job = False

    cm._whatsapp_call_joining = True
    assert cm._is_idle_pending_rewarm() is False
    cm._whatsapp_call_joining = False

    cm._meet_session_id = "gmeet-1"
    assert cm._is_idle_pending_rewarm() is False
    cm._meet_session_id = None

    # Back to a genuinely idle, unwarmed worker.
    assert cm._is_idle_pending_rewarm() is True


def test_idle_pending_rewarm_false_when_clients_connected(monkeypatch, ready_path):
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    cm = _build_call_manager()
    cm._worker_proc = _alive_worker()
    socket_server = MagicMock()
    socket_server.has_connected_clients = True
    cm._socket_server = socket_server
    assert cm._is_idle_pending_rewarm() is False


def test_idle_pending_rewarm_false_when_worker_dead(monkeypatch, ready_path):
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    cm = _build_call_manager()
    dead = MagicMock()
    dead.poll.return_value = -9
    cm._worker_proc = dead
    # A dead worker is handled by the exit branch, not the re-warm branch.
    assert cm._is_idle_pending_rewarm() is False


@pytest.mark.asyncio
async def test_restart_worker_terminates_and_restarts(monkeypatch):
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    cm = _build_call_manager()
    proc = _alive_worker()
    cm._worker_proc = proc

    import unify.conversation_manager.domains.call_manager as cm_mod

    terminated: list = []
    monkeypatch.setattr(cm_mod, "terminate_process", lambda p, _t: terminated.append(p))
    restarted: list = []
    monkeypatch.setattr(cm, "start_persistent_worker", lambda: restarted.append(True))

    await cm._restart_worker()

    assert terminated == [proc]
    assert restarted == [True]


@pytest.mark.asyncio
async def test_await_ready_returns_immediately_when_ready(monkeypatch, ready_path):
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    ready_path.write_text("")
    cm = _build_call_manager()
    cm._worker_proc = _alive_worker()
    assert await cm.await_ready_for_new_call(timeout=0.1) is True


@pytest.mark.asyncio
async def test_await_ready_times_out_when_worker_never_warms(monkeypatch, ready_path):
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    cm = _build_call_manager()
    cm._worker_proc = _alive_worker()
    # Marker never appears -> times out, no exception, returns False.
    assert await cm.await_ready_for_new_call(timeout=0.3, poll_interval=0.05) is False


@pytest.mark.asyncio
async def test_await_ready_resolves_when_marker_appears(monkeypatch, ready_path):
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    cm = _build_call_manager()
    cm._worker_proc = _alive_worker()

    import asyncio

    async def _warm_soon():
        await asyncio.sleep(0.1)
        ready_path.write_text("")

    warm = asyncio.create_task(_warm_soon())
    try:
        assert (
            await cm.await_ready_for_new_call(timeout=2.0, poll_interval=0.05) is True
        )
    finally:
        await warm
