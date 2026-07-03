"""Tests for the persistent LiveKit agent worker architecture.

Covers:
- Worker startup and process management
- Job dispatch with metadata (replacing subprocess spawning)
- Entrypoint config parsing from job metadata
- IPC socket initialisation from metadata
- Worker watchdog / restart
- Fallback to legacy subprocess when worker is not running
"""

import asyncio
import json
import os
import time as _time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


async def _wait_for_condition(predicate, *, timeout: float = 5.0, poll: float = 0.01):
    start = _time.perf_counter()
    while _time.perf_counter() - start < timeout:
        if predicate():
            return True
        await asyncio.sleep(poll)
    return False


from unify.conversation_manager.domains.call_manager import (
    CallConfig,
    LivekitCallManager,
    make_room_name,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def config():
    return CallConfig(
        assistant_id="42",
        user_id="user_abc",
        assistant_bio="Test assistant bio",
        assistant_number="+15551234567",
        voice_provider="elevenlabs",
        voice_id="test_voice_id",
        job_name="unity-test-pod-abc123",
    )


@pytest.fixture
def call_manager(config):
    return LivekitCallManager(config)


@pytest.fixture
def sample_contact():
    return {
        "contact_id": 2,
        "first_name": "Alice",
        "surname": "Smith",
        "phone_number": "+15552222222",
    }


@pytest.fixture
def boss_contact():
    return {
        "contact_id": 1,
        "first_name": "Boss",
        "surname": "User",
        "phone_number": "+15551111111",
    }


# ---------------------------------------------------------------------------
# Worker agent name
# ---------------------------------------------------------------------------


class TestWorkerAgentName:
    def test_agent_name_uses_job_name(self, call_manager):
        assert call_manager.worker_agent_name == "unity_unity-test-pod-abc123"

    def test_agent_name_updates_with_job_name(self, call_manager):
        call_manager.job_name = "unity-other-pod-xyz"
        assert call_manager.worker_agent_name == "unity_unity-other-pod-xyz"


# ---------------------------------------------------------------------------
# Persistent worker startup
# ---------------------------------------------------------------------------


class TestPersistentWorkerStartup:
    # All tests in this class exercise the LivekitCallManager
    # start_persistent_worker() path which short-circuits with `return`
    # when LIVEKIT_URL is not set in env (production-side guard so the
    # worker is never spawned in non-livekit pods). In CI/local without
    # LIVEKIT_URL set, every test in this class would silently skip the
    # subprocess spawn and the `mock_run.assert_called_once()` assertions
    # would fail with "Called 0 times". Setting LIVEKIT_URL to a non-
    # empty sentinel via autouse monkeypatch lets the production path
    # proceed (the mocked run_script is then exercised normally — no
    # actual livekit connection is made because run_script is patched).
    @pytest.fixture(autouse=True)
    def _stub_livekit_url(self, monkeypatch):
        monkeypatch.setenv("LIVEKIT_URL", "wss://livekit.test.invalid")

    @pytest.mark.asyncio
    async def test_start_persistent_worker_spawns_process(self, call_manager):
        with patch(
            "unify.conversation_manager.domains.call_manager.run_script",
        ) as mock_run:
            mock_proc = MagicMock()
            mock_proc.poll.return_value = None
            mock_run.return_value = mock_proc

            call_manager.start_persistent_worker()

            mock_run.assert_called_once()
            args = mock_run.call_args[0]
            assert "worker.py" in str(args[0])
            assert "dev" in args
            assert "unity_unity-test-pod-abc123" in args
            assert call_manager._worker_proc is mock_proc

    @pytest.mark.asyncio
    async def test_start_persistent_worker_idempotent(self, call_manager):
        with patch(
            "unify.conversation_manager.domains.call_manager.run_script",
        ) as mock_run:
            mock_proc = MagicMock()
            mock_proc.poll.return_value = None
            mock_run.return_value = mock_proc

            call_manager.start_persistent_worker()
            call_manager.start_persistent_worker()

            mock_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_persistent_worker_restarts_if_dead(self, call_manager):
        with patch(
            "unify.conversation_manager.domains.call_manager.run_script",
        ) as mock_run:
            dead_proc = MagicMock()
            dead_proc.poll.return_value = 1
            call_manager._worker_proc = dead_proc

            new_proc = MagicMock()
            new_proc.poll.return_value = None
            mock_run.return_value = new_proc

            call_manager.start_persistent_worker()

            mock_run.assert_called_once()
            assert call_manager._worker_proc is new_proc

    @pytest.mark.asyncio
    async def test_refresh_keeps_worker_warm_when_unify_key_changes(self, call_manager):
        with (
            patch(
                "unify.conversation_manager.domains.call_manager.run_script",
            ) as mock_run,
            patch.object(
                call_manager,
                "cleanup_persistent_worker",
                new_callable=AsyncMock,
            ) as mock_cleanup,
        ):
            mock_proc = MagicMock()
            mock_proc.poll.return_value = None
            mock_run.return_value = mock_proc
            call_manager.start_persistent_worker()

            await call_manager.refresh_persistent_worker_after_key_change(
                "old-build-key",
                "new-org-key",
            )

            mock_cleanup.assert_not_awaited()
            mock_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_refresh_skips_restart_when_key_unchanged(self, call_manager):
        with (
            patch(
                "unify.conversation_manager.domains.call_manager.run_script",
            ) as mock_run,
            patch.object(
                call_manager,
                "cleanup_persistent_worker",
                new_callable=AsyncMock,
            ) as mock_cleanup,
        ):
            mock_proc = MagicMock()
            mock_proc.poll.return_value = None
            mock_run.return_value = mock_proc
            call_manager.start_persistent_worker()

            await call_manager.refresh_persistent_worker_after_key_change(
                "same-key",
                "same-key",
            )

            mock_cleanup.assert_not_awaited()
            mock_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_refresh_keeps_worker_warm_during_active_call(self, call_manager):
        call_manager._active_job = True
        with (
            patch(
                "unify.conversation_manager.domains.call_manager.run_script",
            ) as mock_run,
            patch.object(
                call_manager,
                "cleanup_persistent_worker",
                new_callable=AsyncMock,
            ) as mock_cleanup,
        ):
            mock_proc = MagicMock()
            mock_proc.poll.return_value = None
            mock_run.return_value = mock_proc
            call_manager.start_persistent_worker()

            await call_manager.refresh_persistent_worker_after_key_change(
                "old-key",
                "new-key",
            )

            mock_cleanup.assert_not_awaited()
            mock_run.assert_called_once()


class TestPersistentWorkerOptions:
    def test_worker_registers_for_publisher_jobs(self, monkeypatch):
        from livekit import agents
        from unify.conversation_manager.medium_scripts import worker

        captured = {}

        def capture_run(opts, *, log_level, devmode, register):
            captured["opts"] = opts
            captured["log_level"] = log_level
            captured["devmode"] = devmode
            captured["register"] = register

        monkeypatch.setattr(worker.sys, "argv", ["worker.py", "dev", "unity_test"])
        monkeypatch.setattr(worker, "clear_worker_signal_files", lambda: None)
        monkeypatch.setattr(worker, "_run_worker_with_registration_signal", capture_run)

        worker.main()

        assert captured["opts"].agent_name == "unity_test"
        assert captured["opts"].worker_type is agents.WorkerType.PUBLISHER
        assert captured["devmode"] is True
        assert captured["register"] is True


# ---------------------------------------------------------------------------
# Job dispatch
# ---------------------------------------------------------------------------


class TestJobDispatch:
    @pytest.mark.parametrize("is_coordinator", [False, True])
    @pytest.mark.asyncio
    async def test_dispatch_job_creates_agent_dispatch(
        self,
        config,
        is_coordinator,
        sample_contact,
        boss_contact,
    ):
        config.is_coordinator = is_coordinator
        call_manager = LivekitCallManager(config)
        worker_proc = MagicMock()
        worker_proc.poll.return_value = None
        call_manager._worker_proc = worker_proc
        mock_dispatch = MagicMock()
        mock_dispatch.id = "dispatch_123"

        mock_lk = AsyncMock()
        mock_lk.agent_dispatch.create_dispatch = AsyncMock(return_value=mock_dispatch)
        mock_lk.aclose = AsyncMock()

        from unify.conversation_manager.medium_scripts.worker import (
            WORKER_REGISTERED_PATH,
        )
        from unify.session_details import SESSION_DETAILS

        SESSION_DETAILS.unify_key = "tenant-assigned-key"
        try:
            with (
                patch(
                    "unify.conversation_manager.domains.call_manager.LiveKitAPI",
                    return_value=mock_lk,
                ),
                patch.object(
                    call_manager,
                    "_ensure_socket_server",
                    new_callable=AsyncMock,
                    return_value="/tmp/test.sock",
                ),
            ):
                with open(WORKER_REGISTERED_PATH, "w", encoding="utf-8"):
                    pass
                dispatched = await call_manager._dispatch_job(
                    "unity_42_phone",
                    "phone",
                    sample_contact,
                    boss_contact,
                    False,
                )
        finally:
            SESSION_DETAILS.reset()

        assert dispatched is True
        mock_lk.agent_dispatch.create_dispatch.assert_called_once()
        req = mock_lk.agent_dispatch.create_dispatch.call_args[0][0]
        assert req.agent_name == "unity_unity-test-pod-abc123"
        assert req.room == "unity_42_phone"

        meta = json.loads(req.metadata)
        assert meta["voice_provider"] == "elevenlabs"
        assert meta["voice_id"] == "test_voice_id"
        assert meta["channel"] == "phone"
        assert meta["outbound"] is False
        assert meta["contact"] == sample_contact
        assert meta["boss"] == boss_contact
        assert meta["is_coordinator"] is is_coordinator
        assert meta["ipc_socket_path"] == "/tmp/test.sock"
        assert meta["unify_key"] == "tenant-assigned-key"
        assert call_manager._active_job is True

    @pytest.mark.asyncio
    async def test_start_call_dispatches_when_worker_running(
        self,
        call_manager,
        sample_contact,
        boss_contact,
    ):
        mock_worker = MagicMock()
        mock_worker.poll.return_value = None
        call_manager._worker_proc = mock_worker

        with patch.object(
            call_manager,
            "_dispatch_job",
            new_callable=AsyncMock,
        ) as mock_dispatch:
            await call_manager.start_call(sample_contact, boss_contact)

            mock_dispatch.assert_called_once_with(
                make_room_name("42", "phone"),
                "phone_call",
                sample_contact,
                boss_contact,
                False,
                extra_metadata=None,
            )

    @pytest.mark.asyncio
    async def test_start_unify_meet_dispatches_when_worker_running(
        self,
        call_manager,
        sample_contact,
        boss_contact,
    ):
        mock_worker = MagicMock()
        mock_worker.poll.return_value = None
        call_manager._worker_proc = mock_worker

        with patch.object(
            call_manager,
            "_dispatch_job",
            new_callable=AsyncMock,
        ) as mock_dispatch:
            await call_manager.start_unify_meet(
                sample_contact,
                boss_contact,
                room_name="unity_42_meet",
            )

            mock_dispatch.assert_called_once_with(
                "unity_42_meet",
                "unify_meet",
                sample_contact,
                boss_contact,
                False,
                extra_metadata=None,
            )


# ---------------------------------------------------------------------------
# Fallback to legacy subprocess
# ---------------------------------------------------------------------------


class TestLegacyFallback:
    @pytest.mark.asyncio
    async def test_start_call_falls_back_without_worker(
        self,
        call_manager,
        sample_contact,
        boss_contact,
    ):
        assert call_manager._worker_proc is None

        with patch(
            "unify.conversation_manager.domains.call_manager.run_script",
        ) as mock_run:
            mock_proc = MagicMock()
            mock_run.return_value = mock_proc

            await call_manager.start_call(sample_contact, boss_contact)

            mock_run.assert_called_once()
            assert call_manager._call_proc is mock_proc

    @pytest.mark.asyncio
    async def test_start_call_falls_back_when_worker_dead(
        self,
        call_manager,
        sample_contact,
        boss_contact,
    ):
        dead_worker = MagicMock()
        dead_worker.poll.return_value = 1
        call_manager._worker_proc = dead_worker

        with patch(
            "unify.conversation_manager.domains.call_manager.run_script",
        ) as mock_run:
            mock_proc = MagicMock()
            mock_run.return_value = mock_proc

            await call_manager.start_call(sample_contact, boss_contact)

            mock_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_inbound_start_call_falls_back_when_worker_unregistered(
        self,
        call_manager,
        sample_contact,
        boss_contact,
    ):
        worker = MagicMock()
        worker.poll.return_value = None
        call_manager._worker_proc = worker

        with (
            patch.object(
                call_manager,
                "_wait_for_worker_registered",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "unify.conversation_manager.domains.call_manager.run_script",
            ) as mock_run,
        ):
            mock_proc = MagicMock()
            mock_run.return_value = mock_proc
            await call_manager.start_call(sample_contact, boss_contact, outbound=False)

        mock_run.assert_called_once()
        assert call_manager._call_proc is mock_proc

    @pytest.mark.asyncio
    async def test_inbound_unify_meet_falls_back_if_worker_restarts_during_wait(
        self,
        call_manager,
        sample_contact,
        boss_contact,
    ):
        worker = MagicMock()
        worker.poll.return_value = None
        call_manager._worker_proc = worker

        async def _worker_replaced(*_args, **_kwargs):
            call_manager._worker_proc = None
            return False

        with (
            patch.object(
                call_manager,
                "_wait_for_worker_registered",
                side_effect=_worker_replaced,
            ),
            patch(
                "unify.conversation_manager.domains.call_manager.run_script",
            ) as mock_run,
        ):
            mock_proc = MagicMock()
            mock_run.return_value = mock_proc
            await call_manager.start_unify_meet(
                sample_contact,
                boss_contact,
                room_name="unity_42_meet",
            )

        mock_run.assert_called_once()
        assert call_manager._call_proc is mock_proc


# ---------------------------------------------------------------------------
# has_active_call guard
# ---------------------------------------------------------------------------


class TestActiveCallGuard:
    @pytest.mark.asyncio
    async def test_rejects_second_call_during_dispatch(
        self,
        call_manager,
        sample_contact,
        boss_contact,
    ):
        call_manager._active_job = True
        call_manager._call_proc = MagicMock()

        with patch.object(
            call_manager,
            "_dispatch_job",
            new_callable=AsyncMock,
        ) as mock_dispatch:
            await call_manager.start_call(sample_contact, boss_contact)
            mock_dispatch.assert_not_called()

    @pytest.mark.asyncio
    async def test_rejects_second_call_during_subprocess(
        self,
        call_manager,
        sample_contact,
        boss_contact,
    ):
        call_manager._call_proc = MagicMock()

        with patch(
            "unify.conversation_manager.domains.call_manager.run_script",
        ) as mock_run:
            await call_manager.start_call(sample_contact, boss_contact)
            mock_run.assert_not_called()


# ---------------------------------------------------------------------------
# Worker registration gate
# ---------------------------------------------------------------------------


class TestWorkerReadiness:
    @pytest.mark.asyncio
    async def test_wait_for_worker_registered_unblocks_when_signal_file_appears(
        self,
        call_manager,
        tmp_path,
    ):
        from unify.conversation_manager.medium_scripts import worker as worker_mod

        registered_path = tmp_path / "unity_worker_registered"
        mock_worker = MagicMock()
        mock_worker.poll.return_value = None
        call_manager._worker_proc = mock_worker

        async def _touch_later() -> None:
            await asyncio.sleep(0.05)
            registered_path.write_text("", encoding="utf-8")

        with patch.object(worker_mod, "WORKER_REGISTERED_PATH", str(registered_path)):
            wait_task = asyncio.create_task(
                call_manager._wait_for_worker_registered(
                    mock_worker,
                    timeout=2.0,
                ),
            )
            touch_task = asyncio.create_task(_touch_later())
            await asyncio.wait([wait_task, touch_task], timeout=3.0)
            await wait_task

        assert registered_path.exists()


# ---------------------------------------------------------------------------
# Stale dispatch recovery
# ---------------------------------------------------------------------------


class TestStaleDispatchClearing:
    @pytest.mark.asyncio
    async def test_start_call_clears_orphaned_dispatch_and_proceeds(
        self,
        call_manager,
        sample_contact,
        boss_contact,
    ):
        from unify.conversation_manager.medium_scripts.worker import (
            WORKER_REGISTERED_PATH,
        )

        from unify.conversation_manager.in_memory_event_broker import (
            InMemoryEventBroker,
        )

        call_manager.set_event_broker(InMemoryEventBroker())
        call_manager._active_job = True
        mock_worker = MagicMock()
        mock_worker.poll.return_value = None
        call_manager._worker_proc = mock_worker

        mock_dispatch = MagicMock()
        mock_dispatch.id = "dispatch_stale_recovery"
        mock_lk = AsyncMock()
        mock_lk.agent_dispatch.create_dispatch = AsyncMock(return_value=mock_dispatch)
        mock_lk.aclose = AsyncMock()

        with (
            patch(
                "unify.conversation_manager.domains.call_manager.LiveKitAPI",
                return_value=mock_lk,
            ),
            patch.object(
                call_manager,
                "_ensure_socket_server",
                new_callable=AsyncMock,
                return_value="/tmp/test.sock",
            ),
        ):
            with open(WORKER_REGISTERED_PATH, "w", encoding="utf-8"):
                pass
            await call_manager.start_call(sample_contact, boss_contact)

        mock_lk.agent_dispatch.create_dispatch.assert_called_once()
        assert call_manager._active_job is True

    @pytest.mark.asyncio
    async def test_clear_stale_dispatch_state_preserves_active_job_when_ipc_client_connected(
        self,
        call_manager,
    ):
        from unify.conversation_manager.in_memory_event_broker import (
            InMemoryEventBroker,
        )

        broker = InMemoryEventBroker()
        call_manager.set_event_broker(broker)
        await call_manager._ensure_socket_server()
        assert call_manager._socket_server is not None

        socket_path = call_manager._socket_server.socket_path
        assert socket_path is not None

        from unify.conversation_manager.domains.ipc_socket import (
            CallEventSocketClient,
        )

        client = CallEventSocketClient(socket_path)
        await client.send_event("test:ping", '{"ok": true}')
        await _wait_for_condition(
            lambda: call_manager._socket_server is not None
            and call_manager._socket_server.has_connected_clients,
        )

        call_manager._active_job = True
        assert call_manager._clear_stale_dispatch_state() is False
        assert call_manager._active_job is True

        await client.close()
        if call_manager._socket_server:
            await call_manager._socket_server.stop()


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


class TestCleanup:
    @pytest.mark.asyncio
    async def test_cleanup_resets_active_job_flag(self, call_manager):
        call_manager._active_job = True
        await call_manager.cleanup_call_proc()
        assert call_manager._active_job is False

    @pytest.mark.asyncio
    async def test_cleanup_does_not_terminate_worker(self, call_manager):
        mock_worker = MagicMock()
        mock_worker.poll.return_value = None
        call_manager._worker_proc = mock_worker
        call_manager._active_job = True

        await call_manager.cleanup_call_proc()

        assert call_manager._worker_proc is mock_worker
        assert call_manager._active_job is False


# ---------------------------------------------------------------------------
# Entrypoint metadata parsing
# ---------------------------------------------------------------------------


class TestEntrypointMetadata:
    def test_load_config_from_metadata_valid(self):
        from unify.conversation_manager.medium_scripts.call import (
            _load_config_from_metadata,
        )

        ctx = MagicMock()
        ctx.job.metadata = json.dumps(
            {
                "voice_provider": "elevenlabs",
                "voice_id": "abc123",
                "channel": "phone",
                "outbound": False,
                "contact": {"first_name": "Alice"},
                "boss": {"first_name": "Boss"},
                "assistant_bio": "A helpful assistant",
                "is_coordinator": True,
                "ipc_socket_path": "/tmp/sock.sock",
            },
        )
        result = _load_config_from_metadata(ctx)
        assert result is not None
        assert result["voice_provider"] == "elevenlabs"
        assert result["contact"]["first_name"] == "Alice"
        assert result["is_coordinator"] is True
        assert result["ipc_socket_path"] == "/tmp/sock.sock"

    def test_load_config_from_metadata_empty(self):
        from unify.conversation_manager.medium_scripts.call import (
            _load_config_from_metadata,
        )

        ctx = MagicMock()
        ctx.job.metadata = ""
        assert _load_config_from_metadata(ctx) is None

    def test_load_config_from_metadata_invalid_json(self):
        from unify.conversation_manager.medium_scripts.call import (
            _load_config_from_metadata,
        )

        ctx = MagicMock()
        ctx.job.metadata = "not json"
        assert _load_config_from_metadata(ctx) is None

    def test_hydrate_session_details_from_metadata_sets_coordinator_flag(self):
        from unify.conversation_manager.medium_scripts.call import (
            _hydrate_session_details_from_metadata,
        )
        from unify.session_details import SESSION_DETAILS

        SESSION_DETAILS.reset()
        try:
            _hydrate_session_details_from_metadata(
                {
                    "assistant_bio": "Coordinator bio",
                    "assistant_id": "42",
                    "user_id": "user_abc",
                    "assistant_name": "Avery Coordinator",
                    "is_coordinator": True,
                },
            )

            assert SESSION_DETAILS.assistant.about == "Coordinator bio"
            assert SESSION_DETAILS.assistant.agent_id == 42
            assert SESSION_DETAILS.user.id == "user_abc"
            assert SESSION_DETAILS.assistant.name == "Avery Coordinator"
            assert SESSION_DETAILS.is_coordinator is True
        finally:
            SESSION_DETAILS.reset()

    def test_hydrate_session_details_from_metadata_sets_unify_key(self):
        from unify.conversation_manager.medium_scripts.call import (
            _hydrate_session_details_from_metadata,
        )
        from unify.session_details import SESSION_DETAILS

        SESSION_DETAILS.reset()
        try:
            _hydrate_session_details_from_metadata(
                {
                    "assistant_bio": "Bio",
                    "unify_key": "assigned-tenant-key",
                },
            )

            assert SESSION_DETAILS.unify_key == "assigned-tenant-key"
        finally:
            SESSION_DETAILS.reset()


# ---------------------------------------------------------------------------
# IPC socket initialisation from metadata
# ---------------------------------------------------------------------------


class TestIPCSocketInit:
    def test_init_socket_for_job_sets_env_and_singleton(self):
        from unify.conversation_manager.domains.ipc_socket import (
            CM_EVENT_SOCKET_ENV,
            init_socket_for_job,
        )

        client = init_socket_for_job("/tmp/test_job.sock")
        assert client is not None
        assert os.environ[CM_EVENT_SOCKET_ENV] == "/tmp/test_job.sock"
        assert client._socket_path == "/tmp/test_job.sock"

        del os.environ[CM_EVENT_SOCKET_ENV]
