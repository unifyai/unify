from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from unity.conversation_manager.domains.call_manager import (
    CallConfig,
    LivekitCallManager,
)
from unity.conversation_manager.domains.event_handlers import EventHandler
from unity.conversation_manager.events import RecordingReady
from unity.gateway.common.livekit import make_call_scoped_sip_uri


@pytest.mark.asyncio
async def test_start_call_uses_provided_room_name(monkeypatch):
    manager = LivekitCallManager(
        CallConfig(
            assistant_id="123",
            user_id="user-123",
            assistant_bio="",
            assistant_number="+15550000000",
            voice_provider="test",
            voice_id="voice",
        ),
    )
    monkeypatch.setattr(manager, "_ensure_socket_server", AsyncMock())
    start_subprocess = AsyncMock()
    monkeypatch.setattr(manager, "_start_call_subprocess", start_subprocess)

    contact = {"contact_id": 2, "whatsapp_number": "+15550000001"}
    boss = {"contact_id": 1}
    await manager.start_call(
        contact,
        boss,
        channel="whatsapp_call",
        room_name="unity_wa_room_123_CA111",
    )

    assert manager.room_name == "unity_wa_room_123_CA111"
    start_subprocess.assert_awaited_once()
    assert start_subprocess.await_args.args[0] == "unity_wa_room_123_CA111"


@pytest.mark.asyncio
async def test_start_unify_meet_passes_opening_config_to_worker(monkeypatch):
    manager = LivekitCallManager(
        CallConfig(
            assistant_id="123",
            user_id="user-123",
            assistant_bio="",
            assistant_number="+15550000000",
            voice_provider="test",
            voice_id="voice",
        ),
    )
    monkeypatch.setattr(manager, "_ensure_socket_server", AsyncMock(return_value=None))
    dispatch_job = AsyncMock()
    start_subprocess = AsyncMock()
    monkeypatch.setattr(manager, "_dispatch_job", dispatch_job)
    monkeypatch.setattr(manager, "_start_call_subprocess", start_subprocess)

    opening_config = {
        "mode": "simulated",
        "simulated_utterance": "Hi, I'm Marty.",
        "source": "marty_onboarding_intro",
    }
    contact = {"contact_id": 1, "is_system": False}
    boss = {"contact_id": 1}

    manager._worker_proc = MagicMock()
    manager._worker_proc.poll.return_value = None
    await manager.start_unify_meet(
        contact,
        boss,
        "unity_123_meet",
        opening_config=opening_config,
    )

    dispatch_job.assert_awaited_once()
    assert dispatch_job.await_args.kwargs["extra_metadata"] == {
        "opening_config": opening_config,
    }

    manager._worker_proc = None
    manager._active_job = False
    await manager.start_unify_meet(
        contact,
        boss,
        "unity_123_meet",
        opening_config=opening_config,
    )

    start_subprocess.assert_awaited_once()
    assert json.loads(
        start_subprocess.await_args.kwargs["extra_env"]["opening_config"],
    ) == (opening_config)


class _FakeCredentials:
    def get_optional(self, name: str, default: str = "") -> str:
        if name == "LIVEKIT_SIP_URI":
            return "tenant.sip.livekit.cloud"
        return default


def test_local_call_scoped_sip_uri_uses_unique_target_and_headers():
    uri, sip_target = make_call_scoped_sip_uri(
        "+15550800000",
        "CA:111",
        _FakeCredentials(),
        headers={
            "Unity-Call-Session": "CA-111",
            "X-Unity-Room": "unity_wa_room_123_CA-111",
        },
    )

    assert sip_target == "15550800000-CA-111"
    assert uri.startswith("sip:15550800000-CA-111@tenant.sip.livekit.cloud?")
    assert "X-Unity-Call-Session=CA-111" in uri
    assert "X-Unity-Room=unity_wa_room_123_CA-111" in uri


@pytest.mark.parametrize(
    ("recording_keys", "expected_exchange_id"),
    [
        ({"CA111": 10, "unity_wa_room_123_CA111": 20, "legacy_conf": 30}, 10),
        ({"unity_wa_room_123_CA111": 20, "legacy_conf": 30}, 20),
        ({"legacy_conf": 30}, 30),
    ],
)
@pytest.mark.asyncio
async def test_recording_ready_prefers_call_session_then_room_then_conference(
    recording_keys,
    expected_exchange_id,
):
    transcript_manager = MagicMock()
    cm = MagicMock()
    cm._recording_exchange_ids = dict(recording_keys)
    cm.transcript_manager = transcript_manager
    cm._session_logger = MagicMock()

    await EventHandler.handle_event(
        RecordingReady(
            conference_name="legacy_conf",
            recording_url="https://storage.googleapis.com/bucket/call.mp3",
            call_session_id="CA111",
            provider_call_sid="CA111",
            room_name="unity_wa_room_123_CA111",
        ),
        cm,
    )

    transcript_manager.update_exchange_metadata.assert_called_once()
    exchange_id, metadata = transcript_manager.update_exchange_metadata.call_args.args
    assert exchange_id == expected_exchange_id
    assert metadata["recording_url"].endswith("/call.mp3")
    assert metadata["recording_call_session_id"] == "CA111"
    assert metadata["recording_room_name"] == "unity_wa_room_123_CA111"
