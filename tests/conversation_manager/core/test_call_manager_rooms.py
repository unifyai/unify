from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from unify.conversation_manager.domains.call_manager import (
    CallConfig,
    LivekitCallManager,
)
from unify.conversation_manager.domains.event_handlers import EventHandler
from unify.conversation_manager.events import RecordingReady
from unify.gateway.common.livekit import make_call_scoped_sip_uri


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
        "simulated_utterance": "Hi, I'm T-W1N.",
        "source": "twin_onboarding_intro",
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
        call_session_id="session-123",
    )

    dispatch_job.assert_awaited_once()
    assert dispatch_job.await_args.kwargs["extra_metadata"] == {
        "opening_config": opening_config,
        "call_session_id": "session-123",
    }

    manager._worker_proc = None
    manager._active_job = False
    await manager.start_unify_meet(
        contact,
        boss,
        "unity_123_meet",
        opening_config=opening_config,
        call_session_id="session-456",
    )

    start_subprocess.assert_awaited_once()
    assert json.loads(
        start_subprocess.await_args.kwargs["extra_env"]["opening_config"],
    ) == (opening_config)
    assert (
        start_subprocess.await_args.kwargs["extra_env"]["CALL_SESSION_ID"]
        == "session-456"
    )


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


@pytest.mark.asyncio
async def test_recording_ready_recovers_exchange_from_stored_metadata():
    """A recycled container has no in-memory map, so fall back to the store.

    Egress finalises minutes after the room closes, by which time the pod that
    ran the call is usually gone. Without this the file exists in GCS but is
    never linked to its transcript.
    """
    transcript_manager = MagicMock()
    transcript_manager.resolve_exchange_id_by_metadata = MagicMock(
        side_effect=lambda key, value: 77 if key == "provider_call_sid" else None,
    )
    cm = MagicMock()
    cm._recording_exchange_ids = {}
    cm.transcript_manager = transcript_manager
    cm._session_logger = MagicMock()

    await EventHandler.handle_event(
        RecordingReady(
            conference_name="legacy_conf",
            recording_url="https://storage.googleapis.com/bucket/call.mp3",
            call_session_id="",
            provider_call_sid="CA111",
            room_name="unity_wa_room_123_CA111",
        ),
        cm,
    )

    transcript_manager.update_exchange_metadata.assert_called_once()
    exchange_id, metadata = transcript_manager.update_exchange_metadata.call_args.args
    assert exchange_id == 77
    assert metadata["recording_url"].endswith("/call.mp3")


@pytest.mark.asyncio
async def test_recording_ready_gives_up_when_no_identifier_resolves():
    transcript_manager = MagicMock()
    transcript_manager.resolve_exchange_id_by_metadata = MagicMock(return_value=None)
    cm = MagicMock()
    cm._recording_exchange_ids = {}
    cm.transcript_manager = transcript_manager
    cm._session_logger = MagicMock()

    await EventHandler.handle_event(
        RecordingReady(
            conference_name="unknown_conf",
            recording_url="https://storage.googleapis.com/bucket/call.mp3",
        ),
        cm,
    )

    transcript_manager.update_exchange_metadata.assert_not_called()


# ---------------------------------------------------------------------------
# _start_session_recording -- recording is requested at call start
# ---------------------------------------------------------------------------


def _recording_cm(**call_manager_attrs) -> MagicMock:
    cm = MagicMock()
    cm._session_logger = MagicMock()
    defaults = {
        "room_name": "unity_42_phone",
        "assistant_id": "42",
        "user_id": "7",
        "call_session_id": "",
        "provider_call_sid": "CA111",
        "conference_name": "Unity_conf_1",
        "unify_meet_call_session_id": "",
    }
    defaults.update(call_manager_attrs)
    for key, value in defaults.items():
        setattr(cm.call_manager, key, value)
    return cm


@pytest.mark.asyncio
async def test_phone_call_started_requests_recording_with_linkage_ids(monkeypatch):
    """The call-started path is where the room is first known to carry audio."""
    from unify.conversation_manager.domains import event_handlers
    from unify.conversation_manager import utils as cm_utils
    from unify.conversation_manager.events import PhoneCallStarted

    start = MagicMock(return_value=True)
    monkeypatch.setattr(cm_utils, "start_call_recording", start)

    cm = _recording_cm()
    await event_handlers._start_session_recording(
        PhoneCallStarted(contact={"contact_id": 2}),
        cm,
    )

    start.assert_called_once()
    args, kwargs = start.call_args
    assert args[0] == "unity_42_phone"
    assert args[1] == "42"
    assert kwargs["provider_call_sid"] == "CA111"
    assert kwargs["conference_name"] == "Unity_conf_1"


@pytest.mark.asyncio
async def test_unify_meet_started_requests_recording_with_session_id(monkeypatch):
    from unify.conversation_manager.domains import event_handlers
    from unify.conversation_manager import utils as cm_utils
    from unify.conversation_manager.events import UnifyMeetStarted

    start = MagicMock(return_value=True)
    monkeypatch.setattr(cm_utils, "start_call_recording", start)

    cm = _recording_cm(
        room_name="unity_call_CS_9",
        unify_meet_call_session_id="CS_9",
        provider_call_sid="",
        conference_name="",
    )
    await event_handlers._start_session_recording(
        UnifyMeetStarted(contact={"contact_id": 1}, call_session_id="CS_9"),
        cm,
    )

    start.assert_called_once()
    args, kwargs = start.call_args
    assert args[0] == "unity_call_CS_9"
    assert kwargs["call_session_id"] == "CS_9"
    # A meet has no telephony leg, so these stay empty rather than leaking
    # phone-call state from a previous session on the same call manager.
    assert kwargs["provider_call_sid"] == ""
    assert kwargs["conference_name"] == ""


@pytest.mark.asyncio
@pytest.mark.parametrize("event_name", ["GoogleMeetStarted", "TeamsMeetStarted"])
async def test_browser_meets_request_recording_keyed_on_the_meet_session(
    monkeypatch,
    event_name,
):
    """Browser meets are recorded now that the meeting is a published track.

    They were excluded while their audio was bridged through a pod-local device
    and never reached the LiveKit room, leaving the compositor nothing to mix.
    The linkage key is the meet session id -- the same key the utterances are
    written under -- because browser meets carry no telephony identifiers.
    """
    from unify.conversation_manager.domains import event_handlers
    from unify.conversation_manager import utils as cm_utils
    from unify.conversation_manager import events

    start = MagicMock(return_value=True)
    monkeypatch.setattr(cm_utils, "start_call_recording", start)

    event_cls = getattr(events, event_name)
    cm = _recording_cm(room_name="unity_42_gmeet")
    cm.call_manager.meet_session_id = "meet-sess-1"
    await event_handlers._start_session_recording(
        event_cls(contact={"contact_id": 2}),
        cm,
    )

    start.assert_called_once()
    args, kwargs = start.call_args
    assert args[0] == "unity_42_gmeet"
    assert kwargs["call_session_id"] == "meet-sess-1"
    # No telephony leg, so these stay empty rather than leaking phone state.
    assert kwargs["provider_call_sid"] == ""
    assert kwargs["conference_name"] == ""


@pytest.mark.asyncio
async def test_session_recording_skipped_without_a_room(monkeypatch):
    from unify.conversation_manager.domains import event_handlers
    from unify.conversation_manager import utils as cm_utils
    from unify.conversation_manager.events import PhoneCallStarted

    start = MagicMock(return_value=True)
    monkeypatch.setattr(cm_utils, "start_call_recording", start)

    cm = _recording_cm(room_name="")
    await event_handlers._start_session_recording(
        PhoneCallStarted(contact={"contact_id": 2}),
        cm,
    )

    start.assert_not_called()


@pytest.mark.asyncio
async def test_session_recording_failure_never_escapes(monkeypatch):
    """A recording problem must not disturb a live call."""
    from unify.conversation_manager.domains import event_handlers
    from unify.conversation_manager import utils as cm_utils
    from unify.conversation_manager.events import PhoneCallStarted

    monkeypatch.setattr(
        cm_utils,
        "start_call_recording",
        MagicMock(side_effect=RuntimeError("comms down")),
    )

    cm = _recording_cm()
    await event_handlers._start_session_recording(
        PhoneCallStarted(contact={"contact_id": 2}),
        cm,
    )
