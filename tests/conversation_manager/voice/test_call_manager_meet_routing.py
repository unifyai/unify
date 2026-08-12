"""Symbolic tests for browser-meet channel routing in `LivekitCallManager`.

These tests stub out every external dependency of ``_start_meet`` (LiveKit
room creation, the meeting backend behind the ``meet_provider`` seam, the IPC
socket server, and the call subprocess) and verify only the channel-dispatch
logic:

- The LiveKit room name is derived from ``_MEET_ROOM_SUFFIX[channel]`` via
  ``make_room_name`` (``unity_<id>_gmeet`` vs ``unity_<id>_teams``).
- The backend join is asked for exactly the requested channel, meeting URL,
  display name, and room name.
- The active-channel state (``_call_channel``, ``has_active_google_meet``,
  ``has_active_teams_meet``) is set correctly and exclusively per channel.
- The backend session id is captured into ``_meet_session_id`` and the
  joining flag is cleared on success; a failed join reports its reason and
  runs cleanup.

No LLM or LiveKit calls are involved.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from unify.conversation_manager.domains import call_manager as call_manager_module
from unify.conversation_manager.domains.browser_meeting import MeetJoinResult
from unify.conversation_manager.domains.call_manager import (
    CallConfig,
    LivekitCallManager,
    make_room_name,
)

_ASSISTANT_ID = "42"
_MEET_URL = "https://example.test/meeting/abc"
_CONTACT = {"contact_id": 2, "first_name": "Alice", "is_system": False}
_BOSS = {"contact_id": 1, "first_name": "Boss", "is_system": True}


def _build_call_manager() -> LivekitCallManager:
    """Build a `LivekitCallManager` with empty config and no event broker."""
    cfg = CallConfig(
        assistant_id=_ASSISTANT_ID,
        user_id="user-1",
        assistant_bio="bio",
        assistant_number="+15555550000",
        voice_provider="elevenlabs",
        voice_id="voice-1",
        assistant_name="Assistant",
        job_name="job-1",
    )
    return LivekitCallManager(cfg, event_broker=None)


def _patch_meet_dependencies(
    monkeypatch,
    cm: LivekitCallManager,
    *,
    join_result: MeetJoinResult | None = None,
):
    """Patch all external dependencies of ``_start_meet``.

    Returns a dict of capture buckets:
      * ``room_creates``: list of ``CreateRoomRequest`` payloads handed to LiveKit.
      * ``provider``: the fake meeting backend behind the ``meet_provider``
        seam (``preflight`` passes; ``join`` returns ``join_result``).
      * ``subprocess_calls``: list of ``(room_name, channel, contact, boss,
        outbound, extra_env)`` tuples for the fallback subprocess path.
    """
    room_creates: list = []
    subprocess_calls: list = []

    fake_lk = MagicMock()
    fake_lk.aclose = AsyncMock()

    async def _fake_create_room(req):
        room_creates.append(req)

    fake_lk.room.create_room = _fake_create_room

    def _lk_factory(**_kwargs):
        return fake_lk

    monkeypatch.setattr(call_manager_module, "LiveKitAPI", _lk_factory)

    provider = MagicMock()
    provider.preflight = AsyncMock(return_value=None)
    provider.join = AsyncMock(
        return_value=join_result or MeetJoinResult(ok=True, session_id="session-xyz"),
    )
    provider.leave = AsyncMock()
    provider.state = AsyncMock(return_value=None)
    cm._meet_provider = provider

    async def _noop_ensure_socket():
        return None

    monkeypatch.setattr(cm, "_ensure_socket_server", _noop_ensure_socket)

    async def _capture_subprocess(
        room_name,
        channel,
        contact,
        boss,
        outbound,
        *,
        extra_env=None,
    ):
        subprocess_calls.append(
            (room_name, channel, contact, boss, outbound, extra_env),
        )

    monkeypatch.setattr(cm, "_start_call_subprocess", _capture_subprocess)

    cm._worker_proc = None
    cm._socket_server = None

    return {
        "room_creates": room_creates,
        "provider": provider,
        "subprocess_calls": subprocess_calls,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("channel", "expected_room_suffix"),
    [
        ("google_meet", "gmeet"),
        ("teams_meet", "teams"),
    ],
)
async def test_start_meet_routes_per_channel(
    monkeypatch,
    channel: str,
    expected_room_suffix: str,
):
    """`_start_meet(channel, ...)` must use the channel-specific LiveKit room
    suffix from `_MEET_ROOM_SUFFIX`, ask the meeting backend to join that
    channel, capture the returned session id, and flip only the matching
    `has_active_*` property."""
    cm = _build_call_manager()
    captured = _patch_meet_dependencies(monkeypatch, cm)

    ok = await cm._start_meet(channel, _MEET_URL, _CONTACT, _BOSS)

    assert ok is True
    assert cm._call_channel == channel
    assert cm._meet_session_id == "session-xyz"
    assert cm._meet_joining is False
    assert cm._meet_lobby_waiting is False
    assert cm._disconnect_contact == _CONTACT

    expected_room_name = make_room_name(_ASSISTANT_ID, expected_room_suffix)
    assert cm.room_name == expected_room_name

    assert len(captured["room_creates"]) == 1
    create_req = captured["room_creates"][0]
    assert create_req.name == expected_room_name
    assert create_req.empty_timeout >= 3600
    assert create_req.departure_timeout >= 3600

    provider = captured["provider"]
    provider.join.assert_awaited_once_with(
        channel=channel,
        meeting_url=_MEET_URL,
        display_name="Assistant",
        room_name=expected_room_name,
    )

    assert len(captured["subprocess_calls"]) == 1
    sp_room, sp_channel, sp_contact, sp_boss, sp_outbound, sp_extra = captured[
        "subprocess_calls"
    ][0]
    assert sp_room == expected_room_name
    assert sp_channel == channel
    assert sp_contact == _CONTACT
    assert sp_boss == _BOSS
    assert sp_outbound is False
    assert sp_extra is not None
    assert sp_extra["meet_url"] == _MEET_URL
    assert sp_extra["meet_display_name"] == "Assistant"

    if channel == "teams_meet":
        assert cm.has_active_teams_meet is True
        assert cm.has_active_google_meet is False
    else:
        assert cm.has_active_google_meet is True
        assert cm.has_active_teams_meet is False
    assert cm.has_active_meet() is True

    # The backend state watcher is started on success; cancel it so the fake
    # provider is never polled after the test ends.
    assert cm._meet_state_task is not None
    cm._meet_state_task.cancel()


@pytest.mark.asyncio
async def test_start_teams_meet_wrapper_delegates_to_start_meet(monkeypatch):
    """The public `start_teams_meet` wrapper must forward to `_start_meet`
    with the ``teams_meet`` channel argument unchanged."""
    cm = _build_call_manager()

    seen: dict = {}

    async def _capture(channel, meet_url, contact, boss, display_name=""):
        seen["channel"] = channel
        seen["meet_url"] = meet_url
        seen["contact"] = contact
        seen["boss"] = boss
        seen["display_name"] = display_name
        return True

    monkeypatch.setattr(cm, "_start_meet", _capture)

    ok = await cm.start_teams_meet(
        _MEET_URL,
        _CONTACT,
        _BOSS,
        display_name="Custom Name",
    )

    assert ok is True
    assert seen == {
        "channel": "teams_meet",
        "meet_url": _MEET_URL,
        "contact": _CONTACT,
        "boss": _BOSS,
        "display_name": "Custom Name",
    }


@pytest.mark.asyncio
async def test_start_meet_join_failure_clears_state(monkeypatch):
    """When the meeting backend reports a failed join, ``_start_meet`` must
    surface the failure reason, clear the joining flag, leave no captured
    session id, and run cleanup so the per-channel ``has_active_*`` property
    goes back to False."""
    cm = _build_call_manager()
    captured = _patch_meet_dependencies(
        monkeypatch,
        cm,
        join_result=MeetJoinResult(ok=False, failure_reason="bot was denied entry"),
    )

    cleanup_calls: list = []

    async def _capture_cleanup(channel):
        cleanup_calls.append(channel)

    monkeypatch.setattr(cm, "_cleanup_meet", _capture_cleanup)

    ok = await cm._start_meet("teams_meet", _MEET_URL, _CONTACT, _BOSS)

    assert ok is False
    assert cm._meet_session_id is None
    assert cm._meet_joining is False
    assert cm._meet_lobby_waiting is False
    assert cm.meet_join_failure_reason == "bot was denied entry"
    assert cleanup_calls == ["teams_meet"]
    provider = captured["provider"]
    assert provider.join.await_args.kwargs["channel"] == "teams_meet"


# ---------------------------------------------------------------------------
# Desktop screenshare state across meetings
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_presenting_state_does_not_survive_the_meeting(monkeypatch):
    """This manager outlives individual meetings; the presenting flag must not.

    Left set, the next meeting starts believing a share is already up: the start
    tool is replaced by the stop tool, and ``start_meet_screenshare`` short-circuits
    on its idempotence guard and reports success without putting anything on the
    screenshare surface. The assistant then claims to be showing a screen nobody
    can see.
    """
    cm = _build_call_manager()
    cm._meet_session_id = "bot_1"
    cm._call_channel = "google_meet"
    cm._meet_presenting = True

    monkeypatch.setattr(
        call_manager_module,
        "delete_livekit_room",
        AsyncMock(),
        raising=False,
    )
    monkeypatch.setattr(cm, "cleanup_call_proc", AsyncMock())
    cm._meet_provider = MagicMock()
    cm._meet_provider.leave = AsyncMock()

    await cm._cleanup_meet("google_meet")

    assert cm.is_presenting_to_meet is False
    assert cm._meet_session_id is None


@pytest.mark.asyncio
async def test_starting_a_share_without_a_managed_desktop_is_refused(monkeypatch):
    """Only the managed desktop is ever shared.

    A user's own linked machine is not the assistant's to put in front of a room
    of people, so this is refused rather than resolved to whatever desktop happens
    to be reachable.
    """
    cm = _build_call_manager()
    cm._meet_session_id = "bot_1"
    cm._call_channel = "google_meet"
    cm._meet_provider = MagicMock()
    cm._meet_provider.present = AsyncMock(return_value=True)

    monkeypatch.setattr(
        call_manager_module.SESSION_DETAILS.assistant,
        "desktop_mode",
        "none",
        raising=False,
    )

    assert await cm.start_meet_screenshare() is False
    cm._meet_provider.present.assert_not_called()
    assert cm.is_presenting_to_meet is False


@pytest.mark.asyncio
async def test_a_failed_stop_still_clears_the_flag() -> None:
    """Otherwise the state is unrecoverable for the rest of the meeting.

    A stop that fails leaves the surface up, but continuing to claim we are
    presenting hides the start tool behind the idempotence guard with no way back.
    """
    cm = _build_call_manager()
    cm._meet_session_id = "bot_1"
    cm._call_channel = "google_meet"
    cm._meet_presenting = True
    cm._meet_provider = MagicMock()
    cm._meet_provider.stop_present = AsyncMock(return_value=False)

    assert await cm.stop_meet_screenshare() is False
    assert cm.is_presenting_to_meet is False
