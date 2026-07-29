"""Contract tests for the Recall.ai meeting-bot client."""

import json
from unittest.mock import MagicMock, patch

import pytest

from unify.conversation_manager.domains.recall import client as recall
from unify.conversation_manager.domains.recall.client import (
    RecallClient,
    RecallError,
    RecallNotConfigured,
)

API_KEY = "test-recall-key"  # pragma: allowlist secret


def _mock_session(
    *,
    status: int = 200,
    body: dict | None = None,
    text: str | None = None,
):
    """An ``aiohttp.ClientSession`` whose ``request`` is an async context manager."""
    response = MagicMock()
    response.status = status

    async def _text():
        if text is not None:
            return text
        return json.dumps(body if body is not None else {})

    response.text = _text

    request_cm = MagicMock()

    async def _aenter(*_args):
        return response

    async def _aexit(*_args):
        return False

    request_cm.__aenter__ = _aenter
    request_cm.__aexit__ = _aexit

    session = MagicMock()
    session.request = MagicMock(return_value=request_cm)
    session.__aenter__ = _aenter_session(session)
    session.__aexit__ = _aexit
    return session


def _aenter_session(session):
    async def _aenter(*_args):
        return session

    return _aenter


def _client(session) -> RecallClient:
    with patch("aiohttp.ClientSession", return_value=session):
        return RecallClient(
            api_key=API_KEY,
            base_url="https://us-west-2.recall.ai/api/v1",
        )


def _create_payload(session) -> dict:
    return session.request.call_args.kwargs["json"]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def test_missing_api_key_is_a_distinct_error() -> None:
    """An unprovisioned environment must be distinguishable from a broken one.

    RECALL_API_KEY is an optional pod secret, so its absence is a normal state
    that should route to the other provider rather than look like an outage.
    """
    with patch.dict("os.environ", {"RECALL_API_KEY": ""}, clear=False):
        with pytest.raises(RecallNotConfigured):
            RecallClient()


def test_unknown_region_fails_loudly() -> None:
    """Regions are separate deployments; a typo would 401, not 404.

    Failing at config time turns an authentication mystery into a clear error.
    """
    with patch.dict("os.environ", {"RECALL_REGION": "eu-west-9"}, clear=False):
        with pytest.raises(RecallError, match="not a Recall deployment"):
            recall.recall_base_url()


def test_default_region_matches_our_workspace() -> None:
    """Pods get no RECALL_REGION, so the default is what hosted joins use.

    Our workspace is in Frankfurt. Defaulting elsewhere would point every
    hosted join at a deployment our key is not valid in, which reads as bad
    credentials rather than a wrong region.
    """
    with patch.dict("os.environ", {"RECALL_REGION": ""}, clear=False):
        assert recall.recall_region() == "eu-central-1"
        assert recall.recall_base_url() == "https://eu-central-1.recall.ai/api/v1"


def test_region_drives_the_base_url() -> None:
    with patch.dict("os.environ", {"RECALL_REGION": "us-east-1"}, clear=False):
        assert recall.recall_base_url() == "https://us-east-1.recall.ai/api/v1"


def test_recall_configured_reflects_the_key() -> None:
    with patch.dict("os.environ", {"RECALL_API_KEY": "k"}, clear=False):
        assert recall.recall_configured() is True
    with patch.dict("os.environ", {"RECALL_API_KEY": "  "}, clear=False):
        assert recall.recall_configured() is False


# ---------------------------------------------------------------------------
# Bot creation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_bot_always_pins_zero_retention() -> None:
    """Recall retains indefinitely by default for new accounts.

    Leaving this unset accrues customer meeting media at a subprocessor for
    ever. We never read their recording -- transcription happens locally off
    the LiveKit track -- so nothing should be stored at all, and the guarantee
    belongs in the client rather than at each call site.
    """
    session = _mock_session(body={"id": "bot_1"})
    with patch("aiohttp.ClientSession", return_value=session):
        await _client(session).create_bot(
            meeting_url="https://meet.google.com/abc-defg-hij",
            bot_name="Unify",
            bridge_page_url="https://comms/meet/bridge?token=t",
        )

    assert _create_payload(session)["recording_config"] == {"retention": None}


@pytest.mark.asyncio
async def test_create_bot_sets_its_own_leave_timeouts() -> None:
    """Recall's defaults are wrong for us at both ends.

    ``everyone_left_timeout`` defaults to 2s, which is tight enough that a
    participant dropping and rejoining can end the meeting under them.
    ``noone_joined_timeout`` defaults to 1200s, which bills twenty minutes of
    bot time for a meeting nobody attends.
    """
    session = _mock_session(body={"id": "bot_1"})
    with patch("aiohttp.ClientSession", return_value=session):
        await _client(session).create_bot(
            meeting_url="https://meet.google.com/abc",
            bot_name="Unify",
            bridge_page_url="https://comms/meet/bridge?token=t",
        )

    leave = _create_payload(session)["automatic_leave"]
    assert leave["everyone_left_timeout"] == recall.EVERYONE_LEFT_TIMEOUT_S
    assert leave["noone_joined_timeout"] == recall.NOONE_JOINED_TIMEOUT_S
    assert leave["everyone_left_timeout"] > 2
    assert leave["noone_joined_timeout"] < 1200


@pytest.mark.asyncio
async def test_create_bot_renders_the_bridge_as_camera_by_default() -> None:
    session = _mock_session(body={"id": "bot_1"})
    with patch("aiohttp.ClientSession", return_value=session):
        await _client(session).create_bot(
            meeting_url="https://meet.google.com/abc",
            bot_name="Unify",
            bridge_page_url="https://comms/meet/bridge?token=t",
        )

    output = _create_payload(session)["output_media"]
    assert set(output) == {"camera"}
    assert output["camera"]["kind"] == "webpage"
    assert output["camera"]["config"]["url"] == "https://comms/meet/bridge?token=t"


@pytest.mark.asyncio
async def test_create_bot_can_render_the_bridge_as_screenshare() -> None:
    session = _mock_session(body={"id": "bot_1"})
    with patch("aiohttp.ClientSession", return_value=session):
        await _client(session).create_bot(
            meeting_url="https://meet.google.com/abc",
            bot_name="Unify",
            bridge_page_url="https://comms/meet/bridge?token=t",
            as_screenshare=True,
        )

    assert set(_create_payload(session)["output_media"]) == {"screenshare"}


@pytest.mark.asyncio
async def test_realtime_endpoint_is_omitted_when_no_relay_is_given() -> None:
    """No relay configured must not register an endpoint Recall would retry.

    A dead endpoint burns 30 reconnect attempts per bot and then gets disabled
    workspace-wide, which would break the relay for every later bot too.
    """
    session = _mock_session(body={"id": "bot_1"})
    with patch("aiohttp.ClientSession", return_value=session):
        await _client(session).create_bot(
            meeting_url="https://meet.google.com/abc",
            bot_name="Unify",
            bridge_page_url="https://comms/meet/bridge?token=t",
        )

    assert "realtime_endpoints" not in _create_payload(session)["recording_config"]
    assert "realtime_endpoints" not in _create_payload(session)


@pytest.mark.asyncio
async def test_realtime_endpoint_subscribes_only_to_read_events() -> None:
    """Every unread event is data-channel traffic for the length of the call."""
    session = _mock_session(body={"id": "bot_1"})
    with patch("aiohttp.ClientSession", return_value=session):
        await _client(session).create_bot(
            meeting_url="https://meet.google.com/abc",
            bot_name="Unify",
            bridge_page_url="https://comms/meet/bridge?token=t",
            realtime_events_url="wss://comms/meet/events?room=r&token=t",
        )

    endpoints = _create_payload(session)["recording_config"]["realtime_endpoints"]
    assert len(endpoints) == 1
    assert endpoints[0]["type"] == "websocket"
    assert endpoints[0]["events"] == [
        "participant_events.join",
        "participant_events.leave",
        "participant_events.update",
        "participant_events.speech_on",
        "participant_events.speech_off",
        "participant_events.chat_message",
    ]


@pytest.mark.asyncio
async def test_create_bot_uses_the_token_auth_scheme() -> None:
    """Recall expects ``Token <key>``, not ``Bearer``; Bearer silently 401s."""
    session = _mock_session(body={"id": "bot_1"})
    with patch("aiohttp.ClientSession", return_value=session):
        await _client(session).create_bot(
            meeting_url="https://meet.google.com/abc",
            bot_name="Unify",
            bridge_page_url="https://comms/meet/bridge?token=t",
        )

    headers = session.request.call_args.kwargs["headers"]
    assert headers["authorization"] == f"Token {API_KEY}"


# ---------------------------------------------------------------------------
# State parsing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_comes_from_the_latest_status_change() -> None:
    """Recall reports lifecycle as an append-only list, not a single field.

    Reading the first entry would leave every bot looking like it is still
    joining for the whole call.
    """
    session = _mock_session(
        body={
            "id": "bot_1",
            "status_changes": [
                {"code": "joining_call"},
                {"code": "in_waiting_room"},
                {"code": "in_call_recording"},
            ],
        },
    )
    with patch("aiohttp.ClientSession", return_value=session):
        state = await _client(session).get_bot("bot_1")

    assert state.status == "in_call_recording"
    assert state.in_call is True
    assert state.in_waiting_room is False
    assert state.terminal is False


@pytest.mark.asyncio
async def test_no_status_changes_reads_as_joining() -> None:
    """A just-accepted bot has an empty list; that is joining in all but name."""
    session = _mock_session(body={"id": "bot_1", "status_changes": []})
    with patch("aiohttp.ClientSession", return_value=session):
        state = await _client(session).get_bot("bot_1")

    assert state.status == "joining_call"
    assert state.terminal is False


@pytest.mark.asyncio
async def test_waiting_room_is_not_treated_as_in_call() -> None:
    """Lobby is a successful join in progress, not presence in the meeting."""
    session = _mock_session(
        body={"id": "bot_1", "status_changes": [{"code": "in_waiting_room"}]},
    )
    with patch("aiohttp.ClientSession", return_value=session):
        state = await _client(session).get_bot("bot_1")

    assert state.in_waiting_room is True
    assert state.in_call is False


@pytest.mark.asyncio
async def test_failure_sub_code_is_captured() -> None:
    """The sub_code is what a "could not join because..." line is built from."""
    session = _mock_session(
        body={
            "id": "bot_1",
            "status_changes": [
                {"code": "joining_call"},
                {"code": "fatal", "sub_code": "bot_denied_entry"},
            ],
        },
    )
    with patch("aiohttp.ClientSession", return_value=session):
        state = await _client(session).get_bot("bot_1")

    assert state.status == "fatal"
    assert state.sub_code == "bot_denied_entry"
    assert state.terminal is True


@pytest.mark.asyncio
async def test_in_call_not_recording_still_counts_as_present() -> None:
    """We transcribe locally, so recording state is irrelevant to usability."""
    session = _mock_session(
        body={"id": "bot_1", "status_changes": [{"code": "in_call_not_recording"}]},
    )
    with patch("aiohttp.ClientSession", return_value=session):
        state = await _client(session).get_bot("bot_1")

    assert state.in_call is True


@pytest.mark.asyncio
async def test_participants_carry_platform_identity() -> None:
    """This is the payload that replaces DOM-scraped speaker labels."""
    session = _mock_session(
        body={
            "id": "bot_1",
            "meeting_participants": [
                {"id": 1, "name": "Ada", "email": "ada@example.com", "is_host": True},
                {"id": 2, "name": "Grace", "extra_data": {"email": "g@example.com"}},
                {"id": 3, "name": "No Email"},
                "not-a-participant",
            ],
        },
    )
    with patch("aiohttp.ClientSession", return_value=session):
        state = await _client(session).get_bot("bot_1")

    assert [p.id for p in state.participants] == ["1", "2", "3"]
    assert state.participants[0].email == "ada@example.com"
    assert state.participants[0].is_host is True
    # Teams nests the address under extra_data rather than exposing it flat.
    assert state.participants[1].email == "g@example.com"
    assert state.participants[2].email is None


@pytest.mark.asyncio
async def test_payload_without_an_id_is_rejected() -> None:
    """A stateless bot reference is unusable; fail at the boundary."""
    session = _mock_session(body={"status_changes": []})
    with patch("aiohttp.ClientSession", return_value=session):
        with pytest.raises(RecallError, match="no id"):
            await _client(session).get_bot("bot_1")


# ---------------------------------------------------------------------------
# Failures
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_error_becomes_a_recall_error() -> None:
    session = _mock_session(status=422, text='{"detail":"bad meeting url"}')
    with patch("aiohttp.ClientSession", return_value=session):
        with pytest.raises(RecallError, match="HTTP 422"):
            await _client(session).get_bot("bot_1")


@pytest.mark.asyncio
async def test_non_json_body_becomes_a_recall_error() -> None:
    session = _mock_session(text="<html>gateway timeout</html>")
    with patch("aiohttp.ClientSession", return_value=session):
        with pytest.raises(RecallError, match="non-JSON"):
            await _client(session).get_bot("bot_1")


@pytest.mark.asyncio
async def test_leave_call_swallows_failures() -> None:
    """Teardown must reach its end state even for a bot that already left.

    Raising here would strand call_manager mid-cleanup with a live LiveKit room.
    """
    session = _mock_session(status=404, text='{"detail":"not found"}')
    with patch("aiohttp.ClientSession", return_value=session):
        await _client(session).leave_call("bot_1")


@pytest.mark.asyncio
async def test_participant_video_is_off_by_default() -> None:
    """It streams for the whole call, so a bot with no use for vision skips it."""
    session = _mock_session(body={"id": "bot_1"})
    with patch("aiohttp.ClientSession", return_value=session):
        await _client(session).create_bot(
            meeting_url="https://meet.google.com/abc",
            bot_name="Unify",
            bridge_page_url="https://comms/meet/bridge?token=t",
            realtime_events_url="wss://comms/meet/events?room=r&token=t",
        )

    payload = _create_payload(session)
    assert "video_mixed_layout" not in payload["recording_config"]
    events = payload["recording_config"]["realtime_endpoints"][0]["events"]
    assert not any(e.startswith("video_") for e in events)


@pytest.mark.asyncio
async def test_participant_video_needs_gallery_layout() -> None:
    """Separate per-participant video only arrives in gallery layout.

    Without it the platform sends one composited stream and there is no
    screenshare track to pick out -- the subscription would be silently useless.
    """
    session = _mock_session(body={"id": "bot_1"})
    with patch("aiohttp.ClientSession", return_value=session):
        await _client(session).create_bot(
            meeting_url="https://meet.google.com/abc",
            bot_name="Unify",
            bridge_page_url="https://comms/meet/bridge?token=t",
            realtime_events_url="wss://comms/meet/events?room=r&token=t",
            capture_participant_video=True,
        )

    payload = _create_payload(session)
    assert payload["recording_config"]["video_mixed_layout"] == "gallery_view_v2"
    # Retention must survive being edited alongside it.
    assert payload["recording_config"]["retention"] is None
    endpoints = payload["recording_config"]["realtime_endpoints"]
    assert "video_separate_png.data" in endpoints[0]["events"]


@pytest.mark.asyncio
async def test_participant_video_stays_png() -> None:
    """H264 would cost a decoder and the web_4_core variant for ~1000px."""
    session = _mock_session(body={"id": "bot_1"})
    with patch("aiohttp.ClientSession", return_value=session):
        await _client(session).create_bot(
            meeting_url="https://meet.google.com/abc",
            bot_name="Unify",
            bridge_page_url="https://comms/meet/bridge?token=t",
            realtime_events_url="wss://comms/meet/events?room=r&token=t",
            capture_participant_video=True,
        )

    events = _create_payload(session)["recording_config"]["realtime_endpoints"][0][
        "events"
    ]
    assert "video_separate_h264.data" not in events
    assert "variant" not in _create_payload(session)


@pytest.mark.asyncio
async def test_realtime_endpoints_are_nested_under_recording_config() -> None:
    """Recall ignores unknown top-level keys.

    Registering at the top level silently registers nothing: the bot never
    sends an event and it looks exactly like a relay that is down. Pinning the
    nesting is the only cheap way to catch that.
    """
    session = _mock_session(body={"id": "bot_1"})
    with patch("aiohttp.ClientSession", return_value=session):
        await _client(session).create_bot(
            meeting_url="https://meet.google.com/abc",
            bot_name="Unify",
            bridge_page_url="https://comms/meet/bridge?token=t",
            realtime_events_url="wss://comms/meet/events?room=r&token=t",
        )

    payload = _create_payload(session)
    assert "realtime_endpoints" not in payload, "must not be top level"
    endpoints = payload["recording_config"]["realtime_endpoints"]
    assert endpoints[0]["url"] == "wss://comms/meet/events?room=r&token=t"
    # The compliance pin shares this dict with three features now.
    assert payload["recording_config"]["retention"] is None
