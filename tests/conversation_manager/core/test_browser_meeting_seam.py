"""Backend selection and the agent-service translation layer."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unify.conversation_manager.domains.agent_service_meeting import (
    AgentServiceMeetProvider,
)
from unify.conversation_manager.domains.browser_meeting import (
    AGENT_SERVICE_PROVIDER,
    RECALL_PROVIDER,
    MeetJoinResult,
    build_meet_provider,
)

RECALL_ENV = {
    "MEET_PROVIDER": "recall",
    "RECALL_API_KEY": "k",  # pragma: allowlist secret
    "MEET_BRIDGE_PAGE_URL": "https://comms.example.com/meet/bridge",
}


def _call_manager() -> MagicMock:
    cm = MagicMock()
    cm.assistant_id = 25
    return cm


# ---------------------------------------------------------------------------
# Backend selection
# ---------------------------------------------------------------------------


def test_default_selects_the_local_browser() -> None:
    """Deploying the Recall wiring must not itself change how meets are joined."""
    with patch.dict("os.environ", {"MEET_PROVIDER": ""}, clear=False):
        provider = build_meet_provider(_call_manager())

    assert provider.name == AGENT_SERVICE_PROVIDER


def test_recall_is_selected_when_configured() -> None:
    with patch.dict("os.environ", RECALL_ENV, clear=False):
        provider = build_meet_provider(_call_manager())

    assert provider.name == RECALL_PROVIDER


def test_recall_without_a_key_falls_back_rather_than_failing() -> None:
    """A misconfigured pod should still join meetings, loudly.

    During the transition the browser is still there to fall back to, and a
    meeting nobody attends is worse than one joined the old way.
    """
    env = dict(RECALL_ENV)
    env["RECALL_API_KEY"] = ""
    with patch.dict("os.environ", env, clear=False):
        provider = build_meet_provider(_call_manager())

    assert provider.name == AGENT_SERVICE_PROVIDER


# ---------------------------------------------------------------------------
# agent-service translation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_join_forwards_and_wraps_the_result() -> None:
    cm = _call_manager()
    cm._agent_service_join = AsyncMock(
        return_value=MeetJoinResult(ok=True, session_id="sess_1", lobby=True),
    )
    provider = AgentServiceMeetProvider(cm)

    result = await provider.join(
        channel="google_meet",
        meeting_url="https://meet.google.com/abc",
        display_name="Unify",
        room_name="unity_25_gmeet",
    )

    assert result.session_id == "sess_1"
    assert result.lobby is True
    assert cm._agent_service_join.await_args.kwargs["meet_url"] == (
        "https://meet.google.com/abc"
    )


@pytest.mark.asyncio
async def test_active_status_is_in_the_call() -> None:
    cm = _call_manager()
    cm._agent_service_state = AsyncMock(
        return_value={"status": "active", "activeSpeaker": "Ada"},
    )
    state = await AgentServiceMeetProvider(cm).state(
        channel="google_meet",
        session_id="sess_1",
    )

    assert state.ended is False
    assert state.lobby is False
    assert state.active_speaker == "Ada"


@pytest.mark.asyncio
async def test_lobby_status_is_not_ended() -> None:
    """Waiting for admission is a join in progress, not a finished meeting."""
    cm = _call_manager()
    cm._agent_service_state = AsyncMock(return_value={"status": "lobby"})
    state = await AgentServiceMeetProvider(cm).state(
        channel="google_meet",
        session_id="sess_1",
    )

    assert state.lobby is True
    assert state.ended is False


@pytest.mark.asyncio
async def test_any_other_status_reads_as_ended() -> None:
    """agent-service knows only active/lobby as live states.

    Treating an unrecognised status as still-live would leave the session
    pinned open, blocking every later call with "session already active".
    """
    cm = _call_manager()
    for status in ("ended", "removed", "error", ""):
        cm._agent_service_state = AsyncMock(return_value={"status": status})
        state = await AgentServiceMeetProvider(cm).state(
            channel="google_meet",
            session_id="sess_1",
        )
        assert state.ended is True, status


@pytest.mark.asyncio
async def test_unknown_session_returns_no_state() -> None:
    """A failed poll is not evidence the meeting ended."""
    cm = _call_manager()
    cm._agent_service_state = AsyncMock(return_value=None)
    assert (
        await AgentServiceMeetProvider(cm).state(
            channel="google_meet",
            session_id="sess_1",
        )
        is None
    )


@pytest.mark.asyncio
async def test_preflight_is_delegated() -> None:
    """The readiness wait must stay ahead of fast-brain dispatch."""
    cm = _call_manager()
    cm._agent_service_preflight = AsyncMock(return_value="agent_service_unavailable")
    assert await AgentServiceMeetProvider(cm).preflight() == (
        "agent_service_unavailable"
    )
