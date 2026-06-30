from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from unify.conversation_manager.conversation_manager import (
    IDLE_SMALLTALK_RECENT_COMMS_SECONDS,
    _idle_status_smalltalk_allowed,
)
from unify.conversation_manager.domains.contact_index import Message

pytestmark = pytest.mark.no_unify_context

_NOW = datetime(2026, 6, 30, 9, 30, tzinfo=timezone.utc)


def _message(*, role: str, seconds_ago: float) -> Message:
    return Message(
        name="Caller" if role == "user" else "Assistant",
        content="hello",
        timestamp=_NOW - timedelta(seconds=seconds_ago),
        role=role,
    )


def _allowed(
    *,
    messages: list[Message] | None = None,
    in_flight_actions: dict[int, dict] | None = None,
    inflight_voice_speech: str = "",
) -> bool:
    return _idle_status_smalltalk_allowed(
        in_flight_actions=in_flight_actions or {},
        global_thread=messages or [],
        inflight_voice_speech=inflight_voice_speech,
        now=_NOW,
    )


def test_idle_status_smalltalk_allowed_when_call_is_quiet():
    assert _allowed()


def test_idle_status_smalltalk_blocked_by_in_flight_action():
    assert not _allowed(in_flight_actions={1: {"query": "book the flight"}})


def test_idle_status_smalltalk_blocked_by_pending_voice_speech():
    assert not _allowed(inflight_voice_speech="I found the answer.")


def test_idle_status_smalltalk_blocked_by_recent_assistant_comms():
    assert not _allowed(
        messages=[
            _message(
                role="assistant",
                seconds_ago=IDLE_SMALLTALK_RECENT_COMMS_SECONDS - 0.1,
            ),
        ],
    )


def test_idle_status_smalltalk_allowed_at_recent_comms_boundary():
    assert _allowed(
        messages=[
            _message(role="assistant", seconds_ago=IDLE_SMALLTALK_RECENT_COMMS_SECONDS),
        ],
    )


def test_idle_status_smalltalk_ignores_recent_user_comms():
    assert _allowed(messages=[_message(role="user", seconds_ago=1)])
