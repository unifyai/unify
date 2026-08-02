"""
tests/conversation_manager/core/test_console_presence.py
========================================================

Console orientation text reaches the runtime on the presence heartbeat, and is
only put in a prompt while the user is actually looking at the Console.

Someone who emailed hours ago has no Console open, so describing its surfaces to
them is prompt bloat that also invites the assistant to talk about a screen
nobody is looking at. These cover the delivery, the gate, and the boundary
between them.
"""

from __future__ import annotations

import pytest

from unify.conversation_manager import conversation_manager as cm_module
from unify.conversation_manager.conversation_manager import CONSOLE_PRESENCE_TTL_S
from unify.conversation_manager.events import AssistantPresenceObserved
from unify.conversation_manager.prompt_builders import build_system_prompt

pytestmark = pytest.mark.no_unify_context

BRIEF = "Console knowledge\n-----------------\nBrief surfaces."
FULL = "Console knowledge\n-----------------\nFull surfaces with hints."


class _FakeClock:
    """Stands in for ``time.monotonic`` so TTL expiry needs no sleeping."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _PresenceHost:
    """The slice of ConversationManager the presence methods actually touch."""

    record_console_presence = cm_module.ConversationManager.record_console_presence
    console_guidance = cm_module.ConversationManager.console_guidance

    def __init__(self) -> None:
        self._console_guidance: dict[str, str] = {}
        self._console_guidance_version = ""
        self._console_presence_at = None
        self.logged: list[tuple[str, str]] = []
        self._session_logger = self

    def info(self, tag: str, message: str) -> None:
        self.logged.append((tag, message))


@pytest.fixture
def clock(monkeypatch) -> _FakeClock:
    fake = _FakeClock()
    monkeypatch.setattr(cm_module.time, "monotonic", fake)
    return fake


@pytest.fixture
def host(clock) -> _PresenceHost:
    return _PresenceHost()


# =============================================================================
# The gate
# =============================================================================


class TestPresenceGate:
    def test_no_guidance_before_any_presence(self, host):
        """A session that began over email has never heard from the Console."""
        assert host.console_guidance("brief") == ""
        assert host.console_guidance("full") == ""

    def test_guidance_available_once_console_reports_presence(self, host):
        host.record_console_presence(version="v1", brief=BRIEF, full=FULL)

        assert host.console_guidance("brief") == BRIEF
        assert host.console_guidance("full") == FULL

    def test_guidance_withheld_once_presence_goes_stale(self, host, clock):
        host.record_console_presence(version="v1", brief=BRIEF, full=FULL)
        clock.advance(CONSOLE_PRESENCE_TTL_S + 1)

        assert host.console_guidance("brief") == ""

    def test_guidance_survives_right_up_to_the_ttl(self, host, clock):
        """The window must not be so tight that an idle-but-present user flaps."""
        host.record_console_presence(version="v1", brief=BRIEF, full=FULL)
        clock.advance(CONSOLE_PRESENCE_TTL_S - 1)

        assert host.console_guidance("brief") == BRIEF

    def test_ttl_outlasts_consoles_keep_warm_interval(self):
        """Console keep-warm is every 5 minutes; the gate must be looser than that.

        If it were not, a present-but-idle user would drop out of the block
        between beats, and every flip costs a prompt-cache miss.
        """
        console_keep_warm_s = 5 * 60
        assert CONSOLE_PRESENCE_TTL_S > console_keep_warm_s

    def test_a_later_heartbeat_extends_the_window(self, host, clock):
        host.record_console_presence(version="v1", brief=BRIEF, full=FULL)
        clock.advance(CONSOLE_PRESENCE_TTL_S - 1)
        host.record_console_presence(version="v1")
        clock.advance(CONSOLE_PRESENCE_TTL_S - 1)

        assert host.console_guidance("brief") == BRIEF

    def test_unknown_detail_yields_nothing(self, host):
        host.record_console_presence(version="v1", brief=BRIEF, full=FULL)

        assert host.console_guidance("nonsense") == ""


# =============================================================================
# What a heartbeat carries
# =============================================================================


class TestHeartbeatPayload:
    def test_version_only_heartbeat_keeps_the_text_it_already_has(self, host):
        """Most heartbeats carry no text by design; that is not absence."""
        host.record_console_presence(version="v1", brief=BRIEF, full=FULL)
        host.record_console_presence(version="v1")

        assert host.console_guidance("brief") == BRIEF

    def test_version_only_heartbeat_still_refreshes_presence(self, host, clock):
        host.record_console_presence(version="v1", brief=BRIEF, full=FULL)
        clock.advance(CONSOLE_PRESENCE_TTL_S - 1)
        host.record_console_presence(version="v1")
        clock.advance(2)

        assert host.console_guidance("brief") == BRIEF

    def test_a_new_version_replaces_the_text(self, host):
        host.record_console_presence(version="v1", brief=BRIEF, full=FULL)
        host.record_console_presence(version="v2", brief="new brief", full="new full")

        assert host.console_guidance("brief") == "new brief"
        assert host.console_guidance("full") == "new full"

    def test_a_version_change_is_logged(self, host):
        host.record_console_presence(version="v1", brief=BRIEF, full=FULL)
        host.record_console_presence(version="v2", brief="new", full="new")

        assert any("v2" in message for _, message in host.logged)

    def test_presence_from_a_console_with_no_guidance_is_still_presence(self, host):
        host.record_console_presence()

        assert host._console_presence_at is not None
        assert host.console_guidance("brief") == ""


# =============================================================================
# Event boundary
# =============================================================================


class TestPresenceEvent:
    def test_event_carries_the_guidance_fields(self):
        event = AssistantPresenceObserved(
            reason="selection",
            source="assistant_profile",
            console_guidance_version="abc123",
            console_guidance_brief=BRIEF,
            console_guidance_full=FULL,
        )

        assert event.console_guidance_version == "abc123"
        assert event.console_guidance_brief == BRIEF

    def test_guidance_fields_default_to_empty(self):
        """Version-only heartbeats omit them, and older senders never set them."""
        event = AssistantPresenceObserved(reason="activity")

        assert event.console_guidance_version == ""
        assert event.console_guidance_brief == ""
        assert event.console_guidance_full == ""

    def test_event_is_not_logged_as_conversation(self):
        """Presence is ambient; it must not land in the transcript."""
        assert AssistantPresenceObserved.loggable is False


# =============================================================================
# Prompt injection
# =============================================================================


class TestPromptInjection:
    _BASE = {
        "bio": "A helpful assistant.",
        "contact_id": 1,
        "first_name": "Alice",
        "surname": "Smith",
    }

    def test_block_present_when_guidance_supplied(self):
        prompt = build_system_prompt(**self._BASE, console_guidance=BRIEF).flatten()

        assert BRIEF in prompt

    def test_block_absent_when_the_gate_withheld_it(self):
        """An off-console session builds the same prompt minus the block."""
        with_block = build_system_prompt(**self._BASE, console_guidance=BRIEF).flatten()
        without_block = build_system_prompt(**self._BASE, console_guidance="").flatten()

        assert "Console knowledge" not in without_block
        assert len(without_block) < len(with_block)

    def test_coordinator_takes_the_full_variant(self):
        prompt = build_system_prompt(
            **self._BASE,
            is_coordinator=True,
            console_guidance=FULL,
        ).flatten()

        assert FULL in prompt
