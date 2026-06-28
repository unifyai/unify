"""
tests/conversation_manager/voice/test_stale_generation_invalidation.py
======================================================================

Regression tests for the FastBrain stale generation invalidation mechanism.

When a significant IPC event (slow brain notification, outbound message sent)
arrives while the FastBrain LLM is mid-generation, the system should cancel
the in-flight generation and re-trigger with updated context.  This prevents
the FastBrain from speaking stale responses (e.g., offering to send scopes
that were already sent).

Origin: production incident 2026-04-07 where the FastBrain asked "Would you
like me to send these over in the chat?" after the slow brain had already
sent the message.

These tests verify the symbolic preconditions and wiring that
``_invalidate_current_generation`` depends on.
"""

from __future__ import annotations


import pytest

from unity.conversation_manager.events import (
    SMSSent,
    EmailSent,
    UnifyMessageSent,
    WhatsAppSent,
)
from unity.conversation_manager.medium_scripts.common import (
    render_participant_comms,
)

# ===========================================================================
# Test: outbound events produce the "[You ..." prefix used by invalidation
# ===========================================================================


class TestOutboundEventPrefixConvention:
    """The invalidation trigger in ``on_participant_comms`` uses
    ``text.startswith("[You ")`` to identify outbound actions.  These tests
    verify that ``render_participant_comms`` maintains this convention for
    every outbound event type.
    """

    @pytest.fixture
    def participant_ids(self):
        return {1}

    @staticmethod
    def _render(event, participant_ids: set[int]) -> str | None:
        return render_participant_comms(event.to_json(), participant_ids)

    def test_unify_message_sent_prefix(self, participant_ids):
        event = UnifyMessageSent(
            contact={"contact_id": 1, "first_name": "Alex", "surname": "Demo"},
            content="Here are the OAuth scopes.",
        )
        text = self._render(event, participant_ids)
        assert text is not None
        assert text.startswith("[You "), (
            f"UnifyMessageSent must produce '[You ...' prefix for invalidation.\n"
            f"Got: {text}"
        )

    def test_sms_sent_prefix(self, participant_ids):
        event = SMSSent(
            contact={"contact_id": 1, "first_name": "Alex", "surname": "Demo"},
            content="Here is the link.",
        )
        text = self._render(event, participant_ids)
        assert text is not None
        assert text.startswith("[You "), (
            f"SMSSent must produce '[You ...' prefix for invalidation.\n" f"Got: {text}"
        )

    def test_email_sent_prefix(self, participant_ids):
        event = EmailSent(
            contact={"contact_id": 1, "first_name": "Alex", "surname": "Demo"},
            subject="OAuth setup instructions",
            body="Step-by-step instructions for setting up OAuth.",
        )
        text = self._render(event, participant_ids)
        assert text is not None
        assert text.startswith("[You "), (
            f"EmailSent must produce '[You ...' prefix for invalidation.\n"
            f"Got: {text}"
        )

    def test_whatsapp_sent_prefix(self, participant_ids):
        event = WhatsAppSent(
            contact={"contact_id": 1, "first_name": "Alex", "surname": "Demo"},
            content="Here are the scopes.",
        )
        text = self._render(event, participant_ids)
        assert text is not None
        assert text.startswith("[You "), (
            f"WhatsAppSent must produce '[You ...' prefix for invalidation.\n"
            f"Got: {text}"
        )


# ===========================================================================
# Test: triggers_turn filter correctly classifies notifications
# ===========================================================================


class TestNotificationTriggersTurnClassification:
    """``on_notification`` invalidates in-flight generation only for awareness
    notifications: the condition is ``triggers_turn and not should_speak``.
    ``triggers_turn`` classifies the source (False for meet_interaction /
    proactive_speech); the ``not should_speak`` gate ensures spoken guidance,
    whose real content is already queued, never regenerates a filler (which would
    re-enter the "thinking" state and defer that very line).
    """

    @staticmethod
    def _triggers_turn(
        should_speak: bool,
        message: str,
        notification_source: str,
    ) -> bool:
        return notification_source not in ("meet_interaction", "proactive_speech")

    @classmethod
    def _invalidates_generation(
        cls,
        should_speak: bool,
        message: str,
        notification_source: str,
    ) -> bool:
        return (
            cls._triggers_turn(should_speak, message, notification_source)
            and not should_speak
        )

    def test_silent_slow_brain_notification_invalidates(self):
        """Awareness (should_speak=False) slow-brain notifications regenerate the
        filler with the new context."""
        assert self._invalidates_generation(
            should_speak=False,
            message="",
            notification_source="slow_brain",
        )

    def test_speak_mode_notification_does_not_invalidate(self):
        """Spoken guidance no longer invalidates generation: the real content is
        queued and regenerating a filler would only defer it."""
        assert self._triggers_turn(
            should_speak=True,
            message="I've sent those scopes in the chat.",
            notification_source="slow_brain",
        )
        assert not self._invalidates_generation(
            should_speak=True,
            message="I've sent those scopes in the chat.",
            notification_source="slow_brain",
        )

    def test_meet_interaction_does_not_invalidate(self):
        assert not self._invalidates_generation(
            should_speak=False,
            message="",
            notification_source="meet_interaction",
        )

    def test_proactive_speech_does_not_invalidate(self):
        """Proactive speech is fire-and-forget filler — it should never
        invalidate in-flight fast brain generation."""
        assert not self._invalidates_generation(
            should_speak=False,
            message="",
            notification_source="proactive_speech",
        )


# ===========================================================================
# Test: _invalidate_current_generation wiring exists in call.py
# ===========================================================================


class TestInvalidationWiringExists:
    """Verify that call.py contains the invalidation call sites. This is a
    lightweight structural check — if the wiring is accidentally removed by
    a refactor, these tests will catch it immediately.
    """

    @pytest.fixture(scope="class")
    def call_source(self):
        from pathlib import Path

        return Path(
            "unity/conversation_manager/medium_scripts/call.py",
        ).read_text()

    def test_invalidation_helper_defined(self, call_source):
        assert "_invalidate_current_generation" in call_source

    def test_user_turn_generating_flag_defined(self, call_source):
        assert "user_turn_generating" in call_source

    def test_notification_handler_calls_invalidation(self, call_source):
        assert "notification_during_generation" in call_source

    def test_participant_comms_handler_calls_invalidation(self, call_source):
        assert "outbound_action_during_generation" in call_source

    def test_spoken_guidance_does_not_invalidate(self, call_source):
        """The invalidation call site is gated on ``not should_speak``."""
        assert "triggers_turn and not should_speak" in call_source


# ===========================================================================
# Test: queued slow-brain speech releases on a free floor (not on "thinking")
# ===========================================================================


class TestQueuedSpeechFloorGate:
    """A queued slow-brain line is held ONLY while the floor is occupied (the
    user speaking, or assistant audio in flight) - never merely because the agent
    is "thinking". This mirrors ``_queued_speech_block_reason`` in call.py.
    """

    @staticmethod
    def _block_reason(user_is_speaking: bool, assistant_speech_in_flight: bool) -> str:
        if user_is_speaking:
            return "user_speaking"
        if assistant_speech_in_flight:
            return "assistant_speaking"
        return ""

    def test_free_floor_releases(self):
        assert self._block_reason(False, False) == ""

    def test_user_speaking_blocks(self):
        assert self._block_reason(True, False) == "user_speaking"

    def test_assistant_speaking_blocks(self):
        assert self._block_reason(False, True) == "assistant_speaking"

    def test_thinking_alone_does_not_block(self):
        """The agent generating a reply (no audio, no user speech) must not hold
        a ready line - the historic ~13s stall."""
        assert self._block_reason(False, False) == ""


class TestQueuedSpeechGateWiring:
    """Structural checks that the queued-speech drain uses the floor-free gate and
    is re-checked when the user frees the floor."""

    @pytest.fixture(scope="class")
    def call_source(self):
        from pathlib import Path

        return Path(
            "unity/conversation_manager/medium_scripts/call.py",
        ).read_text()

    def test_floor_free_predicate_defined(self, call_source):
        assert "_queued_speech_block_reason" in call_source

    def test_drain_does_not_gate_on_full_quiescence(self, call_source):
        """maybe_speak_queued must use the floor-free reason, not the stricter
        _is_pipeline_quiescent (which blocks on agent_state thinking)."""
        drain = call_source.split("def maybe_speak_queued")[1].split("def ")[0]
        assert "_queued_speech_block_reason" in drain
        assert "_is_pipeline_quiescent" not in drain

    def test_user_stop_rechecks_queue(self, call_source):
        """Leaving the 'speaking' state re-checks the queue so a ready line plays
        at the next silent moment, not the next agent-state cycle."""
        handler = call_source.split("def _on_user_state_changed")[1].split(
            "@session.on",
        )[0]
        assert "maybe_speak_queued()" in handler

    def test_proactive_speech_still_uses_full_quiescence(self, call_source):
        """Proactive speech is intentionally unchanged - still gated on full
        pipeline quiescence."""
        assert "_is_pipeline_quiescent" in call_source
