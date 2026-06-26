"""
tests/conversation_manager/voice/test_speech_dedup.py
=====================================================

Tests for the speech deduplication gate.

Dedup runs in the fast brain subprocess at speak time (inside
``maybe_speak_queued`` -> ``_dedup_and_speak``).  Before playing queued slow
brain speech, a lightweight LLM check compares the proposed text against recent
assistant utterances in the fast brain's chat context and returns one of three
verdicts: ``SPEAK`` (unchanged), ``SUPPRESS`` (drop), or ``REWRITE`` (speak a
trimmed version, streamed token-by-token into TTS).

Test categories:

1. **Unit tests** - SpeechDeduplicationChecker in isolation (streaming fakes).
2. **Symbolic integration tests** - verify the slow brain no longer runs dedup
   and passes ``should_speak`` through to the fast brain unmodified.
3. **Eval tests** - end-to-end with real LLM judgment on overlapping content.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Iterable

import pytest

from unity.conversation_manager.domains.speech_dedup import (
    DedupOutcome,
    SpeechDecision,
    SpeechDeduplicationChecker,
)
from unity.conversation_manager.events import (
    InboundPhoneUtterance,
    InboundUnifyMeetUtterance,
    PhoneCallStarted,
    UnifyMeetReceived,
    UnifyMeetStarted,
)
from unity.conversation_manager.cm_types import Medium, Mode

from tests.conversation_manager.conftest import BOSS, TEST_CONTACTS
from tests.helpers import _handle_project

# =============================================================================
# Streaming fake helpers
# =============================================================================


async def _drain(stream: AsyncIterator[str] | None) -> str:
    """Concatenate all chunks yielded by a rewrite token stream."""
    if stream is None:
        return ""
    parts = []
    async for chunk in stream:
        parts.append(chunk)
    return "".join(parts)


def _fake_client(chunks: Iterable[str], captured: dict | None = None):
    """Build a fake unillm client whose streamed ``generate`` yields *chunks*.

    Mirrors the real client surface used by the gate: ``set_stream`` plus an
    awaitable ``generate`` that resolves to an async iterator of content chunks.
    Splitting words across chunk boundaries exercises incremental header
    parsing.
    """
    chunk_list = list(chunks)

    class _Client:
        def set_stream(self, _value: bool) -> None:
            pass

        async def generate(self, *, messages=None, **_kwargs):
            if captured is not None:
                captured["messages"] = messages

            async def _gen():
                for chunk in chunk_list:
                    yield chunk

            return _gen()

    return _Client()


def _patch_client(monkeypatch, client) -> None:
    monkeypatch.setattr(
        "unity.conversation_manager.domains.speech_dedup.new_llm_client",
        lambda *a, **kw: client,
    )


# =============================================================================
# Unit tests - DedupOutcome / decision parsing
# =============================================================================


@pytest.mark.asyncio
class TestSpeechDeduplicationCheckerUnit:

    async def test_empty_utterances_skips_check(self):
        """With no recent context, the checker returns SPEAK without an LLM call."""
        checker = SpeechDeduplicationChecker()

        result = await checker.evaluate(
            proposed_speech="The task is complete.",
            recent_utterances=[],
        )

        assert isinstance(result, DedupOutcome)
        assert result.decision is SpeechDecision.SPEAK
        assert result.reasoning == "no recent context to compare against"
        assert result.should_suppress is False

    async def test_should_suppress_property(self):
        """should_suppress is True only for the SUPPRESS decision."""
        assert DedupOutcome(SpeechDecision.SUPPRESS).should_suppress is True
        assert DedupOutcome(SpeechDecision.SPEAK).should_suppress is False
        assert DedupOutcome(SpeechDecision.REWRITE).should_suppress is False

    async def test_decision_mapping(self, monkeypatch):
        """Each header token maps to the correct SpeechDecision."""
        cases = {
            SpeechDecision.SPEAK: ["SPEAK | genuinely new info"],
            SpeechDecision.SUPPRESS: ["SUPPRESS | already said it"],
            SpeechDecision.REWRITE: ["REWRITE\n", "trimmed body"],
        }
        for expected, chunks in cases.items():
            _patch_client(monkeypatch, _fake_client(chunks))
            checker = SpeechDeduplicationChecker(model="fake@test")
            result = await checker.evaluate(
                proposed_speech="Proposed line.",
                recent_utterances=["A prior utterance."],
            )
            assert result.decision is expected, f"{chunks} -> {result.decision}"

    async def test_rewrite_streams_trimmed_text(self, monkeypatch):
        """REWRITE exposes a token stream that drains to the body with the
        header stripped, even when the marker is split across chunks."""
        chunks = ["RE", "WRITE", "\n", "Confirmed ", "back on ", "WhatsApp."]
        _patch_client(monkeypatch, _fake_client(chunks))
        checker = SpeechDeduplicationChecker(model="fake@test")

        result = await checker.evaluate(
            proposed_speech="The Matrix - correct. Confirmed back on WhatsApp.",
            recent_utterances=["Yes - got it. The Matrix is the right reply."],
        )

        assert result.decision is SpeechDecision.REWRITE
        assert await _drain(result.text_stream) == "Confirmed back on WhatsApp."

    async def test_rewrite_body_on_same_line_recovered(self, monkeypatch):
        """If the model omits the newline and jams body onto the REWRITE line,
        the body is still recovered after the marker."""
        _patch_client(monkeypatch, _fake_client(["REWRITE: Confirmed on WhatsApp."]))
        checker = SpeechDeduplicationChecker(model="fake@test")

        result = await checker.evaluate(
            proposed_speech="The Matrix - correct. Confirmed on WhatsApp.",
            recent_utterances=["Yes - the Matrix is right."],
        )

        assert result.decision is SpeechDecision.REWRITE
        assert await _drain(result.text_stream) == "Confirmed on WhatsApp."

    async def test_rewrite_empty_body_stream_is_empty(self, monkeypatch):
        """A REWRITE header with no body yields an empty stream (the speak path
        degrades this to suppression)."""
        _patch_client(monkeypatch, _fake_client(["REWRITE\n"]))
        checker = SpeechDeduplicationChecker(model="fake@test")

        result = await checker.evaluate(
            proposed_speech="Redundant line.",
            recent_utterances=["Already said this."],
        )

        assert result.decision is SpeechDecision.REWRITE
        assert await _drain(result.text_stream) == ""

    async def test_unrecognized_header_fails_open_to_speak(self, monkeypatch):
        """If the model ignores the protocol, the gate fails open to SPEAK so the
        original proposal is still delivered."""
        _patch_client(monkeypatch, _fake_client(["Sure, here's what I think...\n"]))
        checker = SpeechDeduplicationChecker(model="fake@test")

        result = await checker.evaluate(
            proposed_speech="Something to say.",
            recent_utterances=["A prior utterance."],
        )

        assert result.decision is SpeechDecision.SPEAK

    async def test_evaluate_error_fails_open(self):
        """On LLM error, the checker returns SPEAK so speech is allowed rather
        than silently suppressed."""
        checker = SpeechDeduplicationChecker(model="invalid-model@nowhere")

        result = await checker.evaluate(
            proposed_speech="The task is done.",
            recent_utterances=["Done with the task."],
        )

        assert result.decision is SpeechDecision.SPEAK
        assert result.should_suppress is False
        assert "failed" in result.reasoning.lower()

    async def test_evaluate_sends_user_role_message(self, monkeypatch):
        """LiteLLM/Anthropic reject chat completions with only ``system`` messages.

        Asserts we always include at least one non-system message so provider
        transforms do not empty the payload.
        """
        captured: dict = {}
        _patch_client(
            monkeypatch,
            _fake_client(["SPEAK | ok"], captured=captured),
        )
        checker = SpeechDeduplicationChecker(model="claude-3-5-haiku@anthropic")

        result = await checker.evaluate(
            proposed_speech="The report is ready.",
            recent_utterances=["I already told you the report is ready."],
        )

        assert result.decision is SpeechDecision.SPEAK
        assert captured.get("messages") is not None
        roles = [m["role"] for m in captured["messages"]]
        assert "system" in roles
        assert "user" in roles
        assert any(m["role"] != "system" for m in captured["messages"])


# =============================================================================
# Unit tests - context / notification awareness
# =============================================================================


@pytest.mark.asyncio
class TestSpeechGateContextAwareness:
    """Tests for the prompt context the gate sees (utterances + notifications)."""

    async def test_empty_context_skips_check(self):
        """No LLM call when both utterances and notifications are empty."""
        checker = SpeechDeduplicationChecker()

        result = await checker.evaluate(
            proposed_speech="Want me to set it up?",
            recent_utterances=[],
            recent_notifications=[],
        )

        assert result.should_suppress is False
        assert result.decision is SpeechDecision.SPEAK

    async def test_notifications_only_triggers_check(self, monkeypatch):
        """When there are notifications but no utterances, the checker still
        runs (notifications alone can reveal contradiction)."""
        captured: dict = {}
        _patch_client(
            monkeypatch,
            _fake_client(["SUPPRESS | contradicts state"], captured=captured),
        )

        checker = SpeechDeduplicationChecker()
        await checker.evaluate(
            proposed_speech="Let me walk you through that.",
            recent_utterances=[],
            recent_notifications=["Setup completed successfully."],
        )

        assert captured.get("messages") is not None
        system_msg = captured["messages"][0]["content"]
        assert "Setup completed successfully" in system_msg

    async def test_evaluate_includes_notifications_in_prompt(self, monkeypatch):
        """The prompt includes recent notifications alongside recent utterances
        so the LLM can detect contradictions."""
        captured: dict = {}
        _patch_client(
            monkeypatch,
            _fake_client(["SUPPRESS | already done"], captured=captured),
        )

        checker = SpeechDeduplicationChecker()
        await checker.evaluate(
            proposed_speech="Want me to help set up Gmail?",
            recent_utterances=["Everything is done."],
            recent_notifications=["Gmail check completed: 201 unread emails."],
        )

        system_msg = captured["messages"][0]["content"]
        assert "Gmail check completed" in system_msg
        assert "Everything is done" in system_msg

    async def test_backward_compat_without_notifications(self, monkeypatch):
        """Calling evaluate without recent_notifications still works
        (parameter is optional with default None)."""
        captured: dict = {}
        _patch_client(
            monkeypatch,
            _fake_client(["SUPPRESS | same info"], captured=captured),
        )

        checker = SpeechDeduplicationChecker()
        result = await checker.evaluate(
            proposed_speech="Found restaurants.",
            recent_utterances=["I found three Italian places."],
        )

        assert result.should_suppress is True
        system_msg = captured["messages"][0]["content"]
        assert "(none)" in system_msg  # notifications section shows "(none)"

    async def test_self_notification_only_skips_check(self):
        """The slow brain's guidance is injected as a ``[notification]`` using the
        same text it then proposes for speech. When that self-copy is the only
        context, it is dropped and the check short-circuits to SPEAK (no LLM
        call) - the user has not actually heard it yet."""
        proposed = "Correct - The Matrix. Email channel works."
        checker = SpeechDeduplicationChecker()

        result = await checker.evaluate(
            proposed_speech=proposed,
            recent_utterances=[],
            recent_notifications=[proposed],
        )

        assert result.decision is SpeechDecision.SPEAK
        assert result.reasoning == "no recent context to compare against"

    async def test_self_notification_excluded_from_prompt(self, monkeypatch):
        """A notification equal to the proposed speech is filtered out before the
        prompt is built, so the gate can never match the proposal against itself.
        Other, genuinely distinct notifications are preserved."""
        proposed = "Correct - The Matrix. Email channel works."
        other = "Email check completed for Daniel."
        captured: dict = {}
        _patch_client(
            monkeypatch,
            _fake_client(["SPEAK | novel"], captured=captured),
        )

        checker = SpeechDeduplicationChecker()
        await checker.evaluate(
            proposed_speech=proposed,
            recent_utterances=["Got it - I'm checking that now."],
            recent_notifications=[proposed, other],
        )

        system_msg = captured["messages"][0]["content"]
        # The self-copy is not rendered as a notification bullet ("- {text}"),
        # while the distinct notification is retained.
        assert f"- {proposed}" not in system_msg
        assert f"- {other}" in system_msg


# =============================================================================
# Eval tests - real LLM judgment
# =============================================================================


@pytest.mark.eval
@pytest.mark.asyncio
class TestSpeechGateEval:
    """Eval tests verifying the LLM's three-way judgment on overlapping content."""

    async def test_detects_offering_setup_when_already_complete(self):
        """The gate should suppress speech that offers setup steps when a
        notification confirms the setup is already complete."""
        from unity.settings import SETTINGS

        checker = SpeechDeduplicationChecker(
            model=SETTINGS.conversation.FAST_BRAIN_MODEL,
        )

        result = await checker.evaluate(
            proposed_speech=(
                "Want me to walk you through enabling domain-wide delegation "
                "on that service account?"
            ),
            recent_utterances=[
                "Everything is set up and working. You have about 201 unread emails.",
            ],
            recent_notifications=[
                "Gmail check completed successfully. Dan has about 201 unread emails.",
                "Everything is fully set up. The Gmail API is already enabled.",
            ],
        )

        assert result.should_suppress is True, (
            f"Expected suppression for setup offer when notifications confirm "
            f"completion. Decision: {result.decision}, reasoning: {result.reasoning}"
        )

    async def test_allows_genuinely_new_information(self):
        """The gate should not suppress speech that contains genuinely new
        information not present in utterances or notifications."""
        from unity.settings import SETTINGS

        checker = SpeechDeduplicationChecker(
            model=SETTINGS.conversation.FAST_BRAIN_MODEL,
        )

        result = await checker.evaluate(
            proposed_speech="Your 3pm meeting with Sarah was moved to 4pm.",
            recent_utterances=["Let me check on that."],
            recent_notifications=[
                "Calendar event updated: meeting with Sarah rescheduled to 4pm.",
            ],
        )

        assert result.should_suppress is False, (
            f"Expected novel info to pass through. Decision: {result.decision}, "
            f"reasoning: {result.reasoning}"
        )

    async def test_never_spoken_ack_not_suppressed(self):
        """Regression: the email "The Matrix" acknowledgement was wrongly
        suppressed because its own injected ``[notification]`` copy was treated
        as "already spoken aloud".

        The proposed acknowledgement matches a recent notification (the slow
        brain's own guidance) but NO spoken utterance covers it - the user has
        only heard "Got it, I'm checking". It must NOT be suppressed.
        """
        from unity.settings import SETTINGS

        ack = "Correct - The Matrix. That's correct, well done. Email channel works."
        checker = SpeechDeduplicationChecker(
            model=SETTINGS.conversation.FAST_BRAIN_MODEL,
        )

        result = await checker.evaluate(
            proposed_speech=ack,
            recent_utterances=["Got it - I'm checking that now."],
            recent_notifications=[ack],
        )

        assert result.should_suppress is False, (
            f"Never-spoken acknowledgement must not be suppressed just because it "
            f"matches its own guidance notification. Decision: {result.decision}, "
            f"reasoning: {result.reasoning}"
        )

    async def test_partial_overlap_triggers_rewrite(self):
        """When the proposal repeats an acknowledgement already spoken but also
        carries new info, the gate should REWRITE to keep only the new part."""
        from unity.settings import SETTINGS

        checker = SpeechDeduplicationChecker(
            model=SETTINGS.conversation.FAST_BRAIN_MODEL,
        )

        proposed = (
            "The Matrix - that's correct. I've confirmed back on WhatsApp too, "
            "so that round is done."
        )
        result = await checker.evaluate(
            proposed_speech=proposed,
            recent_utterances=["Yes - got it. The Matrix is the right reply."],
        )

        assert result.should_suppress is False, (
            f"Partial overlap with new info should not be fully suppressed. "
            f"Reasoning: {result.reasoning}"
        )
        if result.decision is SpeechDecision.REWRITE:
            rewritten = await _drain(result.text_stream)
            assert rewritten.strip(), "Rewrite body must be non-empty."
            assert len(rewritten) < len(proposed), (
                "Rewrite should be shorter than the redundant proposal. "
                f"Got: {rewritten!r}"
            )
            assert (
                "whatsapp" in rewritten.lower()
            ), f"Rewrite should preserve the new WhatsApp info. Got: {rewritten!r}"

    async def test_default_model_streaming_roundtrip(self):
        """Production path: no ``model=`` uses ``UNIFY_MODEL``.

        Catches provider-specific request-shape or streaming issues that mocks
        miss.
        """
        checker = SpeechDeduplicationChecker()
        result = await checker.evaluate(
            proposed_speech="Found three Italian restaurants nearby.",
            recent_utterances=[
                "Yeah, I found three Italian places near you - "
                "the top rated is Chez Laurent.",
            ],
        )
        assert isinstance(result, DedupOutcome)
        assert isinstance(result.decision, SpeechDecision)


# =============================================================================
# Symbolic integration tests - slow brain passes should_speak through
# =============================================================================


@pytest.mark.asyncio
class TestSlowBrainPassesSpeakThrough:
    """Verify the slow brain no longer runs dedup and passes should_speak
    through to the fast brain unmodified.

    After the refactor, dedup is a fast-brain-only concern.  The slow brain
    publishes FastBrainNotification with the LLM's original should_speak value.
    """

    @pytest.fixture
    def boss_contact(self):
        return TEST_CONTACTS[1]

    async def test_slow_brain_does_not_have_dedup_checker(
        self,
        initialized_cm,
    ):
        """ConversationManager no longer carries a _speech_dedup_checker."""
        cm = initialized_cm.cm
        assert not hasattr(cm, "_speech_dedup_checker")

    async def test_should_speak_passed_through_with_recent_utterances(
        self,
        initialized_cm,
        boss_contact,
    ):
        """Even when recent assistant utterances exist in the voice thread,
        the slow brain publishes should_speak as the LLM produced it (no
        server-side suppression)."""
        cm = initialized_cm.cm

        await initialized_cm.step(PhoneCallStarted(contact=boss_contact))
        assert cm.mode == Mode.CALL

        cm.contact_index.push_message(
            contact_id=boss_contact["contact_id"],
            sender_name="You",
            thread_name=Medium.PHONE_CALL,
            message_content="That's done - found three Italian restaurants near you.",
            role="assistant",
        )

        cm.completed_actions[0] = {
            "query": "Search for nearby Italian restaurants",
            "handle_actions": [
                {
                    "action_name": "act_completed",
                    "query": "Found 3 Italian restaurants nearby.",
                    "status": "completed",
                },
            ],
        }

        published: list[dict] = []
        original_publish = cm.event_broker.publish

        async def capture_publish(channel: str, message: str) -> int:
            if channel == "app:call:notification":
                published.append(json.loads(message))
            return await original_publish(channel, message)

        cm.event_broker.publish = capture_publish

        try:
            initialized_cm.all_tool_calls.clear()

            await initialized_cm.step_until_wait(
                InboundPhoneUtterance(
                    contact=boss_contact,
                    content="Any restaurants nearby?",
                ),
                max_steps=5,
            )

            for event_data in published:
                payload = event_data.get("payload", event_data)
                if payload.get("source") == "slow_brain" and payload.get("message"):
                    assert payload.get("should_speak") is True, (
                        "The slow brain should pass should_speak=True through "
                        "unmodified; dedup is now a fast-brain concern.\n"
                        f"Payload: {payload}"
                    )
        finally:
            cm.event_broker.publish = original_publish


# =============================================================================
# Eval test - end-to-end slow brain passthrough
# =============================================================================


@pytest.mark.eval
@pytest.mark.asyncio
class TestSpeechDedupEval:
    """End-to-end eval test verifying the slow brain passes should_speak
    through to the fast brain without running dedup."""

    @_handle_project
    async def test_slow_brain_passes_speak_through_e2e(
        self,
        initialized_cm,
    ):
        """Verify the slow brain passes should_speak=True through to the
        fast brain without running dedup.

        Dedup now runs in the fast brain subprocess at speak time.  The slow
        brain publishes the LLM's original decision unmodified.
        """
        cm = initialized_cm

        await cm.step(UnifyMeetReceived(contact=BOSS))
        await cm.step(UnifyMeetStarted(contact=BOSS))
        assert cm.cm.mode == Mode.MEET

        cm.cm.completed_actions[0] = {
            "query": "Count unread emails in Gmail inbox",
            "handle_actions": [
                {
                    "action_name": "act_completed",
                    "query": "You have 47 unread emails in your inbox.",
                    "status": "completed",
                },
            ],
        }

        cm.cm.contact_index.push_message(
            contact_id=BOSS["contact_id"],
            sender_name="You",
            thread_name=Medium.UNIFY_MEET,
            message_content=(
                "Yeah, the email check came back - you've got 47 unread "
                "emails in your inbox."
            ),
            role="assistant",
        )

        published: list[dict] = []
        original_publish = cm.cm.event_broker.publish

        async def capture_publish(channel: str, message: str) -> int:
            if channel == "app:call:notification":
                published.append(json.loads(message))
            return await original_publish(channel, message)

        cm.cm.event_broker.publish = capture_publish

        try:
            cm.all_tool_calls.clear()

            await cm.step_until_wait(
                InboundUnifyMeetUtterance(
                    contact=BOSS,
                    content="How did the email check go?",
                ),
                max_steps=5,
            )

            for event_data in published:
                payload = event_data.get("payload", event_data)
                if payload.get("source") == "slow_brain" and payload.get("message"):
                    assert payload.get("should_speak") is True, (
                        "The slow brain should pass should_speak=True through "
                        "unmodified; dedup is now a fast-brain concern.\n"
                        f"Payload: {payload}\n"
                        f"Tool calls: {cm.all_tool_calls}"
                    )
        finally:
            cm.cm.event_broker.publish = original_publish
