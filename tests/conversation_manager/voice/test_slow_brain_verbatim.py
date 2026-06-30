"""
tests/conversation_manager/voice/test_slow_brain_verbatim.py
============================================================

The slow brain owns all substantive speech and its ``guide_voice_agent`` output
is spoken **verbatim** by the fast brain (there is no speech-dedup gate, and
``guide_voice_agent`` is speak-only, so the publish path always emits
``should_speak=True``). These tests document that contract plus the render-only
in-flight-speech overlay.
"""

from __future__ import annotations

import json

import pytest

from unify.conversation_manager.events import PhoneCallStarted
from unify.conversation_manager.cm_types import Medium, Mode

from tests.conversation_manager.conftest import TEST_CONTACTS


@pytest.mark.asyncio
class TestSlowBrainPassesSpeakThrough:
    """The slow brain passes ``should_speak`` through to the fast brain
    unmodified - there is no dedup gate editing or suppressing its speech."""

    @pytest.fixture
    def boss_contact(self):
        return TEST_CONTACTS[1]

    async def test_stash_inflight_speech_sets_field_without_persisting(
        self,
        initialized_cm,
        boss_contact,
    ):
        """The in-flight-speech overlay is render-only: stashing the line sets a
        field but does NOT write to the stored transcript (so future turns see
        only what was actually spoken)."""
        cm = initialized_cm.cm
        await initialized_cm.step(PhoneCallStarted(contact=boss_contact))
        assert cm.mode == Mode.CALL

        before = len(
            cm.contact_index.get_messages_for_contact(
                boss_contact["contact_id"],
                Medium.PHONE_CALL,
            ),
        )
        line = "The next step is to click Trigger email from T-W1N."
        cm._stash_inflight_voice_speech(line)

        assert cm._inflight_voice_speech == line
        after = len(
            cm.contact_index.get_messages_for_contact(
                boss_contact["contact_id"],
                Medium.PHONE_CALL,
            ),
        )
        assert after == before, "Stash must not persist to the transcript."

    async def test_stash_inflight_speech_ignores_empty(
        self,
        initialized_cm,
        boss_contact,
    ):
        """Empty guidance stashes nothing."""
        cm = initialized_cm.cm
        await initialized_cm.step(PhoneCallStarted(contact=boss_contact))
        cm._stash_inflight_voice_speech("   ")
        assert cm._inflight_voice_speech == ""

    async def test_publish_slow_brain_guidance_is_always_speak(
        self,
        initialized_cm,
        boss_contact,
    ):
        """``guide_voice_agent`` is speak-only, so the slow-brain publish path
        always emits ``should_speak=True`` - there is no dedup/suppression gate.

        Deterministic: exercises ``_publish_slow_brain_fast_brain_guidance``
        directly rather than running a full LLM turn (the prior real-LLM /
        e2e-eval variants were tautological once the publish hard-codes
        ``should_speak=True``)."""
        cm = initialized_cm.cm
        await initialized_cm.step(PhoneCallStarted(contact=boss_contact))
        assert cm.mode == Mode.CALL

        published: list[dict] = []
        original_publish = cm.event_broker.publish

        async def capture_publish(channel: str, message: str) -> int:
            if channel == "app:call:notification":
                published.append(json.loads(message))
            return await original_publish(channel, message)

        cm.event_broker.publish = capture_publish
        try:
            await cm._publish_slow_brain_fast_brain_guidance(
                message="The next step is to click Trigger email from T-W1N.",
            )
        finally:
            cm.event_broker.publish = original_publish

        slow = [
            p.get("payload", p)
            for p in published
            if (p.get("payload", p)).get("source") == "slow_brain"
        ]
        assert slow, "expected a slow_brain notification to be published"
        assert all(p.get("should_speak") is True for p in slow), (
            "slow-brain guidance must always publish should_speak=True; "
            f"published={slow}"
        )
