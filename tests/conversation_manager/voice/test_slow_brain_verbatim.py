"""
tests/conversation_manager/voice/test_slow_brain_verbatim.py
============================================================

The slow brain owns all substantive speech and its ``guide_voice_agent`` output
is spoken **verbatim** by the fast brain (there is no speech-dedup gate). These
tests document that contract: the slow brain publishes the LLM's original
``should_speak`` value unmodified, with no server-side suppression or rewrite.
"""

from __future__ import annotations

import json

import pytest

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


@pytest.mark.asyncio
class TestSlowBrainPassesSpeakThrough:
    """The slow brain passes ``should_speak`` through to the fast brain
    unmodified - there is no dedup gate editing or suppressing its speech."""

    @pytest.fixture
    def boss_contact(self):
        return TEST_CONTACTS[1]

    async def test_slow_brain_does_not_have_dedup_checker(
        self,
        initialized_cm,
    ):
        """ConversationManager carries no speech-dedup checker."""
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
                        "unmodified; speech is spoken verbatim.\n"
                        f"Payload: {payload}"
                    )
        finally:
            cm.event_broker.publish = original_publish


@pytest.mark.eval
@pytest.mark.asyncio
class TestSlowBrainVerbatimEval:
    """End-to-end eval verifying the slow brain passes should_speak through to
    the fast brain, spoken verbatim (no dedup)."""

    @_handle_project
    async def test_slow_brain_passes_speak_through_e2e(
        self,
        initialized_cm,
    ):
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
                        "unmodified; speech is spoken verbatim.\n"
                        f"Payload: {payload}\n"
                        f"Tool calls: {cm.all_tool_calls}"
                    )
        finally:
            cm.cm.event_broker.publish = original_publish
