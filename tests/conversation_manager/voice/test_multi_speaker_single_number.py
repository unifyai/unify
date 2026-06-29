"""
tests/conversation_manager/voice/test_multi_speaker_single_number.py
=====================================================================

Tests for multi-speaker scenarios on a single phone number / single contact.

It is very common for multiple people to be speaking on the other end of a
single phone number associated with a single contact (e.g. the boss introduces
a colleague mid-call, or a speakerphone with several participants).

In the slow brain's transcript, ALL messages from the phone number are labeled
with the REGISTERED CONTACT'S name even when a different person is speaking, so
the slow brain must infer the actual speaker from the text content alone.

Markers: all tests are eval tests (LLM reasoning about multi-speaker dynamics).
"""

from __future__ import annotations

import pytest

from unity.conversation_manager.events import (
    PhoneCallStarted,
    InboundPhoneUtterance,
)
from unity.conversation_manager.cm_types import Mode

from tests.conversation_manager.conftest import BOSS

pytestmark = pytest.mark.eval


@pytest.mark.asyncio
class TestSlowBrainMultiSpeakerAwareness:
    """
    Tests that the slow brain (Main CM Brain) correctly understands multi-speaker
    dynamics on a single phone line.

    CRITICAL CONTEXT: In the slow brain's transcript, ALL messages from the
    phone number are labeled with the REGISTERED CONTACT'S name. For example:

        [Default User @ DATE]: Hey, I'm introducing Richard.
        [Default User @ DATE]: Hi Alex, this is Richard. Nice to meet you!

    Both lines show "Default User" even though the second is actually Richard.
    The slow brain must understand this discrepancy from the text content alone.
    """

    async def test_slow_brain_does_not_initiate_separate_call(
        self,
        initialized_cm,
    ):
        """
        When the boss introduces someone on the call, the slow brain should NOT
        interpret this as a request to call that person separately.

        In the slow brain's transcript, this looks like:
            [Default User @ DATE]: Hey, Richard is here with us now.
        The slow brain must understand Richard is ALREADY present.
        """
        boss = BOSS

        await initialized_cm.step(PhoneCallStarted(contact=boss))
        assert initialized_cm.cm.mode == Mode.CALL

        initialized_cm.all_tool_calls.clear()
        result = await initialized_cm.step_until_wait(
            InboundPhoneUtterance(
                contact=boss,
                content=(
                    "Hey, I'm here with my friend Richard. He's on the line "
                    "with us now. Richard, go ahead and introduce yourself."
                ),
            ),
            max_steps=5,
        )

        assert "make_call" not in initialized_cm.all_tool_calls, (
            f"Slow brain tried to call Richard separately!\n"
            f"Tool calls: {initialized_cm.all_tool_calls}\n\n"
            f"Richard is ALREADY on the line. The boss said 'he's on the line\n"
            f"with us now'. The slow brain should not interpret this as a\n"
            f"request to call Richard."
        )

    async def test_slow_brain_understands_new_speaker_data_request(
        self,
        initialized_cm,
    ):
        """
        A new speaker (introduced on the call) asks for specific data. Even though
        the transcript shows the message as coming from the boss's contact, the
        slow brain should understand the social context and process the data request.

        In the slow brain's transcript, this looks like:
            [Default User @ DATE]: Alex, I've got Richard here. He has a question.
            [Default User @ DATE]: Hi Alex, Richard here. What's the Henderson project status?

        Both labeled "Default User" but the slow brain should understand that the
        second message's data request is legitimate and trigger an act() call.
        """
        boss = BOSS

        await initialized_cm.step(PhoneCallStarted(contact=boss))

        # Introduction (don't run LLM — just register the utterance)
        await initialized_cm.step(
            InboundPhoneUtterance(
                contact=boss,
                content="Alex, I've got my colleague Richard here. He has a question for you.",
            ),
            run_llm=False,
        )

        # Richard asks a data question (still labeled as boss in the transcript)
        initialized_cm.all_tool_calls.clear()
        result = await initialized_cm.step_until_wait(
            InboundPhoneUtterance(
                contact=boss,
                content=(
                    "Hi Alex, Richard here. Dan mentioned you've been tracking "
                    "the Henderson project. Can you tell me the current status?"
                ),
            ),
            max_steps=5,
        )

        # The slow brain should recognize this as a legitimate data request
        assert result.llm_ran, (
            f"Slow brain should have run to handle the data request!\n"
            f"Richard asked about the Henderson project status — this requires\n"
            f"a data lookup via `act`."
        )

    async def test_slow_brain_does_not_confuse_speaker_with_contact_label(
        self,
        initialized_cm,
    ):
        """
        The hardest test: the transcript shows messages labeled with the boss's name,
        but the TEXT content clearly indicates a different person is speaking. The slow
        brain should reason about the multi-speaker scenario rather than blindly
        trusting the contact label.

        Transcript as seen by slow brain:
            [Default User @ DATE]: I'm handing the phone to Richard now.
            [Default User @ DATE]: Hey, so I run a logistics company and we need help with invoicing.

        The second message is labeled "Default User" but is clearly from Richard
        (who runs a logistics company, not the boss who is a CEO). The slow brain
        should NOT treat this as the boss asking about their own invoicing.
        """
        boss = BOSS

        await initialized_cm.step(PhoneCallStarted(contact=boss))

        # Boss hands off
        await initialized_cm.step(
            InboundPhoneUtterance(
                contact=boss,
                content="I'm handing the phone to Richard now. He wants to discuss his business needs.",
            ),
            run_llm=False,
        )

        # Richard speaks (labeled as boss in transcript)
        initialized_cm.all_tool_calls.clear()
        result = await initialized_cm.step_until_wait(
            InboundPhoneUtterance(
                contact=boss,
                content=(
                    "Hey, so I run a logistics company and we need help "
                    "with invoicing and shipment tracking. Is that something "
                    "you can handle?"
                ),
            ),
            max_steps=5,
        )

        # The key check: the slow brain should NOT have called make_call
        # (would indicate it thinks it needs to contact Richard separately)
        assert "make_call" not in initialized_cm.all_tool_calls, (
            f"Slow brain tried to call someone after a speaker handoff!\n"
            f"Tool calls: {initialized_cm.all_tool_calls}\n\n"
            f"The boss handed the phone to Richard. The subsequent message\n"
            f"is from Richard (on the same call), not a request to call anyone."
        )
