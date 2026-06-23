"""
tests/conversation_manager/voice/test_briefed_opening.py
========================================================

Tests for the ``briefed`` call opening mode.

A ``briefed`` opening injects a durable, caller-supplied system briefing into
the call context and steers the opening turn authoritatively from it (used for
the first onboarding voice call so the intro is spoken immediately, instead of
waiting for the slow-brain wakeup to shape a generic holding greeting).

The deterministic tests lock the sidecar prompt-construction contract; the eval
test verifies the opener actually follows the briefing rather than defaulting to
a generic hello.
"""

from __future__ import annotations

import pytest

from tests.helpers import _handle_project
from droid.conversation_manager.prompt_builders import (
    _BRIEFED_OPENING_GUARDRAIL,
    _OPENING_GREETING_GUARDRAIL,
    build_opening_greeting_messages,
    build_voice_agent_prompt,
)
from droid.settings import SETTINGS

from tests.conversation_manager.voice.test_fast_brain_deferral import (
    get_fast_brain_response,
)

MODEL = SETTINGS.conversation.FAST_BRAIN_MODEL

SAMPLE_BRIEFING = (
    "[Briefing for your opening turn]\n"
    "This is the user's first onboarding voice call with you. Open with a "
    "warm, natural first-meeting introduction in your own words. Cover:\n"
    "- Greet the user by first name and introduce yourself as Twin, their "
    "digital twin / stand-in.\n"
    "- Explain that onboarding is a short shared walkthrough covering "
    "Communication, Workspace, Integrations, and that they can skip ahead.\n"
    "- Make this the concrete next step: starting the communication-channel "
    "reference quiz by clicking the 'Trigger email from Twin' row."
)


# ---------------------------------------------------------------------------
#  Deterministic: sidecar prompt construction
# ---------------------------------------------------------------------------


def test_authoritative_briefing_uses_briefed_guardrail():
    """A briefed opening preserves the injected system briefing and ends with
    the authoritative briefed guardrail (which overrides the generic hello)."""
    messages = build_opening_greeting_messages(
        system_prompt="SYSTEM",
        history_messages=[{"role": "system", "content": SAMPLE_BRIEFING}],
        authoritative_briefing=True,
    )

    assert messages[0] == {"role": "system", "content": "SYSTEM"}
    assert any(
        m.get("content") == SAMPLE_BRIEFING for m in messages
    ), "The injected system briefing must remain visible to the opener LLM."
    assert messages[-1]["content"] == _BRIEFED_OPENING_GUARDRAIL
    assert messages[-1]["content"] != _OPENING_GREETING_GUARDRAIL


def test_default_opening_uses_generic_guardrail():
    """Without an authoritative briefing the generic opening guardrail is used."""
    messages = build_opening_greeting_messages(
        system_prompt="SYSTEM",
        history_messages=[],
        authoritative_briefing=False,
    )

    assert messages[-1]["content"] == _OPENING_GREETING_GUARDRAIL


# ---------------------------------------------------------------------------
#  Eval: the opener follows the briefing
# ---------------------------------------------------------------------------


@pytest.mark.eval
@pytest.mark.asyncio
@_handle_project
async def test_briefed_opener_follows_briefing():
    """With a coordinator onboarding briefing in context and the authoritative
    briefed guardrail, the opening turn should orient the user (introduce Twin /
    onboarding / the next step) rather than emit a bare generic greeting."""
    prompt = build_voice_agent_prompt(
        bio="A helpful and efficient assistant.",
        boss_first_name="Alex",
        boss_surname="Thompson",
        boss_email_address="alex.thompson@example.com",
        is_boss_user=True,
        contact_rolling_summary=None,
        is_coordinator=True,
    ).flatten()

    conversation = [
        {"role": "system", "content": SAMPLE_BRIEFING},
        {"role": "system", "content": _BRIEFED_OPENING_GUARDRAIL},
    ]

    response = await get_fast_brain_response(prompt, conversation, MODEL)
    low = response.lower()

    assert any(
        marker in low
        for marker in (
            "twin",
            "onboarding",
            "walkthrough",
            "reference",
            "quiz",
            "set up",
        )
    ), (
        "The briefed opener should follow the onboarding briefing, not emit a "
        f"bare generic greeting.\nResponse: {response}"
    )
