"""
tests/conversation_manager/voice/test_briefed_opening.py
========================================================

Tests for the ``briefed`` call opening mode.

A ``briefed`` opening injects a durable, caller-supplied system briefing into
the call context and steers the opening turn authoritatively from it (used for
the first onboarding voice call so the intro is spoken immediately, instead of
waiting for the slow-brain wakeup to shape a generic holding greeting).

The deterministic tests lock the sidecar prompt-construction contract.
"""

from __future__ import annotations

from unify.conversation_manager.prompt_builders import (
    _BRIEFED_OPENING_GUARDRAIL,
    _OPENING_GREETING_GUARDRAIL,
    build_opening_greeting_messages,
)

SAMPLE_BRIEFING = (
    "[Briefing for your opening turn]\n"
    "This is the user's first onboarding voice call with you. Open with a "
    "warm, natural first-meeting introduction in your own words. Cover:\n"
    "- Greet the user by first name and introduce yourself as T-W1N, their "
    "digital twin / stand-in.\n"
    "- Explain that onboarding is a short shared walkthrough covering "
    "Communication, Workspace, Integrations, and that they can skip ahead.\n"
    "- Make this the concrete next step: starting the communication-channel "
    "reference quiz by clicking the 'Trigger email from T-W1N' row."
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
