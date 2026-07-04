"""
tests/conversation_manager/voice/test_fast_brain_turn.py
========================================================

Deterministic tests for the unified fast-brain turn selector.
"""

from __future__ import annotations

import json

import pytest

from unify.conversation_manager.domains import fast_brain_turn
from unify.conversation_manager.domains.fast_brain_turn import (
    FAST_BRAIN_TURN_PROMPT,
    PendingContinuation,
    _GUIDANCE_NOTE,
    _RESUME_LEAD_INS,
    build_fast_brain_turn_messages,
    compute_resume_text,
    select_fast_brain_turn,
)
from unify.conversation_manager.events import (
    FAST_BRAIN_TURN_CONTINUATION,
    FAST_BRAIN_TURN_DEFER,
    FAST_BRAIN_TURN_SILENCE,
    FAST_BRAIN_TURN_SMALLTALK,
)

_DEFAULT = fast_brain_turn._DEFAULT_PHRASE


def _patch_client(
    monkeypatch,
    decision: dict,
    *,
    raises: bool = False,
    captured: dict | None = None,
):
    class _Client:
        def set_response_format(self, _model):
            pass

        async def generate(self, *, messages=None, **_kw):
            if captured is not None:
                captured["messages"] = messages
            if raises:
                raise RuntimeError("boom")
            return json.dumps(decision)

    monkeypatch.setattr(
        fast_brain_turn,
        "new_llm_client",
        lambda *a, **kw: _Client(),
    )


def test_prompt_covers_classifications_and_identity():
    p = FAST_BRAIN_TURN_PROMPT
    assert "silence" in p
    assert "defer" in p
    assert "smalltalk" in p
    assert "continuation" in p
    assert "STAY ONE PERSON" in p
    assert "interrupted mid-sentence" in p
    assert _DEFAULT == "One moment."


def test_prompt_forbids_hollow_still_on_deferrals():
    p = FAST_BRAIN_TURN_PROMPT
    assert "still on it" in p.lower()
    assert "hollow" in p.lower()


def test_resume_lead_in_bank_has_variety():
    assert len(_RESUME_LEAD_INS) >= 5
    assert len(set(_RESUME_LEAD_INS)) == len(_RESUME_LEAD_INS)


def test_resume_text_rewinds_to_cut_sentence_start():
    full = "Your number is saved. Next, click Trigger email and I'll fire it off."
    spoken = "Your number is saved. Next, click Trigger"
    assert compute_resume_text(full, spoken) == (
        "Next, click Trigger email and I'll fire it off."
    )


def test_resume_text_no_boundary_returns_raw_remainder():
    full = "click Trigger email and I'll fire it off"
    spoken = "click Trigger"
    assert compute_resume_text(full, spoken) == "email and I'll fire it off"


def test_build_messages_includes_interrupted_context_when_pending():
    pending = PendingContinuation(
        resume_text="Next, click Connect Slack.",
        remainder="Next, click Connect Slack.",
        spoken_prefix="Saved.",
    )
    msgs = build_fast_brain_turn_messages(
        system_prompt="PERSONA",
        history_messages=[],
        user_text="okay",
        pending_continuation=pending,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
        recent_assistant_text="",
    )
    system_text = "\n".join(m["content"] for m in msgs if m["role"] == "system")
    assert "Next, click Connect Slack." in system_text
    assert "continuation" in system_text.lower()


@pytest.mark.asyncio
async def test_empty_input_returns_default_without_llm(monkeypatch):
    def _boom(*a, **kw):
        raise AssertionError("new_llm_client must not be called for empty input")

    monkeypatch.setattr(fast_brain_turn, "new_llm_client", _boom)
    resolved = await select_fast_brain_turn(
        user_text="",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
    )
    assert resolved.classification == FAST_BRAIN_TURN_DEFER
    assert resolved.intended_speech == _DEFAULT


@pytest.mark.asyncio
async def test_silence_classification(monkeypatch):
    _patch_client(monkeypatch, {"classification": "silence", "content": ""})
    resolved = await select_fast_brain_turn(
        user_text="okay",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
    )
    assert resolved.classification == FAST_BRAIN_TURN_SILENCE
    assert resolved.intended_speech == ""


@pytest.mark.asyncio
async def test_defer_returns_content(monkeypatch):
    _patch_client(
        monkeypatch,
        {"classification": "defer", "content": "Yes — let me check on that."},
    )
    resolved = await select_fast_brain_turn(
        user_text="did you send it?",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
    )
    assert resolved.classification == FAST_BRAIN_TURN_DEFER
    assert resolved.intended_speech == "Yes — let me check on that."


@pytest.mark.asyncio
async def test_smalltalk_returns_content(monkeypatch):
    _patch_client(
        monkeypatch,
        {
            "classification": "smalltalk",
            "content": "Doing great, thanks for asking!",
        },
    )
    resolved = await select_fast_brain_turn(
        user_text="how are you?",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
    )
    assert resolved.classification == FAST_BRAIN_TURN_SMALLTALK
    assert resolved.intended_speech == "Doing great, thanks for asking!"


@pytest.mark.asyncio
async def test_overlong_defer_falls_back(monkeypatch):
    long_answer = "x" * 200
    _patch_client(monkeypatch, {"classification": "defer", "content": long_answer})
    resolved = await select_fast_brain_turn(
        user_text="question",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
    )
    assert resolved.classification == FAST_BRAIN_TURN_DEFER
    assert resolved.intended_speech == _DEFAULT


@pytest.mark.asyncio
async def test_llm_error_falls_back_to_defer(monkeypatch):
    _patch_client(monkeypatch, {}, raises=True)
    resolved = await select_fast_brain_turn(
        user_text="anything",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
    )
    assert resolved.classification == FAST_BRAIN_TURN_DEFER
    assert resolved.intended_speech == _DEFAULT


@pytest.mark.asyncio
async def test_continuation_uses_fixed_lead_in_not_model_content(monkeypatch):
    pending = PendingContinuation(
        resume_text="Next, click Connect Slack.",
        remainder="Next, click Connect Slack.",
        spoken_prefix="Saved.",
    )
    _patch_client(
        monkeypatch,
        {
            "classification": "continuation",
            "content": "I hear you, Daniel — you're right.",
        },
    )
    resolved = await select_fast_brain_turn(
        user_text="okay",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=pending,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
    )
    assert resolved.classification == FAST_BRAIN_TURN_CONTINUATION
    assert resolved.intended_speech.endswith("Next, click Connect Slack.")
    assert "I hear you" not in resolved.intended_speech
    assert resolved.declined_continuation is False


@pytest.mark.asyncio
async def test_declined_continuation_when_pending_but_defer(monkeypatch):
    pending = PendingContinuation(
        resume_text="Next, click Connect Slack.",
        remainder="Next, click Connect Slack.",
        spoken_prefix="Saved.",
    )
    _patch_client(
        monkeypatch,
        {"classification": "defer", "content": "Sure — one moment."},
    )
    resolved = await select_fast_brain_turn(
        user_text="wait, stop",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=pending,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
    )
    assert resolved.classification == FAST_BRAIN_TURN_DEFER
    assert resolved.declined_continuation is True


@pytest.mark.asyncio
async def test_interrupted_question_ack_is_defer_not_silence(monkeypatch):
    """Regression: truncated assistant question + agreeing ack -> defer."""
    _patch_client(
        monkeypatch,
        {
            "classification": "defer",
            "content": "Great — let me walk you through onboarding.",
        },
    )
    history = [
        {
            "role": "assistant",
            "content": (
                "Should we start with the onboarding, or would you rather just"
            ),
        },
    ]
    resolved = await select_fast_brain_turn(
        user_text="Yeah. That's great.",
        system_prompt="PERSONA",
        history_messages=history,
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
        recent_assistant_text=history[0]["content"],
    )
    assert resolved.classification == FAST_BRAIN_TURN_DEFER
    assert resolved.intended_speech


@pytest.mark.asyncio
async def test_already_deferred_adds_note(monkeypatch):
    captured: dict = {}
    _patch_client(
        monkeypatch,
        {"classification": "defer", "content": "Still checking."},
        captured=captured,
    )
    await select_fast_brain_turn(
        user_text="you still there?",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=None,
        already_deferred=True,
        guidance="",
        idle_status_smalltalk=False,
    )
    system_msgs = [m["content"] for m in captured["messages"] if m["role"] == "system"]
    assert any("already deferred once" in c.lower() for c in system_msgs)


@pytest.mark.asyncio
async def test_guidance_adds_scoped_block(monkeypatch):
    captured: dict = {}
    note = "The answer is Blade Runner. Confirm if they guess it."
    _patch_client(
        monkeypatch,
        {"classification": "defer", "content": "Yes, exactly!"},
        captured=captured,
    )
    await select_fast_brain_turn(
        user_text="is it Blade Runner?",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=None,
        already_deferred=False,
        guidance=note,
        idle_status_smalltalk=False,
    )
    system_msgs = [m["content"] for m in captured["messages"] if m["role"] == "system"]
    assert note in "\n".join(system_msgs)
    assert _GUIDANCE_NOTE.split("{guidance}")[0].strip() in "\n".join(system_msgs)


def test_general_fast_brain_prompts_have_no_domain_concepts():
    for text in (FAST_BRAIN_TURN_PROMPT, _GUIDANCE_NOTE):
        low = text.lower()
        assert "quiz" not in low
        assert "clue" not in low
        assert "onboard" not in low
