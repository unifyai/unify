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
    FAST_BRAIN_TURN_HANG_UP,
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


# ---------------------------------------------------------------------------
# Held opener (never-spoken line): a pending continuation with an empty
# spoken_prefix is a planned line the caller has not heard at all — it gets the
# held-opener context block and is delivered verbatim with no resume lead-in.
# ---------------------------------------------------------------------------


def _held_opener() -> PendingContinuation:
    return PendingContinuation(
        resume_text="Hi Dan — quick sci-fi quiz to test this channel.",
        remainder="Hi Dan — quick sci-fi quiz to test this channel.",
        spoken_prefix="",
    )


def test_build_messages_uses_held_opener_context_when_nothing_spoken():
    msgs = build_fast_brain_turn_messages(
        system_prompt="PERSONA",
        history_messages=[],
        user_text="Hello, who is this? I was just in the middle of something.",
        pending_continuation=_held_opener(),
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
        recent_assistant_text="",
    )
    system_text = "\n".join(m["content"] for m in msgs if m["role"] == "system")
    assert "have NOT spoken yet" in system_text
    assert "cut you off mid-sentence" not in system_text
    assert "Hi Dan — quick sci-fi quiz to test this channel." in system_text


def test_build_messages_uses_interrupted_context_when_prefix_was_heard():
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
    assert "cut you off mid-sentence" in system_text
    assert "have NOT spoken yet" not in system_text


@pytest.mark.asyncio
async def test_held_opener_continuation_is_verbatim_with_no_lead_in(monkeypatch):
    _patch_client(monkeypatch, {"classification": "continuation", "content": ""})
    resolved = await select_fast_brain_turn(
        user_text="Hello? Sorry, I was walking the dog, hang on a second.",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=_held_opener(),
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
    )
    assert resolved.classification == FAST_BRAIN_TURN_CONTINUATION
    assert resolved.intended_speech == (
        "Hi Dan — quick sci-fi quiz to test this channel."
    )
    assert not any(
        resolved.intended_speech.startswith(lead) for lead in _RESUME_LEAD_INS
    )


# ---------------------------------------------------------------------------
# Call briefing: unspoken context that lets the fast brain conduct the briefed
# interaction itself (via smalltalk) instead of deferring to the slow brain.
# ---------------------------------------------------------------------------

_BRIEFING = (
    "Channel test via one sci-fi quiz clue. Expected answer: Dune (accept "
    "mishearings like June or Doon). On a correct guess, confirm warmly, say "
    "the channel is proven, and that you'll continue in chat."
)


def test_build_messages_includes_briefing_block():
    msgs = build_fast_brain_turn_messages(
        system_prompt="PERSONA",
        history_messages=[],
        user_text="It's from Dune!",
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
        recent_assistant_text="",
        briefing=_BRIEFING,
    )
    system_text = "\n".join(m["content"] for m in msgs if m["role"] == "system")
    assert "Active call briefing" in system_text
    assert _BRIEFING in system_text
    assert "NEVER read the briefing aloud" in system_text
    assert "fully own every interaction the briefing covers" in system_text


def test_build_messages_has_no_briefing_block_without_briefing():
    msgs = build_fast_brain_turn_messages(
        system_prompt="PERSONA",
        history_messages=[],
        user_text="hello",
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
        recent_assistant_text="",
    )
    system_text = "\n".join(m["content"] for m in msgs if m["role"] == "system")
    assert "Active call briefing" not in system_text


@pytest.mark.asyncio
async def test_briefed_smalltalk_allows_longer_replies(monkeypatch):
    """A briefed reply (confirm + wrap-up) may exceed the ordinary smalltalk
    cap without being coerced into a bare defer."""
    long_reply = (
        "Dune — exactly right, Frank Herbert's classic. That proves the "
        "phone channel works in both directions, which is all we needed "
        "from this call. "
    ) * 3
    assert 300 < len(long_reply.strip()) <= 600
    _patch_client(
        monkeypatch,
        {"classification": "smalltalk", "content": long_reply},
    )
    resolved = await select_fast_brain_turn(
        user_text="It's Dune, right?",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
        briefing=_BRIEFING,
    )
    assert resolved.classification == FAST_BRAIN_TURN_SMALLTALK
    assert resolved.intended_speech == " ".join(long_reply.split())


@pytest.mark.asyncio
async def test_unbriefed_smalltalk_keeps_ordinary_cap(monkeypatch):
    long_reply = "x" * 400
    _patch_client(
        monkeypatch,
        {"classification": "smalltalk", "content": long_reply},
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
    assert resolved.classification == FAST_BRAIN_TURN_DEFER
    assert resolved.intended_speech == _DEFAULT


@pytest.mark.asyncio
async def test_held_opener_declined_reports_declined_continuation(monkeypatch):
    _patch_client(
        monkeypatch,
        {"classification": "defer", "content": "Of course — go ahead."},
    )
    resolved = await select_fast_brain_turn(
        user_text="Please don't talk right now, I need a second.",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=_held_opener(),
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
    )
    assert resolved.classification == FAST_BRAIN_TURN_DEFER
    assert resolved.declined_continuation is True


# ---------------------------------------------------------------------------
# Hang-up gate: the slow brain sanctions ending the call; the extra hang_up
# classification (and its prompt block) only exist while the gate is armed.
# ---------------------------------------------------------------------------


def test_response_model_matrix():
    assert (
        fast_brain_turn._response_model(
            interrupted=False,
            hang_up_gated=False,
        )
        is fast_brain_turn.FastBrainTurnDecision
    )
    assert (
        fast_brain_turn._response_model(
            interrupted=True,
            hang_up_gated=False,
        )
        is fast_brain_turn.FastBrainInterruptedTurnDecision
    )
    assert (
        fast_brain_turn._response_model(
            interrupted=False,
            hang_up_gated=True,
        )
        is fast_brain_turn.FastBrainGatedTurnDecision
    )
    assert (
        fast_brain_turn._response_model(
            interrupted=True,
            hang_up_gated=True,
        )
        is fast_brain_turn.FastBrainInterruptedGatedTurnDecision
    )


def test_hang_up_gate_block_injected_only_when_armed():
    kwargs = dict(
        system_prompt="PERSONA",
        history_messages=[],
        user_text="okay great, bye!",
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
        recent_assistant_text="",
    )
    armed = build_fast_brain_turn_messages(
        **kwargs,
        hang_up_gate_reason="channel test complete — wrap up warmly",
    )
    armed_text = "\n".join(m["content"] for m in armed if m["role"] == "system")
    assert "Ending this call is now sanctioned" in armed_text
    assert "channel test complete — wrap up warmly" in armed_text
    assert "hang_up" in armed_text

    disarmed = build_fast_brain_turn_messages(**kwargs, hang_up_gate_reason=None)
    disarmed_text = "\n".join(m["content"] for m in disarmed if m["role"] == "system")
    assert "Ending this call is now sanctioned" not in disarmed_text


@pytest.mark.asyncio
async def test_gated_hang_up_returns_farewell(monkeypatch):
    _patch_client(
        monkeypatch,
        {"classification": "hang_up", "content": "Bye Dan — talk soon!"},
    )
    resolved = await select_fast_brain_turn(
        user_text="Perfect, thanks — bye!",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
        hang_up_gate_reason="channel test complete",
    )
    assert resolved.classification == FAST_BRAIN_TURN_HANG_UP
    assert resolved.intended_speech == "Bye Dan — talk soon!"


@pytest.mark.asyncio
async def test_gated_hang_up_empty_content_gets_default_farewell(monkeypatch):
    _patch_client(monkeypatch, {"classification": "hang_up", "content": ""})
    resolved = await select_fast_brain_turn(
        user_text="bye!",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
        hang_up_gate_reason="wrap up",
    )
    assert resolved.classification == FAST_BRAIN_TURN_HANG_UP
    assert resolved.intended_speech == fast_brain_turn._DEFAULT_FAREWELL


@pytest.mark.asyncio
async def test_hang_up_without_gate_falls_back_to_defer(monkeypatch):
    """Without the gate, hang_up is not in the response model — a model that
    emits it anyway fails validation and resolves to the defer fallback."""
    _patch_client(
        monkeypatch,
        {"classification": "hang_up", "content": "Bye!"},
    )
    resolved = await select_fast_brain_turn(
        user_text="bye!",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=None,
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
    )
    assert resolved.classification == FAST_BRAIN_TURN_DEFER


def test_resolve_hang_up_without_gate_coerces_to_defer():
    resolved = fast_brain_turn._resolve_content(
        FAST_BRAIN_TURN_HANG_UP,
        "Bye!",
        pending_continuation=None,
        hang_up_gated=False,
    )
    assert resolved.classification == FAST_BRAIN_TURN_DEFER


@pytest.mark.asyncio
async def test_gated_hang_up_with_held_opener_reports_declined(monkeypatch):
    """Closing while a never-delivered line is pending surfaces the decline so
    the slow brain knows the remainder was never heard."""
    _patch_client(
        monkeypatch,
        {"classification": "hang_up", "content": "No worries — bye!"},
    )
    resolved = await select_fast_brain_turn(
        user_text="actually I have to run, bye!",
        system_prompt="PERSONA",
        history_messages=[],
        pending_continuation=_held_opener(),
        already_deferred=False,
        guidance="",
        idle_status_smalltalk=False,
        hang_up_gate_reason="wrap up",
    )
    assert resolved.classification == FAST_BRAIN_TURN_HANG_UP
    assert resolved.declined_continuation is True
