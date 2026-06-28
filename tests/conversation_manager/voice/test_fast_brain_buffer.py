"""
tests/conversation_manager/voice/test_fast_brain_buffer.py
==========================================================

Deterministic tests for the fast-brain reply selector.

The fast brain gives one brief, template-guided, in-the-moment reaction to cover
the gap before the slow brain's real answer. It never composes a substantive
answer: a length backstop drops over-long replies and a safe default covers
empty input / errors. These tests lock that contract (the LLM is monkeypatched).
"""

from __future__ import annotations

import pytest

from unity.conversation_manager.domains import fast_brain_buffer
from unity.conversation_manager.domains.fast_brain_buffer import (
    compute_resume_text,
    select_continuation,
    select_fast_reply,
)

_DEFAULT = fast_brain_buffer._DEFAULT_PHRASE


def _patch_client(
    monkeypatch,
    raw=None,
    *,
    raises: bool = False,
    captured: dict | None = None,
):
    """Patch ``new_llm_client`` with a fake whose ``generate`` returns *raw*.

    When ``captured`` is given, the messages passed to ``generate`` are stored
    under ``captured["messages"]`` for assertion.
    """

    class _Client:
        async def generate(self, *, messages=None, **_kw):
            if captured is not None:
                captured["messages"] = messages
            if raises:
                raise RuntimeError("boom")
            return raw

    monkeypatch.setattr(
        fast_brain_buffer,
        "new_llm_client",
        lambda *a, **kw: _Client(),
    )


# ---------------------------------------------------------------------------
# Prompt sanity — the patterns the user asked for are present
# ---------------------------------------------------------------------------


def test_prompt_covers_key_patterns():
    p = fast_brain_buffer._FAST_REPLY_PROMPT
    assert "NEVER actually answer" in p
    assert "take your time" in p
    # The give-space case is still handled (thank + take the pause).
    assert "Thanks" in p
    assert "{the thing}" in p  # template placeholder
    # The default fallback is always safe.
    assert _DEFAULT == "One moment."


def test_prompt_forbids_bare_canned_phrases():
    """Every reply must be contextualized - no standalone canned acks/defers."""
    p = fast_brain_buffer._FAST_REPLY_PROMPT
    assert "MUST CONTEXTUALIZE" in p
    # It explicitly tells the model not to reply with a bare phrase on its own.
    assert "bare" in p.lower()
    assert "on its own" in p.lower()


def test_prompt_keeps_the_fourth_wall():
    """The fast brain must present as one person and never leak that a separate
    system composes the answer."""
    p = fast_brain_buffer._FAST_REPLY_PROMPT
    assert "FOURTH WALL" in p
    assert "one single person" in p
    # The old "a smarter system is composing the real answer" leak is gone.
    assert "smarter system is composing" not in p.lower()


# ---------------------------------------------------------------------------
# select_fast_reply (LLM-backed, monkeypatched)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_input_returns_default_without_llm(monkeypatch):
    """Empty/whitespace input short-circuits to the default (no LLM call)."""

    def _boom(*a, **kw):
        raise AssertionError("new_llm_client must not be called for empty input")

    monkeypatch.setattr(fast_brain_buffer, "new_llm_client", _boom)

    assert await select_fast_reply("") == _DEFAULT
    assert await select_fast_reply("   ") == _DEFAULT


@pytest.mark.asyncio
async def test_returns_short_reply_verbatim(monkeypatch):
    _patch_client(monkeypatch, raw="Thanks.")
    assert await select_fast_reply("take your time") == "Thanks."


@pytest.mark.asyncio
async def test_returns_filled_template(monkeypatch):
    _patch_client(monkeypatch, raw="Yes, I'll let you know once it's done.")
    out = await select_fast_reply("let me know when it's done")
    assert out == "Yes, I'll let you know once it's done."


@pytest.mark.asyncio
async def test_strips_wrapping_quotes(monkeypatch):
    _patch_client(monkeypatch, raw='"Will do."')
    assert await select_fast_reply("let me know") == "Will do."


@pytest.mark.asyncio
async def test_collapses_whitespace(monkeypatch):
    _patch_client(monkeypatch, raw="  Got   it. \n")
    assert await select_fast_reply("done") == "Got it."


@pytest.mark.asyncio
async def test_overlong_reply_falls_back(monkeypatch):
    """A long substantive answer (overstepping into the slow brain's job) is
    dropped for the safe default."""
    long_answer = (
        "Your meeting with Sarah was moved to 4pm tomorrow, and the room changed "
        "to the downtown office, and she asked you to bring the Q3 deck and the "
        "updated budget figures for review."
    )
    assert len(long_answer) > 160
    _patch_client(monkeypatch, raw=long_answer)
    assert await select_fast_reply("what's my schedule?") == _DEFAULT


@pytest.mark.asyncio
async def test_llm_error_falls_back_to_default(monkeypatch):
    _patch_client(monkeypatch, raises=True)
    assert await select_fast_reply("anything at all") == _DEFAULT


@pytest.mark.asyncio
async def test_already_deferred_adds_reassurance_note(monkeypatch):
    captured: dict = {}
    _patch_client(monkeypatch, raw="Bear with me, almost there.", captured=captured)

    await select_fast_reply("you still there?", already_deferred=True)

    system_msgs = [m["content"] for m in captured["messages"] if m["role"] == "system"]
    assert any("already deferred" in c.lower() for c in system_msgs), system_msgs


@pytest.mark.asyncio
async def test_first_reply_has_no_deferred_note(monkeypatch):
    captured: dict = {}
    _patch_client(monkeypatch, raw="Got it.", captured=captured)

    await select_fast_reply("I clicked it", already_deferred=False)

    system_msgs = [m["content"] for m in captured["messages"] if m["role"] == "system"]
    assert not any("already deferred" in c.lower() for c in system_msgs)


@pytest.mark.asyncio
async def test_recent_assistant_line_passed_as_context_with_antirepeat(monkeypatch):
    captured: dict = {}
    _patch_client(monkeypatch, raw="Sure.", captured=captured)

    await select_fast_reply(
        "ok",
        recent_assistant_text="One moment.",
    )

    msgs = captured["messages"]
    # Previous line is provided as assistant context.
    assert any(m["role"] == "assistant" and m["content"] == "One moment." for m in msgs)
    # ...plus an anti-repeat nudge.
    assert any(
        m["role"] == "system" and "do not repeat" in m["content"].lower() for m in msgs
    )


# ---------------------------------------------------------------------------
# compute_resume_text - rewind the unheard tail to a clean sentence start
# ---------------------------------------------------------------------------


def test_resume_text_rewinds_to_cut_sentence_start():
    full = "Your number is saved. Next, click Trigger email and I'll fire it off."
    spoken = "Your number is saved. Next, click Trigger"
    # Resumes from the start of the sentence that was cut, not mid-word.
    assert compute_resume_text(full, spoken) == (
        "Next, click Trigger email and I'll fire it off."
    )


def test_resume_text_no_boundary_returns_raw_remainder():
    full = "click Trigger email and I'll fire it off"
    spoken = "click Trigger"
    assert compute_resume_text(full, spoken) == "email and I'll fire it off"


def test_resume_text_spoken_not_a_prefix_returns_full():
    full = "The spice must flow, Daniel."
    assert compute_resume_text(full, "something unrelated") == full


def test_resume_text_empty_full_is_empty():
    assert compute_resume_text("", "anything") == ""


# ---------------------------------------------------------------------------
# select_continuation - resume lead-in or defer
# ---------------------------------------------------------------------------


def test_continuation_prompt_is_classifier_resuming_on_greetings():
    """The prompt must be a CONTINUE/DEFER classifier that steers toward resuming
    when the caller answers with a greeting or asks why you're calling."""
    p = fast_brain_buffer._CONTINUATION_PROMPT
    assert "CONTINUE" in p
    assert "DEFER" in p
    # Call-answer greetings and "why are you calling" are explicit CONTINUE cases.
    assert "Hello?" in p
    assert "Why are you calling?" in p
    assert "NOT reasons to defer" in p


def test_resume_lead_in_bank_has_variety():
    bank = fast_brain_buffer._RESUME_LEAD_INS
    assert len(bank) >= 5
    assert len(set(bank)) == len(bank)  # all unique


@pytest.mark.asyncio
async def test_continuation_resumes_with_fixed_lead_in(monkeypatch):
    _patch_client(monkeypatch, raw="CONTINUE")
    out = await select_continuation("the next step is to click Connect Slack.", "okay")
    # The lead-in comes from the fixed bank, NOT from the model.
    assert out in fast_brain_buffer._RESUME_LEAD_INS


@pytest.mark.asyncio
async def test_continuation_lead_in_never_echoes_resume(monkeypatch):
    """Regression guard for the duplication bug: the lead-in is always a fixed
    bridge and never contains the resumed content."""
    resume = "I hear you, Daniel — you're right. I did know the answer."
    # Even if the model echoes the resume (the old failure), we ignore its text.
    _patch_client(monkeypatch, raw=resume)
    out = await select_continuation(resume, "my name's Daniel")
    assert out in fast_brain_buffer._RESUME_LEAD_INS
    assert "I hear you" not in out


@pytest.mark.asyncio
async def test_continuation_defer_sentinel_returns_none(monkeypatch):
    _patch_client(monkeypatch, raw="DEFER")
    out = await select_continuation(
        "the next step is to click Connect Slack.",
        "wait, stop",
    )
    assert out is None


@pytest.mark.asyncio
async def test_continuation_empty_inputs_skip_llm(monkeypatch):
    def _boom(*a, **kw):
        raise AssertionError("new_llm_client must not be called for empty input")

    monkeypatch.setattr(fast_brain_buffer, "new_llm_client", _boom)

    assert await select_continuation("", "okay") is None
    assert await select_continuation("some remainder", "   ") is None


@pytest.mark.asyncio
async def test_continuation_llm_error_defers(monkeypatch):
    _patch_client(monkeypatch, raises=True)
    assert await select_continuation("remainder text", "okay") is None


# ---------------------------------------------------------------------------
# Bundled guidance - one-shot, scoped, and fully general (no quiz concepts)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_guidance_adds_scoped_block(monkeypatch):
    captured: dict = {}
    _patch_client(monkeypatch, raw="Yes, exactly!", captured=captured)

    note = "The answer is Blade Runner. Confirm if they guess it; never reveal early."
    await select_fast_reply("is it Blade Runner?", guidance=note)

    system_msgs = [m["content"] for m in captured["messages"] if m["role"] == "system"]
    joined = "\n".join(system_msgs)
    assert note in joined
    flat = " ".join(joined.lower().split())
    assert "handed you a short note" in flat
    assert "never volunteer" in flat


@pytest.mark.asyncio
async def test_no_guidance_keeps_prompt_clean(monkeypatch):
    captured: dict = {}
    _patch_client(monkeypatch, raw="Got it — on it now.", captured=captured)

    await select_fast_reply("I clicked it")

    system_msgs = [m["content"] for m in captured["messages"] if m["role"] == "system"]
    joined = "\n".join(system_msgs)
    assert "handed you a short note" not in joined


def test_general_fast_brain_prompts_have_no_domain_concepts():
    """The fast brain's general prompts must never bake in onboarding/quiz terms."""
    for text in (
        fast_brain_buffer._FAST_REPLY_PROMPT,
        fast_brain_buffer._GUIDANCE_NOTE,
    ):
        low = text.lower()
        assert "quiz" not in low
        assert "clue" not in low
        assert "guess" not in low
        assert "onboard" not in low
