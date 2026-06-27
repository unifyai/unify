"""
tests/conversation_manager/voice/test_fast_brain_buffer.py
==========================================================

Deterministic tests for the fast-brain buffer-phrase selector.

The fast brain no longer free-generates replies; on each user turn it emits one
short filler phrase (or a natural pair) copied from a fixed set while the slow
brain composes the real answer. These tests lock the contract: the model's raw
output is validated against the allowed set and re-emitted as canonical phrases,
never echoed; anything unrecognized falls back to the safe default.
"""

from __future__ import annotations

import pytest

from unity.conversation_manager.domains import fast_brain_buffer
from unity.conversation_manager.domains.fast_brain_buffer import (
    BUFFER_PHRASES,
    WAIT_PHRASES,
    resolve_buffer_phrases,
    select_buffer_phrase,
    select_wait_phrase,
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
# Phrase set sanity
# ---------------------------------------------------------------------------


def test_buffer_phrases_are_nonempty_short_strings():
    assert BUFFER_PHRASES, "There must be at least one buffer phrase."
    assert _DEFAULT in BUFFER_PHRASES
    for phrase in BUFFER_PHRASES:
        assert isinstance(phrase, str)
        assert phrase.strip()
        assert len(phrase) <= 40, f"Buffer phrase unexpectedly long: {phrase!r}"


def test_wait_phrases_are_eight_distinct_lag_acknowledgements():
    assert len(WAIT_PHRASES) == 8
    assert len(set(WAIT_PHRASES)) == 8, "Wait phrases must be distinct."
    for phrase in WAIT_PHRASES:
        assert isinstance(phrase, str) and phrase.strip()
        # Must not imply a fresh lookup (the "amnesia" effect we're avoiding).
        low = phrase.lower()
        assert "check" not in low
        assert "look" not in low


# ---------------------------------------------------------------------------
# select_wait_phrase (deterministic, no LLM)
# ---------------------------------------------------------------------------


def test_select_wait_phrase_returns_member():
    for _ in range(20):
        assert select_wait_phrase() in WAIT_PHRASES


def test_select_wait_phrase_avoids_excluded():
    # With all-but-one excluded, the remaining one must be chosen.
    keep = WAIT_PHRASES[3]
    exclude = [p for p in WAIT_PHRASES if p != keep]
    for _ in range(10):
        assert select_wait_phrase(exclude) == keep


def test_select_wait_phrase_never_repeats_within_a_streak():
    """Drawing a full streak (excluding everything used so far) yields no repeats
    until the whole set is exhausted."""
    used: list[str] = []
    for _ in range(len(WAIT_PHRASES)):
        choice = select_wait_phrase(used)
        assert choice not in used
        used.append(choice)
    assert sorted(used) == sorted(WAIT_PHRASES)


def test_select_wait_phrase_falls_back_when_all_excluded():
    # Streak longer than the set: never raises, still returns a valid phrase.
    assert select_wait_phrase(list(WAIT_PHRASES)) in WAIT_PHRASES


# ---------------------------------------------------------------------------
# resolve_buffer_phrases (pure, deterministic)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("phrase", BUFFER_PHRASES)
def test_resolve_exact_single_phrase(phrase):
    assert resolve_buffer_phrases(phrase) == phrase


def test_resolve_combination():
    assert resolve_buffer_phrases("Sure. One moment.") == "Sure. One moment."


def test_resolve_normalizes_casing_and_whitespace():
    # Lowercased, extra spaces, missing trailing period -> canonical form.
    assert resolve_buffer_phrases("  got it ") == "Got it."
    assert resolve_buffer_phrases("sure.   one moment") == "Sure. One moment."


def test_resolve_strips_wrapping_quotes():
    assert resolve_buffer_phrases('"Got it."') == "Got it."
    assert resolve_buffer_phrases("\u201cOn it.\u201d") == "On it."


def test_resolve_drops_novel_chunks_but_keeps_known_lead_in():
    assert resolve_buffer_phrases("Got it. I'll check your calendar.") == "Got it."


def test_resolve_pure_novel_returns_none():
    assert resolve_buffer_phrases("I'll check your calendar now.") is None
    assert resolve_buffer_phrases("") is None


def test_resolve_caps_at_two():
    assert resolve_buffer_phrases("Got it. Okay. Sure.") == "Got it. Okay."


def test_resolve_dedupes_consecutive_repeats():
    assert resolve_buffer_phrases("One moment. One moment.") == "One moment."


def test_resolve_always_returns_known_phrases_or_none():
    for raw in ["Sure.", "garbage", "", "On it. On it.", "Okay! Got it?"]:
        result = resolve_buffer_phrases(raw)
        if result is not None:
            for part in result.split(". "):
                token = part if part.endswith(".") else part + "."
                assert token in BUFFER_PHRASES


# ---------------------------------------------------------------------------
# select_buffer_phrase (LLM-backed, monkeypatched)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_input_returns_default_without_llm(monkeypatch):
    """Empty/whitespace input short-circuits to the default (no LLM call)."""

    def _boom(*a, **kw):  # would fail if called
        raise AssertionError("new_llm_client must not be called for empty input")

    monkeypatch.setattr(fast_brain_buffer, "new_llm_client", _boom)

    assert await select_buffer_phrase("") == _DEFAULT
    assert await select_buffer_phrase("   ") == _DEFAULT


@pytest.mark.asyncio
async def test_select_returns_single_phrase(monkeypatch):
    _patch_client(monkeypatch, raw="Let me check on that.")
    assert await select_buffer_phrase("What's my schedule?") == "Let me check on that."


@pytest.mark.asyncio
async def test_select_returns_combination(monkeypatch):
    _patch_client(monkeypatch, raw="Sure. One moment.")
    assert await select_buffer_phrase("Can you call them?") == "Sure. One moment."


@pytest.mark.asyncio
async def test_select_allows_short_adlib(monkeypatch):
    """A short, natural ad-lib (not a reference phrase) is now returned as-is."""
    _patch_client(monkeypatch, raw="Ha, yeah — fair point.")
    assert (
        await select_buffer_phrase("That name is a bit much.")
        == "Ha, yeah — fair point."
    )


@pytest.mark.asyncio
async def test_select_strips_wrapping_quotes_from_adlib(monkeypatch):
    _patch_client(monkeypatch, raw='"Right, with you."')
    assert await select_buffer_phrase("ok") == "Right, with you."


@pytest.mark.asyncio
async def test_select_overlong_reply_falls_back(monkeypatch):
    """A long substantive answer (overstepping into the slow brain's job) is
    dropped in favour of a safe reference phrase / default."""
    long_answer = (
        "Your meeting with Sarah was moved to 4pm tomorrow, and the room changed "
        "to the downtown office on the third floor, and she also asked you to "
        "bring the Q3 deck and the updated budget figures."
    )
    assert len(long_answer) > 160
    _patch_client(monkeypatch, raw=long_answer)
    assert await select_buffer_phrase("what's my schedule?") == _DEFAULT


@pytest.mark.asyncio
async def test_select_llm_error_falls_back_to_default(monkeypatch):
    _patch_client(monkeypatch, raises=True)
    assert await select_buffer_phrase("anything at all") == _DEFAULT


@pytest.mark.asyncio
async def test_anti_repeat_note_added_when_previous_was_filler(monkeypatch):
    """When the previous line was itself a filler, the model is nudged not to
    repeat it verbatim."""
    captured: dict = {}
    _patch_client(monkeypatch, raw="Got it.", captured=captured)

    await select_buffer_phrase("Thanks for that.", recent_assistant_text="One moment.")

    system_msgs = [m["content"] for m in captured["messages"] if m["role"] == "system"]
    assert any(
        "previous line" in c and "One moment." in c for c in system_msgs
    ), f"Expected an anti-repeat system note. Got: {system_msgs}"


@pytest.mark.asyncio
async def test_no_anti_repeat_note_when_previous_was_prose(monkeypatch):
    """Slow-brain prose does not resolve to a phrase, so no anti-repeat note is
    added (only the selector prompt + the user message)."""
    captured: dict = {}
    _patch_client(monkeypatch, raw="Sure.", captured=captured)

    await select_buffer_phrase(
        "Okay.",
        recent_assistant_text="Your meeting with Sarah is at 3pm tomorrow.",
    )

    system_msgs = [m["content"] for m in captured["messages"] if m["role"] == "system"]
    assert (
        len(system_msgs) == 1
    ), f"Expected only the selector prompt. Got: {system_msgs}"
    assert not any("previous line" in c for c in system_msgs)
