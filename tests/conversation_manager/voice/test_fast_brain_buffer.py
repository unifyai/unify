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
from unity.conversation_manager.domains.fast_brain_buffer import select_fast_reply

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
    assert "Thanks." in p
    assert "Will do." in p
    assert "{the thing}" in p  # template placeholder
    # The default fallback is always safe.
    assert _DEFAULT == "One moment."


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
