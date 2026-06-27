"""
tests/conversation_manager/voice/test_fast_brain_buffer.py
==========================================================

Deterministic tests for the fast-brain buffer-phrase selector.

The fast brain no longer free-generates replies; on each user turn it emits one
short, universally-safe filler phrase from a fixed set while the slow brain
composes the real answer. These tests lock the contract: the selector always
returns a phrase from ``BUFFER_PHRASES`` and fails safe to the default on empty
input or any error - it never free-generates text.
"""

from __future__ import annotations

import pytest

from unity.conversation_manager.domains import fast_brain_buffer
from unity.conversation_manager.domains.fast_brain_buffer import (
    BUFFER_PHRASES,
    select_buffer_phrase,
)

_DEFAULT = BUFFER_PHRASES[fast_brain_buffer._DEFAULT_INDEX]


def _patch_client(monkeypatch, raw=None, *, raises: bool = False):
    """Patch ``new_llm_client`` with a fake whose ``generate`` returns *raw*."""

    class _Client:
        async def generate(self, *, messages=None, **_kw):
            if raises:
                raise RuntimeError("boom")
            return raw

    monkeypatch.setattr(
        fast_brain_buffer,
        "new_llm_client",
        lambda *a, **kw: _Client(),
    )


def test_buffer_phrases_are_nonempty_short_strings():
    assert BUFFER_PHRASES, "There must be at least one buffer phrase."
    for phrase in BUFFER_PHRASES:
        assert isinstance(phrase, str)
        assert phrase.strip()
        assert len(phrase) <= 40, f"Buffer phrase unexpectedly long: {phrase!r}"


@pytest.mark.asyncio
async def test_empty_input_returns_default_without_llm(monkeypatch):
    """Empty/whitespace input short-circuits to the default (no LLM call)."""

    def _boom(*a, **kw):  # would fail if called
        raise AssertionError("new_llm_client must not be called for empty input")

    monkeypatch.setattr(fast_brain_buffer, "new_llm_client", _boom)

    assert await select_buffer_phrase("") == _DEFAULT
    assert await select_buffer_phrase("   ") == _DEFAULT


@pytest.mark.asyncio
@pytest.mark.parametrize("index", list(range(len(BUFFER_PHRASES))))
async def test_selects_phrase_by_index(monkeypatch, index):
    """A clean digit response maps to the corresponding phrase."""
    _patch_client(monkeypatch, raw=str(index))
    assert (
        await select_buffer_phrase("Can you check my calendar?")
        == BUFFER_PHRASES[index]
    )


@pytest.mark.asyncio
async def test_extracts_first_digit_from_noisy_output(monkeypatch):
    """A non-bare response still resolves to the first digit's phrase."""
    _patch_client(monkeypatch, raw="I'd pick option 3 here.")
    assert await select_buffer_phrase("What's the weather?") == BUFFER_PHRASES[3]


@pytest.mark.asyncio
async def test_out_of_range_index_falls_back_to_default(monkeypatch):
    _patch_client(monkeypatch, raw="99")
    assert await select_buffer_phrase("hello") == _DEFAULT


@pytest.mark.asyncio
async def test_no_digit_falls_back_to_default(monkeypatch):
    _patch_client(monkeypatch, raw="sure thing")
    assert await select_buffer_phrase("hello") == _DEFAULT


@pytest.mark.asyncio
async def test_llm_error_falls_back_to_default(monkeypatch):
    _patch_client(monkeypatch, raises=True)
    assert await select_buffer_phrase("anything at all") == _DEFAULT


@pytest.mark.asyncio
async def test_always_returns_a_known_phrase(monkeypatch):
    """Whatever the model emits, the result is always a member of the fixed set."""
    for raw in ["0", "5", "-1", "", "garbage", "3.5", "10"]:
        _patch_client(monkeypatch, raw=raw)
        result = await select_buffer_phrase("some user turn")
        assert result in BUFFER_PHRASES
