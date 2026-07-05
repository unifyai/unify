"""Tests for the agent-initiated Unify Meet ring.

The assistant rings the owner on Unify Meet (it cannot join their browser for
them); if unanswered within the grace window the conversation falls back to text.
These tests cover the no-answer fallback, its cancellation when the owner
answers, and that the ``start_unify_meet`` tool is offered when idle.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from unify.conversation_manager.conversation_manager import ConversationManager


@pytest.mark.asyncio
async def test_meet_ring_no_answer_falls_back_to_text():
    """An unanswered ring pushes a continue-over-text notification and re-runs.

    The ring's queued pre-armed hang-up gate must also be dropped so it cannot
    leak into a later, unrelated call.
    """
    pushed: list = []
    fake = SimpleNamespace(
        _pending_meet_ring="ring-1",
        _MEET_RING_TIMEOUT_S=0.0,
        notifications_bar=SimpleNamespace(
            push_notif=lambda *a, **k: pushed.append(a),
        ),
        call_manager=SimpleNamespace(pending_hang_up_gate="stale gate"),
        run_llm=AsyncMock(),
    )

    await ConversationManager._await_meet_ring_answer(fake, "ring-1")

    assert fake._pending_meet_ring is None
    assert fake.call_manager.pending_hang_up_gate == ""
    assert pushed, "a continue-over-text notification must be pushed"
    fake.run_llm.assert_awaited_once()


@pytest.mark.asyncio
async def test_meet_ring_answered_skips_fallback():
    """If the ring was answered (pending cleared/superseded), no fallback fires."""
    pushed: list = []
    fake = SimpleNamespace(
        _pending_meet_ring=None,  # answered -> UnifyMeetReceived cleared it
        _MEET_RING_TIMEOUT_S=0.0,
        notifications_bar=SimpleNamespace(
            push_notif=lambda *a, **k: pushed.append(a),
        ),
        run_llm=AsyncMock(),
    )

    await ConversationManager._await_meet_ring_answer(fake, "ring-1")

    assert not pushed
    fake.run_llm.assert_not_awaited()


@pytest.mark.asyncio
async def test_start_unify_meet_tool_offered_when_idle(initialized_cm):
    """``start_unify_meet`` is in the tool set when no voice session is active."""
    from unify.conversation_manager.domains.brain_action_tools import (
        ConversationManagerBrainActionTools,
    )

    tools = ConversationManagerBrainActionTools(initialized_cm.cm).as_tools()
    assert "start_unify_meet" in tools
