"""
tests/conversation_manager/actions/test_screen_share_visual_question.py
========================================================================

Tests that the CM slow brain answers simple visual questions about a shared
screen directly — without deferring to ``act``.

When screen sharing is active and screenshots are in the buffer, the CM LLM
receives the image as part of its context. Basic visual questions ("what can
you see on my screen?") should be answered inline, not delegated to the Actor.
"""

from __future__ import annotations

import pytest

from tests.helpers import _handle_project
from tests.conversation_manager.cm_helpers import (
    assert_efficient,
    filter_events_by_type,
)
from tests.conversation_manager.core.slow_brain_benchmark_helpers import (
    assert_visual_question_without_act,
    run_visual_question_without_act,
)
from unify.conversation_manager.events import (
    UnifyMessageSent,
)

pytestmark = pytest.mark.eval


@pytest.mark.asyncio
@_handle_project
async def test_visual_question_answered_without_act(initialized_cm):
    """User asks what's on their shared screen — CM should answer directly.

    The CM LLM receives the screenshot as an image in its context. A simple
    visual question like "what can you see on my screen?" should be answered
    inline, not delegated via act().
    """
    cm = initialized_cm

    result = await run_visual_question_without_act(cm)

    assert_visual_question_without_act(result)

    reply_events = filter_events_by_type(result.output_events, UnifyMessageSent)
    assert (
        len(reply_events) >= 1
    ), "CM should have sent a reply describing what it sees on screen"

    assert_efficient(result, 3)
