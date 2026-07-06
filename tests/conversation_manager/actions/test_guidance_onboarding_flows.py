"""
tests/conversation_manager/actions/test_guidance_onboarding_flows.py
=====================================================================

Tests for nuanced onboarding, explanatory, and guidance flows where the
boundary between "conversational context" and "actionable content worth
storing" is blurred.

These scenarios test whether the CM correctly:

1. Recognises when the user is teaching it something worth persisting
   (even without an explicit "save this" directive).
2. Accumulates multi-message explanations and stores them in batch when
   the explanation is complete, rather than acting on each fragment.
3. Engages with screen-share tutorials via ``persist=True`` sessions
   that can absorb ongoing teaching and then transition to interactive
   execution when the user says "now you try".
4. References screenshot filepaths in act queries when visual
   demonstrations are involved.

These are eval tests.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.helpers import _handle_project
from tests.conversation_manager.cm_helpers import (
    assert_act_triggered,
    assert_efficient,
    filter_events_by_type,
    has_steering_tool_call,
)
from tests.conversation_manager.conftest import BOSS
from unify.conversation_manager.events import (
    UnifyMessageReceived,
    UnifyMessageSent,
    ActorHandleStarted,
)
from unify.conversation_manager.cm_types import ScreenshotEntry

pytestmark = pytest.mark.eval

# Minimal valid 8x8 white JPEG used as a stand-in screenshot.
# Content is irrelevant — what matters is the LLM seeing screenshot entries
# with filepaths in the rendered state alongside the user's verbal descriptions.
# Must be actual JPEG since the multimodal API validates the content type.
_TINY_JPEG_B64 = (
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkS"
    "Ew8UHRofHh0aHBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/2wBDAQkJ"
    "CQwLDBgNDRgyIRwhMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIy"
    "MjIyMjIyMjIyMjIyMjL/wAARCAAIAAgDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEA"
    "AAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEG"
    "E1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RF"
    "RkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKj"
    "pKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP0"
    "9fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgEC"
    "BAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLR"
    "ChYkNOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0"
    "dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbH"
    "yMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwD3+iig"
    "gD//2Q=="
)


def _make_screenshot(utterance: str, filepath: str) -> ScreenshotEntry:
    """Create a screenshot entry with a pre-set filepath (skips disk I/O)."""
    return ScreenshotEntry(
        b64=_TINY_JPEG_B64,
        utterance=utterance,
        timestamp=datetime.now(timezone.utc),
        source="user",
        filepath=filepath,
    )


def _assert_context_setting_phase(result, description: str) -> None:
    """Context-setting may optionally open a persist session; act is allowed."""
    actor_events = filter_events_by_type(result.output_events, ActorHandleStarted)
    if not actor_events:
        return
    for event in actor_events:
        assert event.action_name == "act", (
            f"Context-setting should only trigger act(), not other tools — "
            f"{description}. Got: {event.action_name}"
        )


def _teaching_query_text(result, cm) -> str:
    """Best-effort query text from act dispatch or persist-session interject."""
    actor_events = filter_events_by_type(result.output_events, ActorHandleStarted)
    if actor_events:
        return actor_events[0].query
    if cm.cm.in_flight_actions:
        handle_data = list(cm.cm.in_flight_actions.values())[-1]
        for action in reversed(handle_data.get("handle_actions", [])):
            name = action.get("action_name", "")
            if name.startswith("interject_") or name == "act":
                return action.get("query") or action.get("message") or ""
        return handle_data.get("query", "")
    replies = filter_events_by_type(result.output_events, UnifyMessageSent)
    return " ".join(e.content for e in replies)


def _assert_teaching_step_handled(result, cm, description: str) -> str:
    """Teaching may start act or continue an open persist session via interject."""
    actor_events = filter_events_by_type(result.output_events, ActorHandleStarted)
    handled = (
        len(actor_events) >= 1
        or "act" in cm.all_tool_calls
        or has_steering_tool_call(cm, "interject_")
    )
    assert handled, (
        f"{description}. Expected act or interject_ steering. "
        f"Tool calls: {cm.all_tool_calls}"
    )
    return _teaching_query_text(result, cm)


def _assert_persist_true(cm, description: str) -> None:
    """Assert that the most recent act() call used persist=True."""
    actions = cm.cm.in_flight_actions
    assert actions, f"No in-flight actions found — {description}"
    last_action = list(actions.values())[-1]
    assert last_action.get("persist") is True, (
        f"Expected persist=True, got persist={last_action.get('persist')}. "
        f"{description}"
    )


def _get_act_query(result) -> str:
    """Extract the query string from the ActorHandleStarted event."""
    actor_events = filter_events_by_type(result.output_events, ActorHandleStarted)
    assert actor_events, "No ActorHandleStarted event found"
    return actor_events[0].query


def _has_act(result) -> bool:
    """Check whether an act() call was made during this step."""
    return len(filter_events_by_type(result.output_events, ActorHandleStarted)) > 0


# ---------------------------------------------------------------------------
#  1. Explicit "remember this" with facts → one-shot act to store
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@_handle_project
async def test_explicit_remember_facts_triggers_act(initialized_cm):
    """User explicitly asks the assistant to remember important facts.

    "Remember this, it's important: {facts}" should trigger act to store
    the information — even though the user didn't say "save to knowledge"
    or "store this guidance". The intent to persist is clear from context.
    """
    cm = initialized_cm

    result = await cm.step_until_wait(
        UnifyMessageReceived(
            contact=BOSS,
            content=(
                "Remember this, it's important: our API rate limit is 100 "
                "requests per minute for the standard tier and 1000 for premium. "
                "The rate limit resets every 60 seconds."
            ),
        ),
    )

    assert_act_triggered(
        result,
        ActorHandleStarted,
        "'Remember this' should trigger act to store the facts",
        cm=cm,
    )
    query = _get_act_query(result)
    query_lower = query.lower()
    assert (
        "rate limit" in query_lower or "100" in query_lower
    ), f"Expected act query to reference the rate limit facts. Got: {query}"
    assert_efficient(result, 3)


# ---------------------------------------------------------------------------
#  2. Detailed single-message explanation → act to store is desirable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@_handle_project
async def test_detailed_procedural_explanation_handled(initialized_cm):
    """User gives a detailed procedural explanation in one message.

    Without an explicit "remember this", the LLM may either proactively
    store the procedure via act, or just acknowledge it. Both outcomes
    are acceptable. If it does call act, the query should capture the
    procedural content.
    """
    cm = initialized_cm

    result = await cm.step_until_wait(
        UnifyMessageReceived(
            contact=BOSS,
            content=(
                "So here's how the quarterly reporting works: first you pull "
                "the data from our analytics dashboard, then you cross-reference "
                "it with the finance team's numbers, and the tricky part is the "
                "currency conversion — you have to use the exchange rate from the "
                "first business day of the quarter, not the current rate."
            ),
        ),
    )

    if _has_act(result):
        query = _get_act_query(result)
        query_lower = query.lower()
        assert "quarterly" in query_lower or "reporting" in query_lower, (
            f"Act was triggered but query doesn't capture the procedure. "
            f"Got: {query}"
        )
    assert_efficient(result, 3)


# ---------------------------------------------------------------------------
#  4. Screen share demonstration → act to store visual guidance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@_handle_project
async def test_visual_demonstration_triggers_guidance_storage(initialized_cm):
    """User demonstrates a process on screen and explains it verbally.

    A detailed visual demonstration with verbal explanation should trigger
    act to store as guidance. The query should reference the screenshot
    filepaths so the Actor can process the visual content.
    """
    cm = initialized_cm
    cm.cm.user_screen_share_active = True

    # Phase 1: Context-setting — may start a persist session or just acknowledge.
    result1 = await cm.step_until_wait(
        UnifyMessageReceived(
            contact=BOSS,
            content="Let me show you how our admin panel works.",
        ),
    )
    if _has_act(result1):
        _assert_persist_true(cm, "Screen-share context may start a persist session")

    # Phase 2: Detailed visual explanation with screenshot
    cm.cm._screenshot_buffer.append(
        _make_screenshot(
            utterance="See this section here? Whenever a customer calls about billing...",
            filepath="Screenshots/User/admin-panel-billing.jpg",
        ),
    )
    result2 = await cm.step_until_wait(
        UnifyMessageReceived(
            contact=BOSS,
            content=(
                "See this section here? Whenever a customer calls about a "
                "billing issue, you always check this Override section first, "
                "then click the Transactions tab to see the full history."
            ),
        ),
    )

    query = _assert_teaching_step_handled(
        result2,
        cm,
        "Visual demonstration should trigger act or interject to store guidance",
    )
    query_lower = query.lower()
    assert "billing" in query_lower or "override" in query_lower, (
        f"Expected act query to reference the billing/override procedure. "
        f"Got: {query}"
    )
    assert "screenshot" in query_lower or "screen" in query_lower, (
        f"Expected act query to reference the screenshot/screen context. "
        f"Got: {query}"
    )
    assert_efficient(result2, 3)


# ---------------------------------------------------------------------------
#  5. Screen share teaching with accumulated screenshots → act references paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@_handle_project
async def test_screen_share_teaching_references_screenshot_paths(initialized_cm):
    """Visual tutorial should produce an act query referencing screenshot filepaths.

    When the user teaches via screen sharing across multiple messages, the
    eventual act call should reference the accumulated screenshot filepaths
    so the Actor can process and store the visual content as guidance.
    """
    cm = initialized_cm
    cm.cm.user_screen_share_active = True

    # Phase 1: Context — may start a persist session or just acknowledge.
    result1 = await cm.step_until_wait(
        UnifyMessageReceived(
            contact=BOSS,
            content="I want to show you how our inventory system works.",
        ),
    )
    if _has_act(result1):
        _assert_persist_true(cm, "Screen-share context may start a persist session")

    # Phase 2: Teaching with screenshot
    cm.cm._screenshot_buffer.append(
        _make_screenshot(
            utterance="This is the main inventory dashboard",
            filepath="Screenshots/User/inventory-dashboard.jpg",
        ),
    )
    result2 = await cm.step_until_wait(
        UnifyMessageReceived(
            contact=BOSS,
            content=(
                "This is the main inventory dashboard. When stock drops below "
                "the red line, you need to trigger a reorder. Make sure you "
                "remember this process — it's critical."
            ),
        ),
    )

    query = _assert_teaching_step_handled(
        result2,
        cm,
        "Visual teaching with 'remember this' should trigger act or interject",
    )
    query_lower = query.lower()
    assert (
        "inventory" in query_lower or "reorder" in query_lower
    ), f"Expected act query to capture inventory procedure. Got: {query}"
    assert "screenshot" in query_lower or "screen" in query_lower, (
        f"Expected act query to reference screenshot/screen visual context. "
        f"Got: {query}"
    )
    assert_efficient(result2, 3)


# ---------------------------------------------------------------------------
#  7. Act query must include the specific screenshot filepath
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@_handle_project
async def test_act_query_includes_screenshot_filepath(initialized_cm):
    """The act query must include the exact screenshot filepath.

    The CodeActActor has no other way to locate screenshot files — the CM
    must pass the filepaths explicitly in the freeform act query text.
    This test verifies the specific filepath (not just "screenshot") appears
    in the query so the Actor can load the image from disk.
    """
    cm = initialized_cm
    cm.cm.user_screen_share_active = True

    filepath = "Screenshots/User/crm-contact-list.jpg"

    cm.cm._screenshot_buffer.append(
        _make_screenshot(
            utterance="This is the contact list page",
            filepath=filepath,
        ),
    )
    result = await cm.step_until_wait(
        UnifyMessageReceived(
            contact=BOSS,
            content=(
                "This is the CRM contact list. Remember this: when a client "
                "calls, you pull up their record here and check the Notes column "
                "before transferring them."
            ),
        ),
    )

    assert_act_triggered(
        result,
        ActorHandleStarted,
        "Visual instruction should trigger act",
        cm=cm,
    )
    query = _get_act_query(result)
    assert filepath in query, (
        f"Expected the exact screenshot filepath '{filepath}' in the act "
        f"query so the CodeActActor can locate the image. Got: {query}"
    )
    assert_efficient(result, 3)
