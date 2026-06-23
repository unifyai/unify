from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from droid.conversation_manager.domains import coordinator_onboarding as onboarding
from droid.conversation_manager.prompt_builders import (
    _build_coordinator_onboarding_narration_block,
    _build_coordinator_voice_opening_block,
)
from droid.settings import SETTINGS


def test_session_started_notification_mentions_skipped_steps() -> None:
    event = onboarding._coordinator_onboarding_event_from_payload(
        {
            "subtype": "onboarding_session_started",
            "details": {
                "medium": "chat",
                "completed_step_ids": ["workspace"],
                "skipped_step_ids": ["apps"],
            },
        },
        message="User just opened onboarding.",
    )

    assert event is not None
    text = onboarding._coordinator_onboarding_notification_text(event)

    assert "workspace" in text
    assert "apps" in text
    assert "passed over for now, not done" in text
    assert "already done or skipped" in text


def test_step_skipped_notification_does_not_mark_step_done() -> None:
    event = onboarding._coordinator_onboarding_event_from_payload(
        {
            "subtype": "step_skipped",
            "details": {
                "step_id": "workspace",
                "skipped_step_ids": ["workspace"],
            },
        },
        message="User skipped the 'workspace' onboarding step.",
    )

    assert event is not None
    text = onboarding._coordinator_onboarding_notification_text(event)

    assert "leave that step for now" in text
    assert "Do not say the skipped step is complete" in text


def test_step_started_notification_names_active_comms_step() -> None:
    event = onboarding._coordinator_onboarding_event_from_payload(
        {
            "subtype": "onboarding_step_started",
            "details": {
                "step_id": "sms-message",
                "completed_step_ids": ["phone-number"],
            },
        },
        message="User started the 'sms-message' onboarding step.",
    )

    assert event is not None
    text = onboarding._coordinator_onboarding_notification_text(event)

    assert "active step id is `sms-message`" in text
    assert "according to the onboarding prompt rules" in text
    assert "Do not skip ahead to Connect or Delegate" in text


def test_reference_quiz_notification_briefs_text_channel() -> None:
    event = onboarding._coordinator_onboarding_event_from_payload(
        {
            "subtype": "reference_quiz_clue_requested",
            "details": {
                "game": "guess_the_reference",
                "trigger_step_id": "slack-reference",
                "reply_step_id": "slack-message",
                "channel": "slack_message",
                "tool_name": "send_slack_message",
                "clue": 'The clue is: "Phone home."',
                "quote": "Phone home.",
                "answer": "Battlestar Galactica",
                "framing": "Play a guess-the-reference mini-game.",
                "interaction": {"type": "reference_quiz"},
            },
        },
        message="User triggered a reference clue.",
    )

    assert event is not None
    text = onboarding._coordinator_onboarding_notification_text(event)

    assert "guess-the-reference mini-game" in text
    assert "send_slack_message" in text
    assert "Execute this trigger now" in text
    assert "Phone home." in text
    assert "Do not reveal" in text
    assert "Battlestar Galactica" in text
    assert "Explain the quiz before" in text
    assert "include that context before the clue" in text
    assert "acknowledgement alone does not satisfy the trigger" in text
    assert "outbound transcript row" in text


def test_reference_quiz_notification_briefs_call_context() -> None:
    event = onboarding._coordinator_onboarding_event_from_payload(
        {
            "subtype": "reference_quiz_clue_requested",
            "details": {
                "trigger_step_id": "phone-call-reference",
                "reply_step_id": "phone-call",
                "channel": "phone_call",
                "tool_name": "make_call_to_boss",
                "clue": 'The clue is: "To infinity and beyond!"',
                "quote": "To infinity and beyond!",
                "answer": "The Empire Strikes Back / Luke",
                "framing": "Play the reference game over a call.",
                "interaction": {"type": "reference_quiz"},
            },
        },
        message="User triggered a phone clue.",
    )

    assert event is not None
    text = onboarding._coordinator_onboarding_notification_text(event)

    assert "make_call_to_boss" in text
    assert "Execute this trigger now" in text
    assert "call context" in text
    assert "Play the reference game over a call." in text
    assert "The Empire Strikes Back / Luke" in text


def test_onboarding_narration_block_documents_reference_quiz_not_space_oddity_scripts() -> (
    None
):
    block = _build_coordinator_onboarding_narration_block()

    assert "reference_quiz_clue_requested" in block
    assert "task contract" in block
    assert "tool_name" in block
    assert "Use the supplied `tool_name` when present, in this same LLM turn" in block
    assert "Do not use `act` for the send" in block
    assert "do not send a bare clue" in block
    assert "Completion is detected only after my outbound message/call appears" in block
    assert "Do not hardcode onboarding game design here" in block
    assert "make_call_to_boss" not in block
    assert "Ground Control to Major" not in block
    assert "tin can far above the world" not in block


def test_voice_opening_block_gives_broader_first_orientation() -> None:
    block = _build_coordinator_voice_opening_block(
        next_targets=[
            {
                "id": "email-reference",
                "title": "Trigger email from Twin",
                "nudge_voice": "clicking Trigger email from Twin",
                "interaction": {"type": "reference_quiz"},
            },
            {
                "id": "workspace",
                "title": "Give me access to your workspace",
                "nudge_voice": "connecting their workspace",
            },
        ],
    )

    assert "meaningful onboarding orientation" in block
    assert "digital twin / stand-in" in block
    assert "communication channels" in block
    assert "recurring tasks" in block
    assert "computer use" in block
    assert "Pause onboarding for now" in block
    assert "clicking Trigger email from Twin" in block
    assert "overrides the generic Brevity/Opening rule" in block
    assert "explain the game design" in block
    assert "not a monologue I must finish" in block


def test_voice_opening_block_prevents_repeated_full_intro() -> None:
    block = _build_coordinator_voice_opening_block(
        next_targets=[
            {
                "id": "workspace",
                "title": "Give me access to your workspace",
                "nudge_voice": "connecting their workspace",
            },
        ],
    )

    assert "orientation has already happened" in block
    assert "Do NOT re-introduce myself" in block
    assert "repeat the onboarding overview" in block


def test_voice_opening_block_omits_onboarding_tour_without_next_targets() -> None:
    block = _build_coordinator_voice_opening_block(next_targets=[])

    assert "No valid onboarding next target was provided" in block
    assert "do not give the broad onboarding orientation" in block
    assert "Pause onboarding for now" not in block
    assert "communication channels" not in block


def test_voice_opening_block_includes_active_phone_call_guidance() -> None:
    block = _build_coordinator_voice_opening_block(
        next_targets=[],
        active_onboarding_step="phone-call",
    )

    assert "Active onboarding step: phone-call" in block
    assert "reference quiz trigger" in block
    assert "hardcoded reference text" in block


def _onboarding_event():
    event = onboarding._coordinator_onboarding_event_from_payload(
        {
            "subtype": "onboarding_step_started",
            "details": {"step_id": "sms-message"},
        },
        message="User started the 'sms-message' onboarding step.",
    )
    assert event is not None
    return event


@pytest.mark.asyncio
async def test_onboarding_handler_inert_without_console_ui(monkeypatch) -> None:
    """With no Console front-end, onboarding events are dropped entirely:
    no notification is pushed and no LLM run is requested."""
    monkeypatch.setattr(SETTINGS, "DROID_CONSOLE_UI", False)
    cm = MagicMock()

    result = await onboarding._handle_coordinator_onboarding_event(
        _onboarding_event(),
        cm,
    )

    assert result is False
    cm.notifications_bar.push_notif.assert_not_called()


@pytest.mark.asyncio
async def test_onboarding_handler_active_with_console_ui(monkeypatch) -> None:
    """With a Console present, the handler pushes a notification and asks for
    an LLM run (default behavior, unchanged)."""
    monkeypatch.setattr(SETTINGS, "DROID_CONSOLE_UI", True)
    cm = MagicMock()

    result = await onboarding._handle_coordinator_onboarding_event(
        _onboarding_event(),
        cm,
    )

    assert result is True
    cm.notifications_bar.push_notif.assert_called_once()
