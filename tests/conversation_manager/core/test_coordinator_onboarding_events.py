from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from unity.conversation_manager.domains import coordinator_onboarding as onboarding
from unity.conversation_manager.prompt_builders import (
    _build_coordinator_onboarding_narration_block,
    _build_coordinator_voice_opening_block,
    _voice_next_onboarding_suggestion,
)
from unity.settings import SETTINGS

COMMS_STEP_IDS = [
    "email-reply",
    "whatsapp-number",
    "whatsapp-message",
    "whatsapp-call",
    "phone-number",
    "sms-message",
    "phone-call",
    "slack-connect",
    "slack-message",
    "discord-connect",
    "discord-message",
]


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
    assert "done or explicitly skipped" in text


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
                "clue": 'The clue is: "Phone home."',
                "quote": "Phone home.",
                "answer": "Battlestar Galactica",
            },
        },
        message="User triggered a reference clue.",
    )

    assert event is not None
    text = onboarding._coordinator_onboarding_notification_text(event)

    assert "guess the reference" in text
    assert "send_slack_message" in text
    assert "Phone home." in text
    assert "Do not reveal" in text
    assert "Battlestar Galactica" in text


def test_reference_quiz_notification_briefs_call_context() -> None:
    event = onboarding._coordinator_onboarding_event_from_payload(
        {
            "subtype": "reference_quiz_clue_requested",
            "details": {
                "trigger_step_id": "phone-call-reference",
                "reply_step_id": "phone-call",
                "channel": "phone_call",
                "clue": 'The clue is: "To infinity and beyond!"',
                "quote": "To infinity and beyond!",
                "answer": "The Empire Strikes Back / Luke",
            },
        },
        message="User triggered a phone clue.",
    )

    assert event is not None
    text = onboarding._coordinator_onboarding_notification_text(event)

    assert "make_call_to_boss" in text
    assert "`context` argument" in text
    assert "repeat" in text
    assert "hints" in text
    assert "The Empire Strikes Back / Luke" in text


def test_voice_next_onboarding_suggestion_ignores_done_and_skipped_steps() -> None:
    suggestion = _voice_next_onboarding_suggestion(
        completed_steps=[*COMMS_STEP_IDS, "workspace"],
        skipped_steps=["apps"],
    )

    assert "one-off job" in suggestion


def test_onboarding_narration_block_documents_reference_quiz_not_space_oddity_scripts() -> (
    None
):
    block = _build_coordinator_onboarding_narration_block()

    assert "reference_quiz_clue_requested" in block
    assert "guess-the-reference" in block
    assert "make_call_to_boss" in block
    assert "Ground Control to Major" not in block
    assert "tin can far above the world" not in block


def test_voice_opening_block_includes_active_phone_call_guidance() -> None:
    block = _build_coordinator_voice_opening_block(
        completed_onboarding_steps=[],
        skipped_onboarding_steps=[],
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
    monkeypatch.setattr(SETTINGS, "UNITY_CONSOLE_UI", False)
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
    monkeypatch.setattr(SETTINGS, "UNITY_CONSOLE_UI", True)
    cm = MagicMock()

    result = await onboarding._handle_coordinator_onboarding_event(
        _onboarding_event(),
        cm,
    )

    assert result is True
    cm.notifications_bar.push_notif.assert_called_once()
