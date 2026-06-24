from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from droid.conversation_manager.domains import coordinator_onboarding as onboarding
from droid.conversation_manager.prompt_builders import (
    _build_coordinator_onboarding_narration_block,
    _build_coordinator_voice_opening_block,
)
from droid.conversation_manager.events import EmailSent
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
    assert "clicking its row in the Onboarding checklist" in text


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
    assert "clicking its row in the Onboarding checklist" in text


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
    # The event is framed as a poll, and the clue is the model's to invent.
    assert "POLL" in text
    assert "do NOT send another" in text
    assert "invent my own" in text
    assert "Explain the quiz before" in text
    assert "include that context before the clue" in text


def test_reference_quiz_notification_briefs_call_context() -> None:
    event = onboarding._coordinator_onboarding_event_from_payload(
        {
            "subtype": "reference_quiz_clue_requested",
            "details": {
                "trigger_step_id": "phone-call-reference",
                "reply_step_id": "phone-call",
                "channel": "phone_call",
                "tool_name": "make_call_to_boss",
                "framing": "Play the reference game over a call.",
                "interaction": {"type": "reference_quiz"},
            },
        },
        message="User triggered a phone clue.",
    )

    assert event is not None
    text = onboarding._coordinator_onboarding_notification_text(event)

    assert "make_call_to_boss" in text
    assert "POLL" in text
    assert "call context" in text
    assert "Play the reference game over a call." in text


def test_onboarding_narration_block_documents_reference_quiz_not_space_oddity_scripts() -> (
    None
):
    block = _build_coordinator_onboarding_narration_block()

    assert "reference_quiz_clue_requested" in block
    assert "task contract" in block
    assert "tool_name" in block
    assert "POLL, not a fresh command" in block
    assert "the SAME directive in two" in block
    assert "I invent my own" in block
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
                "title": "Trigger email from T-W1N",
                "nudge_voice": "clicking Trigger email from T-W1N",
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
    assert "clicking Trigger email from T-W1N" in block
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


@pytest.mark.asyncio
async def test_reference_trigger_sets_pending_outbound_context(monkeypatch) -> None:
    monkeypatch.setattr(SETTINGS, "DROID_CONSOLE_UI", True)
    event = onboarding._coordinator_onboarding_event_from_payload(
        {
            "subtype": "reference_quiz_clue_requested",
            "details": {
                "trigger_step_id": "email-reference",
                "reply_step_id": "email-reply",
                "channel": "email",
                "tool_name": "send_email",
            },
        },
        message="User triggered an email clue.",
    )
    assert event is not None
    cm = MagicMock()
    cm._current_event_trace = {"event_id": "evt-1"}

    result = await onboarding._handle_coordinator_onboarding_event(event, cm)

    assert result is True
    cm.set_pending_onboarding_outbound.assert_called_once_with(
        event.details,
        origin_event_id="evt-1",
    )


def test_sent_event_serializes_onboarding_metadata() -> None:
    event = EmailSent(
        contact={"contact_id": 1},
        subject="Subject",
        body="Body",
        to=["dan@unify.ai"],
        onboarding_trigger_step_id="email-reference",
        onboarding_reply_step_id="email-reply",
        onboarding_request_id="llmreq-1",
        onboarding_origin_event_id="evt-1",
    )

    payload = event.to_dict()["payload"]

    assert payload["onboarding_trigger_step_id"] == "email-reference"
    assert payload["onboarding_reply_step_id"] == "email-reply"
    assert payload["onboarding_request_id"] == "llmreq-1"
    assert payload["onboarding_origin_event_id"] == "evt-1"
