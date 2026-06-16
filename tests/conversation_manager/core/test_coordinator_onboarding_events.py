from __future__ import annotations

from unity.conversation_manager.domains import coordinator_onboarding as onboarding
from unity.conversation_manager.prompt_builders import (
    _build_coordinator_voice_opening_block,
    _voice_next_onboarding_suggestion,
)

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


def test_voice_next_onboarding_suggestion_ignores_done_and_skipped_steps() -> None:
    suggestion = _voice_next_onboarding_suggestion(
        completed_steps=[*COMMS_STEP_IDS, "workspace"],
        skipped_steps=["apps"],
    )

    assert "one-off job" in suggestion


def test_voice_opening_block_includes_active_phone_call_guidance() -> None:
    block = _build_coordinator_voice_opening_block(
        completed_onboarding_steps=[],
        skipped_onboarding_steps=[],
        active_onboarding_step="phone-call",
    )

    assert "Active onboarding step: phone call" in block
    assert "tin can far above the world" in block
