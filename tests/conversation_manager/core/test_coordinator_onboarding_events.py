from __future__ import annotations

from unity.conversation_manager.domains import coordinator_onboarding as onboarding
from unity.conversation_manager.prompt_builders import (
    _voice_next_onboarding_suggestion,
)


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


def test_voice_next_onboarding_suggestion_ignores_done_and_skipped_steps() -> None:
    suggestion = _voice_next_onboarding_suggestion(
        completed_steps=["workspace"],
        skipped_steps=["apps"],
    )

    assert "one-off job" in suggestion
