"""Unit tests for the coordinator onboarding reset narration.

``_coordinator_onboarding_notification_text`` is a pure function that turns
one ``CoordinatorOnboardingEvent`` into the brain-facing instruction string.
The reset subtype is the one that must tell the brain a step is no longer
done so a later nudge cannot claim it complete.
"""

from __future__ import annotations

from unify.conversation_manager.domains.coordinator_onboarding import (
    _coordinator_onboarding_notification_text,
)
from unify.conversation_manager.events import CoordinatorOnboardingEvent


def test_reset_notification_marks_step_not_done_and_names_it() -> None:
    event = CoordinatorOnboardingEvent(
        subtype="onboarding_step_reset",
        message="User reset the 'workspace-mailbox' onboarding step.",
        details={"step_id": "workspace-mailbox"},
    )
    text = _coordinator_onboarding_notification_text(event)
    assert "onboarding_step_reset" in text
    assert "`workspace-mailbox`" in text
    # The brain must not treat the step as done and must not spontaneously act.
    assert "no longer complete" in text
    assert "never claim it is done" in text
    assert "live" in text


def test_workspace_demo_notification_requires_full_task_then_explicit_completion() -> (
    None
):
    event = CoordinatorOnboardingEvent(
        subtype="workspace_demo_requested",
        message="The user just clicked 'Ask T-W1N to summarize your mailbox'.",
        details={"step_id": "workspace-mailbox"},
    )
    text = _coordinator_onboarding_notification_text(event)
    assert "workspace_demo_requested" in text
    assert "`workspace-mailbox`" in text
    # The brain delivers the summary and then completes the step explicitly —
    # the checklist does not auto-detect the summary, and the completion call is
    # what finishes the demo.
    assert "set_onboarding_task_state" in text
    assert "does NOT" in text
    assert "not finished until" in text
    assert "after acking that I am on it" in text
    assert "Mandatory:" in text
    assert "checklist click has no visible UI feedback" in text


def test_integration_demo_notification_requires_explicit_completion() -> None:
    event = CoordinatorOnboardingEvent(
        subtype="integration_demo_chip_requested",
        message="The user picked an integration demo chip.",
        details={
            "step_id": "integration-read",
            "instruction": "Pull the latest from one of my connected apps and brief me here",
        },
    )
    text = _coordinator_onboarding_notification_text(event)
    assert "integration_demo_chip_requested" in text
    assert "`integration-read`" in text
    assert "connected integration/app tools" in text
    assert "set_onboarding_task_state" in text
    assert "does NOT auto-detect" in text
    assert "Mandatory:" in text


def test_integration_connect_chip_does_not_complete_apps() -> None:
    event = CoordinatorOnboardingEvent(
        subtype="integration_connect_chip_requested",
        message="The user picked a CRM connect suggestion.",
        details={
            "instruction": "Connect a CRM or sales tool — if you use one",
            "gallery_category": "crm_sales",
            "search_query": "crm sales hubspot pipedrive",
        },
    )
    text = _coordinator_onboarding_notification_text(event)
    assert "integration_connect_chip_requested" in text
    assert "crm_sales" in text
    assert "crm sales hubspot pipedrive" in text
    assert "do not mark `apps` complete" in text
    assert "actual integration credential lands" in text


def test_step_completed_notification_requires_no_action() -> None:
    event = CoordinatorOnboardingEvent(
        subtype="onboarding_step_completed",
        message="The 'workspace-mailbox' onboarding step is now complete.",
        details={"step_id": "workspace-mailbox"},
    )
    text = _coordinator_onboarding_notification_text(event)
    assert "onboarding_step_completed" in text
    assert "`workspace-mailbox`" in text
    # This confirms the brain's own completion; it must not prompt a second turn.
    assert "No action needed now" in text
    assert "Mandatory:" not in text


def test_reset_notification_omits_trigger_ack_suffix() -> None:
    event = CoordinatorOnboardingEvent(
        subtype="onboarding_step_reset",
        message="User reset the 'workspace-mailbox' onboarding step.",
        details={"step_id": "workspace-mailbox"},
    )
    text = _coordinator_onboarding_notification_text(event)
    assert "Mandatory:" not in text


def test_session_started_chat_notification_pending_scripted_delivery() -> None:
    event = CoordinatorOnboardingEvent(
        subtype="onboarding_session_started",
        message="User just opened the onboarding chat with you.",
        details={"medium": "chat"},
    )
    text = _coordinator_onboarding_notification_text(event)
    assert "onboarding_session_started" in text
    assert "already delivered" not in text.lower()
    assert "already in the transcript" not in text.lower()
    assert "scheduled for delivery" in text.lower()
    assert "Mandatory:" not in text


def test_reference_quiz_notification_stays_minimal() -> None:
    event = CoordinatorOnboardingEvent(
        subtype="reference_quiz_clue_requested",
        message="User clicked the email reference-quiz trigger.",
        details={
            "channel": "email",
            "tool_name": "send_email",
            "trigger_step_id": "email-reference",
            "reply_step_id": "email-reply",
            "interaction": {"type": "reference_quiz"},
        },
    )
    text = _coordinator_onboarding_notification_text(event)
    assert "reference_quiz_clue_requested" in text
    assert "sci-fi quote clue" in text
    assert "never list genres or franchises" in text
    assert "clicked the trigger row" in text
    assert "Mandatory:" in text
    assert "Star Wars" not in text
    assert "pop-culture" not in text
