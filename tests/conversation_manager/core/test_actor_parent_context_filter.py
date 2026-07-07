"""Actor parent-context filtering for onboarding completion instructions."""

from __future__ import annotations

from unify.conversation_manager.domains.brain_action_tools import (
    _filter_cm_state_for_actor,
)


def test_filter_cm_state_strips_onboarding_completion_tool_lines() -> None:
    snapshot = {
        "content": (
            "[notification] integration_demo_chip_requested\n"
            "After the brief, call set_onboarding_task_state(step_id, True).\n"
            "Use connected app tools now."
        ),
    }
    filtered = _filter_cm_state_for_actor(snapshot)
    assert "set_onboarding_task_state" not in filtered["content"]
    assert "Use connected app tools now." in filtered["content"]
