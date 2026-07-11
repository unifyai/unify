"""Actor parent-context filtering for CM-only tool surfaces."""

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


def test_filter_cm_state_strips_completed_action_steering_tools() -> None:
    snapshot = {
        "content": (
            "<completed_actions>\n"
            "<action id='3' short_name='search_web' status='completed' type='act'>\n"
            "<original_request>Search the web for X</original_request>\n"
            "<result>Found three sources.</result>\n"
            "<history>\n"
            "  - act_completed: Found three sources.\n"
            "</history>\n"
            "<steering_tools>\n"
            "  - ask_search_web_3: Ask about this completed action\n"
            "</steering_tools>\n"
            "</action>\n"
            "</completed_actions>\n"
            "<active_conversations>\n"
            "User: please continue\n"
            "</active_conversations>"
        ),
    }
    filtered = _filter_cm_state_for_actor(snapshot)
    content = filtered["content"]
    assert "<steering_tools>" not in content
    assert "ask_search_web_3" not in content
    assert "<result>Found three sources.</result>" in content
    assert "<original_request>Search the web for X</original_request>" in content
    assert "User: please continue" in content


def test_filter_cm_state_strips_steering_tools_in_multimodal_parts() -> None:
    snapshot = {
        "content": [
            {
                "type": "text",
                "text": (
                    "<completed_actions>\n"
                    "<steering_tools>\n"
                    "  - ask_done_1: Ask about this completed action\n"
                    "</steering_tools>\n"
                    "</completed_actions>"
                ),
            },
            {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,abc"}},
        ],
    }
    filtered = _filter_cm_state_for_actor(snapshot)
    text_part = filtered["content"][0]
    assert "<steering_tools>" not in text_part["text"]
    assert "ask_done_1" not in text_part["text"]
    assert filtered["content"][1]["type"] == "image_url"
