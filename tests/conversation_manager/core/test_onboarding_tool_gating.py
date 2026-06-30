"""Unit tests for reference-quiz comms tool gating during onboarding.

``masked_reference_quiz_tools`` is a pure, fail-open function: it returns the
set of send-tool names to withhold given the onboarding render and the set of
trigger-step ids clicked this session.
"""

from __future__ import annotations

from unify.conversation_manager.domains.onboarding_tool_gating import (
    masked_reference_quiz_tools,
)


def _trigger_step(step_id: str, tool_name: str, status: str) -> dict:
    return {
        "id": step_id,
        "kind": "trigger",
        "status": status,
        "interaction": {"type": "reference_quiz", "tool_name": tool_name},
    }


def _render(*steps: dict) -> dict:
    return {"steps": list(steps)}


def test_incomplete_unclicked_trigger_is_masked():
    render = _render(_trigger_step("email-reference", "send_email", "available"))
    assert masked_reference_quiz_tools(render, set()) == {"send_email"}


def test_locked_trigger_is_masked():
    render = _render(_trigger_step("whatsapp-message", "send_whatsapp", "locked"))
    assert masked_reference_quiz_tools(render, set()) == {"send_whatsapp"}


def test_done_trigger_is_unmasked():
    render = _render(_trigger_step("email-reference", "send_email", "done"))
    assert masked_reference_quiz_tools(render, set()) == set()


def test_skipped_trigger_is_unmasked():
    render = _render(_trigger_step("email-reference", "send_email", "skipped"))
    assert masked_reference_quiz_tools(render, set()) == set()


def test_clicked_this_session_is_unmasked():
    render = _render(_trigger_step("email-reference", "send_email", "available"))
    assert masked_reference_quiz_tools(render, {"email-reference"}) == set()


def test_call_tool_names_are_normalized_to_unity_names():
    render = _render(
        _trigger_step("whatsapp-call", "make_whatsapp_call_to_boss", "available"),
        _trigger_step("phone-call", "make_call_to_boss", "available"),
    )
    assert masked_reference_quiz_tools(render, set()) == {
        "make_whatsapp_call",
        "make_call",
    }


def test_multiple_channels_gated_independently():
    render = _render(
        _trigger_step("email-reference", "send_email", "done"),
        _trigger_step("whatsapp-message", "send_whatsapp", "available"),
        _trigger_step("sms-message", "send_sms", "available"),
    )
    # email done -> unmasked; whatsapp clicked -> unmasked; sms -> masked.
    assert masked_reference_quiz_tools(render, {"whatsapp-message"}) == {"send_sms"}


def test_non_trigger_and_non_quiz_steps_ignored():
    render = _render(
        {"id": "workspace", "kind": "setup", "status": "available"},
        {
            "id": "weird",
            "kind": "trigger",
            "status": "available",
            "interaction": {"type": "something_else", "tool_name": "send_email"},
        },
    )
    assert masked_reference_quiz_tools(render, set()) == set()


def test_none_render_masks_nothing():
    assert masked_reference_quiz_tools(None, set()) == set()
    assert masked_reference_quiz_tools(None, {"email-reference"}) == set()


def test_malformed_steps_fail_open():
    render = {"steps": [None, "nonsense", {"kind": "trigger"}, 42]}
    assert masked_reference_quiz_tools(render, set()) == set()


def test_missing_tool_name_is_not_masked():
    render = _render(
        {
            "id": "email-reference",
            "kind": "trigger",
            "status": "available",
            "interaction": {"type": "reference_quiz"},
        },
    )
    assert masked_reference_quiz_tools(render, set()) == set()
