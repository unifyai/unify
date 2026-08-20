"""Unit tests for transcript_ops.extract_clarifications against the coalesced
[clarification <call_id>] tail-message shape.
"""

from __future__ import annotations

from unify.common._async_tool.transcript_ops import extract_clarifications


def test_extracts_call_id_and_question_from_coalesced_tail_message():
    msgs = [
        {"role": "user", "content": "Please email jonathan.smith@example.com"},
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "c1",
                    "type": "function",
                    "function": {"name": "send_email", "arguments": "{}"},
                },
            ],
        },
        {
            "role": "tool",
            "tool_call_id": "c1",
            "name": "send_email",
            "content": '{"_placeholder": "pending", "meta:status": "async — result arrives as a check_status message below"}',
        },
        {
            "role": "user",
            "content": (
                "[clarification c1] Tool incomplete, please answer the "
                "following to continue tool execution:\n"
                "Will you be bringing anything (food/drink)?"
            ),
        },
    ]

    result = extract_clarifications(msgs, callid_to_tool_name={"c1": "send_email"})

    assert result == [
        {
            "call_id": "c1",
            "tool": "send_email",
            "question": "Will you be bringing anything (food/drink)?",
        },
    ]


def test_last_message_per_call_id_wins():
    msgs = [
        {
            "role": "user",
            "content": "[clarification c1] Tool incomplete, please answer the following to continue tool execution:\nFirst question?",
        },
        {
            "role": "user",
            "content": "[clarification c1] Tool incomplete, please answer the following to continue tool execution:\nSecond question?",
        },
    ]

    result = extract_clarifications(msgs)

    assert len(result) == 1
    assert result[0]["question"] == "Second question?"


def test_ignores_non_clarification_user_messages_and_missing_tool_name():
    msgs = [
        {"role": "user", "content": "just a normal interjection"},
        {"role": "user", "content": "[progress c1] some status update"},
        {"role": "assistant", "content": "not a user message"},
        {
            "role": "user",
            "content": "[clarification c2] Tool incomplete, please answer the following to continue tool execution:\nWhich color?",
        },
    ]

    result = extract_clarifications(msgs)

    assert result == [{"call_id": "c2", "tool": "", "question": "Which color?"}]


def test_no_clarifications_returns_empty_list():
    msgs = [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "hi there"},
    ]

    assert extract_clarifications(msgs) == []
