"""Tests for prose-only slow-brain completion healing."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from openai.types.chat import ChatCompletion
from openai.types.chat.chat_completion import Choice
from openai.types.chat.chat_completion_message import ChatCompletionMessage

from unify.conversation_manager.cm_types.medium import Medium
from unify.conversation_manager.conversation_manager import ConversationManager
from unify.conversation_manager.domains.prose_send_healing import (
    _email_subject_from_prose,
    _prose_is_healable,
    _resolve_send_tool_name,
    build_slow_brain_completion_mutator,
)
from unillm.clients.completion_mutator import CompletionMutatorContext


def _completion(
    content: str | None,
    *,
    tool_calls=None,
    finish_reason="stop",
) -> ChatCompletion:
    message = ChatCompletionMessage(
        role="assistant",
        content=content,
        tool_calls=tool_calls,
    )
    choice = Choice(index=0, message=message, finish_reason=finish_reason)
    return ChatCompletion(
        id="test-id",
        choices=[choice],
        created=1234567890,
        model="minimax-v3",
        object="chat.completion",
    )


def _cm_with_inbound(
    *,
    medium: Medium,
    contact_id: int = 42,
    is_voice: bool = False,
    extra_context: dict | None = None,
) -> ConversationManager:
    cm = object.__new__(ConversationManager)
    cm.mode = SimpleNamespace(is_voice=is_voice)
    context = {
        "medium": medium.value,
        "contact_id": contact_id,
    }
    if extra_context:
        context.update(extra_context)
    cm._last_inbound_reply_context = context
    return cm


def _mutate(
    cm: ConversationManager,
    completion: ChatCompletion,
    *,
    origin_event_name: str = "UnifyMessageReceived",
    available_tool_names: set[str] | None = None,
    tool_choice: str = "required",
):
    mutator = build_slow_brain_completion_mutator(
        cm,
        trace_meta={"origin_event_name": origin_event_name},
        available_tool_names=available_tool_names
        or {
            "send_unify_message",
            "send_unify_message_to_boss",
            "send_email",
            "send_email_to_boss",
            "wait",
        },
    )
    context = CompletionMutatorContext(
        provider="minimax",
        original_tool_choice=tool_choice,
        request_kw={
            "messages": [{"role": "user", "content": "What next?"}],
            "tools": [{"type": "function", "function": {"name": "wait"}}],
            "tool_choice": tool_choice,
        },
    )
    return mutator(completion, context)


def test_tw1n_prose_heals_to_send_unify_message():
    cm = _cm_with_inbound(medium=Medium.UNIFY_MESSAGE, contact_id=7326)
    completion = _completion("Check your inbox for the verification email.")
    healed = _mutate(
        cm,
        completion,
        available_tool_names={"send_unify_message", "wait"},
    )

    tool_call = healed.choices[0].message.tool_calls[0]
    if hasattr(tool_call, "model_dump"):
        tool_call = tool_call.model_dump(warnings=False)
    assert tool_call["function"]["name"] == "send_unify_message"
    assert json.loads(tool_call["function"]["arguments"]) == {
        "content": "Check your inbox for the verification email.",
        "contact_id": 7326,
    }
    assert healed.choices[0].message.content is None


def test_abstains_on_email_sent_origin():
    cm = _cm_with_inbound(medium=Medium.UNIFY_MESSAGE)
    completion = _completion("Check your inbox for the verification email.")
    result = _mutate(
        cm,
        completion,
        origin_event_name="EmailSent",
        available_tool_names={"send_unify_message", "wait"},
    )
    assert result.choices[0].message.tool_calls is None
    assert result.choices[0].message.content == completion.choices[0].message.content


def test_abstains_when_tool_calls_already_set():
    cm = _cm_with_inbound(medium=Medium.UNIFY_MESSAGE)
    completion = _completion(
        None,
        tool_calls=[
            {
                "id": "call_existing",
                "type": "function",
                "function": {
                    "name": "wait",
                    "arguments": "{}",
                },
            },
        ],
        finish_reason="tool_calls",
    )
    result = _mutate(
        cm,
        completion,
        available_tool_names={"send_unify_message", "wait"},
    )
    assert result is completion


def test_abstains_on_json_content():
    cm = _cm_with_inbound(medium=Medium.UNIFY_MESSAGE)
    completion = _completion('{"content": "hello"}')
    result = _mutate(
        cm,
        completion,
        available_tool_names={"send_unify_message", "wait"},
    )
    assert result.choices[0].message.tool_calls is None


def test_email_subject_heuristic():
    assert _email_subject_from_prose("Thanks! I sent the code.") == "Thanks!"
    long_sentence = "A" * 90 + "."
    assert len(_email_subject_from_prose(long_sentence)) <= 80


def test_email_heal_uses_subject_and_body():
    cm = _cm_with_inbound(
        medium=Medium.EMAIL,
        contact_id=5,
        extra_context={"email_id": "msg-1", "thread_id": "thread-1"},
    )
    completion = _completion("Thanks! I sent the code.")
    healed = _mutate(
        cm,
        completion,
        available_tool_names={"send_email", "wait"},
    )
    tool_call = healed.choices[0].message.tool_calls[0]
    if hasattr(tool_call, "model_dump"):
        tool_call = tool_call.model_dump(warnings=False)
    assert tool_call["function"]["name"] == "send_email"
    args = json.loads(tool_call["function"]["arguments"])
    assert args["body"] == "Thanks! I sent the code."
    assert args["subject"] == "Thanks!"
    assert args["reply_all"] is True
    assert args["email_id_to_reply_to"] == "msg-1"
    assert args["thread_id"] == "thread-1"


@patch("unify.conversation_manager.domains.prose_send_healing.SESSION_DETAILS")
def test_coordinator_maps_to_boss_tool(mock_session_details):
    mock_session_details.is_coordinator = True
    cm = _cm_with_inbound(medium=Medium.UNIFY_MESSAGE, contact_id=1)
    assert _resolve_send_tool_name(Medium.UNIFY_MESSAGE, is_coordinator=True) == (
        "send_unify_message_to_boss"
    )
    completion = _completion("Next, open billing.")
    healed = _mutate(
        cm,
        completion,
        available_tool_names={"send_unify_message_to_boss", "wait"},
    )
    tool_call = healed.choices[0].message.tool_calls[0]
    if hasattr(tool_call, "model_dump"):
        tool_call = tool_call.model_dump(warnings=False)
    assert tool_call["function"]["name"] == "send_unify_message_to_boss"


def test_prose_heuristics():
    assert _prose_is_healable("Hello there.") is True
    assert _prose_is_healable('{"a": 1}') is False
    assert _prose_is_healable("I need to think about this.") is False


@pytest.mark.asyncio
async def test_single_shot_passes_mutator_to_generate():
    from unify.common.single_shot import single_shot_tool_decision

    captured: dict[str, object] = {}

    async def fake_generate(**kwargs):
        captured.update(kwargs)
        client.messages.append(
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "call_mutator_0",
                        "type": "function",
                        "function": {
                            "name": "send_unify_message",
                            "arguments": json.dumps(
                                {"content": "Check your inbox.", "contact_id": 1},
                            ),
                        },
                    },
                ],
            },
        )

    client = MagicMock()
    client.messages = [{"role": "user", "content": "hi"}]
    client.generate = AsyncMock(side_effect=fake_generate)

    async def send_unify_message(content: str, contact_id: int):
        return None

    mutator = MagicMock()
    await single_shot_tool_decision(
        client,
        "hi",
        {"send_unify_message": send_unify_message},
        tool_choice="required",
        completion_mutator=mutator,
    )

    assert captured["completion_mutator"] is mutator
    client.generate.assert_awaited_once()
