"""Unit tests for Orchestra EventBus persist allowlisting."""

from __future__ import annotations

from unify.events.persist_filters import (
    parse_persist_tools,
    should_persist_to_orchestra,
)
from unify.events.types.tool_loop import ToolLoopKind


def test_parse_persist_tools_default_when_empty():
    assert parse_persist_tools("") == frozenset({"execute_code", "execute_function"})
    assert parse_persist_tools(None) == frozenset({"execute_code", "execute_function"})


def test_parse_persist_tools_custom():
    assert parse_persist_tools("execute_code, other_tool ") == frozenset(
        {"execute_code", "other_tool"},
    )


def test_mode_all_persists_everything():
    assert should_persist_to_orchestra(
        "LLM",
        {"model": "x"},
        mode="all",
        tools=frozenset({"execute_code"}),
    )
    assert should_persist_to_orchestra(
        "ManagerMethod",
        {"method": "ask", "manager": "ContactManager"},
        mode="all",
        tools=frozenset({"execute_code"}),
    )


def test_allowlist_manager_method_execute_tools_only():
    tools = frozenset({"execute_code", "execute_function"})
    assert should_persist_to_orchestra(
        "ManagerMethod",
        {"method": "execute_code", "manager": "CodeActActor"},
        mode="allowlist",
        tools=tools,
    )
    assert should_persist_to_orchestra(
        "ManagerMethod",
        {"method": "execute_function", "manager": "CodeActActor"},
        mode="allowlist",
        tools=tools,
    )
    assert not should_persist_to_orchestra(
        "ManagerMethod",
        {"method": "ask", "manager": "ContactManager"},
        mode="allowlist",
        tools=tools,
    )
    assert not should_persist_to_orchestra(
        "ManagerMethod",
        {"method": "act", "manager": "CodeActActor"},
        mode="allowlist",
        tools=tools,
    )


def test_allowlist_tool_loop_tool_result_and_tool_call():
    tools = frozenset({"execute_code", "execute_function"})
    assert should_persist_to_orchestra(
        "ToolLoop",
        {
            "kind": ToolLoopKind.TOOL_RESULT.value,
            "message": {"role": "tool", "name": "execute_code", "content": "ok"},
        },
        mode="allowlist",
        tools=tools,
    )
    assert should_persist_to_orchestra(
        "ToolLoop",
        {
            "kind": ToolLoopKind.TOOL_CALL.value,
            "message": {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "1",
                        "function": {"name": "notify", "arguments": "{}"},
                    },
                    {
                        "id": "2",
                        "function": {"name": "execute_function", "arguments": "{}"},
                    },
                ],
            },
        },
        mode="allowlist",
        tools=tools,
    )
    assert not should_persist_to_orchestra(
        "ToolLoop",
        {
            "kind": ToolLoopKind.TOOL_RESULT.value,
            "message": {"role": "tool", "name": "notify", "content": "ok"},
        },
        mode="allowlist",
        tools=tools,
    )
    assert not should_persist_to_orchestra(
        "ToolLoop",
        {
            "kind": ToolLoopKind.THOUGHT.value,
            "message": {"role": "assistant", "content": "thinking"},
        },
        mode="allowlist",
        tools=tools,
    )


def test_allowlist_drops_other_event_types():
    tools = frozenset({"execute_code"})
    assert not should_persist_to_orchestra(
        "LLM",
        {"model": "gpt"},
        mode="allowlist",
        tools=tools,
    )
    assert not should_persist_to_orchestra(
        "Message",
        {"content": "hi"},
        mode="allowlist",
        tools=tools,
    )
