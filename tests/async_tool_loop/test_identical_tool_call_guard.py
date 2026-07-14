"""Cross-turn identical tool-call guard."""

from __future__ import annotations

import pytest

from unify.common._async_tool.loop import (
    ToolLoopRuntimeState,
    is_identical_tool_call_exempt,
    record_completed_tool_signature,
    should_refuse_identical_tool_call,
    tool_call_signature,
)
from unify.common._async_tool import loop as _loop
from unify.common.async_tool_loop import start_async_tool_loop
from unify.common.llm_client import new_llm_client


def test_tool_call_signature_is_order_independent():
    a = tool_call_signature("ask", {"text": "Who is Alice?", "limit": 10})
    b = tool_call_signature("ask", {"limit": 10, "text": "Who is Alice?"})
    assert a == b
    assert a[0] == "ask"


def test_nested_ask_is_not_exempt_but_dynamic_ask_helpers_are():
    assert not is_identical_tool_call_exempt("ask")
    assert is_identical_tool_call_exempt("ask_call123")
    assert is_identical_tool_call_exempt("check_status_call123")
    assert is_identical_tool_call_exempt("request_clarification")


def test_consecutive_identical_tool_call_is_refused_after_one_completion():
    state = ToolLoopRuntimeState()
    sig = tool_call_signature("ask", {"text": "Find all Alices"})

    assert should_refuse_identical_tool_call(state, sig) is None
    record_completed_tool_signature(state, sig)

    refusal = should_refuse_identical_tool_call(state, sig)
    assert refusal is not None
    assert "just completed with identical" in refusal


def test_identical_ask_allowed_again_after_intervening_different_tool():
    state = ToolLoopRuntimeState()
    ask_sig = tool_call_signature("ask", {"text": "Find all Alices"})
    update_sig = tool_call_signature("update_contact", {"contact_id": 7})

    record_completed_tool_signature(state, ask_sig)
    assert should_refuse_identical_tool_call(state, ask_sig) is not None

    record_completed_tool_signature(state, update_sig)
    assert should_refuse_identical_tool_call(state, ask_sig) is None


def test_total_identical_cap_refuses_even_with_intervening_tools():
    state = ToolLoopRuntimeState()
    ask_sig = tool_call_signature("ask", {"text": "Find all Alices"})

    for i in range(3):
        assert should_refuse_identical_tool_call(state, ask_sig) is None
        record_completed_tool_signature(state, ask_sig)
        record_completed_tool_signature(
            state,
            tool_call_signature("update_contact", {"contact_id": i + 1}),
        )

    refusal = should_refuse_identical_tool_call(state, ask_sig)
    assert refusal is not None
    assert "already executed 3 time(s)" in refusal


@pytest.mark.asyncio
async def test_loop_refuses_repeated_identical_tool_calls(llm_config, monkeypatch):
    """Fake LLM keeps requesting the same tool; only the first execution runs."""
    client = new_llm_client(**llm_config)
    counter = {"n": 0}
    llm_turns = {"n": 0}

    async def short_tool(value: str = "x"):
        counter["n"] += 1
        return f"ok-{counter['n']}:{value}"

    async def _fake_gwp(_client, preprocess_msgs, **gen_kwargs):
        llm_turns["n"] += 1
        if llm_turns["n"] <= 6:
            _client.messages.append(
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": f"call_repeat_{llm_turns['n']}",
                            "type": "function",
                            "function": {
                                "name": "short_tool",
                                "arguments": '{"value":"same"}',
                            },
                        },
                    ],
                },
            )
            return {"ok": True}
        _client.messages.append(
            {"role": "assistant", "content": "done", "tool_calls": None},
        )
        return {"ok": True}

    monkeypatch.setattr(_loop, "generate_with_preprocess", _fake_gwp, raising=True)

    handle = start_async_tool_loop(
        client,
        message="call short_tool",
        tools={"short_tool": short_tool},
        timeout=30,
        max_steps=20,
    )
    result = await handle.result()

    assert counter["n"] == 1, "identical repeats must not re-execute the tool"
    assert isinstance(result, str)
    refused = [
        m
        for m in client.messages
        if m.get("role") == "tool" and "Refused:" in str(m.get("content") or "")
    ]
    assert refused, "expected identical-call refusal tool messages"
    assert "Terminating early" in result or result.strip() == "done"
