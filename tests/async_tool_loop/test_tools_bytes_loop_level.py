"""Loop-level tools-bytes test (T4-6 fixup).

The unit tests in test_tools_bytes_stability.py exercise DynamicToolFactory/
ToolsData directly and can never observe the tools array loop.py actually
assembles (`tmp_tools`, what becomes `gen_kwargs["tools"]`) — which is
exactly where T4-4 (the response tool masking on pending<->idle) slipped
through review. This file drives a real `start_async_tool_loop` with a fully
scripted fake in place of `generate_with_preprocess` (no LLM calls, no
network) and captures the tools array on every dispatch via a spy, the way
the reviewer specified.

Scenario, one scripted assistant turn at a time: spawn a steerable tool ->
attempt the response tool while it's pending (refused) -> recover with an
unrelated action (not a retry) -> pause -> resume -> clarify -> saturate a
max_concurrent=1 tool -> exhaust a max_total_calls=1 tool's quota -> release
everything -> the response tool finally succeeds. The tools array is
asserted byte-identical across every single one of these dispatches.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from pydantic import BaseModel, Field

from unify.common.async_tool_loop import start_async_tool_loop
from unify.common.tool_spec import ToolSpec
from unify.common.llm_client import new_llm_client
from tests.helpers import _handle_project
from tests.async_helpers import (
    _wait_for_tool_request,
    _wait_for_tool_message_prefix,
    _wait_for_assistant_steer_action,
    _wait_for_condition,
)


class _Answer(BaseModel):
    answer: str = Field(..., description="The final answer.")


def _tool_call(call_id: str, name: str, arguments: dict) -> dict:
    return {
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": json.dumps(arguments)},
    }


def _asst_msg(calls: list[dict]) -> dict:
    return {"role": "assistant", "content": "", "tool_calls": calls}


@pytest.mark.asyncio
@pytest.mark.timeout(60)
@_handle_project
async def test_tools_array_byte_identical_across_full_scripted_act(
    llm_config,
    monkeypatch,
) -> None:
    client = new_llm_client(**llm_config)
    client.set_system_message("This turn is fully scripted by the test.")

    from unify.common._async_tool import loop as _loop

    turn_queue: asyncio.Queue = asyncio.Queue()
    captured_tools: list[str] = []

    async def _fake_gwp(_client, _preprocess_msgs, **gen_kwargs):
        tools = gen_kwargs.get("tools") or []
        captured_tools.append(json.dumps(tools, sort_keys=True))
        next_msg = await turn_queue.get()
        _client.messages.append(next_msg)
        return {"ok": True}

    monkeypatch.setattr(_loop, "generate_with_preprocess", _fake_gwp, raising=True)

    # ── tools ─────────────────────────────────────────────────────────────
    slow_release = asyncio.Event()
    clarify_trigger = asyncio.Event()

    async def slow_tool(
        *,
        _pause_event: asyncio.Event,
        _clarification_up_q: asyncio.Queue,
        _clarification_down_q: asyncio.Queue,
    ) -> str:
        await _pause_event.wait()
        await clarify_trigger.wait()
        await _clarification_up_q.put("which color?")
        await _clarification_down_q.get()
        await slow_release.wait()
        return "slow-done"

    slow_tool.__name__ = "slow_tool"
    slow_tool.__qualname__ = "slow_tool"

    capped_release = asyncio.Event()

    async def capped_tool() -> str:
        await capped_release.wait()
        return "capped-done"

    capped_tool.__name__ = "capped_tool"
    capped_tool.__qualname__ = "capped_tool"

    async def quota_tool() -> str:
        return "quota-done"

    quota_tool.__name__ = "quota_tool"
    quota_tool.__qualname__ = "quota_tool"

    handle = start_async_tool_loop(
        client=client,
        message="start",
        tools={
            "slow_tool": slow_tool,
            "capped_tool": ToolSpec(fn=capped_tool, max_concurrent=1),
            "quota_tool": ToolSpec(fn=quota_tool, max_total_calls=1),
        },
        response_format=_Answer,
        max_steps=40,
        timeout=120,
    )

    # `interrupt_llm_with_interjections=True` (the default) only re-issues
    # generate_with_preprocess once nothing is pending or something wakes the
    # loop (interjection, clarification answer, tool completion). With
    # slow_tool gated pending across almost this entire scenario, most
    # subsequent scripted turns below need an explicit interject to grant a
    # new LLM step, mirroring how every live e2e test in this suite drives
    # steering turns against a real model.
    #
    # Always push before nudging: `turn_queue` is unbounded, so `put()`
    # never blocks and is always safe to call first.
    #
    # Nudge only if nothing drains the queue on its own within a short grace
    # period. Some steps (clarification requests, tool completions such as
    # capped_tool/quota_tool finishing) already auto-wake the loop, so a
    # dispatch may already be racing to consume this turn. Sending our own
    # interject() concurrently with that in-flight auto-woken dispatch races
    # `interject_w` against `llm_task` inside the loop's
    # `interrupt_llm_with_interjections` select — and on that race, the
    # `if interject_w in done: ... continue` branch discards the
    # (already-complete) `llm_task` result unconditionally instead of
    # processing it, silently dropping the assistant's tool call. Waiting
    # for the queue to drain before nudging sidesteps the race entirely
    # without needing to know in advance which steps auto-wake.
    async def _next_turn(msg: dict, *, nudge: bool = True) -> None:
        await turn_queue.put(msg)
        if not nudge:
            return
        for _ in range(10):
            if turn_queue.qsize() == 0:
                return
            await asyncio.sleep(0.05)
        await handle.interject("continue")

    # 1) spawn slow_tool (pending) ------------------------------------------
    await _next_turn(
        _asst_msg([_tool_call("call_slow", "slow_tool", {})]),
        nudge=False,
    )
    await _wait_for_tool_request(client, "slow_tool")

    # 2) response tool attempted while pending -> refused --------------------
    await _next_turn(
        _asst_msg([_tool_call("call_fr1", "final_response", {"answer": "too early"})]),
    )
    await _wait_for_tool_message_prefix(client, "final_response")
    refusal_msg = next(
        m
        for m in client.messages
        if m.get("role") == "tool" and m.get("tool_call_id") == "call_fr1"
    )
    assert "still running" in refusal_msg["content"]
    assert "wait" in refusal_msg["content"]
    assert "steer" in refusal_msg["content"]

    # 3) refuse -> recover: the NEXT action is something else, not a retry ---
    await _next_turn(
        _asst_msg(
            [
                _tool_call(
                    "call_pause1",
                    "steer",
                    {
                        "call_id": "call_slow",
                        "action": "pause",
                    },
                ),
            ],
        ),
    )
    await _wait_for_assistant_steer_action(client, "pause")
    await _wait_for_tool_message_prefix(client, "steer:pause ")

    # 4) resume ---------------------------------------------------------------
    await _next_turn(
        _asst_msg(
            [
                _tool_call(
                    "call_resume1",
                    "steer",
                    {
                        "call_id": "call_slow",
                        "action": "resume",
                    },
                ),
            ],
        ),
    )
    await _wait_for_assistant_steer_action(client, "resume")
    await _wait_for_tool_message_prefix(client, "steer:resume ")

    # 5) clarification: trigger it, then answer via steer ---------------------
    clarify_trigger.set()

    # The clarification tail message is a user-role message, not a tool
    # message — wait on it directly instead of the tool-message helper.
    async def _clarification_seen() -> bool:
        return any(
            m.get("role") == "user"
            and str(m.get("content") or "").startswith("[clarification ")
            for m in client.messages
        )

    await _wait_for_condition(_clarification_seen, poll=0.05, timeout=30.0)

    await _next_turn(
        _asst_msg(
            [
                _tool_call(
                    "call_clarify1",
                    "steer",
                    {
                        "call_id": "call_slow",
                        "action": "clarify",
                        "payload": "blue",
                    },
                ),
            ],
        ),
    )
    await _wait_for_assistant_steer_action(client, "clarify")
    await _wait_for_tool_message_prefix(client, "steer:clarify ")

    # 6) saturate capped_tool (max_concurrent=1) -------------------------------
    await _next_turn(_asst_msg([_tool_call("call_capped1", "capped_tool", {})]))
    await _wait_for_tool_request(client, "capped_tool")

    await _next_turn(_asst_msg([_tool_call("call_capped2", "capped_tool", {})]))

    async def _saturation_refused() -> bool:
        return any(
            m.get("role") == "tool"
            and m.get("tool_call_id") == "call_capped2"
            and "max_concurrent" in str(m.get("content") or "")
            for m in client.messages
        )

    await _wait_for_condition(_saturation_refused, poll=0.05, timeout=30.0)
    capped_release.set()

    # 7) exhaust quota_tool (max_total_calls=1) --------------------------------
    await _next_turn(_asst_msg([_tool_call("call_quota1", "quota_tool", {})]))
    await _wait_for_tool_request(client, "quota_tool")

    async def _quota_tool_done() -> bool:
        return any(
            m.get("role") == "tool"
            and m.get("name") == "quota_tool"
            and "quota-done" in str(m.get("content") or "")
            for m in client.messages
        )

    await _wait_for_condition(_quota_tool_done, poll=0.05, timeout=30.0)

    await _next_turn(_asst_msg([_tool_call("call_quota2", "quota_tool", {})]))
    # Over-quota calls are pruned before dispatch (no reply for call_quota2
    # specifically is guaranteed), so just wait for the loop to move past it
    # by observing the next thing we push get consumed below.

    # 8) release the last pending call and finish -------------------------------
    slow_release.set()
    await _wait_for_tool_message_prefix(client, "slow_tool")

    await _next_turn(
        _asst_msg([_tool_call("call_fr2", "final_response", {"answer": "done"})]),
    )
    final = await asyncio.wait_for(handle.result(), timeout=60)
    assert final is not None

    # ── The core assertion: every captured tools array is byte-identical ──
    assert len(captured_tools) >= 8, (
        f"expected at least 8 dispatches to have been observed, got "
        f"{len(captured_tools)}"
    )
    baseline = captured_tools[0]
    for i, snapshot in enumerate(captured_tools):
        assert snapshot == baseline, (
            f"tools array changed at dispatch #{i} — schema is not "
            "byte-identical across spawn/finish/pause/resume/clarify/"
            "saturation/quota/response-tool transitions"
        )
