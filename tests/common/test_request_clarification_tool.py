"""Deterministic contracts for top-level ``request_clarification`` queue routing.

Locks the fix where CodeAct's top-level clarification tool must accept
per-call hidden queues so ``ToolsData`` registers a ``clarification_channels``
entry and the async tool loop can surface questions on ``handle._clar_q``.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from unify.common._async_tool.context_tracker import LoopContextState
from unify.common._async_tool.tools_data import ToolsData
from unify.common.async_tool_loop import ChatContextPropagation
from unify.common.llm_helpers import make_request_clarification_tool, method_to_schema


class _DummyLogger:
    log_steps = False

    def info(self, *a, **kw): ...
    def error(self, *a, **kw): ...
    def debug(self, *a, **kw): ...


class _DummyClient:
    def __init__(self):
        self.messages = []


def _new_tools_data(tools: dict) -> ToolsData:
    return ToolsData(tools, client=_DummyClient(), logger=_DummyLogger())


@pytest.mark.asyncio
async def test_hidden_queues_roundtrip_when_outer_queues_are_none():
    """Outer (None, None) falls back to loop-injected per-call queues."""
    fn = make_request_clarification_tool(None, None)
    up: asyncio.Queue[str] = asyncio.Queue()
    down: asyncio.Queue[str] = asyncio.Queue()

    task = asyncio.create_task(
        fn(
            "Which owner/repo?",
            _clarification_up_q=up,
            _clarification_down_q=down,
        ),
    )
    assert await asyncio.wait_for(up.get(), timeout=1) == "Which owner/repo?"
    await down.put("acme/triggers-test-repo")
    assert await asyncio.wait_for(task, timeout=1) == "acme/triggers-test-repo"


@pytest.mark.asyncio
async def test_outer_queues_win_when_both_channels_present():
    """Nested manager bridges keep writing to the parent/outer channel."""
    outer_up: asyncio.Queue[str] = asyncio.Queue()
    outer_down: asyncio.Queue[str] = asyncio.Queue()
    hidden_up: asyncio.Queue[str] = asyncio.Queue()
    hidden_down: asyncio.Queue[str] = asyncio.Queue()
    fn = make_request_clarification_tool(outer_up, outer_down)

    task = asyncio.create_task(
        fn(
            "Which John?",
            _clarification_up_q=hidden_up,
            _clarification_down_q=hidden_down,
        ),
    )
    assert await asyncio.wait_for(outer_up.get(), timeout=1) == "Which John?"
    assert hidden_up.empty()
    await outer_down.put("John Smith")
    assert await asyncio.wait_for(task, timeout=1) == "John Smith"
    assert hidden_down.empty()


@pytest.mark.asyncio
async def test_tools_data_registers_per_call_channel_for_top_level_clarification():
    """Scheduling top-level request_clarification wires mailbox A end-to-end.

    This is the production dispatch path CodeAct uses when it injects the tool
    with ``clarification_queues=(None, None)``: ToolsData must create per-call
    queues, register ``clarification_channels``, and the tool must round-trip
    through those queues (so loop clar_waiters / answer_clarification can work).
    """
    fn = make_request_clarification_tool(None, None)
    tools_data = _new_tools_data({"request_clarification": fn})
    asst_msg = {"role": "assistant", "content": None, "tool_calls": []}

    await tools_data.schedule_base_tool_call(
        asst_msg,
        name="request_clarification",
        args_json=json.dumps({"question": "Which owner/repo?"}),
        call_id="clar_call_1",
        call_idx=0,
        context_state=LoopContextState(),
        propagate_chat_context=ChatContextPropagation.NEVER,
        assistant_meta={},
    )

    assert len(tools_data.pending) == 1
    task = next(iter(tools_data.pending))
    info = tools_data.info[task]
    assert info.clar_up_queue is not None
    assert info.clar_down_queue is not None
    assert tools_data.clarification_channels["clar_call_1"] == (
        info.clar_up_queue,
        info.clar_down_queue,
    )

    async def _answer() -> None:
        question = await asyncio.wait_for(info.clar_up_queue.get(), timeout=1)
        assert question == "Which owner/repo?"
        await info.clar_down_queue.put("acme/triggers-test-repo")

    answerer = asyncio.create_task(_answer())
    result = await asyncio.wait_for(task, timeout=1)
    await answerer
    assert result == "acme/triggers-test-repo"


def test_hidden_clarification_params_are_stripped_from_llm_schema():
    fn = make_request_clarification_tool(None, None)
    schema = method_to_schema(fn, "request_clarification")
    props = schema["function"]["parameters"]["properties"]
    assert "question" in props
    assert "_clarification_up_q" not in props
    assert "_clarification_down_q" not in props
