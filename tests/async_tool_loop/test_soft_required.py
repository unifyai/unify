"""Tests for the ``llm_soft_required`` dispatch-boundary backfill.

These drive the real ``ToolsData.schedule_base_tool_call`` path that the async
tool loop uses to invoke base tools. They lock the contract that a tool may
advertise an argument as required in its schema (so the model is nudged to
supply it) while still tolerating a model omission at runtime — without ever
raising ``TypeError`` and aborting the trajectory.

The only doubles used are the LLM transcript container (``client.messages``)
and the logger, both unavoidable boundaries; the dispatch, kwarg
normalization, and backfill all run as in production.
"""

from __future__ import annotations

import json

import pytest

from unity.common._async_tool.context_tracker import LoopContextState
from unity.common._async_tool.tools_data import ToolsData
from unity.common.async_tool_loop import ChatContextPropagation
from unity.common.tool_spec import llm_soft_required


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


async def _run_single_tool_call(tools_data: ToolsData, *, name: str, args_json: str):
    """Schedule one base tool call and await its underlying task to completion."""
    asst_msg = {"role": "assistant", "content": None, "tool_calls": []}
    await tools_data.schedule_base_tool_call(
        asst_msg,
        name=name,
        args_json=args_json,
        call_id="call_1",
        call_idx=0,
        context_state=LoopContextState(),
        propagate_chat_context=ChatContextPropagation.NEVER,
        assistant_meta={},
    )
    task = next(iter(tools_data.pending))
    return await task


@pytest.mark.asyncio
async def test_missing_soft_required_arg_is_backfilled_and_does_not_crash():
    """When the model omits a soft-required arg, dispatch backfills the default
    and the tool runs cleanly instead of raising TypeError."""
    received: dict = {}

    @llm_soft_required(thought="")
    async def code_like(thought: str, code: str):
        received["thought"] = thought
        received["code"] = code
        return "ok"

    tools_data = _new_tools_data({"code_like": code_like})
    # `thought` is REQUIRED in the schema but omitted by the model here.
    result = await _run_single_tool_call(
        tools_data,
        name="code_like",
        args_json=json.dumps({"code": "print(1)"}),
    )

    assert result == "ok"
    assert received["thought"] == ""  # backfilled at the dispatch boundary
    assert received["code"] == "print(1)"


@pytest.mark.asyncio
async def test_provided_soft_required_arg_is_not_overwritten():
    """A value the model did supply is passed through untouched."""
    received: dict = {}

    @llm_soft_required(thought="")
    async def code_like(thought: str, code: str):
        received["thought"] = thought
        return "ok"

    tools_data = _new_tools_data({"code_like": code_like})
    await _run_single_tool_call(
        tools_data,
        name="code_like",
        args_json=json.dumps({"thought": "querying metrics", "code": "print(1)"}),
    )

    assert received["thought"] == "querying metrics"


@pytest.mark.asyncio
async def test_unmarked_required_arg_still_errors_when_omitted():
    """A genuinely functional required arg on an UNMARKED tool is not silently
    backfilled — its omission still surfaces as an error the model can
    self-correct against."""

    async def strict_tool(function_name: str, code: str):
        return f"ran {function_name}"

    tools_data = _new_tools_data({"strict_tool": strict_tool})
    with pytest.raises(TypeError):
        await _run_single_tool_call(
            tools_data,
            name="strict_tool",
            args_json=json.dumps({"code": "print(1)"}),  # function_name omitted
        )
