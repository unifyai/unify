"""
tests/async_tool_loop/test_ask_inflight_status.py
==================================================

``handle.ask()`` snapshots the inspected loop's transcript, which dead-ends
silently while an LLM request is in flight — inspectors misread the pause as a
stall. ``generate_with_preprocess`` stamps ``client._llm_inflight_since`` for
the duration of each request, and ``ask()`` surfaces it as an explicit STATUS
entry in the inspection system message. These tests pin both halves.
"""

import asyncio
import time

import pytest

from unify.common import async_tool_loop as atl
from unify.common._async_tool.messages import generate_with_preprocess


class _DummyLoopClient:
    endpoint = "openai/gpt-4o-mini@openrouter"

    def __init__(self) -> None:
        self.messages = [
            {"role": "user", "content": "start"},
            {"role": "assistant", "content": "working"},
        ]


def _make_handle(client) -> atl.AsyncToolLoopHandle:
    async def _outer_done() -> str:
        return "outer-complete"

    task = asyncio.create_task(_outer_done(), name="InflightStatusDummyTask")
    setattr(task, "get_ask_tools", lambda: {})
    return atl.AsyncToolLoopHandle(
        task=task,
        interject_queue=asyncio.Queue(),
        cancel_event=asyncio.Event(),
        stop_event=asyncio.Event(),
        client=client,
        loop_id="InflightStatusLoop",
    )


class _DummyInspectionHandle:
    async def result(self):
        return "inspection-complete"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
@pytest.mark.parametrize("inflight", [True, False])
async def test_ask_marks_inflight_llm_request(monkeypatch, inflight):
    """ask() appends a STATUS entry iff an LLM request is in flight."""
    captured: dict = {}

    def _fake_inspection_start_async_tool_loop(*args, **kwargs):
        captured["client"] = args[0]
        return _DummyInspectionHandle()

    monkeypatch.setattr(
        atl,
        "start_async_tool_loop",
        _fake_inspection_start_async_tool_loop,
    )

    client = _DummyLoopClient()
    if inflight:
        client._llm_inflight_since = time.time() - 42.0

    handle = _make_handle(client)
    helper = await handle.ask("What is taking so long?")
    await helper.result()

    sys_msg = captured["client"].system_message
    marker = (
        "STATUS: the inspected loop is currently waiting on an in-flight LLM request"
    )
    if inflight:
        assert marker in sys_msg, "in-flight request must be surfaced to the inspector"
    else:
        assert marker not in sys_msg, "no marker expected when nothing is in flight"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_generate_with_preprocess_stamps_inflight_window():
    """The stamp is set during generate and always cleared — even on cancel."""

    class _Client:
        messages: list = []
        seen = None

        async def generate(self, **kw):
            self.seen = self._llm_inflight_since
            return "resp"

    c = _Client()
    assert await generate_with_preprocess(c, None) == "resp"
    assert c.seen is not None
    assert c._llm_inflight_since is None

    class _Hang(_Client):
        async def generate(self, **kw):
            await asyncio.sleep(30)

    h = _Hang()
    t = asyncio.create_task(generate_with_preprocess(h, None))
    await asyncio.sleep(0.05)
    assert h._llm_inflight_since is not None
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert h._llm_inflight_since is None
