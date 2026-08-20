"""Unit tests for the byte-stable tools schema (steer() dispatcher).

Covers the core invariant: the outer loop's tools array stops being a
rendering of live per-call state. These are symbolic tests of
DynamicToolFactory/ToolsData directly — no LLM calls — mirroring
test_serialization_determinism.py and test_append_only_transcript.py's style.

These tests exercise DynamicToolFactory/ToolsData in isolation and cannot
observe the tools array loop.py actually assembles and sends
(`tmp_tools`/`gen_kwargs["tools"]`) — see
tests/async_tool_loop/test_tools_bytes_loop_level.py for the test that
drives a real `start_async_tool_loop` with a scripted fake client and
asserts on that assembled array directly.
"""

from __future__ import annotations

import json

from unify.common._async_tool.dynamic_tools_factory import DynamicToolFactory
from unify.common._async_tool.tools_data import ToolsData
from unify.common._async_tool.tools_utils import ToolCallMetadata
from unify.common.tool_spec import ToolSpec
from unify.common.llm_helpers import method_to_schema

# ---------------------------------------------------------------------------
# Fakes — enough surface for ToolsData without a real LLM client
# ---------------------------------------------------------------------------


class _FakeHandle:
    """Minimal SteerableToolHandle-shaped stand-in."""

    def __init__(self, *, interjectable=True, paused=False):
        self._interjectable = interjectable
        self._paused = paused
        if interjectable:

            async def interject(self, content=None, message=None):
                return None

            self.interject = interject.__get__(self)

        async def pause(self):
            self._paused = True

        async def resume(self):
            self._paused = False

        self.pause = pause.__get__(self)
        self.resume = resume.__get__(self)

        async def ask(self, question=None):
            return f"answer to {question}"

        self.ask = ask.__get__(self)

    def _pause_event(self):
        return None


def _make_tools_data() -> ToolsData:
    return ToolsData(tools={}, client=None, logger=None)


class _FakeTask:
    """Minimal asyncio.Task stand-in — just enough surface (``done()``) for
    ToolsData.resolve_call_id's liveness check, without a real event loop."""

    def done(self) -> bool:
        return False


def _add_pending(
    tools_data: ToolsData,
    *,
    call_id: str,
    name: str = "sub_agent_act",
    handle=None,
    waiting_for_clarification=False,
    call_idx: int = 0,
) -> object:
    task = _FakeTask()
    tools_data.info[task] = ToolCallMetadata(
        name=name,
        call_id=call_id,
        call_dict={"function": {"arguments": "{}"}},
        call_idx=call_idx,
        chat_context=None,
        assistant_msg={},
        is_interjectable=(handle is not None and hasattr(handle, "interject")),
        tool_schema={},
        llm_arguments={},
        raw_arguments_json="{}",
        handle=handle,
        clar_up_queue=(object() if waiting_for_clarification else None),
        clar_down_queue=(object() if waiting_for_clarification else None),
        waiting_for_clarification=waiting_for_clarification,
    )
    tools_data.pending.add(task)
    return task


def _dynamic_schema_bytes(tools_data: ToolsData) -> str:
    factory = DynamicToolFactory(tools_data)
    factory.generate()
    schemas = [
        method_to_schema(fn, include_class_name=False)
        for fn in factory.dynamic_tools.values()
    ]
    return json.dumps(schemas, sort_keys=True)


# ---------------------------------------------------------------------------
# 1. The static surface — wait / steer / ask_about_completed_tool — is
#    byte-identical across every state DynamicToolFactory.generate() can see.
# ---------------------------------------------------------------------------


def test_static_surface_present_with_nothing_in_flight():
    tools_data = _make_tools_data()
    factory = DynamicToolFactory(tools_data)
    factory.generate()
    assert set(factory.dynamic_tools.keys()) == {
        "wait",
        "steer",
        "ask_about_completed_tool",
    }


def test_dynamic_schema_byte_identical_across_pending_completed_and_paused_states():
    baseline_data = _make_tools_data()
    baseline_bytes = _dynamic_schema_bytes(baseline_data)

    # One plain (handle-less) pending base tool call.
    plain_data = _make_tools_data()
    _add_pending(plain_data, call_id="call_1")
    assert _dynamic_schema_bytes(plain_data) == baseline_bytes

    # One handle-bearing, interjectable, currently-running pending call.
    running_data = _make_tools_data()
    _add_pending(
        running_data,
        call_id="call_2",
        handle=_FakeHandle(interjectable=True, paused=False),
    )
    assert _dynamic_schema_bytes(running_data) == baseline_bytes

    # Same, but paused.
    paused_data = _make_tools_data()
    _add_pending(
        paused_data,
        call_id="call_3",
        handle=_FakeHandle(interjectable=True, paused=True),
    )
    assert _dynamic_schema_bytes(paused_data) == baseline_bytes

    # A completed, retrospectively-askable tool present.
    completed_data = _make_tools_data()
    completed_data._completed_askable_tools["call_4"] = {
        "name": "sub_agent_act",
        "call_id": "call_4",
        "arg_repr": "request='...'",
        "handle": _FakeHandle(),
    }
    assert _dynamic_schema_bytes(completed_data) == baseline_bytes

    # A pending clarification in flight.
    clarifying_data = _make_tools_data()
    _add_pending(
        clarifying_data,
        call_id="call_5",
        handle=_FakeHandle(),
        waiting_for_clarification=True,
    )
    assert _dynamic_schema_bytes(clarifying_data) == baseline_bytes


def test_ask_about_completed_tool_docstring_is_frozen_regardless_of_backlog():
    empty_data = _make_tools_data()
    empty_factory = DynamicToolFactory(empty_data)
    empty_factory.generate()
    empty_doc = empty_factory.dynamic_tools["ask_about_completed_tool"].__doc__

    busy_data = _make_tools_data()
    for i in range(5):
        busy_data._completed_askable_tools[f"call_{i}"] = {
            "name": f"tool_{i}",
            "call_id": f"call_{i}",
            "arg_repr": "x=1",
            "handle": _FakeHandle(),
        }
    busy_factory = DynamicToolFactory(busy_data)
    busy_factory.generate()
    busy_doc = busy_factory.dynamic_tools["ask_about_completed_tool"].__doc__

    # Unlike the pre-steer() docstring (regenerated with a live listing on
    # every completion), this is now a constant string — no live listing
    # embedded, regardless of how many tools have completed.
    assert empty_doc == busy_doc
    for i in range(5):
        assert f"call_{i}" not in busy_doc


# ---------------------------------------------------------------------------
# 2. Concurrency/quota saturation no longer removes a tool from the schema —
#    the base-tool schema is a plain, unfiltered pass over tools_data.normalized
#    (mirrors the list comprehension in loop.py's non-eager branch); saturation
#    is enforced separately, at execution time.
# ---------------------------------------------------------------------------


def _saturated_tools_data() -> ToolsData:
    def busy_tool(x: int) -> int:
        return x

    tools_data = ToolsData(
        tools={
            "busy_tool": ToolSpec(
                fn=busy_tool,
                max_concurrent=1,
                max_total_calls=1,
            ),
        },
        client=None,
        logger=None,
    )
    _add_pending(tools_data, call_id="in_flight", name="busy_tool")
    tools_data.call_counts["busy_tool"] = 1
    return tools_data


def test_base_tool_schema_construction_ignores_concurrency_and_quota_state():
    tools_data = _saturated_tools_data()

    # Saturated by both concurrency and quota...
    assert tools_data.concurrency_ok("busy_tool") is False
    assert tools_data.quota_ok("busy_tool") is False

    # ...yet the unfiltered schema construction (the shape loop.py now uses
    # for visible_base_tools_schema) still includes it: no `if concurrency_ok
    # and quota_ok` gate survives in the schema path.
    schema_names = {
        name
        for name, spec in tools_data.normalized.items()
        for _ in [method_to_schema(spec.fn, name)]
    }
    assert "busy_tool" in schema_names


def test_saturation_is_still_enforced_at_execution_time():
    tools_data = _saturated_tools_data()

    # Schema visibility is unconditional (above); the cap still bites when
    # the model actually tries to call it — enforcement moved, it didn't
    # disappear.
    assert tools_data.has_exceeded_concurrent_limit_for_tool("busy_tool") is True
    assert tools_data.has_exceeded_quota_for_tool("busy_tool") is True


# ---------------------------------------------------------------------------
# 3. resolve_call_id — exact match, no truncated-suffix ambiguity.
# ---------------------------------------------------------------------------


def test_resolve_call_id_is_exact_match_not_suffix():
    tools_data = _make_tools_data()
    task = _add_pending(tools_data, call_id="call_abcdef123456")

    found_task, found_info = tools_data.resolve_call_id("call_abcdef123456")
    assert found_task is task
    assert found_info.call_id == "call_abcdef123456"

    # A mere suffix match must NOT resolve — this is the exact ambiguity
    # the old `name.split("_")[-1]` / `.endswith(suffix)` matching risked.
    missing_task, missing_info = tools_data.resolve_call_id("123456")
    assert missing_task is None
    assert missing_info is None
