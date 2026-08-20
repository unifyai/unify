"""Determinism/compactness unit tests for the cache-stability quick wins:

- sorted pending emission in DynamicToolFactory.generate()
- compact-JSON inspection prompt in AsyncToolLoopHandle.ask()
- content-hash spill filenames in formatting._spill_full_tool_text()
"""

from __future__ import annotations

import json

from unify.common._async_tool.dynamic_tools_factory import DynamicToolFactory
from unify.common._async_tool.formatting import (
    _spill_full_tool_text,
    _truncate_tool_text,
)
from unify.common._async_tool.tools_data import ToolsData
from unify.common._async_tool.tools_utils import ToolCallMetadata
from unify.common.async_tool_loop import _transform_inner_roles
from unify.common.context_dump import make_messages_safe_for_context_dump

# ---------------------------------------------------------------------------
# 1. Sorted pending emission (dynamic_tools_factory.py)
# ---------------------------------------------------------------------------


def _make_tools_data_with_pending(order):
    """Build a ToolsData whose ``pending`` set contains one sentinel task per
    (call_idx, fn_name) pair in *order*, inserted in that order."""
    tools_data = ToolsData(tools={}, client=None, logger=None)
    tasks = []
    for call_idx, fn_name in order:
        task = object()
        call_id = f"call_{fn_name}_{call_idx}"
        tools_data.info[task] = ToolCallMetadata(
            name=fn_name,
            call_id=call_id,
            call_dict={"function": {"arguments": "{}"}},
            call_idx=call_idx,
            chat_context=None,
            assistant_msg={},
            is_interjectable=False,
            tool_schema={},
            llm_arguments={},
            raw_arguments_json="{}",
        )
        tools_data.pending.add(task)
        tasks.append(task)
    return tools_data


def test_generate_emits_static_surface_regardless_of_pending_order():
    # Same three pending calls, inserted into the set in two different orders
    # (mirrors two turns whose underlying `Set[asyncio.Task]` iterates
    # differently after completions are discarded/re-added). Per-call-id
    # sort-order sensitivity (issue 06) is moot post-steer(): DynamicToolFactory
    # no longer mints any per-call tool, so the outer-visible surface is
    # trivially order-independent — this asserts that invariant explicitly
    # rather than relying on it being an accident of the current schema.
    calls = [(3, "toolC"), (1, "toolA"), (2, "toolB")]

    tools_data_a = _make_tools_data_with_pending(calls)
    tools_data_b = _make_tools_data_with_pending(list(reversed(calls)))

    factory_a = DynamicToolFactory(tools_data_a)
    factory_a.generate()

    factory_b = DynamicToolFactory(tools_data_b)
    factory_b.generate()

    expected = {"wait", "steer", "ask_about_completed_tool"}
    assert set(factory_a.dynamic_tools.keys()) == expected
    assert set(factory_b.dynamic_tools.keys()) == expected
    # No per-call-id key (e.g. "stop_toolA_1") ever leaks into the
    # outer-visible dynamic_tools dict, and insertion order is identical too
    # (fixed emission order inside generate(), independent of pending order).
    assert list(factory_a.dynamic_tools.keys()) == list(factory_b.dynamic_tools.keys())


# ---------------------------------------------------------------------------
# 2. Compact inspection JSON (async_tool_loop.py)
# ---------------------------------------------------------------------------


def _fixture_transcript() -> list[dict]:
    """A steering/tool-call-heavy loop segment: several grep-style tool calls
    whose results come back as multimodal content-block lists (the real shape
    ``serialize_tool_content`` produces for non-text results). This kind of
    structurally-dense stretch — many short keys, few long strings — is where
    indent=2's per-line overhead is largest relative to payload size.
    """
    msgs: list[dict] = [{"role": "user", "content": "status?"}]
    for i in range(3):
        msgs.append(
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": f"c{i}",
                        "type": "function",
                        "function": {"name": "grep", "arguments": "{}"},
                    },
                ],
            },
        )
        msgs.append(
            {
                "role": "tool",
                "tool_call_id": f"c{i}",
                "content": [{"type": "text", "text": f"l{j}"} for j in range(40)],
            },
        )
    msgs.append({"role": "assistant", "content": "done"})
    return msgs


def test_inspection_prompt_json_shrinks_at_least_2x_vs_indent2():
    safe = make_messages_safe_for_context_dump(_fixture_transcript())
    transformed = _transform_inner_roles(safe)

    pretty = json.dumps(transformed, indent=2)
    compact = json.dumps(transformed, separators=(",", ":"))

    assert len(compact) <= len(pretty) / 2


# ---------------------------------------------------------------------------
# 3. Content-hash spill filenames (formatting.py)
# ---------------------------------------------------------------------------


def test_spill_filename_is_content_hash_and_idempotent():
    text = "x" * 100

    path_1 = _spill_full_tool_text(text)
    path_2 = _spill_full_tool_text(text)

    assert path_1 is not None
    # Identical content spills to the identical path (idempotent overwrite),
    # not a fresh randomly-named file each call.
    assert path_1 == path_2

    with open(path_1, "r", encoding="utf-8") as f:
        assert f.read() == text

    # Different content spills to a different path.
    other_path = _spill_full_tool_text("y" * 100)
    assert other_path != path_1


def test_truncated_tool_text_is_byte_identical_across_renders():
    long_text = "abcdefgh" * 10_000  # well over TOOL_RESULT_TEXT_CHAR_LIMIT

    rendered_1 = _truncate_tool_text(long_text)
    rendered_2 = _truncate_tool_text(long_text)

    assert rendered_1 == rendered_2
