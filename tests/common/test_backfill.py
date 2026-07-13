"""Tests for backfill utility."""

from __future__ import annotations

import uuid

import unisdk
from tests.helpers import _handle_project
from unify.common.backfill import (
    backfill_assistant_field,
    backfill_all_contexts_for_assistant,
)


def _count_logs_missing_assistant(logs) -> int:
    """Count logs where _assistant is missing or None (client-side check)."""
    return sum(1 for lg in logs if lg.entries.get("_assistant") is None)


def _unique_ctx(leaf: str) -> str:
    """Per-run context path so parallel CI shards cannot collide on shared names."""
    return f"TestAssistant/{leaf}-{uuid.uuid4().hex[:8]}"


def _reset_context(ctx: str, fields: dict[str, str]) -> None:
    try:
        unisdk.delete_context(ctx)
    except Exception:
        pass
    unisdk.create_context(ctx)
    unisdk.create_fields(fields, context=ctx)


def _assert_created_logs_visible(ctx: str, created_ids: set[int]) -> None:
    """Fail fast if Orchestra create succeeded but context read misses rows."""
    visible = {lg.id for lg in unisdk.get_logs(context=ctx, from_ids=list(created_ids))}
    assert visible == created_ids, (
        f"create/read miss for context {ctx!r}: "
        f"created={sorted(created_ids)} visible={sorted(visible)}"
    )


@_handle_project
def test_backfill_assistant_field():
    """Backfill should update logs missing _assistant."""
    ctx = _unique_ctx("TestBackfill")
    _reset_context(ctx, {"name": "str", "_assistant": "str"})

    created_ids = {
        unisdk.log(context=ctx, name=f"item_{i}", new=True).id for i in range(5)
    }
    _assert_created_logs_visible(ctx, created_ids)

    logs = unisdk.get_logs(context=ctx, from_ids=list(created_ids))
    assert (
        _count_logs_missing_assistant(logs) == 5
    ), f"Expected 5 logs without _assistant, got {_count_logs_missing_assistant(logs)}"

    result = backfill_assistant_field(ctx, "TestAssistant")
    assert result["total_updated"] == 5, f"Expected 5 updated, got {result}"
    assert result["context"] == ctx

    logs_after = unisdk.get_logs(context=ctx, from_ids=list(created_ids))
    assert (
        _count_logs_missing_assistant(logs_after) == 0
    ), "All logs should now have _assistant"

    logs_with = unisdk.get_logs(context=ctx, filter="_assistant == 'TestAssistant'")
    assert len(logs_with) == 5, f"Expected 5 logs with _assistant, got {len(logs_with)}"


@_handle_project
def test_backfill_assistant_field_empty_context():
    """Backfill should handle empty context gracefully."""
    ctx = _unique_ctx("TestBackfillEmpty")
    _reset_context(ctx, {"name": "str", "_assistant": "str"})

    result = backfill_assistant_field(ctx, "TestAssistant")
    assert result["total_updated"] == 0
    assert result["context"] == ctx


@_handle_project
def test_backfill_assistant_field_with_filter():
    """Backfill should respect additional filter."""
    ctx = _unique_ctx("TestBackfillFilter")
    _reset_context(
        ctx,
        {"name": "str", "category": "str", "_assistant": "str"},
    )

    created_ids: set[int] = set()
    for i in range(3):
        created_ids.add(
            unisdk.log(context=ctx, name=f"item_a_{i}", category="A", new=True).id,
        )
    for i in range(2):
        created_ids.add(
            unisdk.log(context=ctx, name=f"item_b_{i}", category="B", new=True).id,
        )
    _assert_created_logs_visible(ctx, created_ids)

    result = backfill_assistant_field(
        ctx,
        "TestAssistant",
        filter="category == 'A'",
    )
    assert result["total_updated"] == 3

    logs_a = unisdk.get_logs(
        context=ctx,
        filter="category == 'A' and _assistant == 'TestAssistant'",
    )
    assert len(logs_a) == 3

    logs_b = unisdk.get_logs(context=ctx, filter="category == 'B'")
    assert (
        _count_logs_missing_assistant(logs_b) == 2
    ), "Category B logs should not have _assistant"


@_handle_project
def test_backfill_all_contexts_for_assistant():
    """Backfill should update all contexts for an assistant."""
    assistant = f"TestMultiCtx-{uuid.uuid4().hex[:8]}"
    ctx1 = f"{assistant}/ContextOne"
    ctx2 = f"{assistant}/ContextTwo"

    for ctx in [ctx1, ctx2]:
        _reset_context(ctx, {"name": "str", "_assistant": "str"})

    created_ctx1 = {
        unisdk.log(context=ctx1, name=f"ctx1_item_{i}", new=True).id for i in range(3)
    }
    created_ctx2 = {
        unisdk.log(context=ctx2, name=f"ctx2_item_{i}", new=True).id for i in range(2)
    }
    _assert_created_logs_visible(ctx1, created_ctx1)
    _assert_created_logs_visible(ctx2, created_ctx2)

    results = backfill_all_contexts_for_assistant(assistant)

    assert ctx1 in results
    assert ctx2 in results
    assert results[ctx1]["total_updated"] == 3
    assert results[ctx2]["total_updated"] == 2


@_handle_project
def test_backfill_idempotent():
    """Running backfill twice should not update already-backfilled logs."""
    ctx = _unique_ctx("TestBackfillIdempotent")
    _reset_context(ctx, {"name": "str", "_assistant": "str"})

    created_ids = {
        unisdk.log(context=ctx, name=f"item_{i}", new=True).id for i in range(3)
    }
    _assert_created_logs_visible(ctx, created_ids)

    result1 = backfill_assistant_field(ctx, "TestAssistant")
    assert result1["total_updated"] == 3

    result2 = backfill_assistant_field(ctx, "TestAssistant")
    assert result2["total_updated"] == 0
