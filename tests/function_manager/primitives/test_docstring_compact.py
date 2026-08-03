"""Compact docstring extraction keeps Anti-patterns for actor prompts."""

from __future__ import annotations

from unify.data_manager.base import BaseDataManager
from unify.function_manager.primitives.registry import ToolSurfaceRegistry


def test_extract_summary_keeps_anti_patterns_for_filter():
    doc = ToolSurfaceRegistry._extract_method_docstring(BaseDataManager, "filter")
    compact = ToolSurfaceRegistry._extract_summary_and_params(doc)
    assert "Anti-patterns" in compact
    assert "reduce(metric='count')" in compact or "reduce" in compact
    assert "Parameters" in compact
    assert "server-side" in compact.lower() or "Never download" in compact


def test_prompt_context_includes_data_efficiency_rule():
    from unify.function_manager.primitives import PrimitiveScope, get_registry

    scope = PrimitiveScope(scoped_managers=frozenset({"data", "files"}))
    context = get_registry().prompt_context(scope)
    assert "Orchestra / `Data/*` efficiency" in context
    assert "reduce" in context
    # The rule used to be phrased as "do not use raw unisdk.get_logs". That
    # naming was dropped when raw UniSDK stopped being taught at all, so
    # assert the behaviour the rule now states rather than a spelling of the
    # thing it warns against.
    assert "do not read another assistant's private contexts" in context
