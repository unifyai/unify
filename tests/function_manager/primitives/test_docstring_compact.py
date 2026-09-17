"""Compact docstring extraction keeps Anti-patterns for actor prompts."""

from __future__ import annotations

from unify.function_manager.primitives.registry import ToolSurfaceRegistry

_DOCSTRING = """Count rows server-side without downloading them.

A second summary paragraph that is not part of the compact form.

Parameters
----------
context : str
    Context to count in.
_internal_hint : str, optional
    Wiring detail hidden from the model.

Returns
-------
int
    The row count.

Anti-patterns
-------------
- Never download a large table into Python just to count it; use
  reduce(metric='count') instead.

Examples
--------
>>> count("Sales")
"""


def test_extract_summary_keeps_anti_patterns():
    compact = ToolSurfaceRegistry._extract_summary_and_params(_DOCSTRING)
    assert compact.startswith("Count rows server-side without downloading them.")
    assert "Parameters" in compact
    assert "context : str" in compact
    assert "Anti-patterns" in compact
    assert "reduce(metric='count')" in compact
    assert "Never download" in compact


def test_extract_summary_omits_other_sections_and_internal_params():
    compact = ToolSurfaceRegistry._extract_summary_and_params(_DOCSTRING)
    assert "second summary paragraph" not in compact
    assert "Returns" not in compact
    assert "The row count" not in compact
    assert "Examples" not in compact
    assert ">>> count" not in compact
    assert "_internal_hint" not in compact
    assert "Wiring detail" not in compact
