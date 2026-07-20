"""Tests for Tasks/Runs → Events join helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from unify.task_scheduler.task_run_events import (
    TaskRunEventTree,
    build_run_key_filter,
    fetch_task_run_events,
    normalize_events_base_context,
)


def test_normalize_events_base_context():
    assert normalize_events_base_context("1/2/Events") == "1/2/Events"
    assert normalize_events_base_context("1/2") == "1/2/Events"
    assert normalize_events_base_context("1/2/Events/ManagerMethod") == "1/2/Events"
    assert normalize_events_base_context("Events") == "Events"
    with pytest.raises(ValueError):
        normalize_events_base_context("  ")


def test_build_run_key_filter_escapes_quotes():
    assert build_run_key_filter("live:scheduled:1:5:abc:once") == (
        'run_key == "live:scheduled:1:5:abc:once"'
    )
    assert build_run_key_filter('rk"x') == 'run_key == "rk\\"x"'
    with pytest.raises(ValueError):
        build_run_key_filter("")


def test_fetch_task_run_events_filters_by_run_key():
    run_key = "live:scheduled:1:42:deadbeef:once"
    mm_rows = [
        {
            "method": "act",
            "run_key": run_key,
            "hierarchy": [
                f"Task.run(task_id=42,instance_id=7,run_key={run_key})",
                "CodeActActor.act(ab12)",
            ],
        },
    ]
    tl_rows = [
        {
            "kind": "thought",
            "run_key": run_key,
            "hierarchy_label": (
                f"Task.run(task_id=42,instance_id=7,run_key={run_key})"
                "->CodeActActor.act(ab12)"
            ),
        },
    ]

    dm = MagicMock()

    def _filter(context, **kwargs):
        assert kwargs["filter"] == f'run_key == "{run_key}"'
        if context.endswith("/ManagerMethod"):
            return list(mm_rows)
        if context.endswith("/ToolLoop"):
            return list(tl_rows)
        return []

    dm.filter.side_effect = _filter

    tree = fetch_task_run_events(
        run_key,
        events_base_context="9/1406",
        data_manager=dm,
    )
    assert isinstance(tree, TaskRunEventTree)
    assert tree.events_base_context == "9/1406/Events"
    assert tree.manager_methods == mm_rows
    assert tree.tool_loops == tl_rows
    roots = tree.hierarchy_roots()
    assert len(roots) == 1
    assert roots[0].startswith("Task.run(task_id=42,instance_id=7,run_key=")
    assert run_key in roots[0]
    assert dm.filter.call_count == 2
