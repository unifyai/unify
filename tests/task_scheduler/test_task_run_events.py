"""Tests for Tasks/Executions → Events join + depth-1 projection."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from unify.task_scheduler.task_run_events import (
    TaskRunEventTree,
    build_run_key_filter,
    fetch_task_run_events,
    find_rows_at_node,
    normalize_events_base_context,
    project_immediate_children,
    resolve_task_run_root_segment,
)

_RUN_KEY = "live:scheduled:1:42:deadbeef:once"
_ROOT = f"Task.run(task_id=42,run_key={_RUN_KEY})"


def _row(
    *,
    hierarchy: list[str],
    event_type: str = "ManagerMethod",
    event_id: str = "e1",
    method: str | None = "act",
    kind: str | None = None,
    phase: str | None = "incoming",
    status: str | None = None,
    error: str | None = None,
    message: dict | None = None,
) -> dict:
    return {
        "_event_type": event_type,
        "type": event_type,
        "event_id": event_id,
        "method": method,
        "kind": kind,
        "phase": phase,
        "status": status,
        "error": error,
        "message": message,
        "hierarchy": hierarchy,
        "hierarchy_label": "->".join(hierarchy),
        "run_key": _RUN_KEY,
    }


def _fixture_rows() -> list[dict]:
    act = f"{_ROOT}->CodeActActor.act(ab12)"
    exec_code = f"{act}->execute_code(cd34)"
    nested = f"{exec_code}->CodeActActor.act(ef56)"
    return [
        _row(
            hierarchy=[_ROOT, "CodeActActor.act(ab12)"],
            event_id="mm-act-in",
            method="act",
            phase="incoming",
        ),
        _row(
            hierarchy=[_ROOT, "CodeActActor.act(ab12)"],
            event_id="mm-act-out",
            method="act",
            phase="outgoing",
            status="ok",
        ),
        _row(
            hierarchy=[_ROOT, "CodeActActor.act(ab12)", "execute_code(cd34)"],
            event_id="mm-exec-in",
            method="execute_code",
            phase="incoming",
        ),
        _row(
            hierarchy=[_ROOT, "CodeActActor.act(ab12)", "execute_code(cd34)"],
            event_id="mm-exec-out",
            method="execute_code",
            phase="outgoing",
            status="failed",
            error="boom " * 50,
        ),
        _row(
            hierarchy=[
                _ROOT,
                "CodeActActor.act(ab12)",
                "execute_code(cd34)",
                "CodeActActor.act(ef56)",
            ],
            event_id="mm-nested",
            method="act",
            phase="incoming",
        ),
        _row(
            hierarchy=[_ROOT, "CodeActActor.act(ab12)", "execute_code(cd34)"],
            event_type="ToolLoop",
            event_id="tl-1",
            method="CodeActActor.act",
            kind="thought",
            phase=None,
            message={"role": "assistant", "content": "huge thought " * 20},
        ),
    ]


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


def test_resolve_task_run_root_segment():
    rows = _fixture_rows()
    assert resolve_task_run_root_segment(rows, run_key=_RUN_KEY) == _ROOT


def test_project_immediate_children_root_and_drill():
    rows = _fixture_rows()
    root_kids = project_immediate_children(rows, run_key=_RUN_KEY, parent_prefix=None)
    assert len(root_kids) == 1
    assert root_kids[0]["segment"] == "CodeActActor.act(ab12)"
    assert root_kids[0]["has_children"] is True
    assert root_kids[0]["phase"] == "outgoing"
    assert "mm-act-in" in root_kids[0]["event_ids"]
    assert "message" not in root_kids[0]

    act_node = root_kids[0]["node_id"]
    mid = project_immediate_children(rows, run_key=_RUN_KEY, parent_prefix=act_node)
    assert len(mid) == 1
    assert mid[0]["segment"] == "execute_code(cd34)"
    assert mid[0]["has_children"] is True
    assert mid[0]["status"] == "failed"
    assert mid[0]["error"] is not None
    assert mid[0]["error"].endswith("…")
    assert "huge thought" not in (mid[0].get("error") or "")

    exec_node = mid[0]["node_id"]
    leaf = project_immediate_children(rows, run_key=_RUN_KEY, parent_prefix=exec_node)
    assert len(leaf) == 1
    assert leaf[0]["segment"] == "CodeActActor.act(ef56)"
    assert leaf[0]["has_children"] is False


def test_find_rows_at_node_exact_only():
    rows = _fixture_rows()
    act = f"{_ROOT}->CodeActActor.act(ab12)"
    at_act = find_rows_at_node(rows, node_id=act)
    assert {r["event_id"] for r in at_act} == {"mm-act-in", "mm-act-out"}
    one = find_rows_at_node(rows, node_id=act, event_id="mm-act-out")
    assert len(one) == 1
    assert one[0]["phase"] == "outgoing"


def test_fetch_task_run_events_filters_by_run_key_and_prefix():
    mm_rows = [
        {
            "method": "act",
            "run_key": _RUN_KEY,
            "hierarchy": [_ROOT, "CodeActActor.act(ab12)"],
        },
    ]
    tl_rows: list[dict] = []
    dm = MagicMock()

    def _filter(context, **kwargs):
        filt = kwargs["filter"]
        assert f'run_key == "{_RUN_KEY}"' in filt
        if "hierarchy_label.startswith" in filt:
            assert _ROOT in filt
        if context.endswith("/ManagerMethod"):
            return list(mm_rows)
        if context.endswith("/ToolLoop"):
            return list(tl_rows)
        return []

    dm.filter.side_effect = _filter

    tree = fetch_task_run_events(
        _RUN_KEY,
        events_base_context="9/1406",
        data_manager=dm,
        hierarchy_prefix=_ROOT,
    )
    assert isinstance(tree, TaskRunEventTree)
    assert tree.events_base_context == "9/1406/Events"
    assert tree.manager_methods == mm_rows
    assert dm.filter.call_count == 2
