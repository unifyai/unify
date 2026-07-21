"""Tests for primitives.tasks.get_run_event_children / get_run_event."""

from __future__ import annotations

from unittest.mock import patch

from unify.function_manager.primitives.registry import get_registry
from unify.task_scheduler.task_run_events import TaskRunEventTree

_RUN_KEY = "live:scheduled:1:42:abc:once"
_ROOT = f"Task.run(task_id=42,run_key={_RUN_KEY})"


def test_registry_exposes_lazy_run_event_primitives():
    methods = get_registry().primitive_methods(manager_alias="tasks")
    assert "get_run_event_children" in methods
    assert "get_run_event" in methods
    assert "ask" in methods
    assert "execute" in methods


def test_get_run_event_children_projects_root_level():
    from unify.task_scheduler.task_scheduler import TaskScheduler

    tree = TaskRunEventTree(
        run_key=_RUN_KEY,
        events_base_context="9/1406/Events",
        manager_methods=[
            {
                "event_id": "e1",
                "method": "act",
                "phase": "incoming",
                "hierarchy": [_ROOT, "CodeActActor.act(ab12)"],
                "hierarchy_label": f"{_ROOT}->CodeActActor.act(ab12)",
                "run_key": _RUN_KEY,
            },
            {
                "event_id": "e2",
                "method": "execute_code",
                "phase": "incoming",
                "hierarchy": [_ROOT, "CodeActActor.act(ab12)", "execute_code(cd34)"],
                "hierarchy_label": (
                    f"{_ROOT}->CodeActActor.act(ab12)->execute_code(cd34)"
                ),
                "run_key": _RUN_KEY,
            },
        ],
        tool_loops=[],
    )
    sched = TaskScheduler.__new__(TaskScheduler)
    with (
        patch(
            "unify.task_scheduler.task_run_events.fetch_task_run_events",
            return_value=tree,
        ) as fetch,
        patch(
            "unify.common.log_utils._get_user_id",
            return_value="9",
        ),
        patch(
            "unify.common.log_utils._get_assistant_id",
            return_value="1406",
        ),
    ):
        out = TaskScheduler.get_run_event_children(sched, run_key=_RUN_KEY)

    assert out["run_key"] == _RUN_KEY
    assert out["parent"] is None
    assert out["events_base_context"] == "9/1406/Events"
    assert len(out["children"]) == 1
    child = out["children"][0]
    assert child["segment"] == "CodeActActor.act(ab12)"
    assert child["has_children"] is True
    assert "message" not in child
    fetch.assert_called_once()
    assert fetch.call_args.kwargs["events_base_context"] == "9/1406/Events"
    assert fetch.call_args.kwargs.get("hierarchy_prefix") is None


def test_get_run_event_returns_one_node_only():
    from unify.task_scheduler.task_scheduler import TaskScheduler

    node = f"{_ROOT}->CodeActActor.act(ab12)"
    tree = TaskRunEventTree(
        run_key=_RUN_KEY,
        events_base_context="9/1406/Events",
        manager_methods=[
            {
                "event_id": "e1",
                "method": "act",
                "phase": "outgoing",
                "hierarchy": [_ROOT, "CodeActActor.act(ab12)"],
                "hierarchy_label": node,
                "run_key": _RUN_KEY,
                "traceback": "line1",
            },
            {
                "event_id": "e2",
                "method": "execute_code",
                "hierarchy": [_ROOT, "CodeActActor.act(ab12)", "execute_code(x)"],
                "hierarchy_label": f"{node}->execute_code(x)",
                "run_key": _RUN_KEY,
            },
        ],
        tool_loops=[],
    )
    sched = TaskScheduler.__new__(TaskScheduler)
    with patch(
        "unify.task_scheduler.task_run_events.fetch_task_run_events",
        return_value=tree,
    ) as fetch:
        out = TaskScheduler.get_run_event(
            sched,
            run_key=_RUN_KEY,
            node_id=node,
            events_base_context="9/1406/Events",
        )

    assert out["node_id"] == node
    assert len(out["events"]) == 1
    assert out["events"][0]["event_id"] == "e1"
    assert out["events"][0]["traceback"] == "line1"
    assert fetch.call_args.kwargs["hierarchy_prefix"] == node
