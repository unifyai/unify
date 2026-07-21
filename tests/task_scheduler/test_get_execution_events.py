"""Tests for primitives.tasks.get_execution_events."""

from __future__ import annotations

from unittest.mock import patch

from unify.task_scheduler.task_run_events import TaskRunEventTree


def test_get_execution_events_returns_tree_details():
    from unify.task_scheduler.task_scheduler import TaskScheduler

    tree = TaskRunEventTree(
        run_key="live:scheduled:1:42:abc:once",
        events_base_context="9/1406/Events",
        manager_methods=[{"method": "act", "run_key": "live:scheduled:1:42:abc:once"}],
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
        outcome = TaskScheduler.get_execution_events(
            sched,
            run_key="live:scheduled:1:42:abc:once",
        )
    assert outcome["outcome"] == "execution event tree loaded"
    assert outcome["details"]["manager_method_count"] == 1
    assert outcome["details"]["run_key"] == tree.run_key
    fetch.assert_called_once()
    assert fetch.call_args.kwargs["events_base_context"] == "9/1406/Events"
