"""A projected occurrence is pending; only a starting run is running.

Both paths share one create-or-adopt POST, and reusing the starting-run helper
for projection created every successor already ``running`` with a fresh
``started_at`` — ten minutes before it was due. Overlap guards then saw a live
concurrent run the moment the predecessor started, and the two skipped each
other forever.
"""

from __future__ import annotations

from unittest.mock import patch

from unify.task_scheduler import machine_state
from unify.task_scheduler.machine_state import (
    TaskRunProvenance,
    create_or_adopt_live_task_run,
    project_task_occurrence,
)


def _provenance(**overrides) -> TaskRunProvenance:
    base = dict(
        assistant_id="1406",
        task_id=12,
        wake="scheduled",
        delivery="offline",
        destination="team:11",
        revision=None,
        scheduled_for="2026-07-30T10:10:00+00:00",
        dispatch_offset_seconds=72.99,
    )
    base.update(overrides)
    return TaskRunProvenance(**base)


def _posted_body(call) -> dict:
    return call.args[1]


def test_projection_posts_a_pending_row() -> None:
    with patch.object(machine_state, "_orchestra_admin_post") as post:
        post.return_value = {"run": {"run_key": "k"}}
        project_task_occurrence(_provenance())

    body = _posted_body(post.call_args)
    assert body["state"] == "scheduled"
    assert "started_at" not in body
    assert body["dispatch_offset_seconds"] == 72.99


def test_projection_stores_the_revision_the_key_digested() -> None:
    """run_key hashed str(revision or ""); the row must carry that same value
    or the dispatcher rebuilds a different key at fire time and mints a twin."""

    with patch.object(machine_state, "_orchestra_admin_post") as post:
        post.return_value = {"run": {"run_key": "k"}}
        project_task_occurrence(_provenance(revision=None))

    assert _posted_body(post.call_args)["revision"] == ""


def test_a_starting_run_is_still_marked_running() -> None:
    with patch.object(machine_state, "_orchestra_admin_post") as post:
        post.return_value = {"run": {"run_key": "k"}}
        create_or_adopt_live_task_run(_provenance())

    body = _posted_body(post.call_args)
    assert body["state"] == "running"
    assert body["started_at"]


def test_projection_carries_the_definitions_entrypoint() -> None:
    """Dispatch reads the entrypoint from the materialized row.

    A projected occurrence stored without one launched as agentic, and a
    symbolic recurring task then refused its own successor at start —
    "activation requested agentic execution, task row provides a symbolic
    entrypoint" — so the series failed one occurrence after every re-arm.
    """

    with patch.object(machine_state, "_orchestra_admin_post") as post:
        post.return_value = {"run": {"run_key": "k"}}
        project_task_occurrence(_provenance(entrypoint=0))

    assert _posted_body(post.call_args)["entrypoint"] == 0
