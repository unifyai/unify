"""Reproduction: re-pointing a task entrypoint after function renumbering.

Mirrors the production shapes exactly — including a null
``requires_computer`` — to pin where a re-point can end up as a wiped
entrypoint instead of the newly resolved id.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from unify.task_scheduler.custom_tasks import collect_custom_tasks
from unify.task_scheduler.task_scheduler import TaskScheduler
from tests.helpers import _handle_project

_LINES = [
    {
        "key": "gtm.sales_nav.crawl",
        "name": "GTM Sales Navigator company crawl",
        "description": "Crawl saved Sales Navigator company lists.",
        "schedule": {"start_at": "2026-07-30T23:00:00Z"},
        "offline": True,
        "destination": "personal",
        "entrypoint_function": "run_gtm_sales_nav_crawl",
    },
    {
        "key": "gtm.enrich.control",
        "name": "Control task",
        "description": "Control with a boolean requires_computer.",
        "schedule": {"start_at": "2026-07-30T11:00:00Z"},
        "offline": True,
        "requires_computer": False,
        "destination": "personal",
        "entrypoint_function": "run_control_tick",
    },
]


@pytest.fixture
def tasks_dir(tmp_path: Path) -> Path:
    d = tmp_path / "tasks"
    d.mkdir()
    (d / "tasks.jsonl").write_text(
        "\n".join(json.dumps(row) for row in _LINES) + "\n",
    )
    return d


@_handle_project
def test_renumbered_entrypoints_repoint_not_wipe(tasks_dir):
    scheduler = TaskScheduler()
    source = collect_custom_tasks(path=tasks_dir)
    assert set(source) == {
        "gtm.sales_nav.crawl",
        "gtm.enrich.control",
    }, "collector dropped a line; shapes must all be accepted"

    scheduler.sync_custom_tasks(
        source_tasks=source,
        function_name_to_id={
            "run_gtm_sales_nav_crawl": 24,
            "run_control_tick": 0,
        },
    )
    rows = {
        r.custom_key: r
        for r in scheduler._filter_tasks(filter="custom_key != None", limit=10)
    }
    assert rows["gtm.sales_nav.crawl"].entrypoint == 24
    assert rows["gtm.enrich.control"].entrypoint == 0

    # Functions renumbered underneath (the production incident shape).
    scheduler._custom_tasks_synced_sources.clear()
    scheduler.sync_custom_tasks(
        source_tasks=source,
        function_name_to_id={
            "run_gtm_sales_nav_crawl": 26,
            "run_control_tick": 28,
        },
    )
    rows = {
        r.custom_key: r
        for r in scheduler._filter_tasks(filter="custom_key != None", limit=10)
    }
    assert rows["gtm.enrich.control"].entrypoint == 28
    assert rows["gtm.sales_nav.crawl"].entrypoint == 26


@_handle_project
def test_deployment_owned_authored_fields_are_not_runtime_mutable(tasks_dir):
    """A runtime update must not damage a reference the source owns.

    The sync short-circuits on its aggregate hash, so a runtime write that
    nulls a derived field (the entrypoint) is never healed — the task dangles
    until someone notices. Authored fields change in the source only.
    """
    scheduler = TaskScheduler()
    scheduler.sync_custom_tasks(
        source_tasks=collect_custom_tasks(path=tasks_dir),
        function_name_to_id={"run_gtm_sales_nav_crawl": 24, "run_control_tick": 26},
    )
    task = next(
        row
        for row in scheduler._filter_tasks(filter="custom_key != None", limit=10)
        if row.custom_key == "gtm.sales_nav.crawl"
    )

    with pytest.raises(ValueError, match="deployment-owned"):
        scheduler._update_task(task_id=task.task_id, entrypoint=None)

    assert scheduler._get_task_or_raise(task.task_id).entrypoint == 24

    # Runtime state stays mutable: arming is not an authored field.
    scheduler._update_task(task_id=task.task_id, enabled=True)
    assert scheduler._get_task_or_raise(task.task_id).enabled is True
