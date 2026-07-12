"""Tests for custom task collection and synchronization."""

import json

import pytest
from pathlib import Path

from unify.common.context_registry import ContextRegistry
from unify.task_scheduler.custom_tasks import (
    TASKS_JSONL_FILENAME,
    collect_custom_tasks,
    collect_tasks_from_directories,
    compute_custom_tasks_hash,
    derive_initial_task_status,
)
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.task_scheduler.types.status import Status
from tests.helpers import _handle_project

_EXAMPLE_TASK_LINES = [
    {
        "key": "ops/daily-check",
        "name": "Daily check",
        "description": "Run the daily operational check.",
        "repeat": [{"frequency": "daily"}],
    },
    {
        "key": "ops/on-event",
        "name": "On inbound email",
        "description": "React to inbound email.",
        "trigger": {"medium": "email"},
        "destination": "team:42",
    },
    {
        "key": "draft/unpublished",
        "name": "Draft task",
        "description": "Not synced.",
        "auto_sync": False,
    },
]


@pytest.fixture
def custom_tasks_dir(tmp_path: Path) -> Path:
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    lines = "\n".join(json.dumps(row) for row in _EXAMPLE_TASK_LINES)
    (tasks_dir / TASKS_JSONL_FILENAME).write_text(lines + "\n")
    return tasks_dir


@pytest.fixture
def task_scheduler_factory():
    schedulers = []

    def _create():
        ContextRegistry.forget(TaskScheduler, "Tasks")
        ContextRegistry.forget(TaskScheduler, "Tasks/Meta")
        scheduler = TaskScheduler()
        schedulers.append(scheduler)
        return scheduler

    yield _create

    for scheduler in schedulers:
        try:
            scheduler.clear()
        except Exception:
            pass


def test_collect_custom_tasks_finds_entries(custom_tasks_dir):
    tasks = collect_custom_tasks(path=custom_tasks_dir)
    assert "ops/daily-check" in tasks
    assert "ops/on-event" in tasks


def test_collect_custom_tasks_excludes_auto_sync_false(custom_tasks_dir):
    tasks = collect_custom_tasks(path=custom_tasks_dir)
    assert "draft/unpublished" not in tasks


def test_collect_custom_tasks_has_required_fields(custom_tasks_dir):
    tasks = collect_custom_tasks(path=custom_tasks_dir)
    entry = tasks["ops/daily-check"]
    assert entry["custom_key"] == "ops/daily-check"
    assert entry["name"] == "Daily check"
    assert len(entry["custom_hash"]) == 16


def test_collect_custom_tasks_preserves_destination(custom_tasks_dir):
    tasks = collect_custom_tasks(path=custom_tasks_dir)
    assert tasks["ops/on-event"]["destination"] == "team:42"
    assert tasks["ops/daily-check"]["destination"] == "personal"


def test_derive_initial_task_status():
    assert (
        derive_initial_task_status(
            schedule=None,
            trigger={"medium": "email"},
        )
        == Status.triggerable
    )
    assert (
        derive_initial_task_status(
            schedule={"start_at": "2026-01-01T09:00:00Z"},
            trigger=None,
        )
        == Status.scheduled
    )
    assert (
        derive_initial_task_status(
            schedule=None,
            trigger=None,
        )
        == Status.scheduled
    )


def test_compute_custom_tasks_hash_is_deterministic(custom_tasks_dir):
    tasks = collect_custom_tasks(path=custom_tasks_dir)
    assert compute_custom_tasks_hash(source_tasks=tasks) == compute_custom_tasks_hash(
        source_tasks=tasks,
    )


def test_collect_tasks_from_directories_later_dir_overrides(tmp_path):
    dir_a = tmp_path / "a"
    dir_a.mkdir()
    (dir_a / TASKS_JSONL_FILENAME).write_text(
        json.dumps(
            {
                "key": "shared",
                "name": "Shared A",
                "description": "Version A",
            },
        )
        + "\n",
    )

    dir_b = tmp_path / "b"
    dir_b.mkdir()
    (dir_b / TASKS_JSONL_FILENAME).write_text(
        json.dumps(
            {
                "key": "shared",
                "name": "Shared B",
                "description": "Version B",
            },
        )
        + "\n",
    )

    merged = collect_tasks_from_directories([dir_a, dir_b])
    assert merged["shared"]["name"] == "Shared B"


@_handle_project
@pytest.mark.asyncio
@pytest.mark.requires_orchestra
async def test_sync_custom_tasks_inserts_new_entries(
    task_scheduler_factory,
    custom_tasks_dir,
):
    scheduler = task_scheduler_factory()
    source = collect_custom_tasks(path=custom_tasks_dir)
    result = scheduler.sync_custom_tasks(source_tasks=source)

    assert result is True
    rows = scheduler._filter_tasks(filter="custom_hash != None", limit=100)
    names = {row.name for row in rows}
    assert "Daily check" in names
    assert "On inbound email" in names
    assert "Draft task" not in names
    assert all(row.enabled is False for row in rows)


@_handle_project
@pytest.mark.asyncio
@pytest.mark.requires_orchestra
async def test_sync_custom_tasks_is_idempotent(
    task_scheduler_factory,
    custom_tasks_dir,
):
    scheduler = task_scheduler_factory()
    source = collect_custom_tasks(path=custom_tasks_dir)

    assert scheduler.sync_custom_tasks(source_tasks=source) is True
    scheduler._custom_tasks_synced = False
    assert scheduler.sync_custom_tasks(source_tasks=source) is False


@_handle_project
@pytest.mark.asyncio
@pytest.mark.requires_orchestra
async def test_sync_custom_tasks_sets_triggerable_status(
    task_scheduler_factory,
    custom_tasks_dir,
):
    scheduler = task_scheduler_factory()
    source = collect_custom_tasks(path=custom_tasks_dir)
    scheduler.sync_custom_tasks(source_tasks=source)

    rows = scheduler._filter_tasks(
        filter="custom_key == 'ops/on-event'",
        limit=1,
    )
    assert len(rows) == 1
    assert rows[0].status == Status.triggerable


@_handle_project
@pytest.mark.asyncio
@pytest.mark.requires_orchestra
async def test_sync_custom_tasks_deletes_removed_entries(
    task_scheduler_factory,
    custom_tasks_dir,
):
    scheduler = task_scheduler_factory()
    source = collect_custom_tasks(path=custom_tasks_dir)
    scheduler.sync_custom_tasks(source_tasks=source)

    reduced = {key: source[key] for key in source if key != "ops/daily-check"}
    scheduler._custom_tasks_synced = False
    scheduler.sync_custom_tasks(source_tasks=reduced)

    rows = scheduler._filter_tasks(filter="custom_hash != None", limit=100)
    names = {row.name for row in rows}
    assert "Daily check" not in names
    assert "On inbound email" in names
