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
)
from unify.task_scheduler.task_scheduler import TaskScheduler
from tests.helpers import _handle_project

_EXAMPLE_TASK_LINES = [
    {
        "key": "ops/daily-check",
        "name": "Daily check",
        "description": "Run the daily operational check.",
        "repeat": [{"frequency": "daily"}],
        "tags": ["ops", "daily"],
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
    scheduler._custom_tasks_synced_sources.clear()
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
    assert rows[0].trigger is not None


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
    scheduler._custom_tasks_synced_sources.clear()
    scheduler.sync_custom_tasks(source_tasks=reduced)

    rows = scheduler._filter_tasks(filter="custom_hash != None", limit=100)
    names = {row.name for row in rows}
    assert "Daily check" not in names
    assert "On inbound email" in names


def test_collect_custom_tasks_carries_tags(custom_tasks_dir):
    """Tags declared in tasks.jsonl reach the planted definition row.

    Tags are labels only — no scheduling semantics — but they must survive the
    jsonl → reconcile → Tasks row path or the console has nothing to filter on.
    A task without tags stays tagless rather than growing an empty list.
    """

    tasks = collect_custom_tasks(str(custom_tasks_dir))
    assert tasks["ops/daily-check"]["tags"] == ["ops", "daily"]
    assert tasks["ops/on-event"]["tags"] is None


def test_tags_participate_in_the_sync_hash(custom_tasks_dir, tmp_path):
    """Editing only a task's tags must count as a change reconcile applies."""

    retagged = tmp_path / "retagged"
    retagged.mkdir()
    lines = []
    for row in _EXAMPLE_TASK_LINES:
        row = dict(row)
        if row["key"] == "ops/daily-check":
            row["tags"] = ["ops", "weekly"]
        lines.append(json.dumps(row))
    (retagged / TASKS_JSONL_FILENAME).write_text("\n".join(lines) + "\n")

    original = collect_custom_tasks(str(custom_tasks_dir))["ops/daily-check"]
    changed = collect_custom_tasks(str(retagged))["ops/daily-check"]
    assert original["custom_hash"] != changed["custom_hash"]


@_handle_project
@pytest.mark.asyncio
@pytest.mark.requires_orchestra
async def test_sync_writes_tags_onto_the_row_and_clears_them_on_removal(
    task_scheduler_factory,
    custom_tasks_dir,
    tmp_path,
):
    """The hash carrying tags is not enough — the row writers must carry them too.

    The first shipped version added tags to the sync hash but not to the insert
    or update entry builders, so reconcile stamped the new hash while writing
    ``tags: None`` — and every later sync then skipped the row as up to date.
    The tags could never arrive without a manual backfill.
    """

    scheduler = task_scheduler_factory()
    source = collect_custom_tasks(path=custom_tasks_dir)
    scheduler.sync_custom_tasks(source_tasks=source)

    rows = scheduler._filter_tasks(filter="custom_key == 'ops/daily-check'", limit=1)
    assert rows[0].tags == ["ops", "daily"]

    # An edit that only changes tags must update the row.
    retagged_dir = tmp_path / "retagged"
    retagged_dir.mkdir()
    lines = []
    for row in _EXAMPLE_TASK_LINES:
        row = dict(row)
        if row["key"] == "ops/daily-check":
            row["tags"] = ["ops", "weekly"]
        lines.append(json.dumps(row))
    (retagged_dir / TASKS_JSONL_FILENAME).write_text("\n".join(lines) + "\n")
    scheduler._custom_tasks_synced_sources.clear()
    scheduler.sync_custom_tasks(source_tasks=collect_custom_tasks(path=retagged_dir))
    rows = scheduler._filter_tasks(filter="custom_key == 'ops/daily-check'", limit=1)
    assert rows[0].tags == ["ops", "weekly"]

    # Removing every tag untags the row rather than leaving the stale list.
    untagged_dir = tmp_path / "untagged"
    untagged_dir.mkdir()
    lines = []
    for row in _EXAMPLE_TASK_LINES:
        row = {k: v for k, v in row.items() if k != "tags"}
        lines.append(json.dumps(row))
    (untagged_dir / TASKS_JSONL_FILENAME).write_text("\n".join(lines) + "\n")
    scheduler._custom_tasks_synced_sources.clear()
    scheduler.sync_custom_tasks(source_tasks=collect_custom_tasks(path=untagged_dir))
    rows = scheduler._filter_tasks(filter="custom_key == 'ops/daily-check'", limit=1)
    assert not rows[0].tags


def test_task_adapter_derived_stale_detects_repointed_entrypoint():
    """A stored entrypoint id must track the function it was resolved from.

    The content hash covers ``entrypoint_function`` (the name), never the
    resolved id, so a re-registered functions store leaves the row dangling
    with a matching hash; ``derived_stale`` is what forces the re-point.
    """
    from types import SimpleNamespace

    from unify.task_scheduler.task_scheduler import _TaskSyncAdapter

    scheduler_stub = SimpleNamespace(_custom_task_sync_workers=lambda: 1)
    adapter = _TaskSyncAdapter(
        scheduler_stub,
        function_name_to_id={"run_gtm_stargazer_enrich_tick": 29},
    )
    fields = {"entrypoint_function": "run_gtm_stargazer_enrich_tick"}

    assert adapter.derived_stale("k", {"entrypoint": 1}, fields)
    assert adapter.derived_stale("k", {"entrypoint": None}, fields)
    assert not adapter.derived_stale("k", {"entrypoint": 29}, fields)
    # Unresolvable names are the writer's warning to raise, not churn here.
    assert not adapter.derived_stale(
        "k",
        {"entrypoint": 1},
        {"entrypoint_function": "run_unknown"},
    )
    assert not adapter.derived_stale("k", {"entrypoint": 1}, {})


@_handle_project
@pytest.mark.asyncio
@pytest.mark.requires_orchestra
async def test_unresolved_entrypoint_update_preserves_stored_id(
    task_scheduler_factory,
    tmp_path,
):
    """A filtered lookup must never turn a symbolic task agentic.

    Resolution can be temporarily incomplete when the reconciling runtime
    cannot discover a function that executes elsewhere. Updating other task
    fields in that state must leave the last known entrypoint intact.
    """
    from unify.function_manager.function_manager import FunctionManager

    function_manager = FunctionManager(include_primitives=False)
    function_manager.add_functions(
        implementations="def run_elsewhere():\n    return 'ok'\n",
    )
    function_id = function_manager.list_function_name_to_ids()["run_elsewhere"]

    tasks_dir = tmp_path / "entrypoint-preservation"
    tasks_dir.mkdir()

    def write_source(description):
        row = {
            "key": "ops/runs-elsewhere",
            "name": "Runs elsewhere",
            "description": description,
            "repeat": [{"frequency": "daily"}],
            "entrypoint_function": "run_elsewhere",
        }
        (tasks_dir / TASKS_JSONL_FILENAME).write_text(json.dumps(row) + "\n")
        return collect_custom_tasks(path=tasks_dir)

    scheduler = task_scheduler_factory()
    scheduler.sync_custom_tasks(
        source_tasks=write_source("Version one."),
        function_name_to_id={"run_elsewhere": function_id},
    )

    scheduler._custom_tasks_synced_sources.clear()
    scheduler.sync_custom_tasks(
        source_tasks=write_source("Version two."),
        function_name_to_id={},
    )

    rows = scheduler._filter_tasks(
        filter="custom_key == 'ops/runs-elsewhere'",
        limit=1,
    )
    assert len(rows) == 1
    assert rows[0].entrypoint == function_id
    assert rows[0].description == "Version two."


def test_entrypoint_resolution_participates_in_the_sync_hash():
    """A function renumbering must invalidate the stored aggregate hash,
    or the reconcile short-circuits and dangling entrypoints never heal."""
    tasks = {
        "ops/daily-check": {
            "custom_hash": "abc",
            "entrypoint_function": "run_daily_check",
        },
    }
    base = compute_custom_tasks_hash(
        source_tasks=tasks,
        entrypoint_resolution={"run_daily_check": 1},
    )
    renumbered = compute_custom_tasks_hash(
        source_tasks=tasks,
        entrypoint_resolution={"run_daily_check": 29},
    )
    assert base != renumbered
    assert base == compute_custom_tasks_hash(
        source_tasks=tasks,
        entrypoint_resolution={"run_daily_check": 1},
    )
