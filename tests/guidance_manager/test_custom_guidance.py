"""
Tests for custom guidance collection and synchronization.
"""

import json

import pytest
from pathlib import Path

from unify.guidance_manager.guidance_manager import GuidanceManager
from unify.guidance_manager.custom_guidance import (
    collect_custom_guidance,
    compute_custom_guidance_hash,
    collect_guidance_from_directories,
    GUIDANCE_JSONL_FILENAME,
)
from unify.common.context_registry import ContextRegistry
from tests.helpers import _handle_project

_EXAMPLE_GUIDANCE_LINES = [
    {
        "key": "ops/triage",
        "title": "How to triage repairs",
        "content": "Check urgent repairs first.",
        "function_names": ["sync_repairs"],
    },
    {
        "key": "ops/escalation",
        "title": "Escalation policy",
        "content": "Escalate when SLA is breached.",
        "destination": "team:42",
    },
    {
        "key": "draft/unpublished",
        "title": "Draft guidance",
        "content": "Not synced.",
        "auto_sync": False,
    },
]


@pytest.fixture
def custom_guidance_dir(tmp_path: Path) -> Path:
    guidance_dir = tmp_path / "guidance"
    guidance_dir.mkdir()
    lines = "\n".join(json.dumps(row) for row in _EXAMPLE_GUIDANCE_LINES)
    (guidance_dir / GUIDANCE_JSONL_FILENAME).write_text(lines + "\n")
    return guidance_dir


@pytest.fixture
def guidance_manager_factory():
    managers = []

    def _create():
        ContextRegistry.forget(GuidanceManager, "Guidance")
        ContextRegistry.forget(GuidanceManager, "Guidance/Meta")
        gm = GuidanceManager()
        managers.append(gm)
        return gm

    yield _create

    for gm in managers:
        try:
            gm.clear()
        except Exception:
            pass


def test_collect_custom_guidance_finds_entries(custom_guidance_dir):
    guidance = collect_custom_guidance(path=custom_guidance_dir)
    assert "ops/triage" in guidance
    assert "ops/escalation" in guidance


def test_collect_custom_guidance_excludes_auto_sync_false(custom_guidance_dir):
    guidance = collect_custom_guidance(path=custom_guidance_dir)
    assert "draft/unpublished" not in guidance


def test_collect_custom_guidance_has_required_fields(custom_guidance_dir):
    guidance = collect_custom_guidance(path=custom_guidance_dir)
    entry = guidance["ops/triage"]
    assert entry["custom_key"] == "ops/triage"
    assert entry["title"] == "How to triage repairs"
    assert "urgent repairs" in entry["content"]
    assert len(entry["custom_hash"]) == 16
    assert entry["is_builtin"] is False


def test_collect_custom_guidance_preserves_destination(custom_guidance_dir):
    guidance = collect_custom_guidance(path=custom_guidance_dir)
    assert guidance["ops/escalation"]["destination"] == "team:42"
    assert guidance["ops/triage"]["destination"] == "personal"


def test_compute_custom_guidance_hash_is_deterministic(custom_guidance_dir):
    guidance = collect_custom_guidance(path=custom_guidance_dir)
    assert compute_custom_guidance_hash(
        source_guidance=guidance,
    ) == compute_custom_guidance_hash(source_guidance=guidance)


def test_collect_custom_guidance_none_path_returns_empty():
    assert collect_custom_guidance(path=None) == {}


def test_collect_guidance_from_directories_later_dir_overrides(tmp_path):
    dir_a = tmp_path / "a"
    dir_a.mkdir()
    (dir_a / GUIDANCE_JSONL_FILENAME).write_text(
        json.dumps(
            {
                "key": "shared",
                "title": "Shared A",
                "content": "Version A",
            },
        )
        + "\n",
    )

    dir_b = tmp_path / "b"
    dir_b.mkdir()
    (dir_b / GUIDANCE_JSONL_FILENAME).write_text(
        json.dumps(
            {
                "key": "shared",
                "title": "Shared B",
                "content": "Version B",
            },
        )
        + "\n",
    )

    merged = collect_guidance_from_directories([dir_a, dir_b])
    assert merged["shared"]["title"] == "Shared B"


@_handle_project
@pytest.mark.asyncio
async def test_sync_custom_guidance_inserts_new_entries(
    guidance_manager_factory,
    custom_guidance_dir,
):
    gm = guidance_manager_factory()
    source = collect_custom_guidance(path=custom_guidance_dir)
    result = gm.sync_custom_guidance(source_guidance=source)

    assert result is True
    rows = gm.filter(filter="custom_hash != None", limit=100)
    titles = {row.title for row in rows}
    assert "How to triage repairs" in titles
    assert "Escalation policy" in titles
    assert "Draft guidance" not in titles


@_handle_project
@pytest.mark.asyncio
async def test_sync_custom_guidance_is_idempotent(
    guidance_manager_factory,
    custom_guidance_dir,
):
    gm = guidance_manager_factory()
    source = collect_custom_guidance(path=custom_guidance_dir)

    assert gm.sync_custom_guidance(source_guidance=source) is True
    gm._custom_guidance_synced = False
    assert gm.sync_custom_guidance(source_guidance=source) is False


@_handle_project
@pytest.mark.asyncio
async def test_sync_custom_guidance_has_custom_hash(
    guidance_manager_factory,
    custom_guidance_dir,
):
    gm = guidance_manager_factory()
    source = collect_custom_guidance(path=custom_guidance_dir)
    gm.sync_custom_guidance(source_guidance=source)

    db_guidance = gm._get_custom_guidance_from_db()
    assert "ops/triage" in db_guidance
    assert db_guidance["ops/triage"]["custom_hash"] is not None


@_handle_project
@pytest.mark.asyncio
async def test_user_guidance_without_custom_hash_is_preserved(
    guidance_manager_factory,
    custom_guidance_dir,
):
    gm = guidance_manager_factory()
    gm.add_guidance(
        title="User-authored note",
        content="Keep this entry.",
    )

    source = collect_custom_guidance(path=custom_guidance_dir)
    gm.sync_custom_guidance(source_guidance=source)

    rows = gm.filter(limit=100)
    titles = {row.title for row in rows}
    assert "User-authored note" in titles
    assert "How to triage repairs" in titles


@_handle_project
@pytest.mark.asyncio
async def test_sync_custom_resolves_function_names(
    guidance_manager_factory,
    custom_guidance_dir,
):
    gm = guidance_manager_factory()
    source = collect_custom_guidance(path=custom_guidance_dir)
    # Resolve names against the personal store; team destinations need a live
    # ContextRegistry team root which this unit test does not provision.
    source["ops/escalation"]["destination"] = "personal"
    function_name_to_id = {"sync_repairs": 42}

    gm.sync_custom(source_guidance=source, function_name_to_id=function_name_to_id)

    db_guidance = gm._get_custom_guidance_from_db()
    assert db_guidance["ops/triage"]["function_ids"] == [42]
