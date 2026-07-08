"""Tests for custom knowledge collection and synchronization."""

import json

import pytest
from pathlib import Path

from unify.common.context_registry import ContextRegistry
from unify.knowledge_manager.custom_knowledge import (
    META_JSON_FILENAME,
    ROWS_JSONL_FILENAME,
    collect_custom_knowledge,
    collect_knowledge_from_directories,
    compute_custom_knowledge_hash,
    knowledge_entry_key,
    merge_knowledge_table_specs,
)
from unify.knowledge_manager.knowledge_manager import KnowledgeManager
from tests.helpers import _handle_project


def _write_table(
    root: Path,
    table_name: str,
    *,
    description: str = "",
    columns: dict[str, str] | None = None,
    seed_key: str,
    rows: list[dict[str, object]],
    destination: str = "personal",
    auto_sync: bool = True,
) -> Path:
    table_dir = root
    for part in table_name.split("/"):
        table_dir = table_dir / part
    table_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "description": description,
        "columns": columns or {},
        "seed_key": seed_key,
        "destination": destination,
        "auto_sync": auto_sync,
    }
    (table_dir / META_JSON_FILENAME).write_text(json.dumps(meta) + "\n")
    lines = "\n".join(json.dumps(row) for row in rows)
    (table_dir / ROWS_JSONL_FILENAME).write_text(lines + ("\n" if lines else ""))
    return table_dir


@pytest.fixture
def custom_knowledge_dir(tmp_path: Path) -> Path:
    root = tmp_path / "knowledge"
    root.mkdir()
    _write_table(
        root,
        "Companies",
        description="Known companies",
        columns={"name": "str", "industry": "str"},
        seed_key="name",
        rows=[
            {"name": "Acme", "industry": "Widgets"},
        ],
    )
    _write_table(
        root,
        "TeamFacts",
        seed_key="fact",
        rows=[{"fact": "shared", "detail": "value"}],
        destination="team:42",
    )
    _write_table(
        root,
        "Skipped",
        seed_key="id",
        rows=[{"id": "1"}],
        auto_sync=False,
    )
    return root


@pytest.fixture
def knowledge_manager_factory():
    managers = []

    def _create():
        ContextRegistry.forget(KnowledgeManager, "Knowledge")
        ContextRegistry.forget(KnowledgeManager, "Knowledge/Meta")
        km = KnowledgeManager(include_contacts=False)
        managers.append(km)
        return km

    yield _create

    for km in managers:
        try:
            km.clear()
        except Exception:
            pass


def test_knowledge_entry_key():
    assert (
        knowledge_entry_key(table_name="Companies", seed_value="Acme")
        == "Companies|Acme"
    )


def test_collect_custom_knowledge_finds_tables(custom_knowledge_dir):
    tables = collect_custom_knowledge(path=custom_knowledge_dir)
    assert "Companies" in tables
    assert "TeamFacts" in tables
    assert "Skipped" not in tables


def test_collect_custom_knowledge_row_fields(custom_knowledge_dir):
    tables = collect_custom_knowledge(path=custom_knowledge_dir)
    row = tables["Companies"]["rows"][0]
    assert row["name"] == "Acme"
    assert row["custom_key"] == "Companies|Acme"
    assert row["custom_hash"]


def test_merge_knowledge_table_specs_merges_columns_and_rows():
    base = {
        "Companies": {
            "columns": {"name": "str"},
            "seed_key": "name",
            "rows": [
                {"name": "Acme", "custom_key": "Companies|Acme", "custom_hash": "a"},
            ],
        },
    }
    overlay = {
        "Companies": {
            "columns": {"industry": "str"},
            "rows": [
                {
                    "name": "Acme",
                    "industry": "Tech",
                    "custom_key": "Companies|Acme",
                    "custom_hash": "b",
                },
            ],
        },
    }
    merged = merge_knowledge_table_specs(base, overlay)
    assert "name" in merged["Companies"]["columns"]
    assert "industry" in merged["Companies"]["columns"]
    assert len(merged["Companies"]["rows"]) == 1
    assert merged["Companies"]["rows"][0]["industry"] == "Tech"


def test_collect_knowledge_from_directories_later_dir_wins(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    _write_table(first, "Companies", seed_key="name", rows=[{"name": "Acme"}])
    _write_table(
        second,
        "Companies",
        seed_key="name",
        rows=[{"name": "Acme", "industry": "Tech"}],
        columns={"industry": "str"},
    )
    tables = collect_knowledge_from_directories([first, second])
    assert tables["Companies"]["rows"][0]["industry"] == "Tech"


def test_compute_custom_knowledge_hash_empty():
    assert compute_custom_knowledge_hash(source_tables={}) == ""


@pytest.mark.asyncio
async def test_sync_custom_knowledge_inserts_rows(
    knowledge_manager_factory,
    custom_knowledge_dir,
):
    _handle_project("KnowledgeManagerCustomSync")
    km = knowledge_manager_factory()
    source = collect_custom_knowledge(path=custom_knowledge_dir)
    assert km.sync_custom(source_tables=source) is True
    result = km._filter(tables=["Companies"], filter="custom_hash != None", limit=10)
    assert len(result["Companies"]) == 1
    assert result["Companies"][0]["name"] == "Acme"


@pytest.mark.asyncio
async def test_sync_custom_knowledge_is_idempotent(
    knowledge_manager_factory,
    custom_knowledge_dir,
):
    _handle_project("KnowledgeManagerCustomSync")
    km = knowledge_manager_factory()
    source = collect_custom_knowledge(path=custom_knowledge_dir)
    assert km.sync_custom(source_tables=source) is True
    km._custom_knowledge_synced = False
    assert km.sync_custom(source_tables=source) is False
