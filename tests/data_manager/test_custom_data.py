"""Tests for custom data collection and synchronization."""

import json

import pytest
from pathlib import Path

from unify.common.context_registry import ContextRegistry
from unify.data_manager.custom_data import (
    META_JSON_FILENAME,
    ROWS_JSONL_FILENAME,
    collect_custom_data,
    collect_data_from_directories,
    compute_custom_data_hash,
    data_entry_key,
    merge_data_table_specs,
)
from unify.data_manager.data_manager import DataManager
from tests.helpers import _handle_project


def _write_table(
    root: Path,
    context: str,
    *,
    description: str = "",
    fields: dict[str, str] | None = None,
    seed_key: str,
    rows: list[dict[str, object]],
    destination: str = "personal",
    auto_sync: bool = True,
) -> Path:
    table_dir = root
    for part in context.split("/"):
        table_dir = table_dir / part
    table_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "description": description,
        "fields": fields or {},
        "seed_key": seed_key,
        "destination": destination,
        "auto_sync": auto_sync,
    }
    (table_dir / META_JSON_FILENAME).write_text(json.dumps(meta) + "\n")
    lines = "\n".join(json.dumps(row) for row in rows)
    (table_dir / ROWS_JSONL_FILENAME).write_text(lines + ("\n" if lines else ""))
    return table_dir


@pytest.fixture
def custom_data_dir(tmp_path: Path) -> Path:
    root = tmp_path / "custom_data"
    root.mkdir()
    _write_table(
        root,
        "CRM/ReferenceCodes",
        description="Reference codes",
        fields={"code": "str", "label": "str"},
        seed_key="code",
        rows=[
            {"code": "A1", "label": "Alpha"},
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
def data_manager_factory():
    managers = []

    def _create():
        ContextRegistry.forget(DataManager, "Data")
        ContextRegistry.forget(DataManager, "Data/Meta")
        dm = DataManager()
        managers.append(dm)
        return dm

    yield _create

    for dm in managers:
        try:
            for table in dm.list_tables(include_column_info=False):
                dm.delete_table(table, dangerous_ok=True)
        except Exception:
            pass


def test_data_entry_key():
    assert (
        data_entry_key(context="CRM/ReferenceCodes", seed_value="A1")
        == "CRM/ReferenceCodes|A1"
    )


def test_collect_custom_data_finds_tables(custom_data_dir):
    tables = collect_custom_data(path=custom_data_dir)
    assert "CRM/ReferenceCodes" in tables
    assert "TeamFacts" in tables
    assert "Skipped" not in tables


def test_collect_custom_data_row_fields(custom_data_dir):
    tables = collect_custom_data(path=custom_data_dir)
    row = tables["CRM/ReferenceCodes"]["rows"][0]
    assert row["code"] == "A1"
    assert row["custom_key"] == "CRM/ReferenceCodes|A1"
    assert row["custom_hash"]


def test_merge_data_table_specs_merges_fields_and_rows():
    base = {
        "CRM/ReferenceCodes": {
            "fields": {"code": "str"},
            "seed_key": "code",
            "rows": [
                {
                    "code": "A1",
                    "custom_key": "CRM/ReferenceCodes|A1",
                    "custom_hash": "a",
                },
            ],
        },
    }
    overlay = {
        "CRM/ReferenceCodes": {
            "fields": {"label": "str"},
            "rows": [
                {
                    "code": "A1",
                    "label": "Alpha",
                    "custom_key": "CRM/ReferenceCodes|A1",
                    "custom_hash": "b",
                },
            ],
        },
    }
    merged = merge_data_table_specs(base, overlay)
    assert "code" in merged["CRM/ReferenceCodes"]["fields"]
    assert "label" in merged["CRM/ReferenceCodes"]["fields"]
    assert len(merged["CRM/ReferenceCodes"]["rows"]) == 1
    assert merged["CRM/ReferenceCodes"]["rows"][0]["label"] == "Alpha"


def test_collect_data_from_directories_later_dir_wins(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    _write_table(first, "CRM/ReferenceCodes", seed_key="code", rows=[{"code": "A1"}])
    _write_table(
        second,
        "CRM/ReferenceCodes",
        seed_key="code",
        rows=[{"code": "A1", "label": "Alpha"}],
        fields={"label": "str"},
    )
    tables = collect_data_from_directories([first, second])
    assert tables["CRM/ReferenceCodes"]["rows"][0]["label"] == "Alpha"


def test_compute_custom_data_hash_empty():
    assert compute_custom_data_hash(source_tables={}) == ""


@pytest.mark.requires_orchestra
@pytest.mark.asyncio
async def test_sync_custom_data_inserts_rows(
    data_manager_factory,
    custom_data_dir,
):
    _handle_project("DataManagerCustomSync")
    dm = data_manager_factory()
    source = {
        context: table
        for context, table in collect_custom_data(path=custom_data_dir).items()
        if (table.get("destination") or "personal") == "personal"
    }
    assert dm.sync_custom(source_tables=source) is True
    rows = dm.filter(
        "CRM/ReferenceCodes",
        filter="custom_hash != None",
        limit=10,
    )
    assert len(rows) == 1
    assert rows[0]["code"] == "A1"


@pytest.mark.requires_orchestra
@pytest.mark.asyncio
async def test_sync_custom_data_is_idempotent(
    data_manager_factory,
    custom_data_dir,
):
    _handle_project("DataManagerCustomSync")
    dm = data_manager_factory()
    source = {
        context: table
        for context, table in collect_custom_data(path=custom_data_dir).items()
        if (table.get("destination") or "personal") == "personal"
    }
    assert dm.sync_custom(source_tables=source) is True
    dm._custom_data_synced = False
    assert dm.sync_custom(source_tables=source) is False
