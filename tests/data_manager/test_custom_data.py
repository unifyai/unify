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
    # Clear the per-source memo, not a global flag: the stored hash is what
    # must short-circuit the second pass.
    dm._custom_data_synced_sources.clear()
    assert dm.sync_custom(source_tables=source) is False


@pytest.mark.requires_orchestra
@pytest.mark.asyncio
async def test_two_sources_seed_one_table_without_pruning_each_other(
    data_manager_factory,
    custom_data_dir,
    tmp_path,
):
    """The reason the data surface had to be scoped before workflows could
    reach it.

    Both sources seed the same table. Unscoped, the first one's pass reads
    every managed row in the context and prunes the other's on the way
    past — and the other's next pass returns the favour, so the two rows
    ping-pong forever.
    """
    _handle_project("DataManagerCustomSync")
    dm = data_manager_factory()

    deployment = {
        context: table
        for context, table in collect_custom_data(path=custom_data_dir).items()
        if (table.get("destination") or "personal") == "personal"
    }
    workflow_root = tmp_path / "workflow_data"
    workflow_root.mkdir()
    _write_table(
        workflow_root,
        "CRM/ReferenceCodes",
        fields={"code": "str", "label": "str"},
        seed_key="code",
        rows=[{"code": "W1", "label": "Workflow code"}],
    )
    workflow = collect_custom_data(path=workflow_root)

    assert dm.sync_custom_data(source_tables=deployment) is True
    assert dm.sync_custom_data(source_tables=workflow, managed_by="wf_demo") is True

    codes = {
        row["code"]: row.get("managed_by")
        for row in dm.filter("CRM/ReferenceCodes", filter="custom_hash != None")
    }
    assert codes == {"A1": "deployment", "W1": "wf_demo"}

    # The deployment reconciles again: its own row is unchanged and the
    # workflow's is none of its business.
    dm._custom_data_synced_sources.clear()
    dm.sync_custom_data(source_tables=deployment)
    codes = {
        row["code"]: row.get("managed_by")
        for row in dm.filter("CRM/ReferenceCodes", filter="custom_hash != None")
    }
    assert codes == {"A1": "deployment", "W1": "wf_demo"}


@pytest.mark.requires_orchestra
@pytest.mark.asyncio
async def test_an_empty_source_prunes_the_tables_it_seeded_last_pass(
    data_manager_factory,
    tmp_path,
):
    """What an uninstall sends, and why the surface records its tables.

    Rows live in many contexts and the context list comes from the source,
    so an empty source names no table at all. Every other surface gets its
    prune for free because its manager reads one context; this one has to
    remember which tables it seeded, or a workflow's rows outlive it.
    """
    _handle_project("DataManagerCustomSync")
    dm = data_manager_factory()

    root = tmp_path / "wf_tables"
    root.mkdir()
    _write_table(
        root,
        "CRM/WorkflowSeeded",
        fields={"code": "str"},
        seed_key="code",
        rows=[{"code": "W1"}, {"code": "W2"}],
    )
    source = collect_custom_data(path=root)

    assert dm.sync_custom_data(source_tables=source, managed_by="wf_demo") is True
    assert len(dm.filter("CRM/WorkflowSeeded", filter="custom_hash != None")) == 2

    # Uninstall: the surface is handed nothing at all.
    assert dm.sync_custom_data(source_tables={}, managed_by="wf_demo") is True
    assert dm.filter("CRM/WorkflowSeeded", filter="custom_hash != None") == []
    # The table itself stays — it is shared infrastructure, and the rows
    # a workflow *produced* are not the bundle's to delete.
    assert dm._table_exists("CRM/WorkflowSeeded", None)


@pytest.mark.requires_orchestra
@pytest.mark.asyncio
async def test_a_table_with_no_rows_is_still_created(
    data_manager_factory,
    tmp_path,
):
    """Schemas-only is the whole point for a bundle: it ships the shape its
    own job fills. Hashing rows alone made such a source indistinguishable
    from an empty one, so the pass short-circuited and the table never
    appeared."""
    _handle_project("DataManagerCustomSync")
    dm = data_manager_factory()

    root = tmp_path / "schema_only"
    root.mkdir()
    _write_table(
        root,
        "CRM/EmptyByDesign",
        description="Filled at run time, never by the bundle.",
        fields={"code": "str", "label": "str"},
        seed_key="code",
        rows=[],
    )
    source = collect_custom_data(path=root)
    assert source["CRM/EmptyByDesign"]["rows"] == []
    assert compute_custom_data_hash(source_tables=source) != ""

    assert dm.sync_custom_data(source_tables=source, managed_by="wf_demo") is True
    assert dm._table_exists("CRM/EmptyByDesign", None)
