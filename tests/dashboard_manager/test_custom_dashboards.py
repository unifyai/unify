"""Tests for custom dashboard collection and synchronization."""

import json

import pytest
from pathlib import Path

from unify.common.context_registry import ContextRegistry
from unify.dashboard_manager.custom_dashboards import (
    META_JSON_FILENAME,
    ROWS_JSONL_FILENAME,
    collect_custom_dashboards,
    compute_custom_dashboards_hash,
    layout_entry_key,
    merge_dashboard_specs,
    tile_entry_key,
)
from unify.dashboard_manager.dashboard_manager import DashboardManager
from tests.helpers import _handle_project


def _write_tile(
    root: Path,
    tile_id: str,
    *,
    title: str,
    html_content: str = "<div>tile</div>",
    destination: str = "personal",
    auto_sync: bool = True,
) -> Path:
    table_dir = root / "tiles" / tile_id
    table_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "seed_key": "id",
        "destination": destination,
        "auto_sync": auto_sync,
    }
    (table_dir / META_JSON_FILENAME).write_text(json.dumps(meta) + "\n")
    row = {
        "id": tile_id,
        "title": title,
        "html_content": html_content,
    }
    (table_dir / ROWS_JSONL_FILENAME).write_text(json.dumps(row) + "\n")
    return table_dir


def _write_layout(
    root: Path,
    layout_id: str,
    *,
    title: str,
    tile_id: str,
    destination: str = "personal",
) -> Path:
    table_dir = root / "layouts" / layout_id
    table_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "seed_key": "id",
        "destination": destination,
    }
    (table_dir / META_JSON_FILENAME).write_text(json.dumps(meta) + "\n")
    row = {
        "id": layout_id,
        "title": title,
        "positions": [{"tile_id": tile_id, "x": 0, "y": 0, "w": 6, "h": 4}],
    }
    (table_dir / ROWS_JSONL_FILENAME).write_text(json.dumps(row) + "\n")
    return table_dir


@pytest.fixture
def custom_dashboards_dir(tmp_path: Path) -> Path:
    root = tmp_path / "dashboards"
    root.mkdir()
    _write_tile(root, "metrics", title="Metrics")
    _write_layout(root, "overview", title="Overview", tile_id="metrics")
    _write_tile(root, "skipped", title="Skipped", auto_sync=False)
    return root


@pytest.fixture
def dashboard_manager_factory():
    managers = []

    def _create():
        ContextRegistry.forget(DashboardManager, "Dashboards/Tiles")
        ContextRegistry.forget(DashboardManager, "Dashboards/Layouts")
        ContextRegistry.forget(DashboardManager, "Dashboards/Meta")
        dm = DashboardManager()
        managers.append(dm)
        return dm

    yield _create

    for dm in managers:
        try:
            for tile in dm.list_tiles(limit=1000):
                dm.delete_tile(tile.token)
            for dashboard in dm.list_dashboards(limit=1000):
                dm.delete_dashboard(dashboard.token)
        except Exception:
            pass


def test_tile_entry_key():
    assert tile_entry_key(tile_id="metrics") == "tile|metrics"


def test_layout_entry_key():
    assert layout_entry_key(layout_id="overview") == "layout|overview"


def test_collect_custom_dashboards_finds_entities(custom_dashboards_dir):
    entities = collect_custom_dashboards(path=custom_dashboards_dir)
    assert "metrics" in entities["tiles"]
    assert "overview" in entities["layouts"]
    assert "skipped" not in entities["tiles"]


def test_collect_custom_dashboards_row_fields(custom_dashboards_dir):
    entities = collect_custom_dashboards(path=custom_dashboards_dir)
    row = entities["tiles"]["metrics"]["rows"][0]
    assert row["title"] == "Metrics"
    assert row["custom_key"] == "tile|metrics"
    assert row["custom_hash"]


def test_merge_dashboard_specs_merges_rows():
    base = {
        "metrics": {
            "seed_key": "id",
            "rows": [
                {"id": "metrics", "custom_key": "tile|metrics", "custom_hash": "a"},
            ],
        },
    }
    overlay = {
        "metrics": {
            "rows": [
                {
                    "id": "metrics",
                    "title": "Updated",
                    "custom_key": "tile|metrics",
                    "custom_hash": "b",
                },
            ],
        },
    }
    merged = merge_dashboard_specs(base, overlay)
    assert merged["metrics"]["rows"][0]["title"] == "Updated"


def test_compute_custom_dashboards_hash_empty():
    assert compute_custom_dashboards_hash(source_entities={}) == ""


@pytest.mark.requires_orchestra
@pytest.mark.asyncio
async def test_sync_custom_dashboards_inserts_tile_and_layout(
    dashboard_manager_factory,
    custom_dashboards_dir,
):
    _handle_project("DashboardManagerCustomSync")
    dm = dashboard_manager_factory()
    source = collect_custom_dashboards(path=custom_dashboards_dir)
    assert dm.sync_custom(source_entities=source) is True
    tiles = dm.list_tiles(filter="custom_hash != None", limit=10)
    assert len(tiles) == 1
    assert tiles[0].title == "Metrics"
    dashboards = dm.list_dashboards(filter="custom_hash != None", limit=10)
    assert len(dashboards) == 1
    assert dashboards[0].title == "Overview"


@pytest.mark.requires_orchestra
@pytest.mark.asyncio
async def test_sync_custom_dashboards_is_idempotent(
    dashboard_manager_factory,
    custom_dashboards_dir,
):
    _handle_project("DashboardManagerCustomSync")
    dm = dashboard_manager_factory()
    source = collect_custom_dashboards(path=custom_dashboards_dir)
    assert dm.sync_custom(source_entities=source) is True
    dm._custom_dashboards_synced = False
    assert dm.sync_custom(source_entities=source) is False
