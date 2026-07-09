"""Tests for custom required-file collection and synchronization."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from unify.file_manager.custom_files import (
    FILES_MAP_FILENAME,
    collect_custom_files,
    collect_files_from_directories,
    compute_custom_files_hash,
    merge_file_mappings,
    normalize_dest_path,
)
from unify.file_manager.filesystem_adapters.local_adapter import LocalFileSystemAdapter
from unify.file_manager.managers.file_manager import FileManager


def _write_files_dir(
    root: Path,
    mapping: dict[str, str],
    *,
    files: dict[str, str] | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    for rel, content in (files or {}).items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    (root / FILES_MAP_FILENAME).write_text(json.dumps(mapping) + "\n")
    return root


def test_normalize_dest_path():
    assert normalize_dest_path("Templates/a.txt") == "/Templates/a.txt"
    assert normalize_dest_path("/Templates/a.txt") == "/Templates/a.txt"
    with pytest.raises(ValueError):
        normalize_dest_path("../escape.txt")


def test_collect_custom_files_expands_dirs_and_files(tmp_path: Path):
    root = _write_files_dir(
        tmp_path / "files",
        {
            "templates/": "/Templates",
            "policies/handbook.pdf": "/Policies/handbook.pdf",
        },
        files={
            "templates/a.txt": "alpha",
            "templates/nested/b.txt": "beta",
            "policies/handbook.pdf": "handbook",
        },
    )
    collected = collect_custom_files(path=root)
    assert set(collected) == {
        "/Templates/a.txt",
        "/Templates/nested/b.txt",
        "/Policies/handbook.pdf",
    }
    assert collected["/Templates/a.txt"]["content_hash"]
    assert Path(collected["/Templates/a.txt"]["source_path"]).read_text() == "alpha"


def test_collect_custom_files_warns_and_skips_missing(tmp_path: Path):
    root = _write_files_dir(
        tmp_path / "files",
        {"missing.txt": "/Missing.txt", "present.txt": "/Present.txt"},
        files={"present.txt": "ok"},
    )
    collected = collect_custom_files(path=root)
    assert set(collected) == {"/Present.txt"}


def test_collect_custom_files_rejects_path_escape(tmp_path: Path):
    root = _write_files_dir(
        tmp_path / "files",
        {"../outside.txt": "/Outside.txt", "ok.txt": "/Ok.txt"},
        files={"ok.txt": "ok"},
    )
    outside = tmp_path / "outside.txt"
    outside.write_text("nope")
    collected = collect_custom_files(path=root)
    assert set(collected) == {"/Ok.txt"}


def test_merge_and_collect_from_directories_later_wins(tmp_path: Path):
    first = _write_files_dir(
        tmp_path / "first",
        {"a.txt": "/Shared/a.txt"},
        files={"a.txt": "first"},
    )
    second = _write_files_dir(
        tmp_path / "second",
        {"a.txt": "/Shared/a.txt"},
        files={"a.txt": "second"},
    )
    merged = collect_files_from_directories([first, second])
    assert Path(merged["/Shared/a.txt"]["source_path"]).read_text() == "second"
    assert merge_file_mappings({"x": {"a": 1}}, {"x": {"a": 2}})["x"]["a"] == 2


def test_compute_custom_files_hash_empty():
    assert compute_custom_files_hash(source_files={}) == ""


def test_local_adapter_write_file_overwrite(tmp_path: Path):
    root = tmp_path / "local"
    adapter = LocalFileSystemAdapter(str(root), enable_sync=False)
    source = tmp_path / "src.txt"
    source.write_text("v1")
    written = adapter.write_file("/Docs/note.txt", source, overwrite=True)
    assert written == "/Docs/note.txt"
    assert (root / "Docs" / "note.txt").read_text() == "v1"
    source.write_text("v2")
    adapter.write_file("/Docs/note.txt", source, overwrite=True)
    assert (root / "Docs" / "note.txt").read_text() == "v2"


def test_local_adapter_write_file_rejects_escape(tmp_path: Path):
    adapter = LocalFileSystemAdapter(str(tmp_path / "local"), enable_sync=False)
    source = tmp_path / "src.txt"
    source.write_text("x")
    with pytest.raises(ValueError):
        adapter.write_file("../escape.txt", source)


def test_sync_custom_files_required_overlay(tmp_path: Path, monkeypatch):
    local_root = tmp_path / "local"
    local_root.mkdir()
    (local_root / "UserNotes").mkdir()
    (local_root / "UserNotes" / "keep.txt").write_text("assistant-owned")

    files_dir = _write_files_dir(
        tmp_path / "files",
        {"seed.txt": "/Seeded/seed.txt"},
        files={"seed.txt": "seed-v1"},
    )
    source = collect_custom_files(path=files_dir)

    adapter = LocalFileSystemAdapter(str(local_root), enable_sync=False)
    fm = FileManager.__new__(FileManager)
    fm._adapter = adapter
    fm._meta_ctx = "FileRecords/Meta"
    fm._custom_files_synced = False
    fm._custom_files_synced_contexts = set()
    fm._default_index_context = "FileRecords/Local"
    fm._default_files_root = "Files/Local"
    fm._context_binding = MagicMock()
    fm._context_binding.get.return_value = None
    fm._fs_alias = "Local"

    monkeypatch.setattr(
        fm,
        "_file_contexts_for_destination",
        lambda destination: ("FileRecords/Local", "Files/Local"),
    )
    monkeypatch.setattr(fm, "_get_stored_custom_files_hash", lambda: "")
    stored: dict[str, str] = {}
    monkeypatch.setattr(
        fm,
        "_store_custom_files_hash",
        lambda value: stored.update({"hash": value}),
    )

    ingest_calls: list[str] = []
    sync_calls: list[str] = []

    def fake_ensure(**kwargs):
        dest = kwargs["dest_path"]
        adapter.write_file(dest, kwargs["source_path"], overwrite=True)
        ingest_calls.append(dest)
        return True

    monkeypatch.setattr(fm, "_ensure_required_file", fake_ensure)

    assert fm.sync_custom(source_files=source) is True
    assert (local_root / "Seeded" / "seed.txt").read_text() == "seed-v1"
    assert (local_root / "UserNotes" / "keep.txt").read_text() == "assistant-owned"
    assert ingest_calls == ["/Seeded/seed.txt"]
    assert stored["hash"] == compute_custom_files_hash(source_files=source)

    # Idempotent when hash matches
    monkeypatch.setattr(
        fm,
        "_get_stored_custom_files_hash",
        lambda: stored["hash"],
    )
    ingest_calls.clear()
    assert fm.sync_custom(source_files=source) is False
    assert ingest_calls == []

    # Removing a map entry must not delete the upstream file
    (local_root / "Seeded" / "seed.txt").write_text("seed-v1")
    monkeypatch.setattr(fm, "_get_stored_custom_files_hash", lambda: "stale")
    assert fm.sync_custom(source_files={}) is True
    assert (local_root / "Seeded" / "seed.txt").exists()
    assert sync_calls == []


def test_ensure_required_file_overwrites_and_reuses_path(tmp_path: Path, monkeypatch):
    local_root = tmp_path / "local"
    adapter = LocalFileSystemAdapter(str(local_root), enable_sync=False)
    source = tmp_path / "src.txt"
    source.write_text("v1")
    content_hash = __import__("hashlib").sha256(b"v1").hexdigest()

    fm = FileManager.__new__(FileManager)
    fm._adapter = adapter
    fm._context_binding = MagicMock()
    fm._context_binding.get.return_value = None

    indexed = {"value": False}

    class _Storage:
        @property
        def indexed_exists(self):
            return indexed["value"]

    monkeypatch.setattr(
        "unify.file_manager.managers.utils.storage.describe_file",
        lambda *_args, **_kwargs: _Storage(),
    )
    monkeypatch.setattr(
        fm,
        "_using_file_destination",
        lambda destination: __import__("contextlib").nullcontext(),
    )
    ingest_calls: list[tuple] = []
    sync_calls: list[str] = []

    def fake_ingest(file_paths, config=None, destination=None):
        ingest_calls.append((file_paths, destination))
        return MagicMock()

    def fake_sync(*, file_path, destination=None):
        sync_calls.append(file_path)
        return {"outcome": "sync complete"}

    monkeypatch.setattr(fm, "ingest_files", fake_ingest)
    monkeypatch.setattr(fm, "_sync", fake_sync)

    changed = fm._ensure_required_file(
        dest_path="/Docs/note.txt",
        source_path=str(source),
        content_hash=content_hash,
        destination=None,
    )
    assert changed is True
    assert (local_root / "Docs" / "note.txt").read_text() == "v1"
    assert ingest_calls == [("/Docs/note.txt", None)]
    assert sync_calls == []

    # Matching content + already indexed → no-op
    indexed["value"] = True
    ingest_calls.clear()
    changed = fm._ensure_required_file(
        dest_path="/Docs/note.txt",
        source_path=str(source),
        content_hash=content_hash,
        destination=None,
    )
    assert changed is False
    assert ingest_calls == []
    assert sync_calls == []

    # Divergent content → overwrite + re-sync (same file_path identity)
    source.write_text("v2")
    new_hash = __import__("hashlib").sha256(b"v2").hexdigest()
    changed = fm._ensure_required_file(
        dest_path="/Docs/note.txt",
        source_path=str(source),
        content_hash=new_hash,
        destination=None,
    )
    assert changed is True
    assert (local_root / "Docs" / "note.txt").read_text() == "v2"
    assert sync_calls == ["/Docs/note.txt"]
