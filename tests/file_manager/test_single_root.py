"""Ingested files land in the one FileRecords index under the session root."""

from __future__ import annotations

import uuid

from tests.helpers import _handle_project
from unify.file_manager.filesystem_adapters.local_adapter import LocalFileSystemAdapter
from unify.file_manager.managers.file_manager import FileManager
from unify.file_manager.types.config import FilePipelineConfig


def _compact_no_embedding_config() -> FilePipelineConfig:
    config = FilePipelineConfig()
    config.embed.strategy = "off"
    config.output.return_mode = "compact"
    config.ingest.table_ingest = False
    return config


@_handle_project
def test_file_ingest_lands_in_the_session_index(tmp_path):
    file_path = tmp_path / f"note-{uuid.uuid4().hex}.txt"
    file_path.write_text("research note", encoding="utf-8")

    manager = FileManager(adapter=LocalFileSystemAdapter(None))
    manager.ingest_files(str(file_path), config=_compact_no_embedding_config())

    rows = manager._data_manager.filter(
        context=manager._ctx,
        filter=f"file_path == {str(file_path)!r}",
    )
    assert len(rows) == 1

    storage = manager.describe(str(file_path))
    assert storage.index_context == manager._ctx
    assert str(file_path) in {row["file_path"] for row in manager.filter_files()}
