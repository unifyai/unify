from __future__ import annotations

import uuid

import pytest
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
def test_file_ingest_personal_destination_lands_in_the_home_root(tmp_path):
    implicit_path = tmp_path / f"implicit-{uuid.uuid4().hex}.txt"
    explicit_path = tmp_path / f"explicit-{uuid.uuid4().hex}.txt"
    implicit_path.write_text("implicit research note", encoding="utf-8")
    explicit_path.write_text("explicit research note", encoding="utf-8")

    manager = FileManager(adapter=LocalFileSystemAdapter(None))

    manager.ingest_files(str(implicit_path), config=_compact_no_embedding_config())
    manager.ingest_files(
        str(explicit_path),
        config=_compact_no_embedding_config(),
        destination="personal",
    )

    for path in (implicit_path, explicit_path):
        rows = manager._data_manager.filter(
            context=manager._ctx,
            filter=f"file_path == {str(path)!r}",
        )
        assert len(rows) == 1

    storage = manager.describe(str(explicit_path))
    assert storage.index_context == manager._ctx

    paths = {row["file_path"] for row in manager.filter_files()}
    assert str(implicit_path) in paths
    assert str(explicit_path) in paths


@_handle_project
def test_file_invalid_destination_returns_tool_error(tmp_path):
    file_path = tmp_path / f"bad-{uuid.uuid4().hex}.txt"
    file_path.write_text("bad destination", encoding="utf-8")
    manager = FileManager(adapter=LocalFileSystemAdapter(None))

    outcome = manager.ingest_files(
        str(file_path),
        config=_compact_no_embedding_config(),
        destination="team:99999999",
    )

    assert outcome["error_kind"] == "invalid_destination"
    assert outcome["details"]["destination"] == "team:99999999"


@_handle_project
def test_file_clear_invalid_destination_returns_tool_error():
    manager = FileManager(adapter=LocalFileSystemAdapter(None))

    outcome = manager.clear(destination="team:99999999")

    assert outcome["error_kind"] == "invalid_destination"
    assert outcome["details"]["destination"] == "team:99999999"


@_handle_project
def test_file_save_attachment_invalid_destination_returns_tool_error():
    manager = FileManager(adapter=LocalFileSystemAdapter(None))

    outcome = manager.save_attachment(
        "attachment-id",
        "report.txt",
        b"report",
        auto_ingest=False,
        destination="team:99999999",
    )

    assert outcome["error_kind"] == "invalid_destination"
    assert outcome["details"]["destination"] == "team:99999999"


@pytest.mark.parametrize(
    "call",
    [
        lambda manager, path: manager.sync(
            file_path=path,
            destination="team:99999999",
        ),
        lambda manager, path: manager.rename_file(
            file_id_or_path=path,
            new_name="renamed.txt",
            destination="team:99999999",
        ),
        lambda manager, path: manager.move_file(
            file_id_or_path=path,
            new_parent_path="Archive",
            destination="team:99999999",
        ),
        lambda manager, path: manager.delete_file(
            file_id_or_path=path,
            destination="team:99999999",
        ),
        lambda manager, path: manager.ingest_files(
            path,
            config=_compact_no_embedding_config(),
            destination="team:99999999",
        ),
    ],
)
@_handle_project
def test_file_write_tools_return_tool_error_for_invalid_destination(tmp_path, call):
    file_path = tmp_path / f"bad-{uuid.uuid4().hex}.txt"
    file_path.write_text("bad destination", encoding="utf-8")
    manager = FileManager(adapter=LocalFileSystemAdapter(None))

    outcome = call(manager, str(file_path))

    assert outcome["error_kind"] == "invalid_destination"
    assert outcome["details"]["destination"] == "team:99999999"
