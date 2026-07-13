"""Regression tests for FileManager helpers that delegate into DataManager.ingest."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from unify.data_manager.simulated import SimulatedDataManager
from unify.file_manager.managers.utils.ingest_ops import ingest_table_batch
from unify.file_manager.managers.utils.task_functions import execute_create_file_record
from unify.file_manager.types.config import FilePipelineConfig
from unify.file_manager.types.file import FileRecord


def test_ingest_table_batch_forwards_resume_row_count_validation():
    dm = SimulatedDataManager()

    result = ingest_table_batch(
        data_manager=dm,
        context="test/fm_bridge",
        rows=[{"x": 1}, {"x": 2}, {"x": 3}],
        fields={"x": "int"},
        skip_rows=1,
        expected_total_rows=3,
    )

    assert result.rows_inserted == 2
    assert [row["x"] for row in dm.filter("test/fm_bridge")] == [2, 3]


def test_ingest_table_batch_rejects_resume_row_count_mismatch():
    dm = SimulatedDataManager()

    with pytest.raises(ValueError, match="expected 4"):
        ingest_table_batch(
            data_manager=dm,
            context="test/fm_bridge_mismatch",
            rows=[{"x": 1}, {"x": 2}, {"x": 3}],
            fields={"x": "int"},
            skip_rows=1,
            expected_total_rows=4,
        )

    assert dm.filter("test/fm_bridge_mismatch") == []


def test_create_file_record_retries_delayed_read_visibility(monkeypatch):
    file_manager = MagicMock()
    file_manager._ctx = "scope/FileRecords/Local"
    file_manager._adapter.get_file.side_effect = FileNotFoundError
    parse_result = MagicMock(trace=None)

    monkeypatch.setattr(
        "unify.file_manager.managers.utils.source_info.source_info_for_file",
        lambda **_: SimpleNamespace(
            size_bytes=1,
            created_at=None,
            modified_at=None,
        ),
    )
    monkeypatch.setattr(
        FileRecord,
        "to_file_record_entry",
        lambda **_: MagicMock(),
    )
    monkeypatch.setattr(
        "unify.file_manager.managers.utils.ops.create_file_record",
        lambda *_args, **_kwargs: {
            "outcome": "file created successfully",
            "details": {},
        },
    )

    lookup_results = iter([None, 7])
    monkeypatch.setattr(
        "unify.file_manager.managers.utils.ingest_ops.get_file_id_from_path",
        lambda **_: next(lookup_results),
    )

    result = execute_create_file_record(
        file_manager=file_manager,
        file_path="/tmp/report.txt",
        parse_result=parse_result,
        config=FilePipelineConfig(
            retry={
                "retry_delay_seconds": 0,
                "jitter_ratio": 0,
            },
        ),
    )

    assert result["file_id"] == 7
    assert result["storage_id"] == "7"
