"""Tests for pipeline transport handle construction and materialization gating."""

from __future__ import annotations

from unify.common.pipeline.transport import _can_materialize
from unify.common.pipeline.types import (
    CsvFileHandle,
    InlineRowsHandle,
    ObjectStoreArtifactHandle,
    XlsxSheetHandle,
)

# ---------------------------------------------------------------------------
# _can_materialize gating
# ---------------------------------------------------------------------------


class TestCanMaterialize:
    """Verify which handles are materialised into artifacts."""

    def test_inline_rows_with_data_is_materializable(self):
        handle = InlineRowsHandle(rows=[{"a": 1}], columns=["a"], row_count=1)
        assert _can_materialize(handle) is True

    def test_inline_rows_empty_is_not_materializable(self):
        handle = InlineRowsHandle(rows=[], columns=["a"], row_count=0)
        assert _can_materialize(handle) is False

    def test_object_store_handle_is_not_materializable(self):
        handle = ObjectStoreArtifactHandle(
            storage_uri="file:///artifacts/artifact.jsonl",
            artifact_format="jsonl",
            source_local_path="",
            logical_path="data.csv",
        )
        assert _can_materialize(handle) is False

    def test_csv_with_local_uri_is_materializable(self):
        handle = CsvFileHandle(
            storage_uri="file:///tmp/data.csv",
            logical_path="data.csv",
            source_local_path="/tmp/data.csv",
            columns=["a", "b"],
        )
        assert _can_materialize(handle) is True

    def test_xlsx_with_local_uri_is_materializable(self):
        handle = XlsxSheetHandle(
            storage_uri="file:///tmp/data.xlsx",
            logical_path="data.xlsx",
            source_local_path="/tmp/data.xlsx",
            sheet_name="Sheet1",
            columns=["a", "b"],
        )
        assert _can_materialize(handle) is True
