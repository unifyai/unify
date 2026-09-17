from __future__ import annotations

from unify.data_manager.base import BaseDataManager


def test_data_write_tools_expose_destination_guidance():
    for method_name in (
        "create_table",
        "delete_table",
        "rename_table",
        "create_column",
        "delete_column",
        "rename_column",
        "create_derived_column",
        "join_tables",
        "insert_rows",
        "update_rows",
        "delete_rows",
        "ingest",
        "ensure_vector_column",
        "vectorize_rows",
    ):
        doc = (getattr(BaseDataManager, method_name).__doc__ or "").strip()

        assert "destination : str | None" in doc
        assert "personal" in doc
        assert "team:" not in doc
