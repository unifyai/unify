from __future__ import annotations

import inspect

from unify.data_manager.base import BaseDataManager


def test_data_write_tools_take_no_root_selector():
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
        method = getattr(BaseDataManager, method_name)
        doc = (method.__doc__ or "").strip()

        assert "destination" not in inspect.signature(method).parameters
        assert "destination :" not in doc
        assert "personal root" not in doc
