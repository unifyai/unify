"""Every Data-owned path resolves to one table under the session root."""

from __future__ import annotations

import uuid

from tests.helpers import _handle_project
from unify.common.context_registry import ContextRegistry
from unify.data_manager.data_manager import DataManager
from unify.manager_registry import ManagerRegistry


def _fresh_manager() -> DataManager:
    ManagerRegistry.clear()
    ContextRegistry.clear()
    return DataManager()


@_handle_project
def test_relative_prefixed_and_qualified_paths_name_the_same_table():
    table_suffix = f"single_root/{uuid.uuid4().hex}"
    manager = _fresh_manager()

    qualified = manager.create_table(
        table_suffix,
        fields={"label": "str", "amount": "int"},
    )
    assert qualified.endswith(f"/Data/{table_suffix}")
    assert not qualified.startswith("Data/")

    manager.insert_rows(qualified, [{"label": "first", "amount": 1}])
    manager.insert_rows(f"Data/{table_suffix}", [{"label": "second", "amount": 2}])
    manager.insert_rows(table_suffix, [{"label": "third", "amount": 3}])

    assert manager.describe_table(table_suffix).context == qualified
    assert "label" in manager.get_columns(f"Data/{table_suffix}")
    assert qualified in manager.list_tables(
        prefix=table_suffix,
        include_column_info=False,
    )

    for reference in (table_suffix, f"Data/{table_suffix}", qualified):
        rows = manager.filter(reference, columns=["label", "amount"])
        assert {row["label"] for row in rows} == {"first", "second", "third"}
    assert manager.reduce(table_suffix, metric="sum", columns="amount") == 6

    nested = manager.create_table(
        f"Data/{table_suffix}/nested",
        fields={"label": "str"},
    )
    assert nested == f"{qualified}/nested"
