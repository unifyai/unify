"""Destination routing for Data-owned contexts.

Every Data write lands under the session's personal root. ``destination``
accepts only ``None`` or ``"personal"``; any other label is rejected with a
structured ``invalid_destination`` tool error before anything is written.
"""

from __future__ import annotations

import uuid

import pytest
from tests.helpers import _handle_project
from unify.common.context_registry import ContextRegistry
from unify.data_manager.data_manager import DataManager
from unify.manager_registry import ManagerRegistry

_UNKNOWN_DESTINATION = "shared:99999999"


def _fresh_manager() -> DataManager:
    ManagerRegistry.clear()
    ContextRegistry.clear()
    return DataManager()


@_handle_project
def test_data_writes_route_to_personal_root():
    table_suffix = f"destination_routing/{uuid.uuid4().hex}"
    manager = _fresh_manager()

    default_path = manager.create_table(
        table_suffix,
        fields={"label": "str", "amount": "int"},
    )
    personal_path = manager.create_table(
        table_suffix,
        fields={"label": "str", "amount": "int"},
        destination="personal",
    )

    assert personal_path == default_path
    assert personal_path.endswith(f"/Data/{table_suffix}")
    assert not personal_path.startswith("Data/")

    manager.insert_rows(personal_path, [{"label": "first", "amount": 1}])
    manager.insert_rows(
        table_suffix,
        [{"label": "second", "amount": 2}],
        destination="personal",
    )

    assert manager.describe_table(table_suffix).context == personal_path
    assert "label" in manager.get_columns(table_suffix)
    assert personal_path in manager.list_tables(
        prefix=table_suffix,
        include_column_info=False,
    )

    for reference in (table_suffix, f"Data/{table_suffix}", personal_path):
        rows = manager.filter(reference, columns=["label", "amount"])
        assert {row["label"] for row in rows} == {"first", "second"}
    assert manager.reduce(table_suffix, metric="sum", columns="amount") == 3

    prefixed_path = manager.create_table(
        f"Data/{table_suffix}/prefixed_default",
        fields={"label": "str"},
    )
    assert prefixed_path.endswith(f"/Data/{table_suffix}/prefixed_default")
    assert not prefixed_path.startswith("Data/")


@_handle_project
def test_data_prefixed_paths_resolve_under_personal_root():
    table_suffix = f"prefixed_personal/{uuid.uuid4().hex}"
    manager = _fresh_manager()

    personal_path = manager.create_table(
        f"Data/{table_suffix}",
        fields={"label": "str"},
    )
    manager.insert_rows(
        f"Data/{table_suffix}",
        [{"label": "personal"}],
    )

    assert personal_path.endswith(f"/Data/{table_suffix}")
    assert manager.filter(f"Data/{table_suffix}", columns=["label"]) == [
        {"label": "personal"},
    ]


@_handle_project
def test_data_invalid_destination_returns_tool_error():
    manager = _fresh_manager()

    outcome = manager.create_table(
        f"bad_destination/{uuid.uuid4().hex}",
        destination=_UNKNOWN_DESTINATION,
    )

    assert outcome["error_kind"] == "invalid_destination"
    assert outcome["details"]["destination"] == _UNKNOWN_DESTINATION


@pytest.mark.parametrize(
    "call",
    [
        lambda manager, context: manager.create_table(
            context,
            destination=_UNKNOWN_DESTINATION,
        ),
        lambda manager, context: manager.delete_table(
            context,
            dangerous_ok=True,
            destination=_UNKNOWN_DESTINATION,
        ),
        lambda manager, context: manager.rename_table(
            context,
            f"{context}_renamed",
            destination=_UNKNOWN_DESTINATION,
        ),
        lambda manager, context: manager.create_column(
            context,
            column_name="label",
            column_type="str",
            destination=_UNKNOWN_DESTINATION,
        ),
        lambda manager, context: manager.delete_column(
            context,
            column_name="label",
            destination=_UNKNOWN_DESTINATION,
        ),
        lambda manager, context: manager.rename_column(
            context,
            old_name="label",
            new_name="name",
            destination=_UNKNOWN_DESTINATION,
        ),
        lambda manager, context: manager.create_derived_column(
            context,
            column_name="total",
            equation="amount * 2",
            destination=_UNKNOWN_DESTINATION,
        ),
        lambda manager, context: manager.join_tables(
            left_table=context,
            right_table=f"{context}_right",
            join_expr=f"{context}.id == {context}_right.id",
            dest_table=f"{context}_joined",
            select={f"{context}.id": "id"},
            destination=_UNKNOWN_DESTINATION,
        ),
        lambda manager, context: manager.insert_rows(
            context,
            [{"label": "x"}],
            destination=_UNKNOWN_DESTINATION,
        ),
        lambda manager, context: manager.update_rows(
            context,
            updates={"label": "y"},
            filter="label == 'x'",
            destination=_UNKNOWN_DESTINATION,
        ),
        lambda manager, context: manager.delete_rows(
            context,
            filter="label == 'x'",
            destination=_UNKNOWN_DESTINATION,
        ),
        lambda manager, context: manager.ingest(
            context,
            rows=[{"label": "x"}],
            destination=_UNKNOWN_DESTINATION,
        ),
        lambda manager, context: manager.ensure_vector_column(
            context,
            source_column="label",
            destination=_UNKNOWN_DESTINATION,
        ),
        lambda manager, context: manager.vectorize_rows(
            context,
            source_column="label",
            destination=_UNKNOWN_DESTINATION,
        ),
    ],
)
@_handle_project
def test_data_write_tools_return_tool_error_for_invalid_destination(call):
    manager = _fresh_manager()

    outcome = call(manager, f"bad_destination/{uuid.uuid4().hex}")

    assert outcome["error_kind"] == "invalid_destination"
    assert outcome["details"]["destination"] == _UNKNOWN_DESTINATION
