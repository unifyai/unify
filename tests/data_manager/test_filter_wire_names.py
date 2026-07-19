from __future__ import annotations

from unify.common import join_utils
from unify.data_manager.ops import join_ops, plot_ops, table_view_ops


def test_join_tables_uses_filter_in_pair_of_args(monkeypatch):
    captured = {}

    def fake_join_logs(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(join_ops.unisdk, "join_logs", fake_join_logs)

    join_ops.join_tables_impl(
        left_table="Data/left",
        right_table="Data/right",
        join_expr="Data/left.id == Data/right.id",
        dest_table="Data/joined",
        select={"Data/left.id": "id"},
        left_where="active == True",
        right_where="visible == True",
    )

    assert captured["pair_of_args"] == (
        {"context": "Data/left", "filter": "active == True"},
        {"context": "Data/right", "filter": "visible == True"},
    )


def test_common_create_join_uses_filter_in_pair_of_args(monkeypatch):
    captured = {}

    def fake_join_logs(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(join_utils.unisdk, "join_logs", fake_join_logs)

    join_utils.create_join(
        left_context="Data/left",
        right_context="Data/right",
        dest_context="Data/joined",
        join_expr="Data/left.id == Data/right.id",
        select={"Data/left.id": "id"},
        left_where="active == True",
    )

    assert captured["pair_of_args"] == (
        {"context": "Data/left", "filter": "active == True"},
        {"context": "Data/right"},
    )


def test_filter_join_uses_filter_for_join_query(monkeypatch):
    captured = {}

    def fake_join_query(**kwargs):
        captured.update(kwargs)
        return {"logs": []}

    monkeypatch.setattr(join_ops.unisdk, "join_query", fake_join_query)

    assert (
        join_ops.filter_join_impl(
            tables=["Data/left", "Data/right"],
            join_expr="Data/left.id == Data/right.id",
            select={"Data/left.id": "id"},
            left_where="active == True",
            result_where="id > 1",
        )
        == []
    )
    assert captured["pair_of_args"] == (
        {"context": "Data/left", "filter": "active == True"},
        {"context": "Data/right"},
    )
    assert captured["filter"] == "id > 1"
    assert "filter_expr" not in captured


def test_table_view_project_config_uses_filter():
    assert table_view_ops._build_project_config_dict(
        project_name="Project",
        context="Data/table",
        filter="active == True",
    ) == {
        "project_name": "Project",
        "context": "Data/table",
        "filter": "active == True",
    }


def test_plot_project_config_uses_filter():
    assert plot_ops._build_project_config_dict(
        project_name="Project",
        context="Data/table",
        filter="active == True",
    ) == {
        "project_name": "Project",
        "context": "Data/table",
        "randomize": False,
        "filter": "active == True",
    }
