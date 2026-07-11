from __future__ import annotations

import pytest

from unify.dashboard_manager.dashboard_manager import (
    DashboardManager,
    _require_dashboard_context,
)


def test_require_dashboard_context_accepts_qualified_dashboards_path():
    assert (
        _require_dashboard_context("org123/42/Dashboards/Tiles", "Tiles")
        == "org123/42/Dashboards/Tiles"
    )


def test_table_context_for_root_keeps_dashboards_out_of_data_namespace():
    dm = DashboardManager.__new__(DashboardManager)
    assert (
        dm._table_context_for_root("org123/42", "Dashboards/Tiles")
        == "org123/42/Dashboards/Tiles"
    )


def test_dashboard_manager_rejects_data_nested_dashboard_contexts():
    dm = DashboardManager.__new__(DashboardManager)
    with pytest.raises(RuntimeError, match="Data namespace"):
        dm._table_context_for_root("org123/42/Data", "Dashboards/Tiles")


def test_dashboard_manager_rejects_unqualified_dashboard_contexts():
    with pytest.raises(RuntimeError, match="fully qualified"):
        _require_dashboard_context("Dashboards/Tiles", "Tiles")
