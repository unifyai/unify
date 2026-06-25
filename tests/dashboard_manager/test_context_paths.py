from __future__ import annotations

from unittest.mock import patch

import pytest

from unity.dashboard_manager.dashboard_manager import DashboardManager
from unity.common.context_registry import ContextRegistry


def test_dashboard_manager_uses_registered_dashboard_contexts():
    with patch.object(
        ContextRegistry,
        "get_context",
        side_effect=[
            "org123/42/Dashboards/Tiles",
            "org123/42/Dashboards/Layouts",
        ],
    ):
        dm = DashboardManager()

    assert dm._tiles_ctx == "org123/42/Dashboards/Tiles"
    assert dm._layouts_ctx == "org123/42/Dashboards/Layouts"
    assert "/Data/Dashboards/" not in dm._tiles_ctx
    assert "/Data/Dashboards/" not in dm._layouts_ctx


def test_dashboard_manager_fails_when_context_resolution_fails():
    with patch.object(
        ContextRegistry,
        "get_context",
        side_effect=RuntimeError("context unavailable"),
    ):
        with pytest.raises(RuntimeError, match="context unavailable"):
            DashboardManager()


def test_dashboard_manager_rejects_data_nested_dashboard_contexts():
    with patch.object(
        ContextRegistry,
        "get_context",
        return_value="org123/42/Data/Dashboards/Tiles",
    ):
        with pytest.raises(RuntimeError, match="Data namespace"):
            DashboardManager()


def test_dashboard_manager_rejects_unqualified_dashboard_contexts():
    with patch.object(
        ContextRegistry,
        "get_context",
        return_value="Dashboards/Tiles",
    ):
        with pytest.raises(RuntimeError, match="fully qualified"):
            DashboardManager()
