"""Fixtures for DashboardManager tests."""

import time

import pytest

from droid.common.context_registry import ContextRegistry
from droid.dashboard_manager.simulated import SimulatedDashboardManager
from droid.session_details import SESSION_DETAILS


@pytest.fixture
def simulated_dm():
    """Fresh SimulatedDashboardManager instance."""
    return SimulatedDashboardManager()


@pytest.fixture
def seeded_dm(simulated_dm):
    """SimulatedDashboardManager pre-seeded with sample tiles and a dashboard."""
    t1 = simulated_dm.create_tile(
        "<h1>KPI Card</h1>",
        title="KPI Overview",
        description="Key performance indicators",
    )
    t2 = simulated_dm.create_tile(
        "<div id='chart'>chart placeholder</div>",
        title="Revenue Chart",
    )

    from droid.dashboard_manager.types.dashboard import TilePosition

    simulated_dm.create_dashboard(
        "Test Dashboard",
        tiles=[
            TilePosition(tile_token=t1.token, x=0, y=0, w=6, h=4),
            TilePosition(tile_token=t2.token, x=6, y=0, w=6, h=4),
        ],
    )
    return simulated_dm


@pytest.fixture
def dashboard_manager_teams():
    """Assign unique shared-team memberships for dashboard routing tests."""
    base_id = time.time_ns() % 1_000_000_000
    team_ids = (base_id, base_id + 1)
    original_team_ids = list(SESSION_DETAILS.team_ids)
    SESSION_DETAILS.team_ids = list(team_ids)
    ContextRegistry.clear()
    try:
        yield team_ids
    finally:
        SESSION_DETAILS.team_ids = original_team_ids
        ContextRegistry.clear()
