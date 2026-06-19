"""Tile data-source scope coverage for DashboardManager."""

from droid.dashboard_manager.types.tile import FilterBinding
from droid.session_details import SESSION_DETAILS
from tests.dashboard_manager.helpers import (
    active_read_root,
    create_context_if_missing,
    fresh_dashboard_manager,
    serialized_binding_context,
)
from tests.helpers import _handle_project


def _create_work_order_contexts(personal_root: str, *team_ids: int) -> None:
    """Provision the data roots referenced by live tile bindings."""
    create_context_if_missing(f"{personal_root}/Data/WorkOrders")
    for team_id in team_ids:
        create_context_if_missing(f"Teams/{team_id}/Data/WorkOrders")


@_handle_project
def test_tile_data_scope_uses_dashboard_destination_for_fresh_bindings(
    dashboard_manager_teams,
):
    """Dashboard-scoped bindings inherit the destination root."""
    tile_team, _ = dashboard_manager_teams
    manager = fresh_dashboard_manager()
    personal_root = active_read_root()

    _create_work_order_contexts(personal_root, tile_team)

    inherited = manager.create_tile(
        "<div id='kpi'></div>",
        title="Team Tile",
        data_bindings=[FilterBinding(context="Data/WorkOrders", alias="orders")],
        destination=f"team:{tile_team}",
    )
    assert inherited.succeeded, inherited.error
    inherited_tile = manager.get_tile(inherited.token)
    assert inherited_tile.data_scope == "dashboard"
    assert (
        serialized_binding_context(inherited_tile)
        == f"Teams/{tile_team}/Data/WorkOrders"
    )


@_handle_project
def test_tile_data_scope_can_bind_private_tile_to_shared_data(
    dashboard_manager_teams,
):
    """Explicit data_scope binds fresh data independently from tile destination."""
    _, data_team = dashboard_manager_teams
    manager = fresh_dashboard_manager()
    personal_root = active_read_root()

    _create_work_order_contexts(personal_root, data_team)

    scoped = manager.create_tile(
        "<div id='watch'></div>",
        title="Personal Watch Tile",
        data_bindings=[FilterBinding(context="Data/WorkOrders", alias="orders")],
        destination="personal",
        data_scope=f"team:{data_team}",
    )
    assert scoped.succeeded, scoped.error
    scoped_tile = manager.get_tile(scoped.token)
    assert scoped_tile.data_scope == f"team:{data_team}"
    assert (
        serialized_binding_context(scoped_tile) == f"Teams/{data_team}/Data/WorkOrders"
    )


@_handle_project
def test_tile_data_scope_update_requires_fresh_bindings_and_resets_when_cleared(
    dashboard_manager_teams,
):
    """Updating data_scope is tied to fresh bindings and resets with no bindings."""
    tile_team, data_team = dashboard_manager_teams
    manager = fresh_dashboard_manager()
    personal_root = active_read_root()

    _create_work_order_contexts(personal_root, tile_team, data_team)

    scoped = manager.create_tile(
        "<div id='watch'></div>",
        title="Personal Watch Tile",
        data_bindings=[FilterBinding(context="Data/WorkOrders", alias="orders")],
        destination="personal",
        data_scope=f"team:{data_team}",
    )
    assert scoped.succeeded, scoped.error

    updated = manager.update_tile(
        scoped.token,
        data_bindings=[FilterBinding(context="Data/WorkOrders", alias="orders")],
        data_scope=f"team:{tile_team}",
    )
    assert updated.succeeded, updated.error
    retargeted_tile = manager.get_tile(scoped.token)
    assert retargeted_tile.data_scope == f"team:{tile_team}"
    assert (
        serialized_binding_context(retargeted_tile)
        == f"Teams/{tile_team}/Data/WorkOrders"
    )

    error = manager.update_tile(scoped.token, data_scope=f"team:{data_team}")
    assert not error.succeeded
    assert "fresh data_bindings" in error.error

    unchanged_tile = manager.get_tile(scoped.token)
    assert unchanged_tile.data_scope == f"team:{tile_team}"
    assert (
        serialized_binding_context(unchanged_tile)
        == f"Teams/{tile_team}/Data/WorkOrders"
    )

    cleared = manager.update_tile(scoped.token, data_bindings=[])
    assert cleared.succeeded, cleared.error
    cleared_tile = manager.get_tile(scoped.token)
    assert cleared_tile.data_scope == "dashboard"
    assert not cleared_tile.has_data_bindings

    rebound = manager.update_tile(
        scoped.token,
        data_bindings=[FilterBinding(context="Data/WorkOrders", alias="orders")],
    )
    assert rebound.succeeded, rebound.error
    rebound_tile = manager.get_tile(scoped.token)
    assert rebound_tile.data_scope == "dashboard"
    assert (
        serialized_binding_context(rebound_tile) == f"{personal_root}/Data/WorkOrders"
    )


@_handle_project
def test_tile_data_scope_rejects_invalid_or_unbound_scopes(
    dashboard_manager_teams,
):
    """data_scope must be valid and paired with fresh live bindings."""
    _, data_team = dashboard_manager_teams
    manager = fresh_dashboard_manager()
    personal_root = active_read_root()

    _create_work_order_contexts(personal_root, data_team)

    scoped_without_bindings = manager.create_tile(
        "<div></div>",
        title="Scoped Baked Tile",
        data_scope=f"team:{data_team}",
    )
    assert not scoped_without_bindings.succeeded
    assert "fresh data_bindings" in scoped_without_bindings.error

    invalid = manager.create_tile(
        "<div></div>",
        title="Invalid Data Scope",
        data_bindings=[FilterBinding(context="Data/WorkOrders", alias="orders")],
        data_scope="personal",
    )
    assert not invalid.succeeded
    assert "invalid_destination" in invalid.error


def test_simulated_data_scope_matches_real_binding_roots(simulated_dm):
    """Simulated tile data_scope uses the same fresh-binding root rules."""
    original_team_ids = list(SESSION_DETAILS.team_ids)
    SESSION_DETAILS.team_ids = [7, 8]
    try:
        baked = simulated_dm.create_tile(
            "<p>Baked</p>",
            title="Baked",
            data_scope="team:8",
        )
        assert not baked.succeeded
        assert "fresh data_bindings" in baked.error

        shared = simulated_dm.create_tile(
            "<p>Shared</p>",
            title="Shared",
            destination="team:7",
            data_bindings=[FilterBinding(context="Data/Sales", alias="sales")],
        )
        assert shared.succeeded, shared.error
        shared_tile = simulated_dm.get_tile(shared.token)
        assert serialized_binding_context(shared_tile) == "Teams/7/Data/Sales"

        scoped = simulated_dm.create_tile(
            "<p>Scoped</p>",
            title="Scoped",
            destination="personal",
            data_scope="team:8",
            data_bindings=[FilterBinding(context="Data/Sales", alias="sales")],
        )
        assert scoped.succeeded, scoped.error
        scoped_tile = simulated_dm.get_tile(scoped.token)
        assert serialized_binding_context(scoped_tile) == "Teams/8/Data/Sales"
    finally:
        SESSION_DETAILS.team_ids = original_team_ids
