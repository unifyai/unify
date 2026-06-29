from __future__ import annotations

import unisdk

from tests.destination_routing_helpers import (
    manager_routing_context as manager_routing_context,  # noqa: F401
)
from unity.guidance_manager.guidance_manager import GuidanceManager


def test_guidance_writes_route_to_destination_and_reads_merge_roots(
    manager_routing_context,
):
    """Guidance mutations target one root while filters see personal and shared rows."""

    _, team_id = manager_routing_context
    manager = GuidanceManager()

    manager.add_guidance(title="Private rule", content="For my private drafts.")
    manager.add_guidance(
        title="Team rule",
        content="For shared operations.",
        destination=f"team:{team_id}",
    )

    assert [row.entries["title"] for row in unisdk.get_logs(context=manager._ctx)] == [
        "Private rule",
    ]
    assert [
        row.entries["title"]
        for row in unisdk.get_logs(context=f"Teams/{team_id}/Guidance")
    ] == ["Team rule"]
    # Reads also federate over the read-only builtins library; tenant rows
    # are isolated with the provenance flag.
    assert {row.title for row in manager.filter(filter="is_builtin == False")} == {
        "Private rule",
        "Team rule",
    }
    all_titles = {row.title for row in manager.filter(limit=100)}
    assert {"Private rule", "Team rule"} <= all_titles
    assert any(title.startswith("[anthropic] ") for title in all_titles)

    outcome = manager.delete_guidance(guidance_id=1, destination="team:404404")
    assert outcome["error_kind"] == "invalid_destination"
