"""Personal vs team destination routing for typed knowledge claims."""

from __future__ import annotations

import unisdk

from tests.destination_routing_helpers import (
    manager_routing_context as manager_routing_context,  # noqa: F401
)
from unify.knowledge_manager.knowledge_manager import KnowledgeManager


def test_knowledge_writes_route_to_destination_and_reads_merge_roots(
    manager_routing_context,
):
    """Claims write to one root while filter reads personal and shared rows."""

    _, team_id = manager_routing_context
    manager = KnowledgeManager()

    personal = manager.add_knowledge(
        title="Private escalation",
        content="Personal runbook for escalations.",
    )
    manager.add_knowledge(
        title="Team escalation",
        content="Shared runbook for escalations.",
        destination=f"team:{team_id}",
    )
    personal_id = int(personal["details"]["knowledge_id"])

    personal_rows = unisdk.get_logs(context=manager._ctx)
    shared_rows = unisdk.get_logs(context=f"Teams/{team_id}/Knowledge")

    assert [row.entries["title"] for row in personal_rows] == ["Private escalation"]
    assert [row.entries["title"] for row in shared_rows] == ["Team escalation"]

    # knowledge_id is scoped per context root, so personal and team rows can
    # share the same numeric id. Federated reads merge by title/content.
    titles = {row.title for row in manager.filter()}
    assert titles == {"Private escalation", "Team escalation"}

    assert manager.get_knowledge(knowledge_id=personal_id).title == "Private escalation"

    outcome = manager.delete_knowledge(
        knowledge_id=personal_id,
        destination="team:404404",
    )
    assert outcome["error_kind"] == "invalid_destination"
