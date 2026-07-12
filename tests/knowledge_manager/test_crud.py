"""CRUD roundtrip for typed knowledge claims."""

from __future__ import annotations

import pytest

from tests.helpers import _handle_project
from unify.knowledge_manager.knowledge_manager import KnowledgeManager
from unify.knowledge_manager.types.knowledge import KnowledgeKind, KnowledgeStatus


def test_simulated_add_get_update_delete(simulated_km):
    out = simulated_km.add_knowledge(
        title="Battery warranty",
        content="Tesla battery warranty is eight years.",
        kind=KnowledgeKind.fact,
        topics=["warranty", "tesla"],
    )
    kid = out["details"]["knowledge_id"]
    assert out["outcome"] == "knowledge created successfully"
    assert isinstance(kid, int) and kid > 0

    claim = simulated_km.get_knowledge(knowledge_id=kid)
    assert claim.title == "Battery warranty"
    assert "eight years" in claim.content
    assert claim.kind == KnowledgeKind.fact
    assert claim.status == KnowledgeStatus.active
    assert claim.topics == ["warranty", "tesla"]

    simulated_km.update_knowledge(
        knowledge_id=kid,
        content="Tesla battery warranty is eight years or 100k miles.",
        topics=["warranty", "tesla", "mileage"],
    )
    updated = simulated_km.get_knowledge(knowledge_id=kid)
    assert "100k miles" in updated.content
    assert "mileage" in updated.topics

    simulated_km.delete_knowledge(knowledge_id=kid)
    with pytest.raises(ValueError, match="No knowledge found"):
        simulated_km.get_knowledge(knowledge_id=kid)


@_handle_project
def test_real_add_get_update_delete():
    km = KnowledgeManager()
    out = km.add_knowledge(
        title="Office hours",
        content="Office hours are 9am–5pm Pacific on weekdays.",
        kind="policy",
        topics=["ops"],
    )
    kid = int(out["details"]["knowledge_id"])

    claim = km.get_knowledge(knowledge_id=kid)
    assert claim.title == "Office hours"
    assert claim.kind == KnowledgeKind.policy
    assert claim.status == KnowledgeStatus.active

    km.update_knowledge(knowledge_id=kid, title="Weekday office hours")
    assert km.get_knowledge(knowledge_id=kid).title == "Weekday office hours"

    km.delete_knowledge(knowledge_id=kid)
    with pytest.raises(ValueError, match="No knowledge found"):
        km.get_knowledge(knowledge_id=kid)
