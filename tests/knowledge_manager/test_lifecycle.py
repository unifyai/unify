"""Lifecycle: invalidate, supersede, and status defaults."""

from __future__ import annotations

from tests.helpers import _handle_project
from unify.knowledge_manager.knowledge_manager import KnowledgeManager
from unify.knowledge_manager.types.knowledge import KnowledgeStatus


def test_invalidate_excludes_from_default_filter(simulated_km):
    kid = simulated_km.add_knowledge(
        title="Old SLA",
        content="Response within 48 hours.",
    )["details"]["knowledge_id"]

    out = simulated_km.invalidate_knowledge(knowledge_id=kid)
    assert out["details"]["knowledge_id"] == kid

    claim = simulated_km.get_knowledge(knowledge_id=kid)
    assert claim.status == KnowledgeStatus.invalidated
    assert simulated_km.filter() == []
    assert simulated_km.filter(filter="status == 'invalidated'")[0].knowledge_id == kid


def test_supersede_wires_lineage(simulated_km):
    old_id = simulated_km.add_knowledge(
        title="Battery warranty",
        content="Warranty is five years.",
    )["details"]["knowledge_id"]

    out = simulated_km.supersede_knowledge(
        old_knowledge_id=old_id,
        title="Battery warranty",
        content="Warranty is eight years.",
        kind="fact",
        topics=["warranty"],
    )
    new_id = out["details"]["new_knowledge_id"]
    assert out["details"]["old_knowledge_id"] == old_id
    assert new_id != old_id

    old = simulated_km.get_knowledge(knowledge_id=old_id)
    new = simulated_km.get_knowledge(knowledge_id=new_id)
    assert old.status == KnowledgeStatus.superseded
    assert old.superseded_by_id == new_id
    assert new.status == KnowledgeStatus.active
    assert old_id in new.supersedes_ids

    assert {c.knowledge_id for c in simulated_km.filter()} == {new_id}
    superseded = simulated_km.filter(filter="status == 'superseded'")
    assert {c.knowledge_id for c in superseded} == {old_id}


def test_supersede_with_existing_new_knowledge_id(simulated_km):
    old_id = simulated_km.add_knowledge(
        title="V1",
        content="First version.",
    )[
        "details"
    ]["knowledge_id"]
    new_id = simulated_km.add_knowledge(
        title="V2",
        content="Second version.",
    )[
        "details"
    ]["knowledge_id"]

    simulated_km.supersede_knowledge(
        old_knowledge_id=old_id,
        new_knowledge_id=new_id,
    )
    old = simulated_km.get_knowledge(knowledge_id=old_id)
    new = simulated_km.get_knowledge(knowledge_id=new_id)
    assert old.status == KnowledgeStatus.superseded
    assert old.superseded_by_id == new_id
    assert old_id in new.supersedes_ids


def test_new_claims_default_active(simulated_km):
    kid = simulated_km.add_knowledge(title="T", content="C")["details"]["knowledge_id"]
    assert simulated_km.get_knowledge(knowledge_id=kid).status == KnowledgeStatus.active


@_handle_project
def test_real_invalidate_excludes_from_default_filter():
    km = KnowledgeManager()
    kid = int(
        km.add_knowledge(
            title="Old SLA",
            content="Response within 48 hours.",
        )[
            "details"
        ]["knowledge_id"],
    )

    km.invalidate_knowledge(knowledge_id=kid)
    claim = km.get_knowledge(knowledge_id=kid)
    assert claim.status == KnowledgeStatus.invalidated
    assert km.filter(filter=f"knowledge_id == {kid}") == []
    invalidated = km.filter(filter="status == 'invalidated'")
    assert {c.knowledge_id for c in invalidated} == {kid}


@_handle_project
def test_real_supersede_wires_lineage():
    km = KnowledgeManager()
    old_id = int(
        km.add_knowledge(
            title="Battery warranty",
            content="Warranty is five years.",
        )[
            "details"
        ]["knowledge_id"],
    )

    out = km.supersede_knowledge(
        old_knowledge_id=old_id,
        title="Battery warranty",
        content="Warranty is eight years.",
        kind="fact",
        topics=["warranty"],
    )
    new_id = int(out["details"]["new_knowledge_id"])
    assert new_id != old_id

    old = km.get_knowledge(knowledge_id=old_id)
    new = km.get_knowledge(knowledge_id=new_id)
    assert old.status == KnowledgeStatus.superseded
    assert old.superseded_by_id == new_id
    assert new.status == KnowledgeStatus.active
    assert old_id in new.supersedes_ids
    assert {c.knowledge_id for c in km.filter()} == {new_id}
