"""SimulatedKnowledgeManager parity with BaseKnowledgeManager."""

from __future__ import annotations

from unify.knowledge_manager.base import BaseKnowledgeManager
from unify.knowledge_manager.simulated import SimulatedKnowledgeManager
from unify.knowledge_manager.types.knowledge import KnowledgeStatus

_PUBLIC_METHODS = (
    "search",
    "filter",
    "get_knowledge",
    "add_knowledge",
    "update_knowledge",
    "delete_knowledge",
    "invalidate_knowledge",
    "supersede_knowledge",
    "reconcile_sources",
    "clear",
)


def test_simulated_km_docstrings_match_base():
    for name in _PUBLIC_METHODS:
        base_doc = getattr(BaseKnowledgeManager, name).__doc__
        sim_doc = getattr(SimulatedKnowledgeManager, name).__doc__
        assert base_doc and sim_doc, f"{name} missing docstring"
        assert (
            base_doc.strip() in sim_doc.strip()
        ), f"{name} docstring was not copied via functools.wraps"


def test_simulated_crud_and_clear(simulated_km):
    kid = simulated_km.add_knowledge(
        title="Temp",
        content="temporary fact",
        kind="insight",
    )["details"]["knowledge_id"]
    assert simulated_km.get_knowledge(knowledge_id=kid).status == KnowledgeStatus.active

    simulated_km.clear()
    assert simulated_km.filter() == []
    assert simulated_km.search(k=5) == []

    # Remains usable after clear
    kid2 = simulated_km.add_knowledge(title="After clear", content="ok")["details"][
        "knowledge_id"
    ]
    assert simulated_km.get_knowledge(knowledge_id=kid2).title == "After clear"


def test_simulated_implements_all_base_methods():
    for name in _PUBLIC_METHODS:
        assert hasattr(SimulatedKnowledgeManager, name)
        assert callable(getattr(SimulatedKnowledgeManager, name))
