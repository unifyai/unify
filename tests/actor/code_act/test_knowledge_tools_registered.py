"""Symbolic: CodeActActor.get_tools('act') exposes KnowledgeManager_* tools."""

from __future__ import annotations

from unify.actor.code_act_actor import CodeActActor
from unify.knowledge_manager.simulated import SimulatedKnowledgeManager
from unify.manager_registry import ManagerRegistry


def test_code_act_get_tools_registers_knowledge_manager_surface():
    ManagerRegistry.clear()
    km = SimulatedKnowledgeManager(description="tool registration")
    actor = CodeActActor(knowledge_manager=km)
    try:
        tools = dict(actor.get_tools("act"))
        km_tools = {name for name in tools if name.startswith("KnowledgeManager_")}
        assert km_tools >= {
            "KnowledgeManager_search",
            "KnowledgeManager_filter",
            "KnowledgeManager_get_knowledge",
            "KnowledgeManager_add_knowledge",
            "KnowledgeManager_update_knowledge",
            "KnowledgeManager_delete_knowledge",
            "KnowledgeManager_invalidate_knowledge",
            "KnowledgeManager_supersede_knowledge",
            "KnowledgeManager_reconcile_sources",
        }
        assert not any("primitives.knowledge" in name for name in tools)
    finally:
        ManagerRegistry.clear()
