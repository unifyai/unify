"""Symbolic wiring: MemoryManager.update_knowledge exposes typed KM tools."""

from __future__ import annotations

from unify.common.llm_helpers import methods_to_tool_dict
from unify.knowledge_manager.simulated import SimulatedKnowledgeManager
from unify.memory_manager import prompt_builders as pb


def test_memory_update_knowledge_tool_surface_uses_typed_km_api():
    km = SimulatedKnowledgeManager(description="memory wiring")
    # Mirror the tool dict constructed inside MemoryManager.update_knowledge.
    tools = methods_to_tool_dict(
        km.search,
        km.filter,
        km.get_knowledge,
        km.add_knowledge,
        km.update_knowledge,
        km.invalidate_knowledge,
        km.supersede_knowledge,
        include_class_name=True,
    )
    names = set(tools)
    assert names >= {
        "KnowledgeManager_search",
        "KnowledgeManager_filter",
        "KnowledgeManager_get_knowledge",
        "KnowledgeManager_add_knowledge",
        "KnowledgeManager_update_knowledge",
        "KnowledgeManager_invalidate_knowledge",
        "KnowledgeManager_supersede_knowledge",
    }
    assert not any(name.startswith("primitives.knowledge") for name in names)
    # Memory knowledge loop does not expose hard delete / reconcile.
    assert "KnowledgeManager_delete_knowledge" not in names
    assert "KnowledgeManager_reconcile_sources" not in names


def test_memory_manager_knowledge_prompt_mentions_typed_tools():
    km = SimulatedKnowledgeManager(description="prompt wiring")
    tools = methods_to_tool_dict(
        km.search,
        km.add_knowledge,
        km.supersede_knowledge,
        include_class_name=True,
    )
    prompt = pb.build_knowledge_prompt(tools)
    assert "KnowledgeManager_search" in prompt
    assert "KnowledgeManager_add_knowledge" in prompt
    assert "KnowledgeManager_supersede_knowledge" in prompt
    assert "primitives.knowledge" not in prompt
