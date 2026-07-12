"""Symbolic wiring: StorageCheck exposes Knowledge as the third store."""

from __future__ import annotations

from types import SimpleNamespace

from unify.actor.code_act_actor import _build_storage_tools
from unify.actor.prompt_builders import build_code_act_prompt
from unify.common.llm_helpers import methods_to_tool_dict
from unify.knowledge_manager.simulated import SimulatedKnowledgeManager


def _tool_stub(*_args, **_kwargs):
    return None


def _storage_actor_stub(*, with_knowledge: bool = True):
    # methods_to_tool_dict needs real bound methods (with __self__) so tool
    # names get the KnowledgeManager_ / GuidanceManager_ class prefix.
    function_manager = SimpleNamespace(
        search_functions=_tool_stub,
        filter_functions=_tool_stub,
        list_functions=_tool_stub,
        add_functions=_tool_stub,
        delete_function=_tool_stub,
        reconcile_dependencies=_tool_stub,
        add_venv=_tool_stub,
        list_venvs=_tool_stub,
        get_venv=_tool_stub,
        update_venv=_tool_stub,
        delete_venv=_tool_stub,
        set_function_venv=_tool_stub,
        get_function_venv=_tool_stub,
    )
    from unify.guidance_manager.simulated import SimulatedGuidanceManager

    guidance_manager = SimulatedGuidanceManager(description="storage wiring")
    knowledge_manager = (
        SimulatedKnowledgeManager(description="storage wiring")
        if with_knowledge
        else None
    )

    class _Actor:
        def __init__(self):
            self.function_manager = function_manager
            self.guidance_manager = guidance_manager
            self.knowledge_manager = knowledge_manager

    return _Actor()


def test_storage_tools_include_knowledge_manager_triad():
    tools, _ = _build_storage_tools(actor=_storage_actor_stub(), ask_tools={})
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
    }
    # Storage loop intentionally omits reconcile_sources (passive maintenance).
    assert "KnowledgeManager_reconcile_sources" not in km_tools


def test_storage_tools_omit_knowledge_when_manager_absent():
    tools, _ = _build_storage_tools(
        actor=_storage_actor_stub(with_knowledge=False),
        ask_tools={},
    )
    assert not any(name.startswith("KnowledgeManager_") for name in tools)


def test_code_act_prompt_hard_knowledge_discovery_policy():
    km = SimulatedKnowledgeManager(description="prompt wiring")
    tools = methods_to_tool_dict(
        km.search,
        km.filter,
        km.get_knowledge,
        km.add_knowledge,
        km.supersede_knowledge,
        include_class_name=True,
    )
    # Hard Knowledge guidance is part of the execute_code library triad section.
    tools["execute_code"] = _tool_stub
    prompt = build_code_act_prompt(
        environments={},
        tools=tools,
        discovery_first_policy=True,
    )
    assert "Soft Knowledge discovery" not in prompt
    assert "KnowledgeManager_search" in prompt
    assert "hard discovery-first" in prompt or "Discovery-First Policy" in prompt
    assert "stale_reasons" in prompt
    assert "KnowledgeManager_add_knowledge" in prompt
    assert "supersede_knowledge" in prompt
    assert "primitives.knowledge" not in prompt
