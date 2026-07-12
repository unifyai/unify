"""Docstring / schema stability for KnowledgeManager public methods."""

from __future__ import annotations

import inspect
import json

from unify.common.llm_helpers import method_to_schema, methods_to_tool_dict
from unify.knowledge_manager.base import BaseKnowledgeManager
from unify.knowledge_manager.knowledge_manager import KnowledgeManager
from unify.knowledge_manager.simulated import SimulatedKnowledgeManager

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
)


def test_public_methods_have_substantial_docstrings():
    for name in _PUBLIC_METHODS:
        doc = (getattr(BaseKnowledgeManager, name).__doc__ or "").strip()
        assert doc, f"BaseKnowledgeManager.{name} is missing a docstring"
        assert len(doc) >= 100, f"Docstring for {name} is too short (len={len(doc)})"


def test_actor_facing_tool_schemas_are_stable():
    """Schemas for KnowledgeManager_* tools must be deterministic."""
    km = SimulatedKnowledgeManager()
    tools = methods_to_tool_dict(
        km.search,
        km.filter,
        km.get_knowledge,
        km.add_knowledge,
        km.update_knowledge,
        km.delete_knowledge,
        km.invalidate_knowledge,
        km.supersede_knowledge,
        km.reconcile_sources,
        include_class_name=True,
    )
    assert tools
    for name, fn in tools.items():
        assert name.startswith("KnowledgeManager_"), name
        schema1 = method_to_schema(fn, name)
        schema2 = method_to_schema(fn, name)
        assert schema1 == schema2, f"Schema for {name} is non-deterministic"
        assert (fn.__doc__ or "").strip(), f"Tool {name} missing docstring"
        serialized = json.dumps(schema1)
        assert "related_function_ids" not in serialized
        assert "related_guidance_ids" not in serialized
        assert "orphaned" not in serialized


def test_real_and_simulated_share_signatures():
    for name in _PUBLIC_METHODS:
        real_params = list(
            inspect.signature(getattr(KnowledgeManager, name)).parameters,
        )
        sim_params = list(
            inspect.signature(getattr(SimulatedKnowledgeManager, name)).parameters,
        )
        base_params = list(
            inspect.signature(getattr(BaseKnowledgeManager, name)).parameters,
        )
        assert sim_params == base_params, name
        # KnowledgeManager may append destination guidance / signature on writes;
        # parameter names must still cover the base contract.
        assert set(base_params) <= set(real_params), name


def test_scoped_filter_helpers_compose_status_scope_and_exclusions():
    km = object.__new__(KnowledgeManager)
    km._filter_scope = "kind == 'policy'"
    km._exclude_ids = frozenset({3, 1})

    composed = km._scoped_filter("title == 'A'")
    assert "title == 'A'" in composed
    assert "status == 'active'" in composed
    assert "kind == 'policy'" in composed
    assert "knowledge_id not in [1, 3]" in composed

    # Explicit status in the caller filter suppresses the active default.
    with_status = km._scoped_filter("status == 'invalidated'")
    assert with_status.count("status == 'invalidated'") == 1
    assert "status == 'active'" not in with_status

    # get_knowledge path: default_active=False
    no_default = km._scoped_filter("knowledge_id == 9", default_active=False)
    assert "status == 'active'" not in no_default
    assert "knowledge_id == 9" in no_default
