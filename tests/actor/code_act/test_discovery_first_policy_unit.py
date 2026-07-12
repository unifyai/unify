"""Symbolic: discovery-first policy gates on FM + GM + KM when present."""

from __future__ import annotations

from unify.actor.code_act_actor import (
    _default_tool_policy,
    _discovery_preferred_for_schema,
    _is_discovery_gate_schema,
)


def _identity_filter(tools):
    return tools


def _unpack(result):
    """Normalize 2-tuple or eager 3-tuple policy returns."""
    mode, tools = result[0], result[1]
    opts = result[2] if len(result) == 3 else None
    return mode, tools, opts


def test_default_tool_policy_requires_km_when_present():
    tools = {
        "FunctionManager_search_functions": object(),
        "FunctionManager_list_functions": object(),
        "FunctionManager_filter_functions": object(),
        "FunctionManager_add_functions": object(),
        "GuidanceManager_search": object(),
        "GuidanceManager_filter": object(),
        "GuidanceManager_get_guidance": object(),
        "GuidanceManager_add_guidance": object(),
        "KnowledgeManager_search": object(),
        "KnowledgeManager_filter": object(),
        "KnowledgeManager_get_knowledge": object(),
        "KnowledgeManager_add_knowledge": object(),
        "execute_code": object(),
    }
    policy = _default_tool_policy(
        has_fm_tools=True,
        has_gm_tools=True,
        filter_tools=_identity_filter,
        has_km_tools=True,
    )
    mode, gated, opts = _unpack(policy(0, tools, called_tools=[]))
    assert mode == "required"
    assert opts == {"eager": True}
    assert "execute_code" not in gated
    assert set(gated) == {
        "FunctionManager_search_functions",
        "GuidanceManager_search",
        "KnowledgeManager_search",
    }
    assert "FunctionManager_add_functions" not in gated
    assert "GuidanceManager_add_guidance" not in gated
    assert "KnowledgeManager_add_knowledge" not in gated

    mode, gated, opts = _unpack(
        policy(
            1,
            tools,
            called_tools=[
                "FunctionManager_search_functions",
                "GuidanceManager_search",
            ],
        ),
    )
    assert mode == "required"
    assert opts == {"eager": True}
    assert set(gated) == {"KnowledgeManager_search"}

    mode, full, opts = _unpack(
        policy(
            2,
            tools,
            called_tools=[
                "FunctionManager_search_functions",
                "GuidanceManager_search",
                "KnowledgeManager_search",
            ],
        ),
    )
    assert mode == "auto"
    assert opts is None
    assert "execute_code" in full
    assert "GuidanceManager_add_guidance" in full
    assert "KnowledgeManager_add_knowledge" in full


def test_default_tool_policy_falls_back_when_preferred_missing():
    tools = {
        "FunctionManager_list_functions": object(),
        "GuidanceManager_filter": object(),
        "KnowledgeManager_get_knowledge": object(),
        "execute_code": object(),
    }
    policy = _default_tool_policy(
        has_fm_tools=True,
        has_gm_tools=True,
        filter_tools=_identity_filter,
        has_km_tools=True,
    )
    mode, gated, opts = _unpack(policy(0, tools, called_tools=[]))
    assert mode == "required"
    assert opts == {"eager": True}
    assert set(gated) == {
        "FunctionManager_list_functions",
        "GuidanceManager_filter",
        "KnowledgeManager_get_knowledge",
    }


def test_default_tool_policy_skips_km_gate_when_absent():
    tools = {
        "FunctionManager_search_functions": object(),
        "FunctionManager_list_functions": object(),
        "GuidanceManager_search": object(),
        "GuidanceManager_add_guidance": object(),
        "execute_code": object(),
    }
    policy = _default_tool_policy(
        has_fm_tools=True,
        has_gm_tools=True,
        filter_tools=_identity_filter,
        has_km_tools=False,
    )
    mode, gated, opts = _unpack(policy(0, tools, called_tools=[]))
    assert mode == "required"
    assert opts == {"eager": True}
    assert "GuidanceManager_add_guidance" not in gated
    assert set(gated) == {
        "FunctionManager_search_functions",
        "GuidanceManager_search",
    }

    mode, full, opts = _unpack(
        policy(
            1,
            tools,
            called_tools=[
                "FunctionManager_search_functions",
                "GuidanceManager_search",
            ],
        ),
    )
    assert mode == "auto"
    assert opts is None
    assert "execute_code" in full
    assert "GuidanceManager_add_guidance" in full


def test_discovery_gate_schema_detection():
    assert _is_discovery_gate_schema(
        [
            "FunctionManager_search_functions",
            "GuidanceManager_search",
            "compress_context",
        ],
    )
    assert _is_discovery_gate_schema(
        [
            "FunctionManager_search_functions",
            "GuidanceManager_search",
            "KnowledgeManager_search",
        ],
    )
    assert not _is_discovery_gate_schema(
        [
            "FunctionManager_search_functions",
            "GuidanceManager_search",
            "execute_code",
        ],
    )
    assert not _is_discovery_gate_schema(["FunctionManager_search_functions"])


def test_discovery_preferred_for_schema_orders_families():
    preferred = _discovery_preferred_for_schema(
        [
            "KnowledgeManager_search",
            "FunctionManager_search_functions",
            "GuidanceManager_search",
            "compress_context",
        ],
    )
    assert [name for name, _args in preferred] == [
        "FunctionManager_search_functions",
        "GuidanceManager_search",
        "KnowledgeManager_search",
    ]
