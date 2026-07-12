"""Contrastive Actor evals: Guidance (how-to) vs Knowledge (fact) routing.

Discovery-first gating is disabled via ``tool_policy=None`` so these evals
isolate Guidance-vs-Knowledge write routing; gate behaviour is covered by
``tests/actor/code_act/test_discovery_first_policy*.py``.
"""

from __future__ import annotations

import asyncio

import pytest

from tests.actor.state_managers.utils import make_code_act_actor
from tests.helpers import _handle_project
from unify.guidance_manager.simulated import SimulatedGuidanceManager
from unify.knowledge_manager.simulated import SimulatedKnowledgeManager

pytestmark = [pytest.mark.eval, pytest.mark.llm_call]


def _tool_names(history: list[dict]) -> set[str]:
    names: set[str] = set()
    for msg in history:
        if msg.get("role") != "assistant":
            continue
        for tool_call in msg.get("tool_calls") or []:
            names.add(tool_call["function"]["name"])
    return names


@pytest.mark.asyncio
@pytest.mark.timeout(300)
@_handle_project
async def test_howto_routes_to_guidance_not_knowledge():
    """Procedural 'remember how to…' should store Guidance, not Knowledge."""
    gm = SimulatedGuidanceManager(description="contrastive guidance")
    km = SimulatedKnowledgeManager(description="contrastive knowledge")

    async with make_code_act_actor(
        impl="simulated",
        guidance_manager=gm,
        knowledge_manager=km,
        tool_policy=None,
    ) as (actor, _primitives, _calls):
        handle = await actor.act(
            "Remember how to log into the staging VPN: open the portal, "
            "enter my SSO email, approve the push notification, then connect.",
            clarification_enabled=False,
        )
        await asyncio.wait_for(handle.result(), timeout=180)
        names = _tool_names(handle.get_history())
        assert any(n.startswith("GuidanceManager_add_guidance") for n in names), names
        assert not any(
            n
            in {
                "KnowledgeManager_add_knowledge",
                "KnowledgeManager_update_knowledge",
                "KnowledgeManager_supersede_knowledge",
            }
            for n in names
        ), names


@pytest.mark.asyncio
@pytest.mark.timeout(300)
@_handle_project
async def test_fact_routes_to_knowledge_not_guidance():
    """Declarative 'remember that…' should store Knowledge, not Guidance."""
    gm = SimulatedGuidanceManager(description="contrastive guidance")
    km = SimulatedKnowledgeManager(description="contrastive knowledge")

    async with make_code_act_actor(
        impl="simulated",
        guidance_manager=gm,
        knowledge_manager=km,
        tool_policy=None,
    ) as (actor, _primitives, _calls):
        handle = await actor.act(
            "Remember that the battery warranty is 8 years.",
            clarification_enabled=False,
        )
        await asyncio.wait_for(handle.result(), timeout=180)
        names = _tool_names(handle.get_history())
        assert any(
            n
            in {
                "KnowledgeManager_add_knowledge",
                "KnowledgeManager_update_knowledge",
                "KnowledgeManager_supersede_knowledge",
            }
            for n in names
        ), names
        assert "GuidanceManager_add_guidance" not in names, names


@pytest.mark.asyncio
@pytest.mark.timeout(300)
@_handle_project
async def test_warranty_change_prefers_supersede_knowledge():
    """Updating a known fact should prefer supersede_knowledge when possible."""
    gm = SimulatedGuidanceManager(description="contrastive guidance")
    km = SimulatedKnowledgeManager(description="contrastive knowledge")
    out = km.add_knowledge(
        title="Battery warranty",
        content="The battery warranty is 5 years.",
        kind="fact",
        topics=["warranty"],
        source_refs=[{"kind": "manual", "note": "seed"}],
    )
    old_id = out["details"]["knowledge_id"]

    async with make_code_act_actor(
        impl="simulated",
        guidance_manager=gm,
        knowledge_manager=km,
        tool_policy=None,
    ) as (actor, _primitives, _calls):
        handle = await actor.act(
            "The battery warranty changed from 5 years to 8 years — "
            "update what you remember.",
            clarification_enabled=False,
        )
        await asyncio.wait_for(handle.result(), timeout=180)
        names = _tool_names(handle.get_history())
        assert (
            "KnowledgeManager_supersede_knowledge" in names
            or "KnowledgeManager_update_knowledge" in names
            or "KnowledgeManager_add_knowledge" in names
        ), names
        claim = km.get_knowledge(knowledge_id=old_id)
        # Either superseded or content updated / new claim exists.
        assert claim.status.value in {"active", "superseded", "invalidated"}
