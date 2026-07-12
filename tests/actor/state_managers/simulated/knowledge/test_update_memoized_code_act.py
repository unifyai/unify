"""
CodeActActor routing tests for KnowledgeManager writes with FunctionManager
discovery tools available (simulated managers).

Validates that even with FunctionManager tools exposed, knowledge mutations
route via KnowledgeManager JSON tools.

Discovery-first gating is disabled via ``tool_policy=None`` so these evals
isolate write routing; gate behaviour is covered by
``tests/actor/code_act/test_discovery_first_policy*.py``.
"""

from __future__ import annotations

import pytest

from tests.actor.state_managers.utils import (
    get_code_act_tool_calls,
    make_code_act_actor,
)

pytestmark = [pytest.mark.eval, pytest.mark.llm_call]


UPDATE_QUERIES: list[str] = [
    # Novel claim (seed already has office hours / onboarding).
    "Store: Parking validation is available at reception for visitors.",
    "Update the onboarding policy to require security training in week one.",
]


@pytest.mark.asyncio
@pytest.mark.timeout(240)
@pytest.mark.parametrize("request_text", UPDATE_QUERIES)
async def test_code_act_update_uses_knowledge_manager_with_fm_tools(
    request_text: str,
    seeded_knowledge_manager,
):
    async with make_code_act_actor(
        impl="simulated",
        include_function_manager_tools=True,
        knowledge_manager=seeded_knowledge_manager,
        tool_policy=None,
    ) as (actor, _primitives, _calls):
        handle = await actor.act(
            f"{request_text} Do not ask clarifying questions. Do not create any stubs. Proceed with the best interpretation of the request.",
            clarification_enabled=False,
        )
        result = await handle.result()
        assert result is not None

        tool_calls = set(get_code_act_tool_calls(handle))
        km_tools = {n for n in tool_calls if n.startswith("KnowledgeManager_")}
        assert km_tools, f"Expected KnowledgeManager_* tools, saw: {tool_calls}"
        assert any(
            n
            in {
                "KnowledgeManager_add_knowledge",
                "KnowledgeManager_update_knowledge",
                "KnowledgeManager_supersede_knowledge",
            }
            for n in km_tools
        ), f"Expected a KnowledgeManager write tool, saw: {km_tools}"
