"""
CodeActActor routing tests for KnowledgeManager search (simulated managers).

Validates that CodeActActor uses KnowledgeManager JSON tools (not
``execute_function`` / ``primitives.knowledge``) for knowledge recall.
"""

from __future__ import annotations

import pytest

from tests.actor.state_managers.utils import (
    get_code_act_tool_calls,
    make_code_act_actor,
)

pytestmark = [pytest.mark.eval, pytest.mark.llm_call]


KNOWLEDGE_QUESTIONS: list[str] = [
    "Summarise the employee onboarding policy. (use knowledge only).",
    "What are our office hours? (use knowledge only).",
]


@pytest.mark.asyncio
@pytest.mark.timeout(240)
@pytest.mark.parametrize("question", KNOWLEDGE_QUESTIONS)
async def test_code_act_questions_use_knowledge_manager_tools(
    question: str,
    seeded_knowledge_manager,
):
    async with make_code_act_actor(
        impl="simulated",
        knowledge_manager=seeded_knowledge_manager,
    ) as (actor, _primitives, _calls):
        handle = await actor.act(
            f"{question} Do not ask clarifying questions. Do not create any stubs. Proceed with the best interpretation of the request.",
            clarification_enabled=False,
        )
        result = await handle.result()
        assert result is not None

        tool_calls = set(get_code_act_tool_calls(handle))
        km_tools = {n for n in tool_calls if n.startswith("KnowledgeManager_")}
        assert km_tools, f"Expected KnowledgeManager_* tools, saw: {tool_calls}"
        assert "execute_function" not in tool_calls or not any(
            "primitives.knowledge" in n for n in tool_calls
        )
        assert any(
            n
            in {
                "KnowledgeManager_search",
                "KnowledgeManager_filter",
                "KnowledgeManager_get_knowledge",
            }
            for n in km_tools
        ), f"Expected a KnowledgeManager read tool, saw: {km_tools}"
