from __future__ import annotations

from typing import Any

import pytest
import unillm
from pydantic import BaseModel

from unity.common.reasoning import list_llms, query_llm
from unity.function_manager import venv_runner
from unity.function_manager.execution_env import (
    ENVIRONMENT_MODULES,
    create_execution_globals,
)


def test_execution_globals_expose_llm_helpers_and_unillm():
    globals_dict = create_execution_globals()

    assert globals_dict["query_llm"] is query_llm
    assert globals_dict["list_llms"] is list_llms
    assert "reason" not in globals_dict
    assert globals_dict["unillm"] is unillm
    assert "new_llm_client" not in globals_dict
    assert "unillm" in ENVIRONMENT_MODULES


@pytest.mark.asyncio
async def test_venv_runner_query_llm_routes_through_runtime_rpc(
    monkeypatch: pytest.MonkeyPatch,
):
    class Decision(BaseModel):
        category: str
        needs_reply: bool

    calls: list[tuple[str, dict[str, Any]]] = []

    async def fake_rpc_call(path: str, kwargs: dict[str, Any]) -> dict[str, Any]:
        calls.append((path, kwargs))
        return {"category": "scheduling", "needs_reply": True}

    monkeypatch.setattr(venv_runner, "rpc_call_async", fake_rpc_call)

    result = await venv_runner.query_llm(
        "Classify this email.",
        system="Use the inbox rubric.",
        response_format=Decision,
    )

    assert isinstance(result, Decision)
    assert result.category == "scheduling"
    assert result.needs_reply is True

    assert calls[0][0] == "runtime.query_llm"
    sent_kwargs = calls[0][1]
    assert sent_kwargs["prompt"] == "Classify this email."
    assert sent_kwargs["system"] == "Use the inbox rubric."
    assert sent_kwargs["temperature"] == 0.0
    assert sent_kwargs["response_format"]["type"] == "json_schema"
    assert sent_kwargs["response_format"]["json_schema"]["name"] == "Decision"


def test_venv_runner_list_llms_routes_through_runtime_rpc(
    monkeypatch: pytest.MonkeyPatch,
):
    calls: list[tuple[str, dict[str, Any]]] = []

    def fake_rpc_call(path: str, kwargs: dict[str, Any]) -> list[str]:
        calls.append((path, kwargs))
        return ["gpt-5.5@openai"]

    monkeypatch.setattr(venv_runner, "rpc_call_sync", fake_rpc_call)

    assert venv_runner.list_llms("openai") == ["gpt-5.5@openai"]
    assert calls == [("runtime.list_llms", {"provider": "openai"})]
