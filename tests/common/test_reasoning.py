from __future__ import annotations

import inspect
from typing import Any

import pytest
from pydantic import BaseModel

from droid.common import reasoning


class _FakeClient:
    def __init__(self, result: Any):
        self.result = result
        self.generate_calls: list[dict[str, Any]] = []

    async def generate(self, **kwargs: Any) -> Any:
        self.generate_calls.append(kwargs)
        return self.result


@pytest.mark.asyncio
async def test_query_llm_uses_standard_client_defaults(monkeypatch: pytest.MonkeyPatch):
    client = _FakeClient("yes")
    created: list[tuple[str | None, dict[str, Any]]] = []

    def fake_new_llm_client(model: str | None = None, **kwargs: Any) -> _FakeClient:
        created.append((model, kwargs))
        return client

    monkeypatch.setattr(reasoning, "new_llm_client", fake_new_llm_client)

    result = await reasoning.query_llm("Does this need a reply?")

    assert result == "yes"
    assert created == [
        (
            None,
            {
                "async_client": True,
                "stateful": False,
                "origin": "CodeActActor.query_llm",
            },
        ),
    ]
    assert client.generate_calls == [
        {
            "user_message": "Does this need a reply?",
            "system_message": reasoning.DEFAULT_LLM_QUERY_SYSTEM,
            "response_format": None,
            "temperature": 0.0,
        },
    ]


@pytest.mark.asyncio
async def test_query_llm_supports_pydantic_structured_output(
    monkeypatch: pytest.MonkeyPatch,
):
    class Decision(BaseModel):
        category: str
        needs_reply: bool
        confidence: float

    client = _FakeClient(
        '{"category": "billing", "needs_reply": true, "confidence": 0.91}',
    )
    monkeypatch.setattr(
        reasoning,
        "new_llm_client",
        lambda *args, **kwargs: client,
    )

    result = await reasoning.query_llm(
        "Classify this message.",
        system="Use the inbox triage rubric.",
        response_format=Decision,
    )

    assert isinstance(result, Decision)
    assert result.category == "billing"
    assert result.needs_reply is True
    assert result.confidence == 0.91
    assert client.generate_calls[0]["response_format"] is Decision
    assert client.generate_calls[0]["system_message"] == "Use the inbox triage rubric."


@pytest.mark.asyncio
async def test_query_llm_supports_dict_structured_output(
    monkeypatch: pytest.MonkeyPatch,
):
    response_format = {"type": "json_object"}
    client = _FakeClient('{"needs_followup": true, "rationale": "expires soon"}')
    monkeypatch.setattr(
        reasoning,
        "new_llm_client",
        lambda *args, **kwargs: client,
    )

    result = await reasoning.query_llm(
        "Does this certificate need follow-up?",
        response_format=response_format,
    )

    assert result == {"needs_followup": True, "rationale": "expires soon"}
    assert client.generate_calls[0]["response_format"] == response_format


def test_query_llm_docstring_contains_actor_usage_guidance():
    doc = reasoning.query_llm.__doc__ or ""

    assert "Good uses" in doc
    assert "Prefer direct symbolic code instead" in doc
    assert "Structured output for downstream control flow" in doc
    assert "Unstructured -> structured work" in doc
    assert "Unstructured -> unstructured work" in doc
    assert "draft, respond, rewrite" in doc
    assert "EmailDraftDecision" in doc
    assert "Anti-patterns" in doc
    assert "billable UniLLM call" in doc
    assert "substring checks" in doc
    assert "label-specific canned prose or templates" in doc


def test_llm_query_prompt_context_uses_introspected_signatures():
    context = reasoning.get_llm_query_prompt_context()

    assert f"async def query_llm{inspect.signature(reasoning.query_llm)}" in context
    assert f"def list_llms{inspect.signature(reasoning.list_llms)}" in context


def test_llm_query_prompt_context_includes_model_selection_guidance():
    context = reasoning.get_llm_query_prompt_context()

    assert "LLM Query Helpers: `query_llm(...)` And `list_llms(...)`" in context
    assert "Choosing A Model For `query_llm(...)`" in context
    assert "Artificial Analysis (https://artificialanalysis.ai/)" in context
    assert "comparing model price, speed, latency" in context
    assert "ARC Prize leaderboard: https://arcprize.org/leaderboard" in context
    assert "Use `list_llms()` to inspect" in context
    assert "Supported UniLLM endpoints currently registered" not in context
    assert "gpt-5.5@openai, gpt-5.5" not in context
    assert "Do not put benchmark browsing or" in context


def test_list_llms_returns_registered_endpoint_strings():
    endpoints = reasoning.list_llms()

    assert "gpt-4.1-nano@openai" in endpoints
    assert "gpt-5.5@openai" in endpoints
    assert all("@" in endpoint for endpoint in endpoints)


def test_list_llms_filters_by_provider():
    endpoints = reasoning.list_llms("openai")

    assert "gpt-5.5@openai" in endpoints
    assert all(endpoint.endswith("@openai") for endpoint in endpoints)
