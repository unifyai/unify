from __future__ import annotations

import asyncio
from typing import Any

import pytest

from unity.common.act_llm_profiles import CURRENT_ACT_LLM_PROFILE
from unity.common.async_tool_loop import SteerableToolHandle
from unity.actor.code_act_actor import CodeActActor


class _FakeClient:
    def __init__(self, model: str | None, kwargs: dict[str, Any]) -> None:
        self.model = model
        self.kwargs = kwargs
        self.system_message = None

    def set_system_message(self, message: Any) -> "_FakeClient":
        self.system_message = message
        return self


class _ImmediateHandle(SteerableToolHandle):
    def __init__(self, result_text: str = "done") -> None:
        self.result_text = result_text

    async def ask(
        self,
        question: str,
        *,
        _parent_chat_context: list[dict] | None = None,
    ) -> SteerableToolHandle:
        return _ImmediateHandle(f"asked: {question}")

    async def interject(
        self,
        message: str,
        *,
        _parent_chat_context_cont: list[dict] | None = None,
    ) -> None:
        return None

    async def stop(self, reason: str | None = None) -> None:
        return None

    async def pause(self) -> str | None:
        return None

    async def resume(self) -> str | None:
        return None

    def done(self) -> bool:
        return True

    async def result(self) -> str:
        return self.result_text

    async def next_clarification(self) -> dict:
        return {}

    async def next_notification(self) -> dict:
        return {}

    async def answer_clarification(self, call_id: str, answer: str) -> None:
        return None


class _DummyFunctionManager:
    def search_functions(self, *args: Any, **kwargs: Any) -> list:
        return []

    def filter_functions(self, *args: Any, **kwargs: Any) -> list:
        return []

    def list_functions(self, *args: Any, **kwargs: Any) -> list:
        return []

    async def add_functions(self, *args: Any, **kwargs: Any) -> dict:
        return {}

    async def delete_function(self, *args: Any, **kwargs: Any) -> dict:
        return {}


class _DummyGuidanceManager:
    def search(self, *args: Any, **kwargs: Any) -> list:
        return []

    def filter(self, *args: Any, **kwargs: Any) -> list:
        return []

    def get_guidance(self, *args: Any, **kwargs: Any) -> None:
        return None

    def add_guidance(self, *args: Any, **kwargs: Any) -> dict:
        return {}

    def update_guidance(self, *args: Any, **kwargs: Any) -> dict:
        return {}

    def delete_guidance(self, *args: Any, **kwargs: Any) -> dict:
        return {}


def _make_actor() -> CodeActActor:
    return CodeActActor(
        function_manager=_DummyFunctionManager(),  # type: ignore[arg-type]
        guidance_manager=_DummyGuidanceManager(),  # type: ignore[arg-type]
        can_store=False,
    )


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_act_llm_profile_overrides_main_actor_client(monkeypatch):
    calls: list[tuple[str | None, dict[str, Any]]] = []
    active_profiles_at_loop_start: list[str] = []

    def fake_new_llm_client(model: str | None = None, **kwargs: Any) -> _FakeClient:
        calls.append((model, kwargs))
        return _FakeClient(model, kwargs)

    def fake_start_async_tool_loop(*args: Any, **kwargs: Any) -> _ImmediateHandle:
        active_profiles_at_loop_start.append(CURRENT_ACT_LLM_PROFILE.get().name)
        return _ImmediateHandle()

    monkeypatch.setattr(
        "unity.actor.code_act_actor.new_llm_client",
        fake_new_llm_client,
    )
    monkeypatch.setattr(
        "unity.actor.code_act_actor.start_async_tool_loop",
        fake_start_async_tool_loop,
    )

    actor = _make_actor()
    try:
        default_handle, high_handle = await asyncio.gather(
            actor.act(
                "default profile task",
                clarification_enabled=False,
                can_store=False,
            ),
            actor.act(
                "high profile task",
                clarification_enabled=False,
                can_store=False,
                llm_profile="gpt_5_5_high",
            ),
        )
        assert isinstance(default_handle, _ImmediateHandle)
        assert isinstance(high_handle, _ImmediateHandle)
    finally:
        await actor.close()

    assert (None, {}) in calls
    assert ("gpt-5.5@openai", {"reasoning_effort": "high"}) in calls
    assert "default" in active_profiles_at_loop_start
    assert "gpt_5_5_high" in active_profiles_at_loop_start
    assert CURRENT_ACT_LLM_PROFILE.get().name == "default"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_act_rejects_unknown_llm_profile():
    actor = _make_actor()
    try:
        with pytest.raises(ValueError, match="Unknown act LLM profile"):
            await actor.act(
                "bad profile task",
                clarification_enabled=False,
                llm_profile="not_real",
            )
    finally:
        await actor.close()
