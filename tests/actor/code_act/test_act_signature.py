"""CodeActActor.act must expose a closed keyword signature.

Open ``**kwargs`` / ``additionalProperties: true`` schemas are unreliable on
OpenAI tool calling. Known escape hatches (entrypoint repair) are named
parameters; undeclared keys must not be silently accepted.

``CodeActActor.act`` is wrapped with ``functools.wraps(BaseCodeActActor.act)``,
so ``inspect.signature`` / ``method_to_schema`` follow the base contract —
that contract must stay closed and list the repair fields.
"""

from __future__ import annotations

import inspect

import pytest

from unify.actor.base import BaseCodeActActor
from unify.actor.code_act_actor import CodeActActor
from unify.common.llm_helpers import method_to_schema


def _assert_closed_act_signature(sig: inspect.Signature) -> None:
    assert not any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    ), f"act must not accept **kwargs; got {sig}"
    assert "entrypoint_repair_attempts" in sig.parameters
    assert "entrypoint_repair_context" in sig.parameters


def test_base_and_code_act_signatures_are_closed():
    _assert_closed_act_signature(inspect.signature(BaseCodeActActor.act))
    # wraps(BaseCodeActActor.act) makes this follow the base contract.
    _assert_closed_act_signature(inspect.signature(CodeActActor.act))


def test_act_schema_is_closed():
    schema = method_to_schema(
        CodeActActor.act,
        tool_name="act",
        include_class_name=False,
    )
    params = schema["function"]["parameters"]
    props = params.get("properties") or {}
    assert params.get("additionalProperties") is not True, (
        f"expected closed schema, got additionalProperties="
        f"{params.get('additionalProperties')!r}"
    )
    assert "kwargs" not in props
    assert "entrypoint_repair_attempts" in props
    assert "entrypoint_repair_context" in props
    # Underscored internals are hidden from the LLM-visible schema.
    assert "_parent_chat_context" not in props
    assert "_call_id" not in props
    assert "_reuse_actor_slot" not in props


@pytest.mark.asyncio
async def test_act_rejects_unknown_keyword_arguments():
    actor = CodeActActor(environments=[])
    try:
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            await actor.act("noop", not_a_real_param=True)  # type: ignore[call-arg]
    finally:
        await actor.close()
