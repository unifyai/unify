from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from unify.actor.code_act_actor import CodeActActor

pytestmark = [pytest.mark.eval, pytest.mark.llm_call]


class ValidationRecoveryResult(BaseModel):
    saw_validation_error: bool
    final_stdout: str


@pytest.mark.asyncio
@pytest.mark.timeout(180)
async def test_validation_error_self_correction():
    """
    Real-world scenario: Actor makes an invalid call, is refused with a message
    explaining what to change, then self-corrects and succeeds.
    """
    ValidationRecoveryResult.model_rebuild()

    actor = CodeActActor(timeout=60, tool_policy=None)

    handle = await actor.act(
        "You MUST do these steps in order:\n"
        "1) Intentionally make an INVALID execute_code call: state_mode='stateless', session_name='oops', code='print(\"hi\")'.\n"
        "   The call is refused: the tool result is a message saying a stateless call cannot carry a session, with a suggestion for what to change.\n"
        "   saw_validation_error is true if and only if you observed that refusal.\n"
        "2) Self-correct by calling execute_code again, but WITHOUT a session (still stateless), same code.\n"
        "3) Return JSON with keys: saw_validation_error (bool), final_stdout (string).\n"
        "Do not invent outputs; only use what you observe from tool results.\n",
        response_format=ValidationRecoveryResult,
        persist=False,
        clarification_enabled=False,
    )
    try:
        res = await asyncio.wait_for(handle.result(), timeout=170)
        assert isinstance(res, ValidationRecoveryResult)
        assert res.saw_validation_error is True
        assert "hi" in (res.final_stdout or "")
    finally:
        await actor.close()
