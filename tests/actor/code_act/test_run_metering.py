"""
Token accounting is split by purpose per run: planning (the CodeAct loop and
its librarian), verification (verifier passes) and repair (repair loops).
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from tests.helpers import _handle_project
from unify.actor.code_act_actor import CodeActActor
from unify.common import llm_meter
from unify.common.llm_client import purpose_from_origin, tag_origin_with_purpose
from unify.common.llm_meter import RunMeter, current_run_meter, handle_run_stats
from unify.function_manager.function_manager import FunctionManager


def test_meter_attributes_events_by_purpose_from_origin():
    meter = RunMeter()
    token = current_run_meter.set(meter)
    try:
        for purpose, prompt, completion in (
            ("planning", 10, 2),
            ("verification", 7, 1),
            ("repair", 3, 3),
            (None, 5, 5),
        ):
            event = SimpleNamespace(
                origin=tag_origin_with_purpose("X", purpose) if purpose else "X",
                response={
                    "usage": {"prompt_tokens": prompt, "completion_tokens": completion},
                },
                provider_cost=0.001,
            )
            llm_meter._on_llm_event(event)
    finally:
        current_run_meter.reset(token)
    snap = meter.snapshot()
    # An untagged origin counts as planning.
    assert snap["tokens"]["planning"] == {"prompt": 15, "completion": 7}
    assert snap["tokens"]["verification"] == {"prompt": 7, "completion": 1}
    assert snap["tokens"]["repair"] == {"prompt": 3, "completion": 3}
    assert snap["calls"] == {"planning": 2, "verification": 1, "repair": 1}
    assert meter.total("verification") == 8
    # No meter bound: the listener is a no-op.
    llm_meter._on_llm_event(
        SimpleNamespace(
            origin="X#purpose=repair",
            response={"usage": {"prompt_tokens": 1, "completion_tokens": 1}},
            provider_cost=None,
        ),
    )
    assert meter.total("repair") == 6
    assert (
        purpose_from_origin(tag_origin_with_purpose("CodeActActor.act", "planning"))
        == "planning"
    )


@pytest.mark.eval
@pytest.mark.llm_call
@pytest.mark.asyncio
@_handle_project
async def test_symbolic_run_records_verification_tokens_and_no_planning():
    fm = FunctionManager()
    fm.add_functions(
        implementations=(
            "def double(x: int) -> int:\n"
            '    """Return twice x as an int."""\n'
            "    return x * 2\n"
        ),
    )
    root_id = fm._get_function_data_by_name(name="double")["function_id"]
    actor = CodeActActor(function_manager=fm)
    try:
        handle = await actor.act(
            "Double a number.",
            entrypoint=root_id,
            entrypoint_kwargs={"x": 21, "task_id": 1, "run_key": "meter-1"},
            clarification_enabled=False,
            persist=False,
        )
        assert await handle.result() == "42"
        tokens = handle.run_stats["tokens"]
        assert tokens["planning"] == {"prompt": 0, "completion": 0}
        assert tokens["repair"] == {"prompt": 0, "completion": 0}
        assert tokens["verification"]["prompt"] > 0
        assert handle.run_stats["verdicts"]["PASS"] >= 1
        assert handle.run_stats["rewinds"] == 0
    finally:
        await actor.close()


@pytest.mark.llm_call
@pytest.mark.asyncio
@_handle_project
async def test_agentic_run_records_planning_tokens():
    fm = FunctionManager(include_primitives=False)
    actor = CodeActActor(function_manager=fm, timeout=120)
    try:
        handle = await actor.act(
            "Reply with the single word OK and nothing else. Do not run any code.",
            can_store=False,
            can_compose=True,
            clarification_enabled=False,
            persist=False,
        )
        await asyncio.wait_for(handle.result(), timeout=180)
        tokens = handle_run_stats(handle)["tokens"]
        assert tokens["planning"]["prompt"] > 0
        assert tokens["verification"] == {"prompt": 0, "completion": 0}
        assert tokens["repair"] == {"prompt": 0, "completion": 0}
    finally:
        await actor.close()
