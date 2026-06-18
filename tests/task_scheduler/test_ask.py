"""
Integration tests for TaskScheduler.ask
================================================

These are read-only tests that use the shared task_read_scenario from conftest.
"""

# pylint: disable=duplicate-code

from __future__ import annotations

import asyncio
import json
import re

import pytest

pytestmark = [pytest.mark.eval, pytest.mark.llm_call]

from unity.task_scheduler.task_scheduler import TaskScheduler
from unity.task_scheduler.types.status import Status
from unity.common.llm_helpers import _dumps
from unity.common.llm_client import new_llm_client
from tests.assertion_helpers import assertion_failed

# ---------------- Ground-truth helpers ---------------- #


def _answer_semantic(ts: TaskScheduler, question: str) -> str:
    q = question.lower()
    tasks = ts._filter_tasks()

    if "how many scheduled" in q:
        return str(sum(1 for t in tasks if t.status == Status.scheduled))

    if "cancelled" in q and "how many" in q:
        return str(sum(1 for t in tasks if t.status == Status.cancelled))

    return "N/A"


QUESTIONS = [
    "How many scheduled tasks are there?",
    "How many cancelled tasks are there?",
]


def _llm_assert_correct(
    question: str,
    expected: str,
    candidate: str,
    steps: list,  # noqa: D401 – clarity outweighs strict type accuracy
) -> None:
    """Assert *candidate* satisfies *expected* for *question* via an LLM judge.

    On failure, the full reasoning *steps* are appended to the assertion
    message to aid debugging.
    """

    judge = new_llm_client(async_client=False)
    judge.set_system_message(
        "You are a strict unit-test judge. "
        "You will be given a question, a ground-truth answer derived directly "
        "from the data, and a candidate answer produced by the system under test. "
        'Respond ONLY with valid JSON of the form {"correct": true} or {"correct": false}. '
        "Mark correct⇢true if a reasonable human would accept the candidate as answering the question fully and accurately; otherwise false.",
    )

    payload = _dumps(
        {"question": question, "ground_truth": expected, "candidate": candidate},
        indent=4,
    )
    result = judge.generate(payload)

    match = re.search(r"\{.*\}", result, re.S)
    assert match, assertion_failed(
        "Expected JSON format from LLM judge",
        result,
        steps,
        "LLM judge returned unexpected format",
    )
    verdict = json.loads(match.group(0))
    assert verdict.get("correct") is True, assertion_failed(
        expected,
        candidate,
        steps,
        f"Question: {question}",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("question", QUESTIONS)
@pytest.mark.timeout(300)
async def test_ask_semantic_with_llm_judgement(
    question: str,
    task_scheduler_read_scenario: tuple[TaskScheduler, list[int]],
) -> None:
    ts, _ = task_scheduler_read_scenario
    try:
        handle = await ts.ask(
            text=question,
            _return_reasoning_steps=True,
        )
        candidate, steps = await handle.result()
        expected = _answer_semantic(ts, question)
        _llm_assert_correct(question, expected, candidate, steps)
    except Exception as exc:
        raise exc


@pytest.mark.asyncio
@pytest.mark.timeout(300)
async def test_ask_with_interjection(
    task_scheduler_read_scenario: tuple[TaskScheduler, list[int]],
) -> None:
    """Ask a question, interject with a follow-up, and ensure the final answer covers both."""
    ts, _ = task_scheduler_read_scenario
    try:
        # 1) Initial question ⇢ scheduled task count
        handle = await ts.ask(
            text="How many scheduled tasks are there?",
            _return_reasoning_steps=True,
        )

        # 2) Mid-conversation interjection ⇢ cancelled task count
        await handle.interject("Also, how many cancelled tasks are there?")

        # 3) Await combined answer
        answer, steps = await handle.result()
        scheduled_cnt = _answer_semantic(ts, QUESTIONS[0])
        cancelled_cnt = _answer_semantic(ts, QUESTIONS[1])

        # 4) Assert presence of both pieces of information
        assert scheduled_cnt in answer, assertion_failed(
            f"Answer containing scheduled count '{scheduled_cnt}'",
            answer,
            steps,
            "Scheduled count not mentioned in combined answer",
        )
        assert cancelled_cnt in answer, assertion_failed(
            f"Answer containing cancelled count '{cancelled_cnt}'",
            answer,
            steps,
            "Cancelled count not mentioned in combined answer",
        )
    except Exception as exc:
        raise exc


@pytest.mark.asyncio
@pytest.mark.timeout(300)
async def test_ask_stop(
    task_scheduler_read_scenario: tuple[TaskScheduler, list[int]],
) -> None:
    """Test that we can stop the conversation mid-way."""
    ts, _ = task_scheduler_read_scenario
    try:
        # Start with a request that would take some time to complete
        handle = await ts.ask(
            text="List all tasks, then summarize each one in detail.",
        )

        # Give the LLM a moment to start processing, then stop it
        await asyncio.sleep(0.05)
        await handle.stop(cancel=True)
        await handle.result()
        assert handle.done()
    except Exception as exc:
        raise exc


@pytest.mark.asyncio
@pytest.mark.timeout(300)
async def test_ask_uses_reduce_for_numeric_aggregation(
    task_scheduler_read_scenario: tuple[TaskScheduler, list[int]],
) -> None:
    """Verify LLM uses reduce tool for numeric aggregation questions."""
    ts, _ = task_scheduler_read_scenario
    handle = await ts.ask(
        text="What is the sum of all task_id values?",
        _return_reasoning_steps=True,
    )
    answer, steps = await handle.result()

    # Assert reduce tool was called
    reduce_called = any(
        any(
            "reduce" in (tc.get("function", {}).get("name", "") or "").lower()
            for tc in (step.get("tool_calls") or [])
        )
        for step in steps
        if step.get("role") == "assistant"
    )
    assert reduce_called, assertion_failed(
        "reduce tool to be called",
        f"steps without reduce: {[s for s in steps if s.get('role') == 'assistant']}",
        steps,
        "LLM should use reduce tool for numeric aggregation",
    )
