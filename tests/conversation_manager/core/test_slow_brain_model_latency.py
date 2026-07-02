"""
Live slow-brain latency benchmarks using real ConversationManager tests.

Each scenario mirrors an existing CM eval/integration test and runs through
``initialized_cm.step_until_wait`` so timings include render state, tool
surface construction, and ``single_shot_tool_decision`` — not an isolated LLM stub.

Run all models × repeats::

    cd unity
    UNILLM_CACHE=false ./scripts/run_slow_brain_model_benchmark.sh

Or directly::

    UNILLM_CACHE=false uv run pytest \
      tests/conversation_manager/core/test_slow_brain_model_latency.py -v
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.helpers import _handle_project
from tests.conversation_manager.core.slow_brain_benchmark_helpers import (
    BENCHMARK_REPEAT_COUNT,
    SLOW_BRAIN_BENCHMARK_MODELS,
    benchmark_results,
    clear_benchmark_results,
    format_benchmark_summary,
    install_single_shot_timer,
    run_benchmark_scenario,
    write_benchmark_json,
)

pytestmark = [
    pytest.mark.llm_call,
    pytest.mark.slow,
    pytest.mark.slow_brain_benchmark,
    pytest.mark.requires_orchestra,
]

_BENCHMARK_SCENARIOS = (
    "contact-preference-lookup",
    "knowledge-query-act",
    "visual-question-no-act",
    "rich-state-task-triage",
)


@pytest.fixture(scope="module", autouse=True)
def _disable_unillm_cache_for_benchmarks() -> None:
    os.environ["UNILLM_CACHE"] = "false"
    clear_benchmark_results()
    yield


def pytest_generate_tests(metafunc):
    if "slow_brain_model" in metafunc.fixturenames:
        metafunc.parametrize(
            "slow_brain_model",
            SLOW_BRAIN_BENCHMARK_MODELS,
            ids=[cfg["label"] for cfg in SLOW_BRAIN_BENCHMARK_MODELS],
        )
    if "benchmark_repeat" in metafunc.fixturenames:
        metafunc.parametrize(
            "benchmark_repeat",
            range(1, BENCHMARK_REPEAT_COUNT + 1),
            ids=[f"run-{index}" for index in range(1, BENCHMARK_REPEAT_COUNT + 1)],
        )


def pytest_sessionfinish(session, exitstatus):  # noqa: ARG001
    results = benchmark_results()
    if not results:
        return
    summary = format_benchmark_summary(results)
    print(summary, flush=True)
    output = os.environ.get("SLOW_BRAIN_BENCHMARK_JSON")
    if output:
        write_benchmark_json(Path(output), results)


@pytest.mark.asyncio
@_handle_project
@pytest.mark.parametrize("scenario_id", _BENCHMARK_SCENARIOS)
async def test_slow_brain_model_latency(
    initialized_cm,
    monkeypatch: pytest.MonkeyPatch,
    slow_brain_model: dict[str, str],
    benchmark_repeat: int,
    scenario_id: str,
):
    timer = install_single_shot_timer(monkeypatch)
    sample = await run_benchmark_scenario(
        scenario_id=scenario_id,
        cm=initialized_cm,
        model_cfg=slow_brain_model,
        repeat_index=benchmark_repeat,
        timer=timer,
        validate=False,
    )

    print(
        f"[slow-brain-benchmark] {sample.model_label} {sample.scenario_id} "
        f"repeat={sample.repeat_index} step={sample.step_wall_s:.2f}s "
        f"single_shot={sample.single_shot_total_s:.2f}s "
        f"llm_steps={sample.llm_step_count} ok={sample.ok}",
        flush=True,
    )

    if not sample.ok:
        if (
            sample.scenario_id == "visual-question-no-act"
            and "deepseek" in sample.model_label
            and sample.error
            and "image input" in sample.error.lower()
        ):
            pytest.xfail(f"expected non-vision model failure: {sample.error}")
        pytest.fail(sample.error or "slow-brain benchmark scenario failed")
