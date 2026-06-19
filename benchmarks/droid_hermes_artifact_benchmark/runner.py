"""Runner for the controlled Droid vs Hermes artifact benchmark."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from .arms import ALL_ARMS, HERMES_ARM, DROID_ARM, BenchmarkArm
from .fixtures import OUTPUT_CONTRACT, synthetic_email_batches
from .models import BenchmarkResult, RunTrace, TraceEvent
from .scoring import analyze_results, measure_trace, score_artifact


def _reference_trace(arm: BenchmarkArm, *, seed: int, batch_id: str) -> RunTrace:
    """Create deterministic reference traces for the design comparison.

    These traces are not meant to fake live agent behavior. They encode the
    expected best-case shape for each design so the scoring/reporting pipeline
    can be tested and used as a baseline before plugging in real traces.
    """

    if arm.arm_id == "droid":
        events = (
            TraceEvent(
                phase="first_run",
                action="FunctionManager_search_functions",
                detail="Discovery-first search for existing email triage functions.",
                prompt_tokens=950 + seed * 3,
                completion_tokens=120,
                estimated_cost_usd=0.012,
                tool_call=True,
            ),
            TraceEvent(
                phase="first_run",
                action="execute_code",
                detail="One-off dry-run solution for calibration batch.",
                prompt_tokens=1300 + seed * 5,
                completion_tokens=480,
                estimated_cost_usd=0.025,
                tool_call=True,
            ),
            TraceEvent(
                phase="consolidation",
                action="store_skills",
                detail="Storage review extracts FunctionManager function.",
                prompt_tokens=1800 + seed * 4,
                completion_tokens=520,
                estimated_cost_usd=0.034,
                tool_call=True,
            ),
            TraceEvent(
                phase="repeat_run",
                action="FunctionManager_search_functions",
                detail="Find classify_and_draft_email_batch.",
                prompt_tokens=420 + seed,
                completion_tokens=80,
                estimated_cost_usd=0.005,
                tool_call=True,
            ),
            TraceEvent(
                phase="repeat_run",
                action="execute_function",
                detail="Invoke stored function by exact name.",
                prompt_tokens=180 + seed,
                completion_tokens=60,
                estimated_cost_usd=0.002,
                tool_call=True,
            ),
            TraceEvent(
                phase="repeat_run",
                action="artifact_internal_reason",
                detail="Cheap model calls happen inside function per email needing judgment.",
                prompt_tokens=360,
                completion_tokens=160,
                estimated_cost_usd=0.0003,
                artifact_internal_llm_calls=3,
                tool_call=False,
            ),
        )
    else:
        events = (
            TraceEvent(
                phase="first_run",
                action="skills_list",
                detail="Discover relevant skill affordances.",
                prompt_tokens=850 + seed * 3,
                completion_tokens=110,
                estimated_cost_usd=0.011,
                tool_call=True,
            ),
            TraceEvent(
                phase="first_run",
                action="terminal_run_script",
                detail="Initial dry-run solution and script prototype.",
                prompt_tokens=1250 + seed * 5,
                completion_tokens=500,
                estimated_cost_usd=0.025,
                tool_call=True,
            ),
            TraceEvent(
                phase="consolidation",
                action="skill_manage",
                detail="Create SKILL.md plus scripts/classify_and_draft.py.",
                prompt_tokens=1500 + seed * 4,
                completion_tokens=620,
                estimated_cost_usd=0.031,
                tool_call=True,
            ),
            TraceEvent(
                phase="repeat_run",
                action="load_or_preload_skill_text",
                detail="Cron/preload injects full SKILL.md before use.",
                prompt_tokens=900 + seed,
                completion_tokens=90,
                estimated_cost_usd=0.010,
                tool_call=False,
            ),
            TraceEvent(
                phase="repeat_run",
                action="infer_supporting_script_path",
                detail="Agent decides to run scripts/classify_and_draft.py.",
                prompt_tokens=310 + seed,
                completion_tokens=130,
                estimated_cost_usd=0.004,
                tool_call=False,
            ),
            TraceEvent(
                phase="repeat_run",
                action="terminal_run_script",
                detail="Run inner script in dry-run mode.",
                prompt_tokens=230,
                completion_tokens=120,
                estimated_cost_usd=0.0003,
                artifact_internal_llm_calls=3,
                tool_call=True,
            ),
            TraceEvent(
                phase="repeat_run",
                action="interpret_script_output",
                detail="Agent reads script output and writes final response.",
                prompt_tokens=420,
                completion_tokens=190,
                estimated_cost_usd=0.006,
                tool_call=False,
            ),
        )

    return RunTrace(
        arm_id=arm.arm_id,
        seed=seed,
        batch_id=batch_id,
        events=events,
    )


def run_reference_benchmark(
    seeds: Iterable[int] = range(1, 6),
) -> tuple[BenchmarkResult, ...]:
    """Run the deterministic reference benchmark for both arms."""

    batches = synthetic_email_batches()
    repeat_batch = batches[-1].batch_id
    results: list[BenchmarkResult] = []
    for arm in ALL_ARMS:
        traces = tuple(
            _reference_trace(arm, seed=seed, batch_id=repeat_batch) for seed in seeds
        )
        measurements = tuple(measure_trace(trace) for trace in traces)
        artifact = arm.reference_artifact
        score = score_artifact(artifact)
        results.append(
            BenchmarkResult(
                arm_id=arm.arm_id,
                artifact=artifact,
                score=score,
                traces=traces,
                measurements=measurements,
            ),
        )
    return tuple(results)


def build_payload(results: Iterable[BenchmarkResult]) -> dict[str, object]:
    result_rows = tuple(results)
    return {
        "benchmark": "droid_hermes_artifact_benchmark",
        "output_contract": OUTPUT_CONTRACT.to_dict(),
        "corpus": [batch.to_dict() for batch in synthetic_email_batches()],
        "arms": [DROID_ARM.to_dict(), HERMES_ARM.to_dict()],
        "results": [result.to_dict() for result in result_rows],
        "analysis": analyze_results(result_rows),
    }


def render_markdown_report(payload: dict[str, object]) -> str:
    analysis = payload["analysis"]
    assert isinstance(analysis, dict)
    arms = analysis.get("arms", {})
    assert isinstance(arms, dict)

    lines = [
        "# Droid vs Hermes Artifact Benchmark",
        "",
        "Controlled dry-run benchmark for recurring email classification and draft replies.",
        "",
        "## Decision Rule",
        "",
        (
            "Droid demonstrates the design benefit if repeat runs reduce to "
            "`search function -> execute_function -> artifact-internal cheap LLM calls`."
        ),
        "",
        "Hermes is on par only if the skill becomes a thin launcher with stable, tiny repeat-run overhead.",
        "",
        "## Scores",
        "",
    ]

    for arm_id, row in arms.items():
        assert isinstance(row, dict)
        score = row["artifact_score"]
        summary = row["measurement_summary"]
        assert isinstance(score, dict)
        assert isinstance(summary, dict)
        lines.extend(
            [
                f"### {arm_id}",
                "",
                f"- Artifact score: {score['total']}",
                f"- Direct invocability: {score['direct_invocability']}",
                f"- Future-run autonomy: {score['future_run_autonomy']}",
                f"- Avg total cost estimate: ${summary['avg_estimated_cost_usd']}",
                f"- Avg repeat orchestration tokens: {summary['avg_repeat_orchestration_tokens']}",
                f"- Avg artifact-internal LLM calls: {summary['avg_artifact_internal_llm_calls']}",
                f"- Avg tool calls before execution: {summary['avg_tool_calls_before_execution']}",
                "",
            ],
        )

    lines.extend(
        [
            "## Conclusion",
            "",
            str(analysis.get("conclusion", "")),
            "",
        ],
    )
    return "\n".join(lines)


def write_outputs(output_dir: Path, payload: dict[str, object]) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "payload": output_dir / "benchmark.json",
        "report": output_dir / "report.md",
        "corpus": output_dir / "corpus.json",
        "arms": output_dir / "arms.json",
    }
    paths["payload"].write_text(json.dumps(payload, indent=2), encoding="utf-8")
    paths["report"].write_text(render_markdown_report(payload), encoding="utf-8")
    paths["corpus"].write_text(
        json.dumps(payload["corpus"], indent=2),
        encoding="utf-8",
    )
    paths["arms"].write_text(
        json.dumps(payload["arms"], indent=2),
        encoding="utf-8",
    )
    return {name: str(path) for name, path in paths.items()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional output directory for benchmark.json/report.md.",
    )
    parser.add_argument(
        "--seeds",
        type=int,
        default=5,
        help="Number of deterministic seeded reference runs.",
    )
    args = parser.parse_args(argv)

    results = run_reference_benchmark(range(1, args.seeds + 1))
    payload = build_payload(results)
    if args.out:
        payload = {
            **payload,
            "written_files": write_outputs(args.out, payload),
        }
    print(
        json.dumps(payload if args.out is None else payload["written_files"], indent=2),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
