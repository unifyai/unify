"""Scoring and measurement for the Droid vs Hermes artifact benchmark."""

from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Iterable

from .models import (
    ArtifactKind,
    ArtifactObservation,
    ArtifactQualityScore,
    BenchmarkResult,
    RunTrace,
    TraceMeasurement,
)

EXECUTION_ACTIONS = {
    "execute_function",
    "terminal_run_script",
    "no_agent_script",
    "process_batch",
}


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def score_artifact(artifact: ArtifactObservation) -> ArtifactQualityScore:
    """Score a produced artifact against the benchmark's primary rubric."""

    rationale: list[str] = []

    direct_invocability = 0.1
    if artifact.kind is ArtifactKind.DROID_FUNCTION:
        direct_invocability = (
            1.0 if "execute_function" in artifact.invocation_path else 0.8
        )
        rationale.append(
            "FunctionManager artifact can be invoked by exact function name.",
        )
    elif artifact.kind is ArtifactKind.HERMES_NO_AGENT_SCRIPT:
        direct_invocability = 0.85
        rationale.append(
            "Standalone no-agent script is directly runnable, but outside skill semantics.",
        )
    elif artifact.kind is ArtifactKind.FILESYSTEM_SCRIPT:
        direct_invocability = 0.6
        rationale.append(
            "Filesystem script is runnable, but not a first-class stored function.",
        )
    elif artifact.kind is ArtifactKind.HERMES_SKILL_WITH_SCRIPT:
        direct_invocability = (
            0.55 if artifact.exposes_supporting_script_directly else 0.35
        )
        rationale.append(
            "Supporting script is runnable but nested under skill activation.",
        )
    else:
        rationale.append("Prompt-only artifact requires plan reconstruction.")

    boundary_clarity = (
        sum(
            (
                artifact.has_stable_input_schema,
                artifact.has_stable_output_schema,
                artifact.has_dry_run_mode,
            ),
        )
        / 3.0
    )
    if boundary_clarity == 1.0:
        rationale.append(
            "Artifact has stable input/output schemas and dry-run behavior.",
        )

    semantic_isolation = 0.0
    if artifact.semantic_calls_inside_artifact:
        semantic_isolation += 0.55
        rationale.append(
            "Semantic classification/drafting is inside the reusable artifact.",
        )
    if artifact.cheap_semantic_model:
        semantic_isolation += 0.35
        rationale.append("Artifact records a cheap semantic model strategy.")
    if not artifact.requires_procedural_prompt_reread:
        semantic_isolation += 0.10
    semantic_isolation = _clamp(semantic_isolation)

    interpretive_steps = {
        "load_or_preload_skill_text",
        "infer_supporting_script_path",
        "interpret_script_output",
        "regenerate_workflow",
    }
    interpretation_penalty = 0.18 * len(
        [step for step in artifact.invocation_path if step in interpretive_steps],
    )
    if artifact.requires_procedural_prompt_reread:
        interpretation_penalty += 0.25
        rationale.append("Repeat runs still reread procedural prompt material.")
    if artifact.kind is ArtifactKind.FILESYSTEM_SCRIPT:
        interpretation_penalty += 0.20
        rationale.append(
            "Repeat runs need path or script selection outside FunctionManager.",
        )
    future_run_autonomy = _clamp(1.0 - interpretation_penalty)

    scheduler_readiness = 0.2
    if artifact.scheduler_binding:
        scheduler_readiness = 0.75
        rationale.append(f"Scheduler binding: {artifact.scheduler_binding}.")
    if artifact.kind is ArtifactKind.DROID_FUNCTION and artifact.scheduler_binding:
        scheduler_readiness = 1.0
    elif (
        artifact.kind is ArtifactKind.HERMES_SKILL_WITH_SCRIPT
        and artifact.scheduler_binding
    ):
        scheduler_readiness = 0.65
    elif artifact.kind is ArtifactKind.FILESYSTEM_SCRIPT and artifact.scheduler_binding:
        scheduler_readiness = 0.55

    total = round(
        mean(
            (
                direct_invocability,
                boundary_clarity,
                semantic_isolation,
                future_run_autonomy,
                scheduler_readiness,
            ),
        ),
        3,
    )

    return ArtifactQualityScore(
        arm_id=artifact.arm_id,
        artifact_name=artifact.name,
        direct_invocability=round(direct_invocability, 3),
        boundary_clarity=round(boundary_clarity, 3),
        semantic_isolation=round(semantic_isolation, 3),
        future_run_autonomy=round(future_run_autonomy, 3),
        scheduler_readiness=round(scheduler_readiness, 3),
        total=total,
        rationale=tuple(rationale),
    )


def measure_trace(trace: RunTrace) -> TraceMeasurement:
    """Collect secondary token/tool metrics from one run trace."""

    phase_tokens: dict[str, int] = defaultdict(int)
    total_tokens = 0
    estimated_cost_usd = 0.0
    artifact_internal_llm_calls = 0
    for event in trace.events:
        phase_tokens[event.phase] += event.total_tokens
        total_tokens += event.total_tokens
        estimated_cost_usd += event.estimated_cost_usd
        artifact_internal_llm_calls += event.artifact_internal_llm_calls

    repeat_events = [event for event in trace.events if event.phase == "repeat_run"]
    first_execution_index: int | None = None
    for idx, event in enumerate(repeat_events):
        if event.action in EXECUTION_ACTIONS:
            first_execution_index = idx
            break

    if first_execution_index is None:
        orchestration_events = repeat_events
        first_execution_action = None
    else:
        orchestration_events = repeat_events[:first_execution_index]
        first_execution_action = repeat_events[first_execution_index].action

    return TraceMeasurement(
        arm_id=trace.arm_id,
        seed=trace.seed,
        batch_id=trace.batch_id,
        total_tokens=total_tokens,
        estimated_cost_usd=round(estimated_cost_usd, 6),
        first_run_tokens=phase_tokens["first_run"],
        consolidation_tokens=phase_tokens["consolidation"],
        repeat_run_tokens=phase_tokens["repeat_run"],
        repeat_orchestration_tokens=sum(
            event.total_tokens for event in orchestration_events
        ),
        artifact_internal_llm_calls=artifact_internal_llm_calls,
        tool_calls_before_execution=sum(
            1 for event in orchestration_events if event.tool_call
        ),
        first_execution_action=first_execution_action,
    )


def aggregate_measurements(
    measurements: Iterable[TraceMeasurement],
) -> dict[str, float]:
    rows = list(measurements)
    if not rows:
        return {
            "runs": 0,
            "avg_total_tokens": 0.0,
            "avg_estimated_cost_usd": 0.0,
            "avg_repeat_orchestration_tokens": 0.0,
            "avg_artifact_internal_llm_calls": 0.0,
            "avg_tool_calls_before_execution": 0.0,
        }
    return {
        "runs": float(len(rows)),
        "avg_total_tokens": round(mean(row.total_tokens for row in rows), 2),
        "avg_estimated_cost_usd": round(
            mean(row.estimated_cost_usd for row in rows),
            6,
        ),
        "avg_repeat_orchestration_tokens": round(
            mean(row.repeat_orchestration_tokens for row in rows),
            2,
        ),
        "avg_artifact_internal_llm_calls": round(
            mean(row.artifact_internal_llm_calls for row in rows),
            2,
        ),
        "avg_tool_calls_before_execution": round(
            mean(row.tool_calls_before_execution for row in rows),
            2,
        ),
    }


def analyze_results(results: Iterable[BenchmarkResult]) -> dict[str, object]:
    """Compare benchmark arms and return a compact analysis payload."""

    rows = list(results)
    by_arm = {
        result.arm_id: {
            "artifact_score": result.score.to_dict(),
            "measurement_summary": aggregate_measurements(result.measurements),
        }
        for result in rows
    }

    winner = None
    if rows:
        winner = max(rows, key=lambda result: result.score.total).arm_id

    conclusion = (
        "Droid demonstrates the first-class function benefit when repeat runs "
        "reduce to function search plus execute_function, while Hermes skill "
        "runs still inject or interpret SKILL.md before script execution."
    )
    if winner and winner != "droid":
        conclusion = (
            "Hermes matched or beat Droid on the current scores; inspect whether "
            "the Hermes run used a no-agent script path rather than the normal "
            "skill activation path."
        )

    return {
        "winner": winner,
        "arms": by_arm,
        "conclusion": conclusion,
    }
