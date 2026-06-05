from __future__ import annotations

import json

import pytest

from benchmarks.unity_hermes_artifact_benchmark.arms import HERMES_ARM, UNITY_ARM
from benchmarks.unity_hermes_artifact_benchmark.fixtures import (
    OUTPUT_CONTRACT,
    synthetic_email_batches,
)
from benchmarks.unity_hermes_artifact_benchmark.runner import (
    build_payload,
    render_markdown_report,
    run_reference_benchmark,
)
from benchmarks.unity_hermes_artifact_benchmark.scoring import (
    analyze_results,
    score_artifact,
)

pytestmark = pytest.mark.no_unify_context


def test_synthetic_corpus_has_fixed_expected_outputs():
    batches = synthetic_email_batches()

    assert [batch.batch_id for batch in batches] == [
        "batch-2026-06-04",
        "batch-2026-06-05",
    ]
    assert OUTPUT_CONTRACT.drafts_file.as_posix() == "drafts.json"

    first = batches[0]
    expected_by_id = {outcome.message_id: outcome for outcome in first.expected}
    assert len(first.emails) == 6
    assert expected_by_id["msg-005"].category == "urgent_action"
    assert expected_by_id["msg-005"].needs_reply is True
    assert expected_by_id["msg-004"].draft_reply is None


def test_unity_arm_encodes_functionmanager_repeat_path():
    prompt = UNITY_ARM.prompts.repeat_run
    checklist = "\n".join(UNITY_ARM.checklist.items)

    assert "FunctionManager" in prompt
    assert "execute_function" in prompt
    assert "classify_and_draft_email_batch" in prompt
    assert "Stored artifact is a FunctionManager function" in checklist
    assert UNITY_ARM.reference_artifact.requires_procedural_prompt_reread is False


def test_hermes_arm_encodes_best_case_skill_script_path():
    first_run = HERMES_ARM.prompts.first_run
    repeat_run = HERMES_ARM.prompts.repeat_run

    assert "scripts/classify_and_draft.py" in first_run
    assert "first screenful" in first_run
    assert "full SKILL.md text" in repeat_run
    assert HERMES_ARM.reference_artifact.requires_procedural_prompt_reread is True
    assert HERMES_ARM.reference_artifact.exposes_supporting_script_directly is True


def test_artifact_scoring_favors_first_class_function_shape():
    unity_score = score_artifact(UNITY_ARM.reference_artifact)
    hermes_score = score_artifact(HERMES_ARM.reference_artifact)

    assert unity_score.total > hermes_score.total
    assert unity_score.direct_invocability == 1.0
    assert unity_score.future_run_autonomy == 1.0
    assert hermes_score.future_run_autonomy < unity_score.future_run_autonomy


def test_reference_benchmark_collects_measurements_and_analysis():
    results = run_reference_benchmark(seeds=range(1, 4))
    analysis = analyze_results(results)

    assert {result.arm_id for result in results} == {"unity", "hermes"}
    assert analysis["winner"] == "unity"

    unity = next(result for result in results if result.arm_id == "unity")
    hermes = next(result for result in results if result.arm_id == "hermes")
    assert len(unity.measurements) == 3
    assert len(hermes.measurements) == 3
    assert (
        unity.measurements[0].repeat_orchestration_tokens
        < hermes.measurements[0].repeat_orchestration_tokens
    )
    assert unity.measurements[0].first_execution_action == "execute_function"
    assert hermes.measurements[0].first_execution_action == "terminal_run_script"


def test_payload_and_markdown_report_are_json_serializable():
    payload = build_payload(run_reference_benchmark(seeds=range(1, 2)))
    encoded = json.dumps(payload)
    report = render_markdown_report(payload)

    assert "unity_hermes_artifact_benchmark" in encoded
    assert "Unity vs Hermes Artifact Benchmark" in report
    assert "Avg repeat orchestration tokens" in report
    assert "Unity demonstrates" in report
