from __future__ import annotations

import json

import pytest

from benchmarks.unity_hermes_artifact_benchmark.arms import HERMES_ARM, UNITY_ARM
from benchmarks.unity_hermes_artifact_benchmark.fixtures import (
    OUTPUT_CONTRACT,
    synthetic_email_batches,
    workweek_email_batches,
)
from benchmarks.unity_hermes_artifact_benchmark.daily_email_live import (
    InMemoryFunctionManager,
    _summarize_unillm_cost_events,
    prepare_workspace,
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


def test_workweek_corpus_covers_monday_through_friday():
    batches = workweek_email_batches()

    assert [batch.batch_id.split("-", 1)[0] for batch in batches] == [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
    ]
    assert all(batch.emails for batch in batches)
    assert batches[0].expected[0].needs_reply is True
    assert batches[-1].expected[-1].message_id == "fri-002"


def test_live_workspace_seeds_get_emails_helper(tmp_path):
    paths = prepare_workspace(tmp_path)

    assert "emails_by_day.json" in paths["emails_by_day"]
    payload = json.loads((tmp_path / "emails_by_day.json").read_text(encoding="utf-8"))
    assert payload["days"] == ["monday", "tuesday"]
    assert sorted(payload["batches"]) == [
        "monday-2026-06-01",
        "tuesday-2026-06-02",
    ]
    assert "expected" not in json.dumps(payload)
    namespace: dict[str, object] = {}
    helper_source = (tmp_path / "email_fixture.py").read_text(encoding="utf-8")
    assert "expected" not in helper_source
    assert "draft_reply" not in helper_source
    assert "wednesday-2026-06-03" not in helper_source
    exec(helper_source, namespace)
    emails = namespace["get_emails"](day="monday")  # type: ignore[index,operator]
    assert [email["message_id"] for email in emails] == [
        "mon-001",
        "mon-002",
        "mon-003",
    ]


def test_in_memory_function_manager_persists_added_functions(tmp_path):
    prepare_workspace(tmp_path)
    manager = InMemoryFunctionManager()
    source = """
def hello(name: str) -> str:
    \"\"\"Say hello.\"\"\"
    return f"hello {name}"
"""

    result = manager.add_functions(implementations=source)
    search = manager.search_functions(
        query="hello",
        _return_callable=True,
        _namespace={},
        _also_return_metadata=True,
    )

    assert result["added"][0]["name"] == "hello"
    assert search["metadata"][0]["name"] == "hello"
    assert manager._get_function_data_by_name(name="hello") is not None


def test_unillm_cost_event_summary_counts_tokens_and_costs():
    class FakeCostEvent:
        model = "gpt-test@provider"
        prompt_tokens = 100
        completion_tokens = 25
        provider_cost = 0.001
        billed_cost = 0.002
        cache_status = "miss"

    summary = _summarize_unillm_cost_events([FakeCostEvent()])

    assert summary["calls"] == 1
    assert summary["prompt_tokens"] == 100
    assert summary["completion_tokens"] == 25
    assert summary["total_tokens"] == 125
    assert summary["provider_cost_usd"] == 0.001
    assert summary["billed_cost_usd"] == 0.002
    assert summary["by_model"]["gpt-test@provider"]["calls"] == 1


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
    assert unity.measurements[0].estimated_cost_usd > 0
    assert hermes.measurements[0].estimated_cost_usd > 0
    assert unity.measurements[0].artifact_internal_llm_calls == 3
    assert hermes.measurements[0].artifact_internal_llm_calls == 3
    assert unity.measurements[0].first_execution_action == "execute_function"
    assert hermes.measurements[0].first_execution_action == "terminal_run_script"


def test_payload_and_markdown_report_are_json_serializable():
    payload = build_payload(run_reference_benchmark(seeds=range(1, 2)))
    encoded = json.dumps(payload)
    report = render_markdown_report(payload)

    assert "unity_hermes_artifact_benchmark" in encoded
    assert "Unity vs Hermes Artifact Benchmark" in report
    assert "Avg total cost estimate" in report
    assert "Avg repeat orchestration tokens" in report
    assert "Avg artifact-internal LLM calls" in report
    assert "Unity demonstrates" in report
