"""Unit tests for task-run EventBus lineage helpers."""

from __future__ import annotations

from unify.common._async_tool.loop_config import TOOL_LOOP_LINEAGE
from unify.events.task_run_lineage import (
    CURRENT_TASK_RUN_LINEAGE,
    enrich_payload_with_task_run_lineage,
    format_task_run_lineage_segment,
    parse_task_run_lineage_segment,
    push_task_run_lineage,
    reset_task_run_lineage,
    task_run_lineage_scope,
)


def test_format_and_parse_segment_roundtrip():
    segment = format_task_run_lineage_segment(task_id=42, run_key=None)
    assert segment == "Task.run(task_id=42)"
    parsed = parse_task_run_lineage_segment(segment)
    assert parsed is not None
    assert parsed.task_id == 42
    assert parsed.run_key is None

    with_key = format_task_run_lineage_segment(
        task_id=1,
        run_key="rk-abc",
    )
    assert with_key == "Task.run(task_id=1,run_key=rk-abc)"
    parsed_key = parse_task_run_lineage_segment(with_key)
    assert parsed_key is not None
    assert parsed_key.run_key == "rk-abc"


def test_parse_legacy_instance_id_segment():
    legacy = "Task.run(task_id=9,instance_id=3,run_key=run-1)"
    parsed = parse_task_run_lineage_segment(legacy)
    assert parsed is not None
    assert parsed.task_id == 9
    assert parsed.run_key == "run-1"


def test_push_sets_context_and_tool_loop_lineage():
    tokens = push_task_run_lineage(task_id=9, run_key="run-1")
    try:
        lineage = CURRENT_TASK_RUN_LINEAGE.get()
        assert lineage is not None
        assert lineage.task_id == 9
        assert lineage.run_key == "run-1"
        hierarchy = TOOL_LOOP_LINEAGE.get([])
        assert hierarchy[-1] == "Task.run(task_id=9,run_key=run-1)"
    finally:
        reset_task_run_lineage(tokens)
    assert CURRENT_TASK_RUN_LINEAGE.get() is None


def test_enrich_payload_adds_fields_and_hierarchy_segment():
    with task_run_lineage_scope(task_id=5, run_key="rk"):
        payload = {
            "method": "execute_code",
            "hierarchy": ["CodeActActor.act(ab)", "execute_code(cd)"],
            "hierarchy_label": "CodeActActor.act(ab)->execute_code(cd)",
        }
        enrich_payload_with_task_run_lineage(payload)
        assert payload["task_id"] == 5
        assert payload["run_key"] == "rk"
        assert "instance_id" not in payload
        assert payload["hierarchy"][0] == "Task.run(task_id=5,run_key=rk)"
        assert payload["hierarchy_label"].startswith("Task.run(")
