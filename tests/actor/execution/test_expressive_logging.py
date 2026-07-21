"""Tests for soft-fail logging prompts and stderr observation compaction."""

from __future__ import annotations

from unify.actor.code_act_actor import _STORAGE_WHAT_CAN_BE_STORED
from unify.actor.execution.types import (
    ExecutionResult,
    TextPart,
    compact_diagnostic_text,
)
from unify.function_manager.base import BaseFunctionManager
from unify.function_manager.execution_env import create_execution_globals
from unify.task_scheduler.base import BaseTaskScheduler
from unify.task_scheduler.prompt_builders import build_task_run_guidelines
from unify.task_scheduler.types.activated_by import ActivatedBy
from unify.task_scheduler.types.priority import Priority
from unify.task_scheduler.types.status import Status
from unify.task_scheduler.types.task import Task


def test_create_execution_globals_does_not_inject_progress_helpers():
    g = create_execution_globals()
    assert "get_progress_logger" not in g
    assert "phase" not in g
    assert "skip" not in g
    assert "soft_fail" not in g
    assert "get_tick_logger" not in g
    assert callable(g["run_coro_sync"])


def test_storage_prompt_requires_event_loop_safety():
    prompt = _STORAGE_WHAT_CAN_BE_STORED
    assert "run_coro_sync" in prompt
    assert "asyncio.run" in prompt
    assert "Offline TaskScheduler Jobs already own an event loop" in prompt


def test_add_functions_doc_has_asyncio_antipattern():
    doc = BaseFunctionManager.add_functions.__doc__ or ""
    assert "asyncio.run" in doc
    assert "run_coro_sync" in doc


def test_task_execute_doc_has_asyncio_antipattern():
    doc = BaseTaskScheduler.execute.__doc__ or ""
    assert "asyncio.run" in doc
    assert "run_coro_sync" in doc


def test_compact_diagnostic_text_summarizes_dense_trails():
    lines = [
        "2026-01-01 INFO demo: PHASE a {}",
        "2026-01-01 INFO demo: PHASE b {}",
        "2026-01-01 WARNING demo: SKIP c {}",
        "2026-01-01 ERROR demo: SOFT_FAIL d {}",
        "2026-01-01 INFO demo: PHASE e {}",
        "ordinary stderr line",
    ]
    compacted = compact_diagnostic_text("\n".join(lines))
    assert "compacted for live context" in compacted
    assert "PHASE=" in compacted
    assert "ordinary stderr line" in compacted
    assert compacted.count("PHASE a") + compacted.count("PHASE b") <= 1


def test_execution_result_compacts_stderr_for_llm_only():
    raw = "\n".join(f"2026-01-01 INFO demo: PHASE step{i} {{}}" for i in range(6))
    result = ExecutionResult(
        result={"status": "ok"},
        stderr=[TextPart(text=raw)],
    )
    assert result.stderr[0].text.count("PHASE") == 6
    llm = result.to_llm_content()
    joined = "\n".join(
        block.get("text", "") for block in llm if isinstance(block, dict)
    )
    assert "compacted for live context" in joined


def test_storage_prompt_requires_expressive_logging():
    prompt = _STORAGE_WHAT_CAN_BE_STORED
    assert "may simplify incidental logging" not in prompt
    assert "logging" in prompt
    assert "PHASE" in prompt and "SOFT_FAIL" in prompt
    assert "progress_logging" not in prompt
    assert "tick_logging" not in prompt


def test_add_functions_doc_has_logging_antipatterns():
    doc = BaseFunctionManager.add_functions.__doc__ or ""
    assert "Anti-patterns" in doc
    assert "SOFT_FAIL" in doc
    assert "logging" in doc
    assert "progress_logging" not in doc
    assert "tick_logging" not in doc


def test_task_execute_doc_has_logging_antipatterns():
    doc = BaseTaskScheduler.execute.__doc__ or ""
    assert "Anti-patterns" in doc
    assert "logging" in doc
    assert "PHASE" in doc
    assert "progress_logging" not in doc
    assert "tick_logging" not in doc


def test_build_task_run_guidelines_point_at_logged_functions():
    task = Task(
        task_id=3,
        instance_id=1,
        name="Invoice follow-up",
        description="Draft an invoice reply.",
        status=Status.triggerable,
        priority=Priority.normal,
    )
    guidelines = build_task_run_guidelines(task, ActivatedBy.trigger)
    assert "executing exactly one TaskScheduler task" in guidelines
    assert "logging" in guidelines
    assert "PHASE" in guidelines
    assert "do not create another task" in guidelines
    assert "progress_logging" not in guidelines
    assert "tick_logging" not in guidelines
