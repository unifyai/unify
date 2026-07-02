"""Shared slow-brain benchmark scenarios and timing helpers.

Scenario runners mirror production ConversationManager tests so latency
measurements use the same ``step_until_wait`` path, fixtures, and prompts
as the real suite.
"""

from __future__ import annotations

import base64
import json
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import pytest

from tests.conversation_manager.cm_helpers import filter_events_by_type
from tests.conversation_manager.conftest import BOSS
from unify.conversation_manager.cm_types import ScreenshotEntry
from unify.conversation_manager.cm_types.medium import Medium
from unify.conversation_manager.events import (
    ActorHandleStarted,
    SMSReceived,
    UnifyMessageReceived,
)
from unify.settings import SETTINGS

if TYPE_CHECKING:
    from tests.conversation_manager.cm_test_driver import CMStepDriver, StepResult

GRUB_IMAGE_PATH = Path(__file__).resolve().parents[2] / "images" / "grub_screen.jpg"

SLOW_BRAIN_BENCHMARK_MODELS: tuple[dict[str, str], ...] = (
    {
        "label": "deepseek-v4-pro-high",
        "model": "deepseek-v4-max@deepseek",
    },
    {
        "label": "minimax-m3-high",
        "model": "minimax-v3@minimax",
    },
)

BENCHMARK_REPEAT_COUNT = 3

_BENCHMARK_RESULTS: list["SlowBrainBenchmarkSample"] = []


@dataclass(frozen=True)
class SlowBrainBenchmarkSample:
    scenario_id: str
    source_test: str
    model_label: str
    model: str
    repeat_index: int
    image_mode: str
    step_wall_s: float
    single_shot_total_s: float
    single_shot_calls: int
    llm_step_count: int
    ok: bool
    error: str | None = None
    tools: tuple[str, ...] = ()


@dataclass
class _SingleShotTimer:
    total_s: float = 0.0
    call_count: int = 0


async def run_contact_preference_lookup(cm: "CMStepDriver") -> "StepResult":
    """Mirrors ``test_contact_preference_lookup`` in test_ask_about_contacts.py."""

    return await cm.step_until_wait(
        SMSReceived(
            contact=BOSS,
            content="Does Sarah prefer phone or email?",
        ),
    )


def assert_contact_preference_lookup(result: "StepResult") -> None:
    events = filter_events_by_type(result.output_events, ActorHandleStarted)
    contact_events = [
        event for event in events if event.action_name == "ask_about_contacts"
    ]
    assert contact_events, (
        f"Expected ask_about_contacts, got "
        f"{[event.action_name for event in events] or 'none'}"
    )
    query = contact_events[0].query.lower()
    assert "sarah" in query, f"Expected Sarah in query, got: {query}"


async def run_knowledge_query_triggers_act(cm: "CMStepDriver") -> "StepResult":
    """Mirrors ``test_knowledge_query_triggers_act`` in test_take_action.py."""

    return await cm.step_until_wait(
        SMSReceived(
            contact=BOSS,
            content="What are our office hours again?",
        ),
    )


def assert_knowledge_query_triggers_act(
    result: "StepResult",
    cm: "CMStepDriver",
) -> None:
    events = filter_events_by_type(result.output_events, ActorHandleStarted)
    act_events = [event for event in events if event.action_name == "act"]
    assert (
        act_events
    ), f"Expected act, got {[event.action_name for event in events] or 'none'}"
    assert "act" in cm.all_tool_calls


async def run_visual_question_without_act(cm: "CMStepDriver") -> "StepResult":
    """Mirrors ``test_visual_question_answered_without_act``."""

    assert GRUB_IMAGE_PATH.exists(), f"Missing benchmark image: {GRUB_IMAGE_PATH}"
    grub_b64 = base64.b64encode(GRUB_IMAGE_PATH.read_bytes()).decode()

    cm.cm.user_screen_share_active = True
    cm.cm._screenshot_buffer.append(
        ScreenshotEntry(
            b64=grub_b64,
            utterance="What can you see on my screen?",
            timestamp=datetime.now(timezone.utc),
            source="user",
        ),
    )

    return await cm.step_until_wait(
        UnifyMessageReceived(
            contact=BOSS,
            content="What can you see on my screen?",
        ),
    )


def assert_visual_question_without_act(result: "StepResult") -> None:
    actor_events = filter_events_by_type(result.output_events, ActorHandleStarted)
    assert (
        not actor_events
    ), "Simple visual question should not trigger act when screenshots are present"


async def run_rich_state_task_triage(cm: "CMStepDriver") -> "StepResult":
    """Busier snapshot similar to coordinator eval / multi-thread CM state."""

    cm.contact_index.push_message(
        contact_id=2,
        sender_name="Alice Smith",
        thread_name=Medium.SMS_MESSAGE,
        message_content="Any update on the Memphis shipment?",
        role="user",
    )
    cm.contact_index.push_message(
        contact_id=3,
        sender_name="Bob Chen",
        thread_name=Medium.EMAIL,
        message_content="Please summarize open tasks and flag vendor blockers.",
        role="user",
    )

    return await cm.step_until_wait(
        UnifyMessageReceived(
            contact=BOSS,
            content=(
                "Summarize what's open across Alice and Bob's threads and tell me "
                "what you would do next."
            ),
        ),
    )


SCENARIO_RUNNERS: dict[str, Callable[..., Any]] = {
    "contact-preference-lookup": run_contact_preference_lookup,
    "knowledge-query-act": run_knowledge_query_triggers_act,
    "visual-question-no-act": run_visual_question_without_act,
    "rich-state-task-triage": run_rich_state_task_triage,
}

SCENARIO_ASSERTIONS: dict[str, Callable[..., None]] = {
    "contact-preference-lookup": assert_contact_preference_lookup,
    "knowledge-query-act": assert_knowledge_query_triggers_act,
    "visual-question-no-act": assert_visual_question_without_act,
    "rich-state-task-triage": lambda result: None,
}

SCENARIO_SOURCE_TESTS: dict[str, str] = {
    "contact-preference-lookup": (
        "tests/conversation_manager/actions/test_ask_about_contacts.py::"
        "test_contact_preference_lookup"
    ),
    "knowledge-query-act": (
        "tests/conversation_manager/actions/test_take_action.py::"
        "test_knowledge_query_triggers_act"
    ),
    "visual-question-no-act": (
        "tests/conversation_manager/actions/test_screen_share_visual_question.py::"
        "test_visual_question_answered_without_act"
    ),
    "rich-state-task-triage": (
        "tests/conversation_manager/core/test_slow_brain_model_latency.py::"
        "rich-state-task-triage"
    ),
}


def install_single_shot_timer(monkeypatch: pytest.MonkeyPatch) -> _SingleShotTimer:
    """Wrap production ``single_shot_tool_decision`` to accumulate LLM wall time."""

    timer = _SingleShotTimer()
    import unify.conversation_manager.conversation_manager as cm_module

    original = cm_module.single_shot_tool_decision

    async def _timed_single_shot(*args: Any, **kwargs: Any):
        started = time.perf_counter()
        try:
            return await original(*args, **kwargs)
        finally:
            timer.total_s += time.perf_counter() - started
            timer.call_count += 1

    monkeypatch.setattr(cm_module, "single_shot_tool_decision", _timed_single_shot)
    return timer


def apply_slow_brain_model(model: str) -> None:
    SETTINGS.UNIFY_MODEL = model


async def run_benchmark_scenario(
    *,
    scenario_id: str,
    cm: "CMStepDriver",
    model_cfg: dict[str, str],
    repeat_index: int,
    timer: _SingleShotTimer,
    validate: bool,
) -> SlowBrainBenchmarkSample:
    runner = SCENARIO_RUNNERS[scenario_id]
    assertion = SCENARIO_ASSERTIONS[scenario_id]
    image_mode = (
        "with_images" if scenario_id == "visual-question-no-act" else "text_only"
    )

    apply_slow_brain_model(model_cfg["model"])

    started = time.perf_counter()
    error: str | None = None
    ok = True
    result = None
    try:
        if scenario_id == "knowledge-query-act":
            result = await runner(cm)
            if validate:
                assertion(result, cm)
        else:
            result = await runner(cm)
            if validate:
                assertion(result)
    except Exception as exc:
        ok = False
        error = f"{type(exc).__name__}: {exc}"
    step_wall_s = time.perf_counter() - started

    sample = SlowBrainBenchmarkSample(
        scenario_id=scenario_id,
        source_test=SCENARIO_SOURCE_TESTS[scenario_id],
        model_label=model_cfg["label"],
        model=model_cfg["model"],
        repeat_index=repeat_index,
        image_mode=image_mode,
        step_wall_s=step_wall_s,
        single_shot_total_s=timer.total_s,
        single_shot_calls=timer.call_count,
        llm_step_count=result.llm_step_count if result is not None else 0,
        ok=ok,
        error=error,
        tools=tuple(cm.all_tool_calls[-5:]),
    )
    _BENCHMARK_RESULTS.append(sample)
    return sample


def benchmark_results() -> list[SlowBrainBenchmarkSample]:
    return list(_BENCHMARK_RESULTS)


def clear_benchmark_results() -> None:
    _BENCHMARK_RESULTS.clear()


def format_benchmark_summary(results: list[SlowBrainBenchmarkSample]) -> str:
    lines = [
        "",
        "Slow-brain benchmark summary (step_until_wait wall time):",
        "scenario                 model                   mode         "
        "median  mean   n   failures",
        "------------------------ ----------------------- ------------ "
        "------  -----  -   --------",
    ]

    groups: dict[tuple[str, str, str], list[SlowBrainBenchmarkSample]] = {}
    for sample in results:
        key = (sample.scenario_id, sample.model_label, sample.image_mode)
        groups.setdefault(key, []).append(sample)

    for key in sorted(groups):
        scenario_id, model_label, image_mode = key
        samples = groups[key]
        ok_samples = [sample for sample in samples if sample.ok]
        failures = len(samples) - len(ok_samples)
        if ok_samples:
            values = [sample.step_wall_s for sample in ok_samples]
            median = statistics.median(values)
            mean = statistics.mean(values)
            lines.append(
                f"{scenario_id:24} {model_label:23} {image_mode:12} "
                f"{median:6.2f}s {mean:6.2f}s {len(ok_samples):3d}   {failures:8d}",
            )
        else:
            lines.append(
                f"{scenario_id:24} {model_label:23} {image_mode:12} "
                f"{'n/a':>6}  {'n/a':>6}   0   {failures:8d}",
            )

    lines.append("")
    lines.append("Vision overhead vs paired text scenario (median step wall time):")
    pairs = {
        "contact-preference-lookup": "visual-question-no-act",
        "rich-state-task-triage": "visual-question-no-act",
    }
    for text_id, vision_id in pairs.items():
        for model_label in sorted({sample.model_label for sample in results}):
            text_vals = [
                sample.step_wall_s
                for sample in results
                if sample.ok
                and sample.model_label == model_label
                and sample.scenario_id == text_id
            ]
            vision_vals = [
                sample.step_wall_s
                for sample in results
                if sample.ok
                and sample.model_label == model_label
                and sample.scenario_id == vision_id
            ]
            if not text_vals or not vision_vals:
                continue
            delta = statistics.median(vision_vals) - statistics.median(text_vals)
            pct = delta / statistics.median(text_vals) * 100
            lines.append(
                f"  {model_label:23} {text_id} -> {vision_id}: "
                f"+{delta:.2f}s ({pct:+.0f}%)",
            )

    failures = [sample for sample in results if not sample.ok]
    if failures:
        lines.append("")
        lines.append("Failures:")
        for sample in failures:
            lines.append(
                f"  {sample.model_label} {sample.scenario_id} "
                f"repeat={sample.repeat_index}: {sample.error}",
            )

    return "\n".join(lines)


def write_benchmark_json(path: Path, results: list[SlowBrainBenchmarkSample]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps([asdict(sample) for sample in results], indent=2) + "\n",
        encoding="utf-8",
    )
