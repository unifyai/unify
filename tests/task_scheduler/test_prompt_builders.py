from datetime import datetime, timezone

from unify.task_scheduler.prompt_builders import (
    build_ask_prompt,
    build_task_execution_request,
    build_task_run_guidelines,
    build_update_prompt,
)
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.task_scheduler.types.activated_by import ActivatedBy
from unify.task_scheduler.types.priority import Priority
from unify.task_scheduler.types.repetition import Frequency, RepeatPattern
from unify.task_scheduler.types.schedule import Schedule
from unify.task_scheduler.types.status import Status
from unify.task_scheduler.types.task import DeliveryMode, ExecutionStyle, Task


def test_task_derives_delivery_mode_and_execution_style_independently():
    agentic_offline = Task(
        task_id=9,
        instance_id=0,
        name="Offline agentic task",
        description="Interpret this description in the headless lane.",
        status=Status.scheduled,
        priority=Priority.normal,
        offline=True,
        entrypoint=None,
        requires_filesystem=True,
        requires_computer=False,
    )
    symbolic_live = Task(
        task_id=10,
        instance_id=0,
        name="Live symbolic task",
        description="Run the durable executor in the live lane.",
        status=Status.scheduled,
        priority=Priority.normal,
        offline=False,
        entrypoint=321,
        requires_filesystem=False,
        requires_computer=True,
    )

    assert agentic_offline.delivery_mode == DeliveryMode.offline
    assert agentic_offline.execution_style == ExecutionStyle.agentic
    assert agentic_offline.requires_filesystem is True
    assert agentic_offline.requires_computer is False
    assert symbolic_live.delivery_mode == DeliveryMode.live
    assert symbolic_live.execution_style == ExecutionStyle.symbolic
    assert symbolic_live.requires_filesystem is False
    assert symbolic_live.requires_computer is True


def test_build_task_execution_request_includes_run_metadata():
    task = Task(
        task_id=7,
        instance_id=2,
        name="Weekly AI report",
        description="Summarize the previous week's AI research.",
        status=Status.scheduled,
        priority=Priority.normal,
        response_policy="Email the user a concise document.",
        schedule=Schedule(start_at=datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)),
        repeat=[RepeatPattern(frequency=Frequency.WEEKLY)],
    )

    request = build_task_execution_request(task)

    assert "Execute this TaskScheduler task as a contained task run." in request
    assert "Task id: 7" in request
    assert "Instance id:" not in request
    assert "Weekly AI report" in request
    assert "Summarize the previous week's AI research." in request
    assert "Task response policy:" in request
    assert "Schedule metadata:" in request
    assert "Repeat metadata:" in request


def test_build_task_execution_request_omits_history_and_info():
    task = Task(
        task_id=7,
        instance_id=3,
        name="Daily briefing",
        description="Prepare the briefing from current sources.",
        status=Status.scheduled,
        priority=Priority.normal,
        info="Previous run found cached facts and notified the user.",
        schedule=Schedule(start_at=datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)),
    )

    request = build_task_execution_request(task)

    assert "Prepare the briefing from current sources." in request
    assert "Previous run found cached facts" not in request
    assert "history" not in request.lower()
    assert "completed trajectory" not in request.lower()


def test_build_task_run_guidelines_keep_child_actor_focused_on_one_task():
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
    assert "do not create another task" in guidelines
    assert "interpret the natural-language description" in guidelines
    assert "Activation reason: trigger" in guidelines
    assert "Task id: 3" in guidelines
    assert "Instance id:" not in guidelines


def test_build_update_prompt_includes_provider_event_guidance() -> None:
    scheduler = TaskScheduler()
    prompt = build_update_prompt(
        scheduler.get_tools("update"),
        num_tasks=0,
        columns=[],
        include_activity=False,
    ).flatten()
    assert "Provider-event triggers" in prompt
    assert "task_revision_conflict" in prompt
    assert "pause_provider_trigger" in prompt
    assert "provider_event_context" in prompt
    assert "**kwargs" in prompt
    assert (
        "list catalog → list eligible connections → describe schema → "
        "resolve required resources → create with trigger_config filled → enable"
        in prompt
    )
    assert "not `live_ready`" in prompt
    assert "delivery_only=true" in prompt
    assert "list_provider_trigger_resources" in prompt
    assert "never watch all of My Drive" in prompt
    assert "select a named space when the user named one" in prompt
    assert "If multiple resources match" in prompt


def test_build_ask_prompt_includes_provider_event_discovery_guidance() -> None:
    scheduler = TaskScheduler()
    prompt = build_ask_prompt(
        scheduler.get_tools("ask"),
        num_tasks=0,
        columns=[],
        include_activity=False,
    ).flatten()
    assert "Provider-event triggers (read-only)" in prompt
    assert "list_provider_trigger_catalog" in prompt
    assert "list_provider_trigger_resources" in prompt
    assert "get_provider_trigger_health" in prompt
    assert "connection-gated" in prompt
    assert "connect the integration first" in prompt
    assert "do not claim the provider lacks that trigger globally" in prompt
    assert "copy a selectable item's `trigger_config`" in prompt
    assert "do not invent provider ids" in prompt
    assert "live_ready=false" in prompt
