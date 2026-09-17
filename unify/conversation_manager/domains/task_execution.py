"""Task-execution wake-reason helpers for conversation-manager handlers."""

import asyncio
from datetime import datetime, timezone
import hashlib
from time import perf_counter
import uuid
from typing import TYPE_CHECKING, Any

from unify.common.task_execution_context import current_task_execution_delegate
from unify.common.startup_timing import log_startup_timing
from unify.conversation_manager.cm_types import Medium
from unify.conversation_manager.domains import brain_action_tools, managers_utils
from unify.conversation_manager.events import (
    ActorHandleStarted,
    TaskDue,
    TaskTriggerRequested,
)
from unify.common.prompt_helpers import now as prompt_now
from unify.logger import LOGGER
from unify.manager_registry import ManagerRegistry
from unify.session_details import SESSION_DETAILS
from unify.task_scheduler.types.activated_by import ActivatedBy
from unify.task_scheduler.types.execution import Delivery, ExecutionState, Wake
from unify.task_scheduler.machine_state import (
    TaskExecutionSnapshot,
    TaskRunProvenance,
    TaskRunReference,
    get_open_task_execution,
    list_trigger_executions,
    remember_live_task_run_provenance,
    update_task_run_record,
    validate_task_due_execution,
)

if TYPE_CHECKING:
    from unify.actor.base import BaseActor
    from unify.common.async_tool_loop import SteerableToolHandle
    from unify.conversation_manager.conversation_manager import ConversationManager

_TASK_CONTEXT_SUMMARY_MAX_CHARS = 220
_TRIGGER_CONTEXT_CANDIDATE_LIMIT = 3


class _ConversationTaskExecutionDelegate:
    """Route due-task execution through the live actor owned by the conversation."""

    def __init__(self, actor: "BaseActor") -> None:
        self._actor = actor

    async def start_task_run(
        self,
        *,
        task_description: str,
        entrypoint: int | None,
        parent_chat_context: list[dict] | None,
        clarification_up_q: asyncio.Queue[str] | None,
        clarification_down_q: asyncio.Queue[str] | None,
        images: Any | None = None,
        **kwargs: Any,
    ) -> "SteerableToolHandle":
        _ = images
        task_guidelines = kwargs.pop("guidelines", None)
        entrypoint_kwargs = kwargs.pop("entrypoint_kwargs", None)
        entrypoint_repair_context = kwargs.pop("entrypoint_repair_context", None)
        destination = kwargs.pop("destination", None)
        if kwargs:
            unexpected = ", ".join(sorted(kwargs))
            raise TypeError(
                "ConversationManagerTaskExecutionDelegate.start_task_run got "
                f"unexpected keyword arguments: {unexpected}",
            )
        return await self._actor.act(
            task_description,
            guidelines=task_guidelines,
            entrypoint=entrypoint,
            entrypoint_kwargs=entrypoint_kwargs,
            entrypoint_repair_context=entrypoint_repair_context,
            destination=destination,
            _parent_chat_context=parent_chat_context,
            _clarification_up_q=clarification_up_q,
            _clarification_down_q=clarification_down_q,
            persist=False,
            _reuse_actor_slot=entrypoint is not None,
        )


def _task_authoring_fields(
    scheduler: Any,
    task_id: int,
) -> tuple[str | None, str | None]:
    """The task's description and response policy, for the turn that reports it.

    `response_policy` is the author's instruction about *delivery* — "Deliver
    the briefing as one chat message". Rendered only into the actor's own
    request it would reach the one place that cannot send chat and never the
    conversation turn that can, leaving whether a completed run reached the
    user to a per-turn judgement made without the author's instruction in
    view.

    Read from the definition rather than carried on `TaskDue`, because the
    conversation manager shares a process with the scheduler and can simply
    ask. A field on the wire would have to be populated by every producer of
    that event and kept in step with the definition it copies.
    """

    try:
        task = scheduler._get_task_or_raise(task_id)
    except ValueError:
        return None, None
    return (
        getattr(task, "description", None),
        getattr(task, "response_policy", None),
    )


def _record_task_start_failure(
    *,
    activation: TaskExecutionSnapshot | None,
    assistant_id: str | None,
    reason: str,
) -> None:
    """Terminalize an occurrence that fired and could not be started.

    Without this the ledger stays exactly as it was: the row remains
    ``scheduled`` with no error and no end, so nothing records that a run was
    lost. Worse than the missing audit line, projection reads the earliest
    open occurrence as the definition's head, so the row that never ran keeps
    that seat and no later occurrence is ever minted -- a transient failure
    to start would end the series for good.

    Recorded here rather than left to the supervisor sweep because this is the
    one place that knows *why*. The sweep expires the same row half an hour
    later with a generic reason; this writes the actual one, immediately.
    """

    if activation is None or not activation.run_key:
        return
    update_task_run_record(
        TaskRunReference(
            assistant_id=activation.assistant_id or (assistant_id or ""),
            run_key=activation.run_key,
            source_task_log_id=activation.source_task_log_id,
        ),
        {
            "state": ExecutionState.failed.value,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "error": reason,
        },
    )


async def _register_live_task_handle(
    cm: "ConversationManager",
    *,
    handle: "SteerableToolHandle",
    query: str,
    task_description: str | None = None,
    response_policy: str | None = None,
) -> int:
    """Register a deterministically started task with CM steering state."""

    handle_id = brain_action_tools._next_handle_id
    brain_action_tools._next_handle_id += 1
    cm.in_flight_actions[handle_id] = {
        "handle": handle,
        "query": query,
        "persist": False,
        "action_type": "task",
        "handle_actions": [
            {
                "action_name": "task_started",
                "query": query,
                "timestamp": prompt_now(),
            },
        ],
        "initial_snapshot_state": getattr(cm, "_current_snapshot_state", None),
        "context_opted_in": False,
    }
    if task_description:
        cm.in_flight_actions[handle_id]["task_description"] = task_description
    if response_policy:
        cm.in_flight_actions[handle_id]["response_policy"] = response_policy
    asyncio.create_task(
        managers_utils.actor_watch_result(
            handle_id,
            handle,
            action_type="task",
        ),
    )
    asyncio.create_task(managers_utils.actor_watch_notifications(handle_id, handle))
    asyncio.create_task(managers_utils.actor_watch_clarifications(handle_id, handle))
    await cm.event_broker.publish(
        f"app:actor:actor_started_handle_{handle_id}",
        ActorHandleStarted(
            handle_id=handle_id,
            action_name="task",
            query=query,
        ).to_json(),
    )
    return handle_id


async def _start_live_task_due_execution(
    event: TaskDue,
    cm: "ConversationManager",
    activation: TaskExecutionSnapshot,
) -> int:
    """Start a validated live due task through the scheduler execution path."""

    if cm.actor is None:
        raise RuntimeError(
            "Cannot execute due task before the live actor is initialized.",
        )

    scheduler = ManagerRegistry.get_task_scheduler()
    delegate = _ConversationTaskExecutionDelegate(cm.actor)
    delegate_token = current_task_execution_delegate.set(delegate)
    try:
        handle = await scheduler.execute(
            task_id=event.task_id,
            _activated_by=ActivatedBy.schedule,
        )
    finally:
        current_task_execution_delegate.reset(delegate_token)

    query = (
        f"Scheduled task due now: '{_task_due_label(event, activation)}' "
        f"(task_id={event.task_id})."
    )
    task_description, response_policy = _task_authoring_fields(
        scheduler,
        event.task_id,
    )
    return await _register_live_task_handle(
        cm,
        handle=handle,
        query=query,
        task_description=task_description,
        response_policy=response_policy,
    )


async def _start_live_task_trigger_execution(
    event: TaskTriggerRequested,
    cm: "ConversationManager",
) -> int:
    """Start an explicitly triggered task through the scheduler execution path."""

    if cm.actor is None:
        raise RuntimeError(
            "Cannot execute triggered task before the live actor is initialized.",
        )

    scheduler = ManagerRegistry.get_task_scheduler()
    task_description, response_policy = _task_authoring_fields(
        scheduler,
        event.task_id,
    )
    delegate = _ConversationTaskExecutionDelegate(cm.actor)
    delegate_token = current_task_execution_delegate.set(delegate)
    try:
        handle = await scheduler.execute(
            task_id=event.task_id,
            _activated_by=ActivatedBy.explicit,
        )
    finally:
        current_task_execution_delegate.reset(delegate_token)

    query = (
        f"Task triggered explicitly: '{_task_trigger_label(event)}' "
        f"(task_id={event.task_id})."
    )
    return await _register_live_task_handle(
        cm,
        handle=handle,
        query=query,
        task_description=task_description,
        response_policy=response_policy,
    )


def _current_task_assistant_id() -> str | None:
    """Return the current assistant id in the string form task state expects."""

    assistant_id = SESSION_DETAILS.assistant.agent_id
    return str(assistant_id) if assistant_id is not None else None


def _compact_task_text(text: str | None, *, fallback: str) -> str:
    """Return one compact human-readable task summary line."""

    candidate = " ".join(str(text or "").split())
    if not candidate:
        candidate = " ".join(fallback.split())
    candidate = candidate.rstrip(" .")
    if len(candidate) <= _TASK_CONTEXT_SUMMARY_MAX_CHARS:
        return candidate
    truncated = candidate[: _TASK_CONTEXT_SUMMARY_MAX_CHARS - 3].rstrip(" ,.;:")
    return f"{truncated}..."


def _sender_display_name(sender_name: str, *, contact_id: int | None) -> str | None:
    """Return a human-readable sender label when the trigger has a known contact."""

    if contact_id is None:
        return None
    candidate = " ".join(str(sender_name or "").split())
    if not candidate or candidate.lower() == "unknown":
        return None
    return candidate


def _task_due_label(
    event: TaskDue,
    activation: TaskExecutionSnapshot | None,
) -> str:
    """Return the human-facing label for one due-task wake."""

    if event.task_label:
        return event.task_label
    if activation and activation.task_name:
        return activation.task_name
    return f"task {event.task_id}"


def _task_due_summary(
    event: TaskDue,
    activation: TaskExecutionSnapshot | None,
) -> str:
    """Return one compact summary for one due-task wake."""

    label = _task_due_label(event, activation)
    return _compact_task_text(event.task_summary or None, fallback=label)


def _task_due_recurrence_hint(
    event: TaskDue,
    activation: TaskExecutionSnapshot | None,
) -> str:
    """Return whether the due task should be treated as recurring or one-off."""

    if activation and activation.recurring:
        return "recurring"
    return event.recurrence_hint or "one_off"


def _task_due_notification_text(
    event: TaskDue,
    activation: TaskExecutionSnapshot | None,
) -> str:
    """Return the slow-brain instruction for a validated due task."""

    label = _task_due_label(event, activation)
    summary = _task_due_summary(event, activation)
    parts = [f"Scheduled task due now: '{label}'."]
    if summary and summary != label:
        parts.append(f"Summary: {summary}.")
    parts.append(f"Due time: {event.scheduled_for}.")
    if _task_due_recurrence_hint(event, activation) == "recurring":
        parts.append("This is a recurring task.")
    if event.visibility_policy == "silent_by_default":
        parts.append(
            "Default behavior: work silently unless you genuinely need the user.",
        )
    parts.append("The task run has been started automatically.")
    return " ".join(parts)


def _task_trigger_label(event: TaskTriggerRequested) -> str:
    """Return the human-facing label for one explicitly triggered task."""

    return event.task_label or f"task {event.task_id}"


def _task_trigger_summary(event: TaskTriggerRequested) -> str:
    """Return one compact summary for one explicitly triggered task."""

    label = _task_trigger_label(event)
    return _compact_task_text(event.task_summary, fallback=label)


def _task_trigger_notification_text(event: TaskTriggerRequested) -> str:
    """Return the slow-brain instruction for an accepted explicit task trigger."""

    label = _task_trigger_label(event)
    summary = _task_trigger_summary(event)
    parts = [f"Task triggered explicitly: '{label}'."]
    if summary and summary != label:
        parts.append(f"Summary: {summary}.")
    parts.append("The task run has been started automatically.")
    return " ".join(parts)


def _activation_label(candidate: TaskExecutionSnapshot) -> str:
    """Return one human-facing label for a trigger candidate."""

    return candidate.task_name or f"task {candidate.task_id}"


def _activation_summary(candidate: TaskExecutionSnapshot) -> str:
    """Return one compact summary for a trigger candidate."""

    label = _activation_label(candidate)
    return _compact_task_text(candidate.task_summary or None, fallback=label)


def _describe_trigger_candidate(candidate: TaskExecutionSnapshot) -> str:
    """Render one live trigger candidate for slow-brain review."""

    label = _activation_label(candidate)
    summary = _activation_summary(candidate)
    if summary and summary != label:
        return f"'{label}': {summary}"
    return f"'{label}'"


def _build_trigger_execute_call(*, task_id: int, attempt_token: str) -> str:
    """Return the exact execute call the slow brain should use for one trigger."""

    return (
        "primitives.tasks.execute("
        f'task_id={task_id}, trigger_attempt_token="{attempt_token}"'
        ")"
    )


async def _handle_task_due_event(event: TaskDue, cm: "ConversationManager") -> bool:
    """Validate and surface one due-task event to the notification bar.

    ``task_due`` wakes are live-only: offline runs are fired directly by the
    local activation scheduler and never route through the
    ConversationManager.
    """

    assistant_id = _current_task_assistant_id()
    activation, stale_reason = validate_task_due_execution(
        assistant_id=assistant_id,
        task_id=event.task_id,
        revision=event.revision,
        source_task_log_id=event.source_task_log_id,
        scheduled_for=event.scheduled_for,
    )
    if stale_reason is not None:
        cm._session_logger.info(
            "task_due",
            (
                f"Rejected due task {event.task_id}: {stale_reason} "
                f"(assistant_id={assistant_id or '-'})"
            ),
        )
        return False

    assistant_id_for_run = assistant_id or activation.assistant_id or ""
    if assistant_id_for_run:
        remember_live_task_run_provenance(
            TaskRunProvenance(
                assistant_id=assistant_id_for_run,
                task_id=event.task_id,
                wake=Wake.scheduled,
                delivery=Delivery.live,
                source_task_log_id=event.source_task_log_id,
                revision=event.revision,
                scheduled_for=event.scheduled_for,
                task_name=(activation.task_name if activation is not None else None),
            ),
        )
    try:
        handle_id = await _start_live_task_due_execution(event, cm, activation)
    except Exception as exc:
        error_message = (
            f"Scheduled task '{_task_due_label(event, activation)}' failed to start "
            f"through TaskScheduler.execute: {type(exc).__name__}: {exc}"
        )
        _record_task_start_failure(
            activation=activation,
            assistant_id=assistant_id_for_run,
            reason=error_message,
        )
        cm._session_logger.error("task_due", error_message)
        cm.notifications_bar.push_notif("Tasks", error_message, event.timestamp)
        # Ask for a turn. The caller only runs the slow brain when this
        # returns True, and returning False on the failure path means a run
        # the user was waiting on could be lost with the assistant never
        # noticing -- the notification just written above would sit unread
        # until some unrelated later turn. A person is owed the news that
        # their scheduled work did not happen, and this is the only moment
        # anything knows it.
        return True

    cm.notifications_bar.push_notif(
        "Tasks",
        _task_due_notification_text(event, activation),
        event.timestamp,
    )
    cm._session_logger.info(
        "task_due",
        (
            f"Accepted due task {event.task_id} "
            f"(revision={activation.revision or '-'}, "
            f"handle_id={handle_id})"
        ),
    )
    return False


async def _handle_task_trigger_requested_event(
    event: TaskTriggerRequested,
    cm: "ConversationManager",
) -> bool:
    """Start one explicitly triggered task and surface execution status.

    Offline tasks are dispatched headlessly (no live actor). Live tasks run
    through the in-process ``TaskScheduler.execute`` path.
    """

    assistant_id = _current_task_assistant_id()
    activation = None
    if assistant_id:
        activation = get_open_task_execution(
            assistant_id=assistant_id,
            task_id=event.task_id,
        )
    if activation is not None and activation.delivery == "offline":
        return await _handle_offline_task_trigger(
            event,
            cm,
            activation=activation,
        )

    if assistant_id:
        remember_live_task_run_provenance(
            TaskRunProvenance(
                assistant_id=assistant_id,
                task_id=event.task_id,
                wake=Wake.explicit,
                delivery=Delivery.live,
                source_task_log_id=event.source_task_log_id,
                source_ref=event.source_ref,
                task_name=event.task_label or None,
            ),
        )
    try:
        handle_id = await _start_live_task_trigger_execution(event, cm)
    except Exception as exc:
        error_message = (
            f"Triggered task '{_task_trigger_label(event)}' failed to start "
            f"through TaskScheduler.execute: {type(exc).__name__}: {exc}"
        )
        _record_task_start_failure(
            activation=activation,
            assistant_id=assistant_id,
            reason=error_message,
        )
        cm._session_logger.error("task_trigger", error_message)
        cm.notifications_bar.push_notif("Tasks", error_message, event.timestamp)
        # A turn, for the same reason as the scheduled path: somebody asked
        # for this run and is owed the news that it did not start.
        return True

    cm.notifications_bar.push_notif(
        "Tasks",
        _task_trigger_notification_text(event),
        event.timestamp,
    )
    cm._session_logger.info(
        "task_trigger",
        f"Accepted task trigger for task {event.task_id} (handle_id={handle_id})",
    )
    return False


async def _handle_offline_task_trigger(
    event: TaskTriggerRequested,
    cm: "ConversationManager",
    *,
    activation: TaskExecutionSnapshot,
) -> bool:
    """Dispatch one explicitly triggered offline task without requiring a live actor."""

    try:
        result = await _dispatch_offline_explicit_candidate_local(
            cm=cm,
            candidate=activation,
            source_ref=event.source_ref or "",
        )
    except Exception as exc:
        error_message = (
            f"Triggered offline task '{_task_trigger_label(event)}' failed to "
            f"dispatch: {type(exc).__name__}: {exc}"
        )
        cm._session_logger.error("task_trigger", error_message)
        cm.notifications_bar.push_notif("Tasks", error_message, event.timestamp)
        return False

    status = result.get("status", "unknown")
    cm.notifications_bar.push_notif(
        "Tasks",
        (
            f"Offline task '{_task_trigger_label(event)}' dispatched "
            f"({status}, task_id={event.task_id})."
        ),
        event.timestamp,
    )
    cm._session_logger.info(
        "task_trigger",
        (
            f"Accepted offline task trigger for task {event.task_id} "
            f"(status={status})"
        ),
    )
    return False


def _local_offline_dispatcher(cm: "ConversationManager"):
    """The local scheduler's offline dispatcher, or raise when it is not running."""

    materializer = getattr(cm, "_activation_materializer", None)
    dispatcher = getattr(materializer, "_offline", None) if materializer else None
    if dispatcher is None:
        raise RuntimeError(
            "Local activation scheduler is not initialised; "
            "cannot dispatch an offline task.",
        )
    return dispatcher


async def _spawn_offline_runner(
    dispatcher: Any,
    candidate: TaskExecutionSnapshot,
    env: dict[str, str],
    wake: Wake,
) -> dict[str, Any]:
    """Run ``candidate`` as a child ``offline_runner`` process and adopt its watcher.

    The watcher joins the dispatcher's in-flight set so cleanup on CM stop
    cancels it together with the other scheduler watchers. Returns a status
    dict shaped for the caller's logging.
    """

    import os
    import sys

    merged_env = {**os.environ, **env}
    merged_env.setdefault("PYTHONUNBUFFERED", "1")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "unify.task_scheduler.offline_runner",
        env=merged_env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    watcher = asyncio.create_task(
        dispatcher._watch(process, candidate, wake.value),
    )
    dispatcher._inflight.add(watcher)
    watcher.add_done_callback(dispatcher._inflight.discard)
    return {
        "success": True,
        "status": "spawned_local",
        "delivery": "offline",
        "wake": wake.value,
    }


async def _dispatch_offline_explicit_candidate_local(
    *,
    cm: "ConversationManager",
    candidate: TaskExecutionSnapshot,
    source_ref: str,
) -> dict[str, Any]:
    """Execute one explicitly triggered offline task via the local subprocess lane."""

    dispatcher = _local_offline_dispatcher(cm)
    from unify.task_scheduler.local_scheduler.offline_dispatcher import (
        _build_local_offline_runner_env,
    )

    env = _build_local_offline_runner_env(
        candidate,
        wake=Wake.explicit,
        source_ref=source_ref,
        source_medium="api",
    )
    return await _spawn_offline_runner(dispatcher, candidate, env, Wake.explicit)


def _filter_trigger_candidates(
    *,
    medium: Medium,
    contact_id: int | None,
) -> tuple[list[TaskExecutionSnapshot], list[TaskExecutionSnapshot]]:
    """Return mechanically matching live and offline trigger activations."""

    assistant_id = _current_task_assistant_id()
    candidates = list_trigger_executions(
        assistant_id=assistant_id,
        medium=medium.value,
    )
    matching: list[TaskExecutionSnapshot] = []
    for candidate in candidates:
        if contact_id is not None and contact_id in candidate.trigger_omit_contact_ids:
            continue
        if candidate.trigger_from_contact_ids and (
            contact_id is None or contact_id not in candidate.trigger_from_contact_ids
        ):
            continue
        matching.append(candidate)
    live_candidates = [
        candidate for candidate in matching if candidate.delivery == "live"
    ]
    offline_candidates = [
        candidate for candidate in matching if candidate.delivery == "offline"
    ]
    return live_candidates, offline_candidates


def _trigger_candidate_notification_text(
    *,
    medium: Medium,
    sender_name: str,
    candidates: list[tuple[TaskExecutionSnapshot, str]],
) -> str:
    """Return the slow-brain instruction for mechanically matched trigger tasks."""

    candidate_labels = [
        _describe_trigger_candidate(candidate)
        for candidate, _attempt_token in candidates[:_TRIGGER_CONTEXT_CANDIDATE_LIMIT]
    ]
    execute_calls = [
        f"{candidate.task_id} -> {_build_trigger_execute_call(task_id=candidate.task_id, attempt_token=attempt_token)}"
        for candidate, attempt_token in candidates[:_TRIGGER_CONTEXT_CANDIDATE_LIMIT]
    ]
    if len(candidates) > _TRIGGER_CONTEXT_CANDIDATE_LIMIT:
        candidate_labels.append("...")
        execute_calls.append("...")
    return (
        f"This inbound {medium.value.replace('_', ' ')} from {sender_name} "
        "mechanically matched live trigger candidates. "
        f"Candidates: {'; '.join(candidate_labels)}. "
        "Why they matched: the inbound medium and sender fit these trigger filters. "
        "Semantic judgement is still pending. Decide whether this communication "
        "truly satisfies any candidate based on the task summaries and the inbound "
        "itself. If yes, immediately start the best match with its exact execute "
        f"call so the triggering inbound stays attached: {'; '.join(execute_calls)}."
    )


def _build_trigger_source_ref(
    *,
    event: Any,
    medium: Medium,
    contact_id: int | None,
) -> str:
    """Return a stable idempotency key fragment for one inbound trigger event."""

    content = getattr(event, "content", None) or ""
    digest = hashlib.sha256(str(content).encode("utf-8")).hexdigest()[:12]
    timestamp = getattr(event, "timestamp", None)
    timestamp_component = timestamp.isoformat() if timestamp is not None else "unknown"
    contact_component = str(contact_id) if contact_id is not None else "unknown"
    return (
        f"{event.__class__.__name__}:{medium.value}:{contact_component}:"
        f"{timestamp_component}:{digest}"
    )


async def _dispatch_offline_trigger_candidate_local(
    *,
    cm: "ConversationManager",
    candidate: TaskExecutionSnapshot,
    event: Any,
    medium: Medium,
    contact_id: int | None,
    sender_name: str,
) -> dict[str, Any]:
    """Execute one offline trigger candidate via the in-process subprocess lane.

    The candidate is spawned as a child process running
    ``unify.task_scheduler.offline_runner`` with the activation context
    injected through env vars. The dispatcher's env builder accepts the
    trigger override kwargs so the subprocess sees the actual triggering
    inbound (not just the activation row's default trigger_medium).
    """

    dispatcher = _local_offline_dispatcher(cm)
    source_ref = _build_trigger_source_ref(
        event=event,
        medium=medium,
        contact_id=contact_id,
    )
    from unify.task_scheduler.local_scheduler.offline_dispatcher import (
        _build_local_offline_runner_env,
    )

    env = _build_local_offline_runner_env(
        candidate,
        wake=Wake.triggered,
        source_ref=source_ref,
        source_medium=medium.value,
        source_contact_id=contact_id,
        source_contact_display_name=_sender_display_name(
            sender_name,
            contact_id=contact_id,
        ),
    )
    return await _spawn_offline_runner(dispatcher, candidate, env, Wake.triggered)


async def _surface_trigger_task_candidates(
    *,
    cm: "ConversationManager",
    event: Any,
    medium: Medium,
    contact_id: int | None,
    sender_name: str,
    timestamp: Any,
) -> bool:
    """Push one trigger-candidate notification when any live candidates match."""

    _total_t0 = perf_counter()
    _filter_t0 = perf_counter()
    live_candidates, offline_candidates = _filter_trigger_candidates(
        medium=medium,
        contact_id=contact_id,
    )
    log_startup_timing(
        LOGGER,
        "⏱️ [StartupTiming] task_execution.filter_trigger_candidates duration=%.2fs medium=%s contact_id=%s live=%d offline=%d",
        perf_counter() - _filter_t0,
        medium.value,
        contact_id,
        len(live_candidates),
        len(offline_candidates),
    )
    if offline_candidates:
        _offline_t0 = perf_counter()
        offline_statuses: list[str] = []
        for candidate in offline_candidates:
            try:
                result = await _dispatch_offline_trigger_candidate_local(
                    cm=cm,
                    candidate=candidate,
                    event=event,
                    medium=medium,
                    contact_id=contact_id,
                    sender_name=sender_name,
                )
                offline_statuses.append(
                    f"{candidate.task_id}:{result.get('status', 'unknown')}",
                )
            except Exception as exc:
                offline_statuses.append(f"{candidate.task_id}:error")
                cm._session_logger.info(
                    "task_trigger",
                    (
                        f"Offline trigger dispatch failed for task {candidate.task_id} "
                        f"(medium={medium.value}, contact_id={contact_id}): {exc}"
                    ),
                )
        if offline_statuses:
            cm._session_logger.info(
                "task_trigger",
                (
                    f"Offline trigger candidates dispatched for medium={medium.value} "
                    f"contact_id={contact_id}: {', '.join(offline_statuses)}"
                ),
            )
        log_startup_timing(
            LOGGER,
            "⏱️ [StartupTiming] task_execution.dispatch_offline_candidates duration=%.2fs count=%d",
            perf_counter() - _offline_t0,
            len(offline_candidates),
        )
    if not live_candidates:
        log_startup_timing(
            LOGGER,
            "⏱️ [StartupTiming] task_execution.surface_trigger_candidates total=%.2fs result=no_live_candidates",
            perf_counter() - _total_t0,
        )
        return False
    source_ref = _build_trigger_source_ref(
        event=event,
        medium=medium,
        contact_id=contact_id,
    )
    live_candidates_with_tokens: list[tuple[TaskExecutionSnapshot, str]] = []
    for candidate in live_candidates:
        assistant_id = candidate.assistant_id or (_current_task_assistant_id() or "")
        if not assistant_id:
            continue
        attempt_token = uuid.uuid4().hex[:12]
        remember_live_task_run_provenance(
            TaskRunProvenance(
                assistant_id=assistant_id,
                task_id=candidate.task_id,
                wake=Wake.triggered,
                delivery=Delivery.live,
                source_task_log_id=candidate.source_task_log_id,
                revision=candidate.revision,
                source_medium=medium.value,
                source_ref=source_ref,
                source_contact_id=(str(contact_id) if contact_id is not None else None),
                source_contact_display_name=_sender_display_name(
                    sender_name,
                    contact_id=contact_id,
                ),
                task_name=candidate.task_name,
                attempt_token=attempt_token,
            ),
        )
        live_candidates_with_tokens.append((candidate, attempt_token))
    if not live_candidates_with_tokens:
        log_startup_timing(
            LOGGER,
            "⏱️ [StartupTiming] task_execution.surface_trigger_candidates total=%.2fs result=no_tokenized_candidates",
            perf_counter() - _total_t0,
        )
        return False
    candidate_ids = [
        candidate.task_id for candidate, _attempt_token in live_candidates_with_tokens
    ]
    cm.notifications_bar.push_notif(
        "Tasks",
        _trigger_candidate_notification_text(
            medium=medium,
            sender_name=sender_name,
            candidates=live_candidates_with_tokens,
        ),
        timestamp,
    )
    cm._session_logger.info(
        "task_trigger",
        (
            f"Matched trigger candidates {candidate_ids} for medium={medium.value} "
            f"contact_id={contact_id}"
        ),
    )
    log_startup_timing(
        LOGGER,
        "⏱️ [StartupTiming] task_execution.surface_trigger_candidates total=%.2fs result=matched",
        perf_counter() - _total_t0,
    )
    return True
