"""Task-execution wake-reason helpers for conversation-manager handlers."""

import asyncio
import hashlib
from time import perf_counter
import uuid
from typing import TYPE_CHECKING, Any

import requests

from unify.common.task_execution_context import current_task_execution_delegate
from unify.common.startup_timing import log_startup_timing
from unify.conversation_manager.cm_types import Medium
from unify.conversation_manager.domains import brain_action_tools, managers_utils
from unify.conversation_manager.domains.comms_utils import publish_system_error
from unify.conversation_manager.events import (
    ActorHandleStarted,
    FastBrainNotification,
    ProviderEventDispatchRequested,
    TaskDue,
    TaskTriggerRequested,
)
from unify.common.prompt_helpers import now as prompt_now
from unify.logger import LOGGER
from unify.manager_registry import ManagerRegistry
from unify.session_details import SESSION_DETAILS
from unify.task_scheduler.types.activated_by import ActivatedBy
from unify.task_scheduler.types.execution import Delivery, Wake
from unify.task_scheduler.machine_state import (
    TaskExecutionSnapshot,
    TaskRunProvenance,
    get_open_task_execution,
    list_trigger_executions,
    remember_live_task_run_provenance,
    validate_task_due_execution,
)

if TYPE_CHECKING:
    from unify.actor.base import BaseActor
    from unify.common.async_tool_loop import SteerableToolHandle
    from unify.conversation_manager.conversation_manager import ConversationManager

_TASK_CONTEXT_SUMMARY_MAX_CHARS = 220
_TRIGGER_CONTEXT_CANDIDATE_LIMIT = 3
_RESOURCE_READY_TIMEOUT_S = 300.0
_RESOURCE_READY_POLL_S = 1.0


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
        entrypoint_repair_attempts = int(
            kwargs.pop("entrypoint_repair_attempts", 0) or 0,
        )
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
            entrypoint_repair_attempts=entrypoint_repair_attempts,
            entrypoint_repair_context=entrypoint_repair_context,
            destination=destination,
            _parent_chat_context=parent_chat_context,
            _clarification_up_q=clarification_up_q,
            _clarification_down_q=clarification_down_q,
            persist=False,
            _reuse_actor_slot=entrypoint is not None,
        )


async def _register_live_task_handle(
    cm: "ConversationManager",
    *,
    handle: "SteerableToolHandle",
    query: str,
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
    return await _register_live_task_handle(cm, handle=handle, query=query)


async def _start_live_task_trigger_execution(
    event: TaskTriggerRequested,
    cm: "ConversationManager",
) -> int:
    """Start a REST-triggered task through the scheduler execution path."""

    if cm.actor is None:
        raise RuntimeError(
            "Cannot execute triggered task before the live actor is initialized.",
        )

    scheduler = ManagerRegistry.get_task_scheduler()
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
        f"Task triggered via REST API: '{_task_trigger_label(event)}' "
        f"(task_id={event.task_id})."
    )
    return await _register_live_task_handle(cm, handle=handle, query=query)


def _current_task_assistant_id() -> str | None:
    """Return the current assistant id in the string form task state expects."""

    assistant_id = SESSION_DETAILS.assistant.agent_id
    return str(assistant_id) if assistant_id is not None else None


async def _ensure_task_resources_ready(
    cm: "ConversationManager",
    *,
    requires_filesystem: bool,
    requires_computer: bool,
) -> None:
    """Block until desktop / file-sync readiness matches the task's requirements.

    Assistant Local lives on the desktop workspace, so either flag requires
    ``cm.vm_ready``. Filesystem-gated tasks additionally wait for
    ``cm.file_sync_complete``.
    """

    if not requires_filesystem and not requires_computer:
        return

    from unify.conversation_manager.domains.comms_utils import (
        request_deferred_desktop_binding,
    )

    if not cm.vm_ready and (requires_computer or requires_filesystem):
        assistant_id = _current_task_assistant_id()
        if assistant_id:
            await request_deferred_desktop_binding(assistant_id)
        deadline = perf_counter() + _RESOURCE_READY_TIMEOUT_S
        while not cm.vm_ready:
            if perf_counter() >= deadline:
                raise RuntimeError(
                    "Timed out waiting for assistant desktop (vm_ready) "
                    "before starting a resource-gated task.",
                )
            await asyncio.sleep(_RESOURCE_READY_POLL_S)

    if requires_filesystem and not cm.file_sync_complete:
        deadline = perf_counter() + _RESOURCE_READY_TIMEOUT_S
        while not cm.file_sync_complete:
            if perf_counter() >= deadline:
                raise RuntimeError(
                    "Timed out waiting for file sync to complete "
                    "before starting a filesystem-gated task.",
                )
            await asyncio.sleep(_RESOURCE_READY_POLL_S)


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
    activation_description = (
        activation.task_description
        if activation and activation.task_description
        else ""
    )
    return _compact_task_text(
        event.task_summary or activation_description,
        fallback=label,
    )


def _task_due_recurrence_hint(
    event: TaskDue,
    activation: TaskExecutionSnapshot | None,
) -> str:
    """Return whether the due task should be treated as recurring or one-off."""

    if activation and isinstance(activation.repeat, list) and activation.repeat:
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
    """Return the human-facing label for one REST-triggered task."""

    return event.task_label or f"task {event.task_id}"


def _task_trigger_summary(event: TaskTriggerRequested) -> str:
    """Return one compact summary for one REST-triggered task."""

    label = _task_trigger_label(event)
    return _compact_task_text(event.task_summary, fallback=label)


def _task_trigger_notification_text(event: TaskTriggerRequested) -> str:
    """Return the slow-brain instruction for an accepted REST task trigger."""

    label = _task_trigger_label(event)
    summary = _task_trigger_summary(event)
    parts = [f"Task triggered via REST API: '{label}'."]
    if summary and summary != label:
        parts.append(f"Summary: {summary}.")
    parts.append("The task run has been started automatically.")
    return " ".join(parts)


def _task_trigger_fast_brain_context(event: TaskTriggerRequested) -> str:
    """Return silent fast-brain context for one REST-triggered task."""

    label = _task_trigger_label(event)
    summary = _task_trigger_summary(event)
    parts = [f"Background context: the task '{label}' was triggered via REST API."]
    if summary and summary != label:
        parts.append(f"Summary: {summary}.")
    parts.append("The slow brain is handling the triggered task.")
    return " ".join(parts)


def _task_due_fast_brain_context(
    event: TaskDue,
    activation: TaskExecutionSnapshot | None,
) -> str:
    """Return the silent fast-brain context for one validated due task."""

    label = _task_due_label(event, activation)
    summary = _task_due_summary(event, activation)
    parts = [f"Background context: the scheduled task '{label}' is due now."]
    if summary and summary != label:
        parts.append(f"Summary: {summary}.")
    if _task_due_recurrence_hint(event, activation) == "recurring":
        parts.append("This is a recurring task.")
    if event.visibility_policy == "silent_by_default":
        parts.append("Default is silent action unless the user is needed.")
    parts.append("The slow brain is handling the wake reason.")
    return " ".join(parts)


def _activation_label(candidate: TaskExecutionSnapshot) -> str:
    """Return one human-facing label for a trigger candidate."""

    return candidate.task_name or f"task {candidate.task_id}"


def _activation_summary(candidate: TaskExecutionSnapshot) -> str:
    """Return one compact summary for a trigger candidate."""

    label = _activation_label(candidate)
    return _compact_task_text(candidate.task_description, fallback=label)


def _describe_trigger_candidate(candidate: TaskExecutionSnapshot) -> str:
    """Render one live trigger candidate for slow-brain review."""

    label = _activation_label(candidate)
    summary = _activation_summary(candidate)
    if summary and summary != label:
        return f"'{label}': {summary}"
    return f"'{label}'"


def _trigger_candidate_fast_brain_context(
    *,
    medium: Medium,
    sender_name: str,
    candidates: list[TaskExecutionSnapshot],
) -> str:
    """Return silent fast-brain context for live trigger candidates."""

    candidate_descriptions = []
    for candidate in candidates[:_TRIGGER_CONTEXT_CANDIDATE_LIMIT]:
        label = _activation_label(candidate)
        summary = _activation_summary(candidate)
        if summary and summary != label:
            candidate_descriptions.append(f"'{label}' ({summary})")
        else:
            candidate_descriptions.append(f"'{label}'")
    if len(candidates) > _TRIGGER_CONTEXT_CANDIDATE_LIMIT:
        candidate_descriptions.append("...")
    candidate_text = "; ".join(candidate_descriptions)
    return (
        f"Background context: this {medium.value.replace('_', ' ')} from {sender_name} "
        f"may relate to live trigger candidates {candidate_text}. "
        "The slow brain is still deciding whether the trigger truly applies. "
        "Do not mention the task unless it naturally helps the conversation."
    )


def _build_trigger_execute_call(*, task_id: int, attempt_token: str) -> str:
    """Return the exact execute call the slow brain should use for one trigger."""

    return (
        "primitives.tasks.execute("
        f'task_id={task_id}, trigger_attempt_token="{attempt_token}"'
        ")"
    )


def _voice_fast_brain_available(cm: "ConversationManager") -> bool:
    """Return True when a voice fast-brain subprocess exists or is about to start."""

    call_manager = getattr(cm, "call_manager", None)
    if call_manager is None:
        return False
    return bool(
        getattr(cm.mode, "is_voice", False)
        or getattr(call_manager, "has_active_call", False)
        or getattr(call_manager, "has_active_google_meet", False)
        or getattr(call_manager, "has_active_teams_meet", False)
        or getattr(call_manager, "_whatsapp_call_joining", False)
        or getattr(call_manager, "_meet_joining", False)
        or getattr(call_manager, "_socket_server", None) is not None,
    )


def _append_initial_call_notification(cm: "ConversationManager", content: str) -> None:
    """Append silent task context to the next call-start notification payload."""

    existing = getattr(cm.call_manager, "pending_opener", "") or ""
    if existing:
        cm.call_manager.pending_opener = f"{existing}\n\n{content}"
    else:
        cm.call_manager.pending_opener = content


async def _queue_fast_brain_task_context(
    cm: "ConversationManager",
    *,
    content: str,
    source: str,
    contact: dict | None = None,
) -> None:
    """Send silent task context to the voice fast brain before it speaks."""

    if not content or not _voice_fast_brain_available(cm):
        return
    notification = FastBrainNotification(
        contact=contact or getattr(cm.call_manager, "_disconnect_contact", None) or {},
        message=content,
        should_speak=False,
        source=source,
    )
    socket_server = getattr(cm.call_manager, "_socket_server", None)
    if socket_server is None:
        _append_initial_call_notification(cm, content)
        return
    await socket_server.queue_for_clients(
        "app:call:notification",
        notification.to_json(),
    )


def _task_due_event_from_wake_reason(reason: Any) -> TaskDue | None:
    """Convert one startup wake-reason payload into a typed `TaskDue` event.

    Cold-start wake reasons are a heterogeneous list of dicts (task_due,
    coordinator_delegate, …) keyed by a leading ``type`` discriminator.
    Validate the discriminator here, then delegate the actual field
    extraction to :meth:`TaskDue.from_dict` so all `TaskDue` producers
    share one builder.
    """

    if not isinstance(reason, dict) or reason.get("type") != "task_due":
        return None
    return TaskDue.from_dict(reason)


def _task_trigger_event_from_wake_reason(reason: Any) -> TaskTriggerRequested | None:
    """Convert one startup wake-reason payload into a REST task-trigger event."""

    if not isinstance(reason, dict) or reason.get("type") != "task_trigger":
        return None
    return TaskTriggerRequested.from_dict(reason)


async def _handle_task_due_event(event: TaskDue, cm: "ConversationManager") -> bool:
    """Validate and surface one due-task event to the notification bar.

    ``task_due`` wakes are live-only: offline runs never route through the
    ConversationManager. Hosted offline runs execute as dedicated one-shot
    Kubernetes Jobs; local offline runs are fired directly by the local
    activation scheduler.
    """

    assistant_id = _current_task_assistant_id()
    activation, stale_reason = validate_task_due_execution(
        assistant_id=assistant_id,
        task_id=event.task_id,
        revision=event.revision,
        source_task_log_id=event.source_task_log_id,
        scheduled_for=event.scheduled_for,
        destination=event.destination,
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
                destination=event.destination,
                scheduled_for=event.scheduled_for,
                task_name=(activation.task_name if activation is not None else None),
                task_description=(
                    activation.task_description if activation is not None else None
                ),
            ),
        )
    try:
        requires_filesystem = (
            activation.requires_filesystem
            if activation is not None
            else event.requires_filesystem
        )
        requires_computer = (
            activation.requires_computer
            if activation is not None
            else event.requires_computer
        )
        await _ensure_task_resources_ready(
            cm,
            requires_filesystem=requires_filesystem,
            requires_computer=requires_computer,
        )
        handle_id = await _start_live_task_due_execution(event, cm, activation)
    except Exception as exc:
        error_message = (
            f"Scheduled task '{_task_due_label(event, activation)}' failed to start "
            f"through TaskScheduler.execute: {type(exc).__name__}: {exc}"
        )
        cm._session_logger.error("task_due", error_message)
        cm.notifications_bar.push_notif("Tasks", error_message, event.timestamp)
        publish_system_error(
            error_message,
            error_type="scheduled_task_start_failed",
        )
        return False

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
    await _queue_fast_brain_task_context(
        cm,
        content=_task_due_fast_brain_context(event, activation),
        source="task_due",
    )
    return False


async def _handle_task_trigger_requested_event(
    event: TaskTriggerRequested,
    cm: "ConversationManager",
) -> bool:
    """Start one REST-triggered task and surface execution status.

    Offline tasks are dispatched headlessly (no live actor). Live tasks keep
    the existing in-process ``TaskScheduler.execute`` path.
    """

    assistant_id = _current_task_assistant_id()
    activation = None
    if assistant_id:
        activation = get_open_task_execution(
            assistant_id=assistant_id,
            task_id=event.task_id,
            destination=event.destination,
        )
    if activation is not None and activation.delivery == "offline":
        return await _handle_offline_rest_task_trigger(
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
                destination=event.destination,
                source_ref=event.source_ref,
                task_name=event.task_label or None,
                task_description=event.task_summary or None,
            ),
        )
    try:
        if activation is not None:
            await _ensure_task_resources_ready(
                cm,
                requires_filesystem=activation.requires_filesystem,
                requires_computer=activation.requires_computer,
            )
        handle_id = await _start_live_task_trigger_execution(event, cm)
    except Exception as exc:
        error_message = (
            f"REST-triggered task '{_task_trigger_label(event)}' failed to start "
            f"through TaskScheduler.execute: {type(exc).__name__}: {exc}"
        )
        cm._session_logger.error("task_trigger", error_message)
        cm.notifications_bar.push_notif("Tasks", error_message, event.timestamp)
        publish_system_error(
            error_message,
            error_type="rest_task_trigger_start_failed",
        )
        return False

    cm.notifications_bar.push_notif(
        "Tasks",
        _task_trigger_notification_text(event),
        event.timestamp,
    )
    cm._session_logger.info(
        "task_trigger",
        f"Accepted REST task trigger for task {event.task_id} (handle_id={handle_id})",
    )
    await _queue_fast_brain_task_context(
        cm,
        content=_task_trigger_fast_brain_context(event),
        source="task_trigger",
    )
    return False


async def _handle_provider_event_dispatch_requested_event(
    event: ProviderEventDispatchRequested,
    cm: "ConversationManager",
) -> bool:
    """Adopt one live provider-event dispatch and start execution."""

    from datetime import datetime

    from unify.common.task_execution_context import current_task_execution_delegate
    from unify.task_scheduler.provider_event_dispatch import (
        ProviderEventDispatchAuthorizationError,
        ProviderEventDispatchRequest,
        ProviderEventDispatchValidationError,
    )
    from unify.task_scheduler.provider_event_execution import (
        handle_provider_event_live_dispatch,
    )

    try:
        issued_at = datetime.fromisoformat(event.issued_at.replace("Z", "+00:00"))
        request = ProviderEventDispatchRequest(
            contract_version=event.contract_version,  # type: ignore[arg-type]
            operation_id=event.operation_id,
            run_id=event.run_id,
            run_key=event.run_key,
            assistant_id=event.assistant_id,
            task_id=event.task_id,
            binding_id=event.binding_id,
            receipt_id=event.receipt_id,
            accepted_revision=event.accepted_revision,
            wake=event.wake,  # type: ignore[arg-type]
            delivery=event.delivery,  # type: ignore[arg-type]
            event_context_ref=event.event_context_ref,
            issued_at=issued_at,
            audience=event.audience,
        )
    except Exception as exc:
        cm._session_logger.error(
            "provider_event_dispatch",
            f"Rejected malformed provider-event dispatch: {type(exc).__name__}: {exc}",
        )
        return False

    actor = getattr(cm, "actor", None)
    token = None
    if actor is not None:
        token = current_task_execution_delegate.set(
            _ConversationTaskExecutionDelegate(actor),
        )
    try:
        outcome, handle = await handle_provider_event_live_dispatch(request)
    except (
        ProviderEventDispatchValidationError,
        ProviderEventDispatchAuthorizationError,
    ) as exc:
        reason = getattr(exc, "reason_code", str(exc))
        cm._session_logger.info(
            "provider_event_dispatch",
            f"Rejected provider-event dispatch {event.operation_id}: {reason}",
        )
        return False
    except Exception as exc:
        error_message = (
            f"Provider-event dispatch {event.operation_id} failed to start: "
            f"{type(exc).__name__}: {exc}"
        )
        cm._session_logger.error("provider_event_dispatch", error_message)
        publish_system_error(
            error_message,
            error_type="provider_event_dispatch_start_failed",
        )
        return False
    finally:
        if token is not None:
            current_task_execution_delegate.reset(token)

    cm._session_logger.info(
        "provider_event_dispatch",
        (
            f"Provider-event dispatch {outcome.operation_id} "
            f"status={outcome.status} adopted_only={outcome.adopted_only} "
            f"captured_task_revision={outcome.captured_task_revision}"
        ),
    )
    if handle is not None and not outcome.adopted_only and outcome.status == "started":
        query = (
            f"Provider event started task {event.task_id} "
            f"(operation {event.operation_id})."
        )
        await _register_live_task_handle(cm, handle=handle, query=query)
        cm.notifications_bar.push_notif("Tasks", query, event.timestamp)
    return False


async def _handle_offline_rest_task_trigger(
    event: TaskTriggerRequested,
    cm: "ConversationManager",
    *,
    activation: TaskExecutionSnapshot,
) -> bool:
    """Dispatch one REST-triggered offline task without requiring a live actor."""

    from unify.settings import SETTINGS

    use_local_dispatch = bool(SETTINGS.task.LOCAL_SCHEDULER_ENABLED)
    try:
        if use_local_dispatch:
            result = await _dispatch_offline_explicit_candidate_local(
                cm=cm,
                candidate=activation,
                source_ref=event.source_ref or "",
            )
        else:
            result = await asyncio.to_thread(
                _dispatch_offline_explicit_candidate,
                candidate=activation,
                source_ref=event.source_ref or "",
            )
    except Exception as exc:
        error_message = (
            f"REST-triggered offline task '{_task_trigger_label(event)}' failed to "
            f"dispatch: {type(exc).__name__}: {exc}"
        )
        cm._session_logger.error("task_trigger", error_message)
        cm.notifications_bar.push_notif("Tasks", error_message, event.timestamp)
        publish_system_error(
            error_message,
            error_type="rest_task_trigger_offline_dispatch_failed",
        )
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
            f"Accepted REST offline task trigger for task {event.task_id} "
            f"(status={status})"
        ),
    )
    return False


def _dispatch_offline_explicit_candidate(
    *,
    candidate: TaskExecutionSnapshot,
    source_ref: str,
) -> dict[str, Any]:
    """Ask Communication to execute one REST-triggered offline task headlessly."""

    from unify.settings import SETTINGS
    from unify.session_details import SESSION_DETAILS

    comms_url = (SETTINGS.conversation.COMMS_URL or "").rstrip("/")
    unify_key = SESSION_DETAILS.unify_key
    if not comms_url:
        raise RuntimeError("UNITY_COMMS_URL is not configured")
    if not unify_key:
        raise RuntimeError("UNIFY_KEY is not configured")
    assistant_id = candidate.assistant_id or (_current_task_assistant_id() or "")
    payload: dict[str, Any] = {
        "assistant_id": assistant_id,
        "task_id": candidate.task_id,
        "source_task_log_id": candidate.source_task_log_id,
        "revision": candidate.revision,
        "delivery": "offline",
        "wake": Wake.explicit.value,
        "source_ref": source_ref,
        "source_medium": "api",
        "task_name": candidate.task_name or None,
        "task_description": candidate.task_description or None,
    }
    if candidate.destination:
        payload["destination"] = candidate.destination
    if candidate.entrypoint is not None:
        payload["entrypoint"] = candidate.entrypoint
    payload["requires_filesystem"] = bool(candidate.requires_filesystem)
    payload["requires_computer"] = bool(candidate.requires_computer)
    response = requests.post(
        f"{comms_url}/infra/task-execution/offline-dispatch",
        json=payload,
        headers={"Authorization": f"Bearer {unify_key}"},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


async def _dispatch_offline_explicit_candidate_local(
    *,
    cm: "ConversationManager",
    candidate: TaskExecutionSnapshot,
    source_ref: str,
) -> dict[str, Any]:
    """Execute one REST-triggered offline task via the local subprocess lane."""

    materializer = getattr(cm, "_activation_materializer", None)
    dispatcher = getattr(materializer, "_offline", None) if materializer else None
    if dispatcher is None:
        raise RuntimeError(
            "Local activation scheduler is not initialised; "
            "cannot dispatch offline explicit trigger locally.",
        )
    from unify.task_scheduler.local_scheduler.offline_dispatcher import (
        _build_local_offline_runner_env,
    )
    import asyncio as _asyncio
    import os as _os
    import sys as _sys

    env = _build_local_offline_runner_env(
        candidate,
        wake=Wake.explicit,
        source_ref=source_ref,
        source_medium="api",
    )
    merged_env = {**_os.environ, **env}
    merged_env.setdefault("PYTHONUNBUFFERED", "1")
    process = await _asyncio.create_subprocess_exec(
        _sys.executable,
        "-m",
        "unify.task_scheduler.offline_runner",
        env=merged_env,
        stdout=_asyncio.subprocess.PIPE,
        stderr=_asyncio.subprocess.PIPE,
    )
    watcher = _asyncio.create_task(
        dispatcher._watch(process, candidate, Wake.explicit.value),
    )
    dispatcher._inflight.add(watcher)
    watcher.add_done_callback(dispatcher._inflight.discard)
    return {
        "success": True,
        "status": "spawned_local",
        "delivery": "offline",
        "wake": Wake.explicit.value,
    }


async def _consume_startup_wake_reasons(cm: "ConversationManager") -> None:
    """Replay startup wake reasons once managers are initialized."""

    from unify.conversation_manager.domains.coordinator_delegate import (
        _coordinator_delegate_event_from_wake_reason,
        _handle_coordinator_delegate_event,
    )

    wake_reasons = list(getattr(cm, "_startup_wake_reasons", []) or [])
    cm._startup_wake_reasons = []
    if not wake_reasons:
        return
    cm._session_logger.info(
        "task_due",
        f"Replaying {len(wake_reasons)} startup wake reason(s)",
    )
    for wake_reason in wake_reasons:
        task_due_event = _task_due_event_from_wake_reason(wake_reason)
        if task_due_event is not None:
            await _handle_task_due_event(task_due_event, cm)
            continue

        task_trigger_event = _task_trigger_event_from_wake_reason(wake_reason)
        if task_trigger_event is not None:
            await _handle_task_trigger_requested_event(task_trigger_event, cm)
            continue

        coordinator_delegate_event = _coordinator_delegate_event_from_wake_reason(
            wake_reason,
        )
        if coordinator_delegate_event is not None:
            await _handle_coordinator_delegate_event(coordinator_delegate_event, cm)
            continue

        cm._session_logger.info(
            "task_due",
            f"Ignoring unparseable startup wake reason: {wake_reason!r}",
        )


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

    explicit_ref = (
        getattr(event, "api_message_id", None)
        or getattr(event, "email_id", None)
        or getattr(event, "conference_name", None)
        or getattr(event, "room_name", None)
        or getattr(event, "meet_url", None)
    )
    if explicit_ref:
        return str(explicit_ref)
    content = (
        getattr(event, "content", None)
        or getattr(event, "body", None)
        or getattr(event, "subject", None)
        or ""
    )
    digest = hashlib.sha256(str(content).encode("utf-8")).hexdigest()[:12]
    timestamp = getattr(event, "timestamp", None)
    timestamp_component = timestamp.isoformat() if timestamp is not None else "unknown"
    contact_component = str(contact_id) if contact_id is not None else "unknown"
    return (
        f"{event.__class__.__name__}:{medium.value}:{contact_component}:"
        f"{timestamp_component}:{digest}"
    )


def _dispatch_offline_trigger_candidate(
    *,
    candidate: TaskExecutionSnapshot,
    event: Any,
    medium: Medium,
    contact_id: int | None,
    sender_name: str,
) -> dict[str, Any]:
    """Ask Communication to execute one offline trigger candidate headlessly."""

    from unify.settings import SETTINGS
    from unify.session_details import SESSION_DETAILS

    comms_url = (SETTINGS.conversation.COMMS_URL or "").rstrip("/")
    # Self-scoped: dispatch as this assistant using its own UNIFY_KEY; Comms
    # verifies it against the assistant's session (no platform admin key).
    unify_key = SESSION_DETAILS.unify_key
    if not comms_url:
        raise RuntimeError("UNITY_COMMS_URL is not configured")
    if not unify_key:
        raise RuntimeError("UNIFY_KEY is not configured")
    assistant_id = candidate.assistant_id or (_current_task_assistant_id() or "")
    response = requests.post(
        f"{comms_url}/infra/task-execution/offline-dispatch",
        json={
            "assistant_id": assistant_id,
            "task_id": candidate.task_id,
            "source_task_log_id": candidate.source_task_log_id,
            "revision": candidate.revision,
            "delivery": "offline",
            "wake": Wake.triggered.value,
            "source_ref": _build_trigger_source_ref(
                event=event,
                medium=medium,
                contact_id=contact_id,
            ),
            "source_medium": medium.value,
            "source_contact_id": contact_id,
            "source_contact_display_name": _sender_display_name(
                sender_name,
                contact_id=contact_id,
            ),
            "requires_filesystem": bool(candidate.requires_filesystem),
            "requires_computer": bool(candidate.requires_computer),
        },
        headers={"Authorization": f"Bearer {unify_key}"},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


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

    The local-runtime equivalent of :func:`_dispatch_offline_trigger_candidate`.
    Instead of POSTing to ``Communication`` and creating a K8s job, the
    candidate is spawned as a child process running
    ``unify.task_scheduler.offline_runner`` with the activation context
    injected through env vars. Returns a status dict shaped like the
    Communication response so the caller's logging keeps working.
    """

    materializer = getattr(cm, "_activation_materializer", None)
    dispatcher = getattr(materializer, "_offline", None) if materializer else None
    if dispatcher is None:
        raise RuntimeError(
            "Local activation scheduler is not initialised; "
            "cannot dispatch offline trigger locally.",
        )
    source_ref = _build_trigger_source_ref(
        event=event,
        medium=medium,
        contact_id=contact_id,
    )
    # Repackage the snapshot's trigger metadata via the dispatcher's env
    # builder. The dispatcher accepts the optional override kwargs so the
    # subprocess sees the actual triggering inbound (not just the activation
    # row's default trigger_medium).
    from unify.task_scheduler.local_scheduler.offline_dispatcher import (
        _build_local_offline_runner_env,
    )
    import asyncio as _asyncio
    import os as _os
    import sys as _sys

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
    merged_env = {**_os.environ, **env}
    merged_env.setdefault("PYTHONUNBUFFERED", "1")
    process = await _asyncio.create_subprocess_exec(
        _sys.executable,
        "-m",
        "unify.task_scheduler.offline_runner",
        env=merged_env,
        stdout=_asyncio.subprocess.PIPE,
        stderr=_asyncio.subprocess.PIPE,
    )
    # Adopt the watcher onto the dispatcher's set so cleanup on CM stop
    # cancels it together with other in-flight scheduler watchers.
    watcher = _asyncio.create_task(
        dispatcher._watch(process, candidate, Wake.triggered.value),
    )
    dispatcher._inflight.add(watcher)
    watcher.add_done_callback(dispatcher._inflight.discard)
    return {
        "success": True,
        "status": "spawned_local",
        "delivery": "offline",
        "wake": Wake.triggered.value,
    }


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
        from unify.settings import SETTINGS

        use_local_dispatch = bool(SETTINGS.task.LOCAL_SCHEDULER_ENABLED)
        _offline_t0 = perf_counter()
        offline_statuses: list[str] = []
        for candidate in offline_candidates:
            try:
                if use_local_dispatch:
                    result = await _dispatch_offline_trigger_candidate_local(
                        cm=cm,
                        candidate=candidate,
                        event=event,
                        medium=medium,
                        contact_id=contact_id,
                        sender_name=sender_name,
                    )
                else:
                    result = await asyncio.to_thread(
                        _dispatch_offline_trigger_candidate,
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
                destination=candidate.destination,
                source_medium=medium.value,
                source_ref=source_ref,
                source_contact_id=(str(contact_id) if contact_id is not None else None),
                source_contact_display_name=_sender_display_name(
                    sender_name,
                    contact_id=contact_id,
                ),
                task_name=candidate.task_name,
                task_description=candidate.task_description,
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
    _queue_t0 = perf_counter()
    await _queue_fast_brain_task_context(
        cm,
        content=_trigger_candidate_fast_brain_context(
            medium=medium,
            sender_name=sender_name,
            candidates=[
                candidate for candidate, _attempt_token in live_candidates_with_tokens
            ],
        ),
        source="task_trigger",
        contact=getattr(event, "contact", None),
    )
    log_startup_timing(
        LOGGER,
        "⏱️ [StartupTiming] task_execution.queue_fast_brain_context duration=%.2fs candidates=%d",
        perf_counter() - _queue_t0,
        len(live_candidates_with_tokens),
    )
    log_startup_timing(
        LOGGER,
        "⏱️ [StartupTiming] task_execution.surface_trigger_candidates total=%.2fs result=matched",
        perf_counter() - _total_t0,
    )
    return True
