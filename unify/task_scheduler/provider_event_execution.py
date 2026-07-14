"""Orchestrate live provider-event dispatch into a captured-revision instance.

Bypasses ``TaskScheduler.execute`` consume/clone, communication-trigger
qualification, and authored-definition mutation.

# TODO: Drop the container-local start-claim inbox dependency
(``get_provider_event_live_dispatch_inbox``) once Orchestra downstream adoption
is the source of truth; keep captured-revision start + event-context adopt.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from unify.session_details import SESSION_DETAILS
from unify.settings import SETTINGS
from unify.task_scheduler.provider_event_context import (
    fetch_provider_event_context,
    provider_event_context_as_untrusted_data,
    verify_precreated_provider_event_run,
)
from unify.task_scheduler.provider_event_dispatch import (
    LiveProviderEventDispatchOutcome,
    ProviderEventDispatchRequest,
    ProviderEventDispatchValidationError,
    dispatch_snapshot,
    public_status_for_inbox_state,
    validate_provider_event_dispatch_request,
)
from unify.task_scheduler.provider_event_dispatch_inbox import (
    ProviderEventLiveDispatchInbox,
)
from unify.task_scheduler.task_scheduler import TaskScheduler

_provider_event_live_dispatch_inbox: ProviderEventLiveDispatchInbox | None = None


def get_provider_event_live_dispatch_inbox() -> ProviderEventLiveDispatchInbox:
    """Return the container-local live provider-event dispatch inbox.

    # TODO: Remove once live rails adopt via Orchestra operations instead of
    this container-local SQLite file.
    """

    global _provider_event_live_dispatch_inbox
    if _provider_event_live_dispatch_inbox is None:
        _provider_event_live_dispatch_inbox = ProviderEventLiveDispatchInbox(
            Path(SETTINGS.task.PROVIDER_EVENT_DISPATCH_INBOX_PATH),
        )
    return _provider_event_live_dispatch_inbox


def resolve_captured_task_revision(*, task_id: int) -> int:
    """Return the authored task revision captured for one live dispatch."""

    scheduler = TaskScheduler()
    task = scheduler._get_provider_event_definition(task_id=task_id)
    revision = task.task_revision
    if revision is None:
        raise ProviderEventDispatchValidationError("task_revision_missing")
    return int(revision)


def _outcome_from_record(
    record: Any,
    *,
    adopted_only: bool,
) -> LiveProviderEventDispatchOutcome:
    return LiveProviderEventDispatchOutcome(
        operation_id=record.operation_id,
        run_id=record.run_id,
        run_key=record.run_key,
        captured_task_revision=record.captured_task_revision,
        status=public_status_for_inbox_state(record.state),
        launch_count=record.launch_count,
        adopted_only=adopted_only,
        terminal_reason=record.terminal_reason,
    )


async def handle_provider_event_live_dispatch(
    request: ProviderEventDispatchRequest,
) -> LiveProviderEventDispatchOutcome:
    """Validate, adopt, and start at most one live provider-event instance."""

    validate_provider_event_dispatch_request(
        request,
        ttl_seconds=SETTINGS.task.PROVIDER_EVENT_DISPATCH_REQUEST_TTL_SECONDS,
    )
    session_assistant = str(SESSION_DETAILS.assistant.agent_id or "")
    if session_assistant and session_assistant != str(request.assistant_id):
        raise ProviderEventDispatchValidationError("assistant_id_mismatch")

    inbox = get_provider_event_live_dispatch_inbox()
    existing = inbox.get(operation_id=request.operation_id)
    if existing is not None and existing.state in {"started", "terminal"}:
        return _outcome_from_record(existing, adopted_only=True)

    captured_task_revision, event_context = await asyncio.gather(
        asyncio.to_thread(resolve_captured_task_revision, task_id=request.task_id),
        asyncio.to_thread(fetch_provider_event_context, request),
    )
    await asyncio.to_thread(verify_precreated_provider_event_run, request)
    untrusted = provider_event_context_as_untrusted_data(event_context)

    snapshot = dispatch_snapshot(
        request,
        captured_task_revision=captured_task_revision,
    )
    adopted = inbox.adopt_or_get(
        operation_id=request.operation_id,
        run_id=request.run_id,
        snapshot=snapshot,
    )
    if adopted.state in {"started", "terminal"}:
        return _outcome_from_record(adopted, adopted_only=True)

    claimed = inbox.claim_start(operation_id=request.operation_id)
    if not claimed.owns_start:
        return _outcome_from_record(claimed, adopted_only=True)

    scheduler = TaskScheduler()
    try:
        await scheduler.start_provider_event_instance(
            request=request,
            captured_task_revision=claimed.captured_task_revision,
            provider_event_context=untrusted,
        )
    except ProviderEventDispatchValidationError:
        inbox.mark_terminal(
            operation_id=request.operation_id,
            reason="live_instance_start_failed",
        )
        raise

    started = inbox.start_if_owner(operation_id=request.operation_id)
    return _outcome_from_record(started, adopted_only=False)


def provider_event_dispatch_status(
    *,
    operation_id: str,
) -> dict[str, Any] | None:
    """Return public status for one live dispatch operation, if adopted."""

    record = get_provider_event_live_dispatch_inbox().get(operation_id=operation_id)
    if record is None:
        return None
    return {
        "operation_id": record.operation_id,
        "run_id": record.run_id,
        "run_key": record.run_key,
        "status": public_status_for_inbox_state(record.state),
        "captured_task_revision": record.captured_task_revision,
        "launch_count": record.launch_count,
        "terminal_reason": record.terminal_reason,
    }
