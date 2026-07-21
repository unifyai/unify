"""Orchestrate live provider-event dispatch against the authored definition.

Bypasses ``TaskScheduler.execute`` consume/clone, communication-trigger
qualification, and authored-definition mutation. Launch ownership is claimed
through Orchestra before execution I/O.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from unify.session_details import SESSION_DETAILS
from unify.settings import SETTINGS
from unify.task_scheduler.provider_event_context import (
    fetch_provider_event_context,
    provider_event_context_as_untrusted_data,
    verify_precreated_provider_event_run,
)
from unify.task_scheduler.provider_event_dispatch import (
    LiveProviderEventDispatchOutcome,
    ProviderEventDispatchAuthorizationError,
    ProviderEventDispatchRequest,
    ProviderEventDispatchValidationError,
    claim_provider_event_dispatch,
    live_launch_identity,
    report_provider_event_dispatch_started,
    report_provider_event_dispatch_terminal,
    validate_provider_event_dispatch_request,
)
from unify.task_scheduler.task_scheduler import TaskScheduler

if TYPE_CHECKING:
    from unify.common.async_tool_loop import SteerableToolHandle


def resolve_captured_task_revision(*, task_id: int) -> int:
    """Return the authored task revision captured for one live dispatch."""

    scheduler = TaskScheduler()
    task = scheduler._get_provider_event_definition(task_id=task_id)
    revision = task.task_revision
    if revision is None:
        raise ProviderEventDispatchValidationError("task_revision_missing")
    return int(revision)


async def handle_provider_event_live_dispatch(
    request: ProviderEventDispatchRequest,
) -> tuple[LiveProviderEventDispatchOutcome, SteerableToolHandle | None]:
    """Validate, claim through Orchestra, and start at most one live execution.

    On a successful start, return the live ``ActiveTask`` handle so the caller
    can await completion (task status + run terminal state). Adopt-only and
    already-terminal claims return ``None`` for the handle.
    """

    validate_provider_event_dispatch_request(
        request,
        ttl_seconds=SETTINGS.task.PROVIDER_EVENT_DISPATCH_REQUEST_TTL_SECONDS,
    )
    session_assistant = str(SESSION_DETAILS.assistant.agent_id or "")
    if session_assistant and session_assistant != str(request.assistant_id):
        raise ProviderEventDispatchValidationError("assistant_id_mismatch")

    captured_task_revision, event_context = await asyncio.gather(
        asyncio.to_thread(resolve_captured_task_revision, task_id=request.task_id),
        asyncio.to_thread(fetch_provider_event_context, request),
    )
    await asyncio.to_thread(verify_precreated_provider_event_run, request)
    untrusted = provider_event_context_as_untrusted_data(event_context)

    launch_identity = live_launch_identity(operation_id=request.operation_id)
    claimed = await asyncio.to_thread(
        claim_provider_event_dispatch,
        request,
        launch_identity=launch_identity,
    )
    if claimed.status in {"started", "terminal"} or claimed.adopted_only:
        return (
            LiveProviderEventDispatchOutcome(
                operation_id=claimed.operation_id,
                run_id=claimed.run_id,
                run_key=claimed.run_key,
                captured_task_revision=captured_task_revision,
                status=claimed.status,
                fencing_token=claimed.fencing_token,
                adopted_only=True,
                launch_identity=claimed.launch_identity or launch_identity,
                terminal_reason=claimed.terminal_reason,
            ),
            None,
        )

    scheduler = TaskScheduler()
    try:
        handle = await scheduler.start_provider_event_instance(
            request=request,
            captured_task_revision=captured_task_revision,
            provider_event_context=untrusted,
        )
    except ProviderEventDispatchValidationError:
        await asyncio.to_thread(
            report_provider_event_dispatch_terminal,
            operation_id=request.operation_id,
            fencing_token=claimed.fencing_token,
            terminal_reason="live_instance_start_failed",
            launch_identity=launch_identity,
            captured_task_revision=captured_task_revision,
        )
        raise
    except ProviderEventDispatchAuthorizationError:
        raise

    outcome = await asyncio.to_thread(
        report_provider_event_dispatch_started,
        operation_id=request.operation_id,
        fencing_token=claimed.fencing_token,
        launch_identity=launch_identity,
        captured_task_revision=captured_task_revision,
    )
    return outcome, handle
