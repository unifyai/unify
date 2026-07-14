"""Live provider-event dispatch helpers for Unity.

# TODO: Purge ``dispatch_provider_event_live`` and its inbox dependency once
Orchestra-backed downstream adoption is wired and the authenticated CM handler
is the only live entrypoint. Keep request validation / audience constants.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Literal

from pydantic import BaseModel, ConfigDict, Field

from unify.task_scheduler.provider_event_dispatch_inbox import (
    LiveDispatchInboxSnapshot,
    ProviderEventLiveDispatchInbox,
)

PROVIDER_EVENT_DISPATCH_AUDIENCE = "unity:provider-event-dispatch"
PublicDispatchStatus = Literal["adopted", "started", "terminal"]


class ProviderEventDispatchRequest(BaseModel):
    """Internal dispatch authorization for provider-event live execution."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["1"] = "1"
    operation_id: str
    run_id: int
    run_key: str
    assistant_id: str
    task_id: int
    binding_id: str
    receipt_id: str
    accepted_activation_revision: str
    source_type: Literal["provider_event"] = "provider_event"
    dispatch_mode: Literal["live", "offline"] = "live"
    event_context_ref: str
    issued_at: datetime
    audience: str = Field(default=PROVIDER_EVENT_DISPATCH_AUDIENCE)


class ProviderEventDispatchValidationError(ValueError):
    """Raised when one dispatch request fails authorization validation."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True)
class LiveProviderEventDispatchOutcome:
    """Result of one live provider-event dispatch attempt."""

    operation_id: str
    run_id: int
    run_key: str
    captured_task_revision: int
    status: PublicDispatchStatus
    launch_count: int
    adopted_only: bool
    terminal_reason: str | None = None


def validate_provider_event_dispatch_request(
    request: ProviderEventDispatchRequest,
    *,
    ttl_seconds: int,
    now: datetime | None = None,
) -> None:
    """Validate audience, dispatch mode, and request freshness."""

    if request.audience != PROVIDER_EVENT_DISPATCH_AUDIENCE:
        raise ProviderEventDispatchValidationError("invalid_audience")
    if request.dispatch_mode != "live":
        raise ProviderEventDispatchValidationError("invalid_dispatch_mode")
    current_time = now or datetime.now(timezone.utc)
    issued_at = request.issued_at
    if issued_at.tzinfo is None:
        issued_at = issued_at.replace(tzinfo=timezone.utc)
    age_seconds = (current_time - issued_at.astimezone(timezone.utc)).total_seconds()
    if age_seconds < 0 or age_seconds > ttl_seconds:
        raise ProviderEventDispatchValidationError("dispatch_request_expired")


def public_status_for_inbox_state(state: str) -> PublicDispatchStatus:
    """Map durable inbox state to the public dispatch status vocabulary."""

    if state == "started":
        return "started"
    if state == "terminal":
        return "terminal"
    return "adopted"


def dispatch_snapshot(
    request: ProviderEventDispatchRequest,
    *,
    captured_task_revision: int,
) -> LiveDispatchInboxSnapshot:
    """Return the authorization snapshot stored with one inbox adoption."""

    return LiveDispatchInboxSnapshot(
        run_key=request.run_key,
        receipt_id=request.receipt_id,
        accepted_activation_revision=request.accepted_activation_revision,
        captured_task_revision=captured_task_revision,
    )


def dispatch_provider_event_live(
    *,
    inbox: ProviderEventLiveDispatchInbox,
    request: ProviderEventDispatchRequest,
    captured_task_revision: int,
    start_instance: Callable[[ProviderEventDispatchRequest, int], None],
) -> LiveProviderEventDispatchOutcome:
    """Adopt one live dispatch operation, then start at most one task instance.

    # TODO: Remove once live adoption is recorded only through Orchestra
    downstream adoption and callers stop passing a container-local inbox.
    """

    if request.dispatch_mode != "live":
        raise ProviderEventDispatchValidationError("invalid_dispatch_mode")

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
        return LiveProviderEventDispatchOutcome(
            operation_id=adopted.operation_id,
            run_id=adopted.run_id,
            run_key=adopted.run_key,
            captured_task_revision=adopted.captured_task_revision,
            status=public_status_for_inbox_state(adopted.state),
            launch_count=adopted.launch_count,
            adopted_only=True,
            terminal_reason=adopted.terminal_reason,
        )

    claimed = inbox.claim_start(operation_id=request.operation_id)
    if not claimed.owns_start:
        return LiveProviderEventDispatchOutcome(
            operation_id=claimed.operation_id,
            run_id=claimed.run_id,
            run_key=claimed.run_key,
            captured_task_revision=claimed.captured_task_revision,
            status=public_status_for_inbox_state(claimed.state),
            launch_count=claimed.launch_count,
            adopted_only=True,
            terminal_reason=claimed.terminal_reason,
        )

    try:
        start_instance(request, claimed.captured_task_revision)
    except ProviderEventDispatchValidationError:
        inbox.mark_terminal(
            operation_id=request.operation_id,
            reason="live_instance_start_failed",
        )
        raise

    started = inbox.start_if_owner(operation_id=request.operation_id)
    return LiveProviderEventDispatchOutcome(
        operation_id=started.operation_id,
        run_id=started.run_id,
        run_key=started.run_key,
        captured_task_revision=started.captured_task_revision,
        status=public_status_for_inbox_state(started.state),
        launch_count=started.launch_count,
        adopted_only=False,
        terminal_reason=started.terminal_reason,
    )


__all__ = [
    "PROVIDER_EVENT_DISPATCH_AUDIENCE",
    "LiveProviderEventDispatchOutcome",
    "ProviderEventDispatchRequest",
    "ProviderEventDispatchValidationError",
    "PublicDispatchStatus",
    "dispatch_provider_event_live",
    "dispatch_snapshot",
    "public_status_for_inbox_state",
    "validate_provider_event_dispatch_request",
]
