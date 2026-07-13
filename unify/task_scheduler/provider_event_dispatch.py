"""Live provider-event dispatch handler for Unity."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Literal

from pydantic import BaseModel, Field

from unify.task_scheduler.provider_event_dispatch_inbox import (
    ProviderEventLiveDispatchInbox,
)


class ProviderEventDispatchRequest(BaseModel):
    """Internal dispatch authorization for provider-event live execution."""

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
    audience: str = Field(default="unity:provider-event-dispatch")


@dataclass(frozen=True)
class LiveProviderEventDispatchOutcome:
    """Result of one live provider-event dispatch attempt."""

    operation_id: str
    run_id: int
    captured_task_revision: int
    inbox_state: str
    launch_count: int
    adopted_only: bool


def dispatch_provider_event_live(
    *,
    inbox: ProviderEventLiveDispatchInbox,
    request: ProviderEventDispatchRequest,
    captured_task_revision: int,
    start_instance: Callable[[ProviderEventDispatchRequest, int], None],
) -> LiveProviderEventDispatchOutcome:
    """Adopt one live dispatch operation, then start at most one task instance."""

    if request.dispatch_mode != "live":
        raise ValueError("live dispatch handler requires dispatch_mode=live")

    adopted = inbox.adopt_or_get(
        operation_id=request.operation_id,
        run_id=request.run_id,
        captured_task_revision=captured_task_revision,
    )
    if adopted.state == "started":
        return LiveProviderEventDispatchOutcome(
            operation_id=adopted.operation_id,
            run_id=adopted.run_id,
            captured_task_revision=adopted.captured_task_revision,
            inbox_state=adopted.state,
            launch_count=adopted.launch_count,
            adopted_only=True,
        )

    claimed = inbox.claim_start(operation_id=request.operation_id)
    if claimed.state != "starting":
        return LiveProviderEventDispatchOutcome(
            operation_id=claimed.operation_id,
            run_id=claimed.run_id,
            captured_task_revision=claimed.captured_task_revision,
            inbox_state=claimed.state,
            launch_count=claimed.launch_count,
            adopted_only=True,
        )

    start_instance(request, captured_task_revision)
    started = inbox.start_if_owner(operation_id=request.operation_id)
    return LiveProviderEventDispatchOutcome(
        operation_id=started.operation_id,
        run_id=started.run_id,
        captured_task_revision=started.captured_task_revision,
        inbox_state=started.state,
        launch_count=started.launch_count,
        adopted_only=False,
    )
