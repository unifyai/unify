"""Unity live provider-event dispatch adoption tests."""

from __future__ import annotations

from datetime import datetime, timezone

from unify.task_scheduler.provider_event_dispatch_inbox import (
    ProviderEventLiveDispatchInbox,
)
from unify.task_scheduler.provider_event_dispatch import (
    ProviderEventDispatchRequest,
    dispatch_provider_event_live,
)


def _request(**overrides) -> ProviderEventDispatchRequest:
    payload = {
        "operation_id": "op-provider-event-live-1",
        "run_id": 7001,
        "run_key": "live:provider_event:assistant-1:task-1:rev:evt-1",
        "assistant_id": "assistant-1",
        "task_id": 101,
        "binding_id": "binding-1",
        "receipt_id": "receipt-1",
        "accepted_activation_revision": "rev-1",
        "event_context_ref": "blob://binding-1/receipt-1",
        "issued_at": datetime.now(timezone.utc),
    }
    payload.update(overrides)
    return ProviderEventDispatchRequest(**payload)


def test_duplicate_live_provider_dispatch_adopts_captured_revision_and_executes_once(
    tmp_path,
) -> None:
    inbox = ProviderEventLiveDispatchInbox(tmp_path / "live-dispatch-inbox.sqlite3")
    launch_calls: list[tuple[str, int]] = []

    def start_instance(request: ProviderEventDispatchRequest, revision: int) -> None:
        launch_calls.append((request.operation_id, revision))

    request = _request()
    first = dispatch_provider_event_live(
        inbox=inbox,
        request=request,
        captured_task_revision=4,
        start_instance=start_instance,
    )
    second = dispatch_provider_event_live(
        inbox=inbox,
        request=request,
        captured_task_revision=99,
        start_instance=start_instance,
    )

    assert first.launch_count == 1
    assert first.captured_task_revision == 4
    assert second.launch_count == 1
    assert second.captured_task_revision == 4
    assert second.adopted_only is True
    assert launch_calls == [("op-provider-event-live-1", 4)]

    adopted = inbox.adopt_or_get(
        operation_id=request.operation_id,
        run_id=request.run_id,
        captured_task_revision=4,
    )
    assert adopted.state == "started"
    assert adopted.launch_count == 1
