"""Provider-event execution against the authored definition (no Task clones)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.helpers import _handle_project
from unify.actor.simulated import SimulatedActor
from unify.task_scheduler.provider_event_dispatch import (
    ProviderEventDispatchRequest,
    ProviderEventDispatchValidationError,
)
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.task_scheduler.types.status import Status

_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "task_trigger_contract"
)


def _provider_event_trigger() -> dict:
    return json.loads(
        (_FIXTURE_DIR / "task_trigger.provider_event.v1.json").read_text(
            encoding="utf-8",
        ),
    )


def _seed_provider_event_definition(
    scheduler: TaskScheduler,
    *,
    task_revision: int = 5,
    enabled: bool = True,
    provider_event_binding_id: str | None = "binding-1",
) -> int:
    next_task_id = int(scheduler._store.get_metric_max(key="task_id") or 0) + 1
    payload = {
        "task_id": next_task_id,
        "instance_id": 0,
        "name": "GitHub issue triage",
        "description": "Triage new GitHub issues.",
        "status": Status.triggerable.value,
        "trigger": _provider_event_trigger(),
        "enabled": enabled,
        "task_revision": task_revision,
        "priority": "normal",
    }
    if provider_event_binding_id is not None:
        payload["provider_event_binding_id"] = provider_event_binding_id
    with scheduler._use_task_destination(None):
        scheduler._store.log(entries=payload, new=True)
    return next_task_id


def _request(*, task_id: int, operation_id: str) -> ProviderEventDispatchRequest:
    return ProviderEventDispatchRequest(
        operation_id=operation_id,
        run_id=9001,
        run_key=(
            f"live:provider_event:42:{task_id}:binding-1:revdigest:" f"{'a' * 64}"
        ),
        assistant_id="42",
        task_id=task_id,
        binding_id="binding-1",
        receipt_id=f"receipt-{operation_id}",
        accepted_revision="rev-accepted-1",
        event_context_ref=f"blob://binding-1/receipt-{operation_id}",
        issued_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
@_handle_project
async def test_provider_event_start_leaves_definition_unarmed():
    scheduler = TaskScheduler(actor=SimulatedActor(steps=None, duration=None))
    task_id = _seed_provider_event_definition(scheduler, task_revision=5)
    definition_before = scheduler._get_task_or_raise(task_id)
    assert definition_before.instance_id == 0
    assert definition_before.status == Status.triggerable

    untrusted = {"kind": "provider_event_context", "trust": "untrusted_data"}
    with patch(
        "unify.task_scheduler.task_scheduler.update_task_run_record",
    ) as update_run:
        handle = await scheduler.start_provider_event_instance(
            request=_request(task_id=task_id, operation_id="op-exec-1"),
            captured_task_revision=5,
            provider_event_context=untrusted,
        )
        update_run.assert_called_once()
        updates = update_run.call_args[0][1]
        assert updates["captured_task_revision"] == 5
        assert updates["revision"] == "rev-accepted-1"
        assert updates["state"] == "running"

    await handle.stop(reason="test cleanup")

    rows = scheduler._filter_tasks(filter=f"task_id == {task_id}")
    assert len(rows) == 1
    definition_after = rows[0]
    assert definition_after.instance_id == 0
    assert definition_after.status == Status.triggerable
    assert definition_after.activated_by is None


@pytest.mark.asyncio
@_handle_project
async def test_start_rejects_disabled_and_non_provider_triggers():
    scheduler = TaskScheduler(actor=SimulatedActor(steps=None, duration=None))
    disabled_id = _seed_provider_event_definition(
        scheduler,
        task_revision=2,
        enabled=False,
    )
    with pytest.raises(ProviderEventDispatchValidationError) as disabled_exc:
        await scheduler.start_provider_event_instance(
            request=_request(task_id=disabled_id, operation_id="op-disabled"),
            captured_task_revision=2,
            provider_event_context={
                "kind": "provider_event_context",
                "trust": "untrusted_data",
            },
        )
    assert disabled_exc.value.reason_code == "task_disabled"

    from unify.conversation_manager.cm_types import Medium
    from unify.task_scheduler.types.trigger import CommunicationTrigger

    plain_id = scheduler._create_task(
        name="Plain task",
        description="Not a provider-event task",
        trigger=CommunicationTrigger(
            medium=Medium.EMAIL,
            from_contact_ids=[1],
        ),
    )["details"]["task_id"]
    with pytest.raises(ProviderEventDispatchValidationError) as mismatch_exc:
        await scheduler.start_provider_event_instance(
            request=_request(task_id=plain_id, operation_id="op-mismatch"),
            captured_task_revision=1,
            provider_event_context={
                "kind": "provider_event_context",
                "trust": "untrusted_data",
            },
        )
    assert mismatch_exc.value.reason_code == "task_trigger_mismatch"

    disabled_rows = scheduler._filter_tasks(filter=f"task_id == {disabled_id}")
    assert len(disabled_rows) == 1
