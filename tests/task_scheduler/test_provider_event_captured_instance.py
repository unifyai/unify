"""Captured-revision instance creation for live provider-event dispatch."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.helpers import _handle_project
from unify.actor.simulated import SimulatedActor
from unify.task_scheduler.provider_event_dispatch import (
    ProviderEventDispatchRequest,
    ProviderEventDispatchValidationError,
)
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.task_scheduler.types.status import Status
from unify.conversation_manager.cm_types import Medium
from unify.task_scheduler.types.trigger import CommunicationTrigger

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
    """Persist one authored provider-event definition via the Tasks store.

    Uses an explicit ``task_id`` so the generic log seam allows the provider-event
    row as an instance/create with existing identity (not an authored create).
    """

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
        accepted_activation_revision="rev-accepted-1",
        event_context_ref=f"blob://binding-1/receipt-{operation_id}",
        issued_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
@_handle_project
async def test_provider_event_captured_instance_leaves_definition_unarmed():
    scheduler = TaskScheduler(actor=SimulatedActor(steps=None, duration=None))
    task_id = _seed_provider_event_definition(scheduler, task_revision=5)
    definition_before = scheduler._get_task_or_raise(task_id)
    assert definition_before.instance_id == 0
    assert definition_before.status == Status.triggerable

    instance = scheduler._create_provider_event_captured_instance(
        definition=definition_before,
        operation_id="op-captured-1",
        captured_task_revision=5,
        binding_id="binding-1",
    )
    assert instance.task_id == task_id
    assert instance.instance_id != definition_before.instance_id
    assert instance.task_revision == 5
    assert instance.info == "provider_event_operation:op-captured-1"
    assert instance.status == Status.triggerable
    assert instance.provider_event_binding_id == "binding-1"
    reread = next(
        row
        for row in scheduler._filter_tasks(filter=f"task_id == {task_id}")
        if row.instance_id == instance.instance_id
    )
    assert reread.provider_event_binding_id == "binding-1"

    scheduler._update_task_status_instance(
        task_id=instance.task_id,
        instance_id=instance.instance_id,
        new_status=Status.active,
        activated_by="explicit",
    )

    instances = scheduler._filter_tasks(filter=f"task_id == {task_id}")
    assert len(instances) == 2
    by_instance = {row.instance_id: row for row in instances}
    definition_after = by_instance[0]
    assert definition_after.status == Status.triggerable
    assert definition_after.activated_by is None
    active = [row for row in instances if row.status == Status.active]
    assert len(active) == 1
    assert active[0].instance_id == instance.instance_id


@_handle_project
def test_captured_instance_uses_request_binding_when_definition_lacks_it():
    scheduler = TaskScheduler(actor=SimulatedActor(steps=None, duration=None))
    task_id = _seed_provider_event_definition(
        scheduler,
        task_revision=5,
        provider_event_binding_id=None,
    )
    definition = scheduler._get_task_or_raise(task_id)
    assert definition.provider_event_binding_id is None

    instance = scheduler._create_provider_event_captured_instance(
        definition=definition,
        operation_id="op-binding-from-request",
        captured_task_revision=5,
        binding_id="binding-from-dispatch",
    )
    assert instance.provider_event_binding_id == "binding-from-dispatch"
    reread = next(
        row
        for row in scheduler._filter_tasks(filter=f"task_id == {task_id}")
        if row.instance_id == instance.instance_id
    )
    assert reread.provider_event_binding_id == "binding-from-dispatch"


@_handle_project
def test_captured_instance_requires_provider_event_binding_id():
    scheduler = TaskScheduler(actor=SimulatedActor(steps=None, duration=None))
    task_id = _seed_provider_event_definition(
        scheduler,
        task_revision=5,
        provider_event_binding_id=None,
    )
    definition = scheduler._get_task_or_raise(task_id)

    with pytest.raises(ValueError, match="provider_event_binding_id is required"):
        scheduler._create_provider_event_captured_instance(
            definition=definition,
            operation_id="op-missing-binding",
            captured_task_revision=5,
        )
    rows = scheduler._filter_tasks(filter=f"task_id == {task_id}")
    assert len(rows) == 1
    assert rows[0].instance_id == 0


@pytest.mark.asyncio
@_handle_project
async def test_same_provider_event_operation_adopts_one_captured_instance():
    scheduler = TaskScheduler(actor=SimulatedActor(steps=None, duration=None))
    task_id = _seed_provider_event_definition(scheduler, task_revision=4)
    definition = scheduler._get_task_or_raise(task_id)

    first = scheduler._create_provider_event_captured_instance(
        definition=definition,
        operation_id="op-same",
        captured_task_revision=4,
    )
    second = scheduler._create_provider_event_captured_instance(
        definition=definition,
        operation_id="op-same",
        captured_task_revision=4,
    )
    assert first.instance_id == second.instance_id
    assert first.info == "provider_event_operation:op-same"
    instances = scheduler._filter_tasks(filter=f"task_id == {task_id}")
    matching = [
        row
        for row in instances
        if str(row.info or "") == "provider_event_operation:op-same"
    ]
    assert len(matching) == 1


@pytest.mark.asyncio
@_handle_project
async def test_concurrent_same_provider_event_operation_adopts_one_captured_instance():
    scheduler = TaskScheduler(actor=SimulatedActor(steps=None, duration=None))
    task_id = _seed_provider_event_definition(scheduler, task_revision=4)
    definition = scheduler._get_task_or_raise(task_id)

    def _create():
        return scheduler._create_provider_event_captured_instance(
            definition=definition,
            operation_id="op-concurrent",
            captured_task_revision=4,
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: _create(), range(4)))

    instance_ids = {row.instance_id for row in results}
    assert len(instance_ids) == 1
    matching = [
        row
        for row in scheduler._filter_tasks(filter=f"task_id == {task_id}")
        if str(row.info or "") == "provider_event_operation:op-concurrent"
    ]
    assert len(matching) == 1
    assert matching[0].instance_id == results[0].instance_id


@pytest.mark.asyncio
@_handle_project
async def test_two_provider_event_operations_create_two_instances():
    scheduler = TaskScheduler(actor=SimulatedActor(steps=None, duration=None))
    task_id = _seed_provider_event_definition(scheduler, task_revision=3)
    definition = scheduler._get_task_or_raise(task_id)

    first = scheduler._create_provider_event_captured_instance(
        definition=definition,
        operation_id="op-a",
        captured_task_revision=3,
    )
    second = scheduler._create_provider_event_captured_instance(
        definition=definition,
        operation_id="op-b",
        captured_task_revision=3,
    )
    assert first.instance_id != second.instance_id
    assert {first.info, second.info} == {
        "provider_event_operation:op-a",
        "provider_event_operation:op-b",
    }
    instances = scheduler._filter_tasks(filter=f"task_id == {task_id}")
    assert len(instances) == 3
    definition_after = next(row for row in instances if row.instance_id == 0)
    assert definition_after.status == Status.triggerable


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

    # Disabled rejection must not have created a captured instance.
    disabled_rows = scheduler._filter_tasks(filter=f"task_id == {disabled_id}")
    assert len(disabled_rows) == 1
