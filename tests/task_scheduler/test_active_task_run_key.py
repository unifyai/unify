"""ActiveTask run_key resolution for EventBus lineage."""

from __future__ import annotations

from unify.task_scheduler.active_task import _resolve_active_task_run_key
from unify.task_scheduler.machine_state import (
    TaskRunProvenance,
    TaskRunReference,
    build_task_run_key,
)
from unify.task_scheduler.types.execution import Delivery, Wake


def test_resolve_run_key_prefers_reference():
    ref = TaskRunReference(
        assistant_id="1",
        run_key="from-ref",
    )
    prov = TaskRunProvenance(
        assistant_id="1",
        task_id=2,
        wake=Wake.scheduled,
        delivery=Delivery.live,
        revision="rev",
    )
    assert (
        _resolve_active_task_run_key(
            task_run_reference=ref,
            task_run_provenance=prov,
        )
        == "from-ref"
    )


def test_resolve_run_key_from_provenance():
    prov = TaskRunProvenance(
        assistant_id="1",
        task_id=42,
        wake=Wake.scheduled,
        delivery=Delivery.live,
        revision="rev-1",
    )
    expected = build_task_run_key(prov)
    assert (
        _resolve_active_task_run_key(
            task_run_reference=None,
            task_run_provenance=prov,
        )
        == expected
    )
    assert expected


def test_resolve_run_key_none_without_inputs():
    assert (
        _resolve_active_task_run_key(
            task_run_reference=None,
            task_run_provenance=None,
        )
        is None
    )
