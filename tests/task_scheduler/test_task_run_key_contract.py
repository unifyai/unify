"""The run key built here must match the one Orchestra projects for the occurrence.

Create-or-adopt converges only when both sides produce the same key. When they
drift, the dispatcher cannot adopt the execution Orchestra projected and mints a
second one for the same slot; two rows read as concurrency, and an overlap guard
then skips every tick — a silent halt, not an error.

Orchestra pins the same fixture against ``_build_open_execution_run_key``, so a
change on either side that the other does not follow fails in that side's own
suite rather than only downstream.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from unify.task_scheduler.machine_state import TaskRunProvenance, build_task_run_key
from unify.task_scheduler.types.execution import Delivery, Wake

_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "task_trigger_contract"
    / "task_run_key_contract.v1.json"
)


def _cases() -> list[dict]:
    return json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))["cases"]


def _provenance(inputs: dict) -> TaskRunProvenance:
    """Map one contract case onto the provenance record that carries those facts."""

    return TaskRunProvenance(
        assistant_id=inputs["assistant_id"],
        task_id=inputs["task_id"],
        wake=Wake.normalize(inputs["wake"]),
        delivery=Delivery.normalize(inputs["delivery"]),
        revision=inputs["revision"],
        destination=inputs["destination"],
        scheduled_for=inputs["due_at"],
    )


@pytest.mark.parametrize("case", _cases(), ids=lambda case: case["name"])
def test_run_key_matches_shared_contract(case: dict) -> None:
    assert build_task_run_key(_provenance(case["inputs"])) == case["run_key"]


def test_free_form_components_are_normalized() -> None:
    """Anything that reaches the key is lowercased and hyphen-separated."""

    key = build_task_run_key(
        TaskRunProvenance(
            assistant_id="1406",
            task_id=12,
            wake=Wake.scheduled,
            delivery=Delivery.offline,
            revision="r1",
            destination="Team:11",
            scheduled_for="2026-07-29T16:50:00+00:00",
        ),
    )

    assert ":team-11:" in key
