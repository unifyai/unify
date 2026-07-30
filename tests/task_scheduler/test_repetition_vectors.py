"""Drift guard for the recurrence arithmetic mirrored into Orchestra.

``unify/task_scheduler/types/repetition.py`` is deliberately copied into
``orchestra/services/task_repetition.py``; the two must stay in lockstep or
Orchestra's release-time head re-projection and Unify's dispatch-time successor
projection would name different slots for the same series. Both repos check in
a byte-identical ``repeat_projection_vectors.json`` and run their own
implementation against every vector, so a change on either side that shifts
the semantics fails here rather than in production.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from unify.task_scheduler.types.repetition import (
    RepeatPattern,
    deterministic_jitter_seconds,
    next_repeated_start_at,
)

_VECTORS = json.loads(
    Path(__file__)
    .with_name("repeat_projection_vectors.json")
    .read_text(
        encoding="utf-8",
    ),
)


@pytest.mark.parametrize(
    "case",
    _VECTORS["next_occurrence"],
    ids=lambda case: case["name"],
)
def test_next_occurrence_matches_shared_vector(case: dict) -> None:
    result = next_repeated_start_at(
        previous_start=datetime.fromisoformat(case["previous_start"]),
        patterns=[RepeatPattern.model_validate(p) for p in case["patterns"]],
        current_occurrence_index=case.get("current_occurrence_index", 0),
        now=datetime.fromisoformat(case["now"]),
    )
    assert (result.isoformat() if result else None) == case["expected"]


@pytest.mark.parametrize(
    "case",
    _VECTORS["dispatch_jitter"],
    ids=lambda case: case["name"],
)
def test_dispatch_jitter_matches_shared_vector(case: dict) -> None:
    offset = deterministic_jitter_seconds(
        task_id=case["task_id"],
        slot=datetime.fromisoformat(case["slot"]),
        patterns=[RepeatPattern.model_validate(p) for p in case["patterns"]],
    )
    assert offset == case["expected"]
