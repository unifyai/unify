"""Generate ``repeat_projection_vectors.json`` from unify's implementation.

The vector file is the drift guard for the recurrence arithmetic mirrored
between ``unify/task_scheduler/types/repetition.py`` and
``orchestra/services/task_repetition.py``. Expected values are always produced
by the canonical unify code, so run this here after any deliberate
recurrence-semantics change (applied to both repos in the same changeset):

    uv run python tests/task_scheduler/generate_repeat_projection_vectors.py

Then copy the regenerated file byte-identically into orchestra's test tree
(``orchestra/tests/test_tasks/repeat_projection_vectors.json``). Each repo's
``test_repetition_vectors.py`` runs its own implementation against every
vector, so a semantic change ported to only one side fails there.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

from unify.task_scheduler.types.repetition import (
    RepeatPattern,
    deterministic_jitter_seconds,
    next_repeated_start_at,
)


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


NEXT_OCCURRENCE_CASES = [
    {
        "name": "minutely_10_advances_one_slot",
        "previous_start": "2026-07-29T22:00:00+00:00",
        "now": "2026-07-29T22:00:00+00:00",
        "patterns": [{"frequency": "minutely", "interval": 10}],
    },
    {
        "name": "minutely_10_crash_catch_up_skips_to_first_future_slot",
        "previous_start": "2026-07-29T22:10:00+00:00",
        "now": "2026-07-30T09:47:23+00:00",
        "patterns": [{"frequency": "minutely", "interval": 10}],
    },
    {
        "name": "hourly_6",
        "previous_start": "2026-07-29T05:15:00+00:00",
        "now": "2026-07-29T05:15:00+00:00",
        "patterns": [{"frequency": "hourly", "interval": 6}],
    },
    {
        "name": "daily_preserves_clock_time",
        "previous_start": "2026-07-29T08:30:00+00:00",
        "now": "2026-07-29T08:30:00+00:00",
        "patterns": [{"frequency": "daily"}],
    },
    {
        "name": "daily_time_of_day_next_day_when_consumed",
        "previous_start": "2026-07-29T09:00:00+00:00",
        "now": "2026-07-29T09:00:00+00:00",
        "patterns": [{"frequency": "daily", "time_of_day": "09:00:00"}],
    },
    {
        "name": "daily_time_of_day_same_day_when_still_ahead",
        "previous_start": "2026-07-29T03:00:00+00:00",
        "now": "2026-07-29T03:00:00+00:00",
        "patterns": [{"frequency": "daily", "time_of_day": "09:00:00"}],
    },
    {
        "name": "weekly_interval_2_without_weekdays",
        "previous_start": "2026-07-28T10:00:00+00:00",
        "now": "2026-07-28T10:00:00+00:00",
        "patterns": [{"frequency": "weekly", "interval": 2}],
    },
    {
        "name": "weekly_weekdays_picks_next_allowed_day",
        "previous_start": "2026-07-29T10:00:00+00:00",
        "now": "2026-07-29T10:00:00+00:00",
        "patterns": [{"frequency": "weekly", "weekdays": ["MO", "FR"]}],
    },
    {
        "name": "weekly_weekdays_interval_2_respects_cadence",
        "previous_start": "2026-07-31T10:00:00+00:00",
        "now": "2026-07-31T10:00:00+00:00",
        "patterns": [{"frequency": "weekly", "interval": 2, "weekdays": ["MO", "FR"]}],
    },
    {
        "name": "weekly_weekdays_time_of_day_override",
        "previous_start": "2026-07-29T10:00:00+00:00",
        "now": "2026-07-29T10:00:00+00:00",
        "patterns": [
            {
                "frequency": "weekly",
                "weekdays": ["FR"],
                "time_of_day": "07:45:00",
            },
        ],
    },
    {
        "name": "monthly_clamps_to_month_end",
        "previous_start": "2026-01-31T12:00:00+00:00",
        "now": "2026-01-31T12:00:00+00:00",
        "patterns": [{"frequency": "monthly"}],
    },
    {
        "name": "yearly_clamps_leap_day",
        "previous_start": "2024-02-29T00:00:00+00:00",
        "now": "2024-02-29T00:00:00+00:00",
        "patterns": [{"frequency": "yearly"}],
    },
    {
        "name": "count_1_is_exhausted",
        "previous_start": "2026-07-29T22:00:00+00:00",
        "now": "2026-07-29T22:00:00+00:00",
        "patterns": [{"frequency": "minutely", "interval": 10, "count": 1}],
    },
    {
        "name": "count_3_index_2_is_exhausted",
        "previous_start": "2026-07-29T22:00:00+00:00",
        "now": "2026-07-29T22:00:00+00:00",
        "current_occurrence_index": 2,
        "patterns": [{"frequency": "minutely", "interval": 10, "count": 3}],
    },
    {
        "name": "count_3_index_1_still_advances",
        "previous_start": "2026-07-29T22:00:00+00:00",
        "now": "2026-07-29T22:00:00+00:00",
        "current_occurrence_index": 1,
        "patterns": [{"frequency": "minutely", "interval": 10, "count": 3}],
    },
    {
        "name": "until_allows_slot_inside_window",
        "previous_start": "2026-07-29T22:00:00+00:00",
        "now": "2026-07-29T22:00:00+00:00",
        "patterns": [
            {
                "frequency": "minutely",
                "interval": 10,
                "until": "2026-07-29T22:15:00+00:00",
            },
        ],
    },
    {
        "name": "until_cuts_off_slot_outside_window",
        "previous_start": "2026-07-29T22:00:00+00:00",
        "now": "2026-07-29T22:00:00+00:00",
        "patterns": [
            {
                "frequency": "minutely",
                "interval": 10,
                "until": "2026-07-29T22:05:00+00:00",
            },
        ],
    },
    {
        "name": "naive_until_compares_against_aware_candidate",
        "previous_start": "2026-07-29T22:00:00+00:00",
        "now": "2026-07-29T22:00:00+00:00",
        "patterns": [
            {
                "frequency": "minutely",
                "interval": 10,
                "until": "2026-07-29T22:15:00",
            },
        ],
    },
    {
        "name": "naive_previous_start_stays_naive",
        "previous_start": "2026-07-29T22:00:00",
        "now": "2026-07-29T22:00:00",
        "patterns": [{"frequency": "minutely", "interval": 10}],
    },
    {
        "name": "full_day_daily_slots_normalize_to_minutely",
        "previous_start": "2026-07-29T12:00:00+00:00",
        "now": "2026-07-29T12:00:00+00:00",
        "patterns": [
            {"frequency": "daily", "time_of_day": "00:00:00"},
            {"frequency": "daily", "time_of_day": "06:00:00"},
            {"frequency": "daily", "time_of_day": "12:00:00"},
            {"frequency": "daily", "time_of_day": "18:00:00"},
        ],
    },
    {
        "name": "earliest_candidate_wins_across_patterns",
        "previous_start": "2026-07-29T22:00:00+00:00",
        "now": "2026-07-29T22:00:00+00:00",
        "patterns": [
            {"frequency": "hourly", "interval": 2},
            {"frequency": "daily"},
        ],
    },
    {
        "name": "minutely_10_survives_a_month_long_outage",
        "previous_start": "2026-06-29T22:10:00+00:00",
        "now": "2026-07-30T09:47:23+00:00",
        "patterns": [{"frequency": "minutely", "interval": 10}],
    },
    {
        "name": "minutely_1_survives_a_multi_year_outage",
        "previous_start": "2024-01-01T00:00:00+00:00",
        "now": "2026-07-30T09:00:30+00:00",
        "patterns": [{"frequency": "minutely", "interval": 1}],
    },
    {
        "name": "hourly_6_survives_a_two_year_outage",
        "previous_start": "2024-07-29T05:15:00+00:00",
        "now": "2026-07-30T14:03:00+00:00",
        "patterns": [{"frequency": "hourly", "interval": 6}],
    },
    {
        "name": "outage_past_a_distant_until_is_exhausted",
        "previous_start": "2026-01-01T00:00:00+00:00",
        "now": "2026-07-30T00:00:00+00:00",
        "patterns": [
            {
                "frequency": "minutely",
                "interval": 10,
                "until": "2026-02-01T00:00:00+00:00",
            },
        ],
    },
]

JITTER_CASES = [
    {
        "name": "no_budget_yields_zero",
        "task_id": 12,
        "slot": "2026-07-30T10:00:00+00:00",
        "patterns": [{"frequency": "minutely", "interval": 10}],
    },
    {
        "name": "budget_300_task_12",
        "task_id": 12,
        "slot": "2026-07-30T10:00:00+00:00",
        "patterns": [
            {"frequency": "minutely", "interval": 10, "jitter_seconds": 300},
        ],
    },
    {
        "name": "budget_300_task_13_differs_by_task",
        "task_id": 13,
        "slot": "2026-07-30T10:00:00+00:00",
        "patterns": [
            {"frequency": "minutely", "interval": 10, "jitter_seconds": 300},
        ],
    },
    {
        "name": "budget_300_task_12_differs_by_slot",
        "task_id": 12,
        "slot": "2026-07-30T10:10:00+00:00",
        "patterns": [
            {"frequency": "minutely", "interval": 10, "jitter_seconds": 300},
        ],
    },
    {
        "name": "largest_budget_across_patterns_wins",
        "task_id": 12,
        "slot": "2026-07-30T10:00:00+00:00",
        "patterns": [
            {"frequency": "hourly", "jitter_seconds": 60},
            {"frequency": "daily", "jitter_seconds": 600},
        ],
    },
]


def main() -> None:
    next_occurrence = []
    for case in NEXT_OCCURRENCE_CASES:
        result = next_repeated_start_at(
            previous_start=_dt(case["previous_start"]),
            patterns=[RepeatPattern.model_validate(p) for p in case["patterns"]],
            current_occurrence_index=case.get("current_occurrence_index", 0),
            now=_dt(case["now"]),
        )
        next_occurrence.append(
            {**case, "expected": result.isoformat() if result else None},
        )

    dispatch_jitter = []
    for case in JITTER_CASES:
        offset = deterministic_jitter_seconds(
            task_id=case["task_id"],
            slot=_dt(case["slot"]),
            patterns=[RepeatPattern.model_validate(p) for p in case["patterns"]],
        )
        dispatch_jitter.append({**case, "expected": offset})

    payload = {
        "_contract": (
            "Shared drift guard for the repeat-rule arithmetic mirrored between "
            "unify/task_scheduler/types/repetition.py and "
            "orchestra/services/task_repetition.py. This file is checked into "
            "both repos with byte-identical content; each repo runs its own "
            "implementation against every vector. Regenerate from unify and "
            "copy to both repos when the semantics deliberately change."
        ),
        "next_occurrence": next_occurrence,
        "dispatch_jitter": dispatch_jitter,
    }
    destination = (
        Path(sys.argv[1])
        if len(sys.argv) > 1
        else Path(__file__).with_name("repeat_projection_vectors.json")
    )
    with open(destination, "w") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


if __name__ == "__main__":
    main()
