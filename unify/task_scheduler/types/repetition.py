"""
Schema describing how a task repeats over time. The model serializes to and
from JSON for storage and transport.

The recurrence subset (pattern schema, normalization, next-occurrence
projection, deterministic dispatch jitter) is deliberately mirrored into
``orchestra/services/task_repetition.py`` so Orchestra can re-project a future
head for a repeating series whose worker died before dispatch. Any change to
those semantics MUST be applied to both files in the same changeset; the two
copies are pinned against each other by the shared
``repeat_projection_vectors.json`` checked into each repo's test tree with
byte-identical content.
"""

from __future__ import annotations

from enum import Enum
from typing import List, Optional
from datetime import datetime, time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from pydantic import BaseModel, Field, field_validator


class Frequency(str, Enum):
    MINUTELY = "minutely"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"


class Weekday(str, Enum):
    MO = "MO"
    TU = "TU"
    WE = "WE"
    TH = "TH"
    FR = "FR"
    SA = "SA"
    SU = "SU"


class RepeatPattern(BaseModel):
    """
    A small subset of RFC-5545 RRULE expressed as first-class fields:

    * **frequency** – base unit of recurrence.
    * **interval**  – "every *n* units"; defaults to 1.
    * **weekdays**  – which days of the week (only when `frequency=weekly`).
    * **count**     – stop after *count* occurrences.
    * **until**     – or stop at this date/time (ISO-8601).
    * **time_of_day** – local *clock* time at which each occurrence starts.

    Calendar slot schedules can still be represented by creating multiple
    `RepeatPattern` instances for a single task.
    """

    frequency: Frequency = Field(..., description="Base unit of recurrence")
    interval: int = Field(
        default=1,
        ge=1,
        description="Number of frequency units between each repeat",
    )
    weekdays: Optional[List[Weekday]] = Field(
        default=None,
        description="Applicable only when frequency == weekly; " "ignored otherwise",
    )
    count: Optional[int] = Field(
        default=None,
        ge=1,
        description="Total number of occurrences before stopping",
    )
    until: Optional[datetime] = Field(
        default=None,
        description="Hard cut-off date/time after which no repeats occur",
    )
    time_of_day: Optional[time] = Field(
        default=None,
        description=(
            "Clock time at which the task should start on each occurrence "
            "(e.g. 09:00).  When omitted the time is resolved dynamically "
            "by the scheduler."
        ),
    )
    timezone: Optional[str] = Field(
        default=None,
        description=(
            "IANA zone the `time_of_day` clock reading belongs to, e.g. "
            "'Asia/Karachi'. Omitted means UTC, which is what every schedule "
            "written before this field existed meant by omission. Set it "
            "whenever the time expresses a human hour -- 'before stand-up', "
            "'end of day' -- because those are claims about somebody's local "
            "clock and mean nothing in UTC: a 17:30 end-of-day log fires at "
            "22:30 for a reader five hours east, and the previous morning for "
            "one eight hours west."
        ),
    )
    jitter_seconds: Optional[int] = Field(
        default=None,
        ge=0,
        description=(
            "Maximum random delay, in seconds, added to each occurrence's "
            "dispatch time. None/0 fires exactly on the computed slot. A "
            "positive value de-correlates a recurring job from the wall clock "
            "(e.g. a browser scrape that shouldn't run at a predictable time). "
            "The jitter is applied to the dispatch time only; the canonical "
            "anchor is preserved across re-arms so occurrences never drift."
        ),
    )

    @field_validator("weekdays")
    def _weekdays_only_for_weekly(cls, v, info):
        if v is not None and info.data.get("frequency") != Frequency.WEEKLY:
            raise ValueError("`weekdays` only makes sense with weekly frequency")
        return v

    @field_validator("time_of_day")
    def _time_without_date(cls, v):
        """
        Disallow accidental full datetimes – the field must be a *time* only.
        """
        if isinstance(v, datetime):
            raise ValueError(
                "`time_of_day` must be a `datetime.time`, not a full datetime",
            )
        return v

    @field_validator("timezone")
    def _resolvable_zone(cls, v):
        """Reject a zone this machine cannot resolve.

        A typo would otherwise surface as a schedule firing at the wrong hour,
        which is indistinguishable from the bug this field exists to fix.
        """
        if v is None:
            return v
        try:
            ZoneInfo(v)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"Unknown IANA timezone: {v!r}") from exc
        return v


_WEEKDAY_TO_INDEX = {
    Weekday.MO: 0,
    Weekday.TU: 1,
    Weekday.WE: 2,
    Weekday.TH: 3,
    Weekday.FR: 4,
    Weekday.SA: 5,
    Weekday.SU: 6,
}


def normalize_repeat_patterns(
    patterns: list[RepeatPattern] | None,
) -> list[RepeatPattern] | None:
    """Collapse obvious interval encodings into a single recurrence rule."""

    if not patterns:
        return patterns
    if len(patterns) <= 1:
        return patterns

    normalized = _normalize_full_day_daily_slots(patterns)
    return normalized if normalized is not None else patterns


def _normalize_full_day_daily_slots(
    patterns: list[RepeatPattern],
) -> list[RepeatPattern] | None:
    """Return a minutely rule for evenly spaced daily slots spanning the day."""

    first = patterns[0]
    if any(
        pattern.frequency != Frequency.DAILY
        or pattern.interval != 1
        or pattern.weekdays is not None
        or pattern.time_of_day is None
        or pattern.count != first.count
        or pattern.until != first.until
        for pattern in patterns
    ):
        return None

    slots = sorted(
        (pattern.time_of_day.hour * 60 + pattern.time_of_day.minute)
        for pattern in patterns
        if pattern.time_of_day is not None
    )
    if len(slots) != len(set(slots)):
        return None
    if not slots or slots[0] != 0:
        return None

    wrapped_slots = slots + [24 * 60]
    gaps = [right - left for left, right in zip(wrapped_slots, wrapped_slots[1:])]
    if not gaps or any(gap != gaps[0] for gap in gaps):
        return None

    interval_minutes = gaps[0]
    if interval_minutes <= 0 or (24 * 60) % interval_minutes != 0:
        return None

    return [
        RepeatPattern(
            frequency=Frequency.MINUTELY,
            interval=interval_minutes,
            count=first.count,
            until=first.until,
        ),
    ]
