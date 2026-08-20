"""A bundle's wall-clock slot belongs to the installer's day, not UTC's.

Every shelf workflow plants a bare clock reading that means a human hour --
"before stand-up", "end of day". Resolved as UTC they land somewhere else
entirely for anyone outside it: a 17:30 ship log arrives at 22:30 five hours
east and the previous morning eight hours west.

A manifest cannot name the zone, because one shelf serves every assistant.
The installer can, and does so at the moment a universal bundle becomes one
assistant's planted task.
"""

from unittest.mock import patch

from unify.task_scheduler.custom_tasks import _localize_repeat

_MODULE = "unify.task_scheduler.custom_tasks.get_assistant_timezone"
_WEEKDAYS = ["MO", "TU", "WE", "TH", "FR"]


def _slot(time_of_day: str, **extra):
    return {
        "frequency": "weekly",
        "weekdays": _WEEKDAYS,
        "time_of_day": time_of_day,
        **extra,
    }


def test_a_bundle_slot_is_anchored_to_the_assistants_zone():
    with patch(_MODULE, return_value="Asia/Karachi"):
        localized = _localize_repeat([_slot("08:30:00")])

    assert localized[0]["timezone"] == "Asia/Karachi"


def test_every_slot_is_anchored_not_only_the_first():
    """A multi-slot cadence must not end up half local and half UTC."""
    with patch(_MODULE, return_value="Asia/Karachi"):
        localized = _localize_repeat([_slot("09:15:00"), _slot("14:15:00")])

    assert [slot["timezone"] for slot in localized] == ["Asia/Karachi"] * 2


def test_an_assistant_with_no_zone_keeps_todays_behaviour():
    """Absent a zone there is nothing better to assume than UTC."""
    with patch(_MODULE, return_value=None):
        localized = _localize_repeat([_slot("08:30:00")])

    assert "timezone" not in localized[0]


def test_a_zone_the_source_named_itself_wins():
    """An author naming one means the schedule is genuinely absolute."""
    with patch(_MODULE, return_value="Asia/Karachi"):
        localized = _localize_repeat([_slot("13:00:00", timezone="UTC")])

    assert localized[0]["timezone"] == "UTC"


def test_a_slot_with_no_clock_reading_is_left_alone():
    """Nothing to anchor: the scheduler resolves the time dynamically."""
    with patch(_MODULE, return_value="Asia/Karachi"):
        localized = _localize_repeat([{"frequency": "daily"}])

    assert "timezone" not in localized[0]


def test_the_source_list_is_not_mutated():
    """Collection runs per reconcile; a mutated source would compound."""
    source = [_slot("08:30:00")]
    with patch(_MODULE, return_value="Asia/Karachi"):
        _localize_repeat(source)

    assert "timezone" not in source[0]


def test_the_resolved_zone_changes_the_task_hash():
    """So an assistant that moves timezone re-plants on the next reconcile.

    The hash covers `repeat`, so carrying the zone inside it is what makes a
    relocation self-healing instead of leaving the user on the hours of a
    country they left.
    """
    from unify.task_scheduler.custom_tasks import _compute_task_hash

    def _hash(zone):
        with patch(_MODULE, return_value=zone):
            repeat = _localize_repeat([_slot("08:30:00")])
        return _compute_task_hash(
            key="daily_briefing/morning",
            destination="personal",
            fields={"repeat": repeat},
        )

    assert _hash("Asia/Karachi") != _hash("America/New_York")
    assert _hash("Asia/Karachi") != _hash(None)
