"""Enumerated states for a task lifecycle."""

from enum import StrEnum


class Status(StrEnum):
    scheduled = "scheduled"
    triggerable = "triggerable"
    active = "active"
    completed = "completed"
    cancelled = "cancelled"
    failed = "failed"


def to_status(value: Status | str | None) -> Status:
    """Convert a status-like value to a Status enum.

    Returns ``Status.scheduled`` for unrecognised or None values.
    """
    if isinstance(value, Status):
        return value
    try:
        return Status(value)
    except Exception:
        return Status.scheduled
