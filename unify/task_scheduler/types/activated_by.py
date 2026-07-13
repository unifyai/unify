"""Activation reasons for a task instance entering the active state."""

from enum import StrEnum


class ActivatedBy(StrEnum):
    """
    Reason a task instance transitioned into the active state.

    This value is set automatically at activation time and never edited directly.
    Maps to :class:`~unify.task_scheduler.types.run_source.RunSource` via
    :meth:`~unify.task_scheduler.types.run_source.RunSource.from_activation_reason`.
    """

    schedule = "schedule"  # start_at reached (one-off or recurring)
    trigger = "trigger"  # inbound trigger event occurred
    explicit = "explicit"  # user explicitly requested immediate start
