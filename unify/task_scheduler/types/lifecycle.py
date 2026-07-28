"""Derived lifecycle view of a task definition.

``Tasks`` rows carry *intent* only: ``enabled`` says whether the definition may
fire, and ``completed_at`` records that a one-shot has already run. Everything
about a *run* — running, completed, failed, cancelled — lives on the
``Tasks/Executions`` row for that run.

Nothing here is stored. :class:`Lifecycle` is computed on read so that the
question "what is this task doing?" has exactly one answer derived from exactly
one copy of each fact. The previous model kept a mutable ``status`` on the
definition that every concurrent execution wrote; the last writer won, and a
single failed occurrence could disarm a standing schedule permanently.
"""

from __future__ import annotations

from enum import StrEnum

__all__ = ["Lifecycle", "derive_lifecycle"]


class Lifecycle(StrEnum):
    """What a definition is doing right now. Derived, never persisted."""

    disarmed = "disarmed"
    completed = "completed"
    running = "running"
    triggerable = "triggerable"
    scheduled = "scheduled"


def derive_lifecycle(
    *,
    enabled: bool,
    has_trigger: bool,
    completed_at: object | None = None,
    has_running_execution: bool = False,
) -> Lifecycle:
    """Project a definition's lifecycle from intent plus live run state.

    ``running`` outranks ``disarmed`` deliberately: disabling a task stops the
    *next* wake, it does not retroactively stop a run already in flight, and an
    operator watching a task they just paused should still see it finish.
    """

    if has_running_execution:
        return Lifecycle.running
    if completed_at:
        return Lifecycle.completed
    if not enabled:
        return Lifecycle.disarmed
    return Lifecycle.triggerable if has_trigger else Lifecycle.scheduled
