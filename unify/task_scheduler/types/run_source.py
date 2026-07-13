"""Run-source vocabulary for persisted task runs."""

from __future__ import annotations

from enum import StrEnum

from unify.task_scheduler.types.activated_by import ActivatedBy


class RunSource(StrEnum):
    """How one task run was started.

    Distinct from :class:`~unify.task_scheduler.types.activated_by.ActivatedBy`,
    which records why a task instance entered the active state.

    Normalize at ingress with :meth:`normalize`, then compare with ``is`` or the
    predicate properties below.
    """

    scheduled = "scheduled"
    triggered = "triggered"
    explicit = "explicit"
    provider_event = "provider_event"

    @classmethod
    def normalize(cls, value: str | RunSource | None) -> RunSource:
        """Normalize one run-source value for comparisons."""

        if isinstance(value, cls):
            return value
        try:
            return cls((value or cls.explicit).strip().lower())
        except ValueError:
            return cls.explicit

    @classmethod
    def from_activation_reason(
        cls,
        reason: ActivatedBy | str | None,
    ) -> RunSource:
        """Map one activation reason onto the persisted run-source vocabulary."""

        if isinstance(reason, ActivatedBy):
            activated_by = reason
        else:
            try:
                activated_by = ActivatedBy(
                    (reason or ActivatedBy.explicit).strip().lower(),
                )
            except ValueError:
                return cls.explicit
        if activated_by is ActivatedBy.schedule:
            return cls.scheduled
        if activated_by is ActivatedBy.trigger:
            return cls.triggered
        return cls.explicit

    def to_activated_by(self) -> ActivatedBy:
        """Return the scheduler activation reason for this run source."""

        if self is RunSource.scheduled:
            return ActivatedBy.schedule
        if self is RunSource.triggered:
            return ActivatedBy.trigger
        return ActivatedBy.explicit

    @property
    def requires_activation_kind_match(self) -> bool:
        """Return whether dispatch must match a projected activation kind."""

        return self is not RunSource.explicit

    @property
    def activation_kind(self) -> str:
        """Return the projected activation kind required for this run source."""

        if self is RunSource.explicit:
            raise ValueError(
                "explicit runs do not require activation_kind matching",
            )
        return self.value

    @property
    def is_explicit(self) -> bool:
        """Return whether the source type represents an on-demand run."""

        return self is RunSource.explicit

    @property
    def is_triggered(self) -> bool:
        """Return whether the source type represents a communication trigger run."""

        return self is RunSource.triggered
