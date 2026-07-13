"""Run-source vocabulary contract tests."""

from __future__ import annotations

from unify.task_scheduler.types.activated_by import ActivatedBy
from unify.task_scheduler.types.run_source import RunSource


def test_run_source_types_cover_all_members() -> None:
    assert frozenset(RunSource) == frozenset(
        {
            RunSource.scheduled,
            RunSource.triggered,
            RunSource.explicit,
            RunSource.provider_event,
        },
    )


def test_normalize_run_source_preserves_triggered_and_explicit() -> None:
    assert RunSource.normalize("triggered") is RunSource.triggered
    assert RunSource.normalize("explicit") is RunSource.explicit
    assert RunSource.normalize("scheduled") is RunSource.scheduled
    assert RunSource.normalize(None) is RunSource.explicit


def test_run_source_predicates() -> None:
    assert RunSource.normalize("triggered").is_triggered
    assert RunSource.normalize("explicit").is_explicit
    assert not RunSource.normalize("provider_event").is_triggered


def test_from_activation_reason_maps_activated_by_values() -> None:
    assert RunSource.from_activation_reason(ActivatedBy.schedule) is RunSource.scheduled
    assert RunSource.from_activation_reason(ActivatedBy.trigger) is RunSource.triggered
    assert RunSource.from_activation_reason(ActivatedBy.explicit) is RunSource.explicit


def test_to_activated_by_maps_run_source_values() -> None:
    assert RunSource.scheduled.to_activated_by() is ActivatedBy.schedule
    assert RunSource.triggered.to_activated_by() is ActivatedBy.trigger
    assert RunSource.explicit.to_activated_by() is ActivatedBy.explicit
    assert RunSource.provider_event.to_activated_by() is ActivatedBy.explicit


def test_activation_kind_matches_run_source_value() -> None:
    assert RunSource.scheduled.activation_kind == "scheduled"
    assert RunSource.triggered.activation_kind == "triggered"
    assert RunSource.provider_event.activation_kind == "provider_event"
