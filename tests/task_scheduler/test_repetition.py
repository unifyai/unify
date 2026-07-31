from datetime import datetime, timedelta, timezone

import pytest
from types import SimpleNamespace

from tests.helpers import _handle_project
from unify.actor.simulated import SimulatedActor
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.task_scheduler.types.repetition import (
    deterministic_jitter_seconds,
    Frequency,
    RepeatPattern,
    Weekday,
    max_jitter_seconds,
    normalize_repeat_patterns,
    next_repeated_start_at,
)
from unify.task_scheduler.types.schedule import Schedule


def _passing_certification_metadata():
    return {
        "certification_evidence": {
            "risk_classification": "read_only",
            "input_contract": {
                "required_inputs": ["scheduled_run_timestamp", "task_id"],
            },
            "equivalence_contract": {
                "result_shape": "string summary",
                "live_step_mapping": [
                    "live web primitive call -> candidate primitives.web.ask call",
                ],
            },
            "managed_primitive_contract": {
                "preserved": True,
                "managed_surfaces": ["primitives.web.ask"],
                "ad_hoc_replacements": [],
            },
            "side_effect_contract": {
                "side_effects": ["read web source before summarizing"],
                "ordering": "read source before summarizing",
            },
            "idempotency_contract": {
                "classification": "read_only",
                "duplicate_run_behavior": "safe",
            },
            "cost_contract": {
                "bounded": True,
                "cost_model": "one managed web primitive call and one summary",
            },
            "failure_contract": {
                "failure_semantics": "return a blocker summary for source failures",
            },
            "observability_contract": {
                "result_summary": "string summary or blocker summary",
            },
            "attestations": {
                "no_hardcoded_live_observations": True,
                "no_removed_validation_gates": True,
                "no_reordered_side_effects": True,
                "no_discarded_recovery_branches": True,
                "no_static_runtime_assumptions": True,
                "no_ad_hoc_logic_replaced_managed_primitives": True,
            },
        },
    }


def test_next_repeated_start_at_skips_past_occurrences():
    pattern = RepeatPattern(frequency=Frequency.DAILY, interval=1)
    previous_start = datetime(2026, 4, 10, 9, 0, tzinfo=timezone.utc)
    now = datetime(2026, 4, 12, 10, 0, tzinfo=timezone.utc)

    next_start = next_repeated_start_at(
        previous_start=previous_start,
        patterns=[pattern],
        current_occurrence_index=0,
        now=now,
    )

    assert next_start == datetime(2026, 4, 13, 9, 0, tzinfo=timezone.utc)


def test_next_repeated_start_at_honors_weekdays_and_count():
    pattern = RepeatPattern(
        frequency=Frequency.WEEKLY,
        interval=1,
        weekdays=[Weekday.MO, Weekday.WE],
        count=2,
    )
    previous_start = datetime(2026, 4, 6, 9, 0, tzinfo=timezone.utc)  # Monday

    next_start = next_repeated_start_at(
        previous_start=previous_start,
        patterns=[pattern],
        current_occurrence_index=0,
        now=datetime(2026, 4, 6, 9, 1, tzinfo=timezone.utc),
    )
    assert next_start == datetime(2026, 4, 8, 9, 0, tzinfo=timezone.utc)

    exhausted = next_repeated_start_at(
        previous_start=previous_start,
        patterns=[pattern],
        current_occurrence_index=1,
        now=datetime(2026, 4, 8, 9, 1, tzinfo=timezone.utc),
    )
    assert exhausted is None


def test_next_repeated_start_at_supports_minutely_cadence():
    pattern = RepeatPattern(frequency=Frequency.MINUTELY, interval=30)
    previous_start = datetime(2026, 5, 19, 9, 30, tzinfo=timezone.utc)

    next_start = next_repeated_start_at(
        previous_start=previous_start,
        patterns=[pattern],
        current_occurrence_index=0,
        now=datetime(2026, 5, 19, 9, 31, tzinfo=timezone.utc),
    )

    assert next_start == datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)


def test_next_repeated_start_at_skips_missed_subdaily_occurrences():
    pattern = RepeatPattern(frequency=Frequency.MINUTELY, interval=30)
    previous_start = datetime(2026, 5, 19, 9, 30, tzinfo=timezone.utc)

    next_start = next_repeated_start_at(
        previous_start=previous_start,
        patterns=[pattern],
        current_occurrence_index=0,
        now=datetime(2026, 5, 19, 10, 7, tzinfo=timezone.utc),
    )

    assert next_start == datetime(2026, 5, 19, 10, 30, tzinfo=timezone.utc)


def test_next_repeated_start_at_honors_subdaily_count_and_until():
    previous_start = datetime(2026, 5, 19, 9, 30, tzinfo=timezone.utc)

    exhausted_by_count = next_repeated_start_at(
        previous_start=previous_start,
        patterns=[RepeatPattern(frequency=Frequency.MINUTELY, interval=30, count=1)],
        current_occurrence_index=0,
        now=datetime(2026, 5, 19, 9, 31, tzinfo=timezone.utc),
    )
    assert exhausted_by_count is None

    exhausted_by_until = next_repeated_start_at(
        previous_start=previous_start,
        patterns=[
            RepeatPattern(
                frequency=Frequency.MINUTELY,
                interval=30,
                until=datetime(2026, 5, 19, 9, 45, tzinfo=timezone.utc),
            ),
        ],
        current_occurrence_index=0,
        now=datetime(2026, 5, 19, 9, 31, tzinfo=timezone.utc),
    )
    assert exhausted_by_until is None


def test_next_repeated_start_at_supports_same_day_daily_time_slots():
    patterns = [
        RepeatPattern(frequency=Frequency.DAILY, interval=1, time_of_day="09:30"),
        RepeatPattern(frequency=Frequency.DAILY, interval=1, time_of_day="10:00"),
        RepeatPattern(frequency=Frequency.DAILY, interval=1, time_of_day="10:30"),
    ]
    previous_start = datetime(2026, 5, 19, 9, 30, tzinfo=timezone.utc)

    next_start = next_repeated_start_at(
        previous_start=previous_start,
        patterns=patterns,
        current_occurrence_index=0,
        now=datetime(2026, 5, 19, 9, 31, tzinfo=timezone.utc),
    )

    assert next_start == datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)


def test_normalize_repeat_patterns_collapses_full_day_half_hour_slots():
    patterns = [
        RepeatPattern(
            frequency=Frequency.DAILY,
            interval=1,
            time_of_day=f"{hour:02d}:{minute:02d}:00",
        )
        for hour in range(24)
        for minute in (0, 30)
    ]

    normalized = normalize_repeat_patterns(patterns)

    assert normalized == [RepeatPattern(frequency=Frequency.MINUTELY, interval=30)]


def _capture_projected_occurrences(monkeypatch) -> list[str]:
    """Record the scheduled_for of every occurrence projected into the ledger."""

    projected: list[str] = []
    monkeypatch.setattr(
        "unify.task_scheduler.task_scheduler.project_task_occurrence",
        lambda provenance: projected.append(provenance.scheduled_for),
    )
    monkeypatch.setattr(
        "unify.task_scheduler.task_scheduler.latest_scheduled_occurrence_for_task",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "unify.task_scheduler.task_scheduler.SESSION_DETAILS",
        SimpleNamespace(
            assistant=SimpleNamespace(agent_id="1406"),
            assistant_context="1406",
        ),
    )
    return projected


@_handle_project
def test_next_occurrence_is_projected_without_touching_the_anchor(monkeypatch):
    """schedule.start_at is the series anchor and is never rewritten."""

    scheduler = TaskScheduler()
    initial_start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(
        hours=1,
    )
    scheduler._create_task(
        name="Daily summary",
        description="Send the daily summary email.",
        schedule=Schedule(start_at=initial_start.isoformat()),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
    )

    projected = _capture_projected_occurrences(monkeypatch)
    scheduler._project_next_occurrence(scheduler._get_task_or_raise(0))

    assert projected == [(initial_start + timedelta(days=1)).isoformat()]

    row = scheduler._get_task_or_raise(0)
    assert row.schedule_start_at == initial_start, "the anchor must not move"
    assert row.enabled is True


def test_max_jitter_seconds_reads_patterns_and_dicts():
    assert max_jitter_seconds(None) == 0
    assert max_jitter_seconds([]) == 0
    assert max_jitter_seconds([RepeatPattern(frequency=Frequency.DAILY)]) == 0
    patterns = [
        RepeatPattern(frequency=Frequency.DAILY, jitter_seconds=600),
        RepeatPattern(frequency=Frequency.DAILY, jitter_seconds=1800),
    ]
    assert max_jitter_seconds(patterns) == 1800
    # Pre-validation dict shape is accepted too.
    assert max_jitter_seconds([{"frequency": "daily", "jitter_seconds": 900}]) == 900


@_handle_project
def test_projected_slots_are_canonical_and_do_not_drift(monkeypatch):
    """The ledger records canonical slots, so a series never accumulates jitter.

    Jitter is a dispatch-time offset. Baking it into the recorded occurrence
    would make it unrecoverable — the offset is seeded on the slot it applies
    to — so every later step would measure from a jittered value.
    """

    scheduler = TaskScheduler()
    anchor = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=1)
    scheduler._create_task(
        name="Daily jittered scrape",
        description="Scrape the feed with dispatch jitter.",
        schedule=Schedule(start_at=anchor.isoformat()),
        repeat=[RepeatPattern(frequency=Frequency.DAILY, jitter_seconds=1800)],
    )

    projected = _capture_projected_occurrences(monkeypatch)
    task = scheduler._get_task_or_raise(0)
    scheduler._project_next_occurrence(task)
    scheduler._project_next_occurrence(task)

    assert len(projected) == 2
    assert projected[0] == projected[1], "same slot must yield the same timestamp"
    assert datetime.fromisoformat(projected[0]) == anchor + timedelta(days=1)

    first_slot = projected[0]
    monkeypatch.setattr(
        "unify.task_scheduler.task_scheduler.latest_scheduled_occurrence_for_task",
        lambda **kwargs: first_slot,
    )
    projected.clear()
    scheduler._project_next_occurrence(task)
    assert datetime.fromisoformat(projected[0]) == anchor + timedelta(days=2)


def test_dispatch_jitter_is_stable_per_slot():
    """Every caller computing the offset for one slot must agree on it."""

    patterns = [RepeatPattern(frequency=Frequency.DAILY, jitter_seconds=1800)]
    slot = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)

    first = deterministic_jitter_seconds(task_id=7, slot=slot, patterns=patterns)
    second = deterministic_jitter_seconds(task_id=7, slot=slot, patterns=patterns)
    other_slot = deterministic_jitter_seconds(
        task_id=7,
        slot=slot + timedelta(days=1),
        patterns=patterns,
    )

    assert first == second
    assert 0.0 <= first <= 1800.0
    assert first != other_slot
    assert deterministic_jitter_seconds(task_id=7, slot=slot, patterns=None) == 0.0


@_handle_project
def test_entrypoint_review_records_symbolic_candidate_without_offline_promotion():
    scheduler = TaskScheduler()
    initial_start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(
        hours=1,
    )
    scheduler._create_task(
        name="Daily description-driven summary",
        description="Summarize updates every day.",
        schedule=Schedule(start_at=initial_start.isoformat()),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
    )

    current = scheduler._get_task_or_raise(0)
    scheduler._project_next_occurrence(current)
    result = scheduler._attach_entrypoint_to_definition(
        task_id=0,
        function_id=321,
        rationale="The successful run revealed a stable workflow.",
    )

    row = scheduler._get_task_or_raise(0)

    assert result["outcome"] == "candidate_recorded"
    assert row.entrypoint == 321
    assert row.offline is False
    assert result["certification_status"] == "required_before_offline_promotion"


@_handle_project
def test_offline_promotion_requires_passing_certification():
    scheduler = TaskScheduler()
    initial_start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(
        hours=1,
    )
    scheduler._create_task(
        name="Daily certified summary",
        description="Summarize updates every day.",
        schedule=Schedule(start_at=initial_start.isoformat()),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
    )

    current = scheduler._get_task_or_raise(0)
    scheduler._project_next_occurrence(current)
    scheduler._attach_entrypoint_to_definition(
        task_id=0,
        function_id=321,
        rationale="The successful run revealed a stable workflow.",
    )

    rejected = scheduler._promote_definition_to_offline(
        task_id=0,
        function_id=321,
        certification_metadata={},
        certification_result={
            "evidence_based": True,
            "executed_entrypoint": False,
        },
    )

    row = scheduler._get_task_or_raise(0)
    assert rejected["outcome"] == "certification_rejected"
    assert "missing_certification_evidence" in rejected["rejection_reasons"]
    assert row.entrypoint == 321
    assert row.offline is False


@_handle_project
def test_offline_promotion_rejects_failed_certification_attestation():
    scheduler = TaskScheduler()
    initial_start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(
        hours=1,
    )
    scheduler._create_task(
        name="Daily certified summary",
        description="Summarize updates every day.",
        schedule=Schedule(start_at=initial_start.isoformat()),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
    )

    current = scheduler._get_task_or_raise(0)
    scheduler._project_next_occurrence(current)
    scheduler._attach_entrypoint_to_definition(
        task_id=0,
        function_id=321,
        rationale="The successful run revealed a stable workflow.",
    )
    metadata = _passing_certification_metadata()
    metadata["certification_evidence"]["attestations"][
        "no_static_runtime_assumptions"
    ] = False

    rejected = scheduler._promote_definition_to_offline(
        task_id=0,
        function_id=321,
        certification_metadata=metadata,
        certification_result={
            "evidence_based": True,
            "executed_entrypoint": False,
        },
    )

    assert rejected["outcome"] == "certification_rejected"
    assert (
        "failed_attestation:no_static_runtime_assumptions"
        in rejected["rejection_reasons"]
    )


@_handle_project
def test_offline_promotion_rejects_ad_hoc_primitive_replacements():
    scheduler = TaskScheduler()
    initial_start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(
        hours=1,
    )
    scheduler._create_task(
        name="Daily certified summary",
        description="Summarize updates every day.",
        schedule=Schedule(start_at=initial_start.isoformat()),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
    )

    current = scheduler._get_task_or_raise(0)
    scheduler._project_next_occurrence(current)
    scheduler._attach_entrypoint_to_definition(
        task_id=0,
        function_id=321,
        rationale="The successful run revealed a stable workflow.",
    )
    metadata = _passing_certification_metadata()
    metadata["certification_evidence"]["managed_primitive_contract"][
        "ad_hoc_replacements"
    ] = ["replaced primitives.web.ask with urllib scraping"]

    rejected = scheduler._promote_definition_to_offline(
        task_id=0,
        function_id=321,
        certification_metadata=metadata,
        certification_result={
            "evidence_based": True,
            "executed_entrypoint": False,
        },
    )

    assert rejected["outcome"] == "certification_rejected"
    assert "ad_hoc_logic_replaced_managed_primitive" in rejected["rejection_reasons"]


@_handle_project
def test_passing_certification_promotes_candidate_future_instances_offline():
    scheduler = TaskScheduler()
    initial_start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(
        hours=1,
    )
    scheduler._create_task(
        name="Daily certified summary",
        description="Summarize updates every day.",
        schedule=Schedule(start_at=initial_start.isoformat()),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
    )

    current = scheduler._get_task_or_raise(0)
    scheduler._project_next_occurrence(current)
    scheduler._attach_entrypoint_to_definition(
        task_id=0,
        function_id=321,
        rationale="The successful run revealed a stable workflow.",
    )

    promoted = scheduler._promote_definition_to_offline(
        task_id=0,
        function_id=321,
        certification_metadata=_passing_certification_metadata(),
        certification_result={
            "evidence_based": True,
            "executed_entrypoint": False,
            "result_summary": "dry run completed",
        },
    )

    row = scheduler._get_task_or_raise(0)
    assert promoted["outcome"] == "offline_promoted"
    assert row.entrypoint == 321
    assert row.offline is True


@pytest.mark.asyncio
@_handle_project
async def test_entrypoint_patch_survives_occurrence_projection():
    scheduler = TaskScheduler(actor=SimulatedActor(steps=0))
    initial_start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(
        hours=1,
    )
    scheduler._create_task(
        name="Daily report",
        description="Run the daily report from the task description.",
        schedule=Schedule(start_at=initial_start.isoformat()),
        repeat=[RepeatPattern(frequency=Frequency.DAILY)],
    )

    handle = await scheduler.execute(task_id=0)
    await handle.result()

    row_after_run = scheduler._get_task_or_raise(0)
    assert row_after_run.entrypoint is None
    # A run projects its successor onto the ledger; the anchor stays put.
    assert row_after_run.schedule_start_at == initial_start

    result = scheduler._attach_entrypoint_to_definition(
        task_id=0,
        function_id=321,
        rationale="The completed run was stable enough to reuse.",
    )
    assert result["outcome"] == "candidate_recorded"

    patched = scheduler._get_task_or_raise(0)
    assert patched.entrypoint == 321
    assert patched.offline is False

    scheduler._project_next_occurrence(patched)
    rearmed = scheduler._get_task_or_raise(0)
    assert rearmed.entrypoint == 321
    assert rearmed.offline is False
    assert rearmed.schedule_start_at == initial_start


@_handle_project
def test_projection_stops_when_repeat_count_is_exhausted():
    scheduler = TaskScheduler()
    initial_start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(
        hours=1,
    )
    scheduler._create_task(
        name="One repeat only",
        description="Run once and do not re-arm.",
        schedule=Schedule(start_at=initial_start.isoformat()),
        repeat=[RepeatPattern(frequency=Frequency.DAILY, count=1)],
    )

    current = scheduler._get_task_or_raise(0)
    before = current.schedule_start_at
    scheduler._project_next_occurrence(current)

    task_rows = scheduler._filter_tasks(filter="task_id == 0")
    assert len(task_rows) == 1
    assert task_rows[0].schedule_start_at == before
