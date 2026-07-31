"""Recurring task definitions must survive failed/killed/superseded runs.

A single occurrence failing (run error, max-runtime kill, SIGTERM, stale
activation) may only terminalize that run — never the recurring definition
row. See issues unifyai/unify#90 and unifyai/unify#91.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from unify.task_scheduler.active_task import ActiveTask
from unify.task_scheduler.machine_state import TaskRunProvenance
from unify.task_scheduler.offline_runner import (
    OfflineTaskConfig,
    _mark_source_task_failed,
)
from unify.task_scheduler.task_scheduler import (
    StaleActivationSuperseded,
    TaskScheduler,
)
from unify.task_scheduler.types.execution import Delivery, Wake
from unify.task_scheduler.types.task import Task

# --------------------------------------------------------------------------- #
# _validate_task_matches_provenance                                            #
# --------------------------------------------------------------------------- #


def _task(start_at: str) -> Task:
    return Task(
        task_id=10,
        name="tick",
        description="recurring tick",
        priority="normal",
        schedule={"start_at": start_at},
    )


def _provenance(scheduled_for: str | None) -> TaskRunProvenance:
    return TaskRunProvenance(
        assistant_id="1406",
        task_id=10,
        wake=Wake.scheduled,
        delivery=Delivery.offline,
        revision="rev",
        scheduled_for=scheduled_for,
    )


def test_stale_scheduled_activation_raises_superseded():
    scheduler = object.__new__(TaskScheduler)
    with pytest.raises(StaleActivationSuperseded):
        scheduler._validate_task_matches_provenance(
            task=_task("2026-07-14T15:10:00+00:00"),
            provenance=_provenance("2026-07-14T15:00:00+00:00"),
        )


def test_matching_scheduled_activation_passes():
    scheduler = object.__new__(TaskScheduler)
    scheduler._validate_task_matches_provenance(
        task=_task("2026-07-14T15:10:00+00:00"),
        provenance=_provenance("2026-07-14T15:10:00+00:00"),
    )


def test_projected_successor_after_the_anchor_passes():
    """Every occurrence after the first is later than the anchor by design.

    `schedule.start_at` is authored intent and is never rewritten, so a
    recurring series only matches it on its first occurrence. Treating any
    mismatch as stale skipped every projected successor — the skip finalized
    the run as a benign "completed" and projected nothing, so a healthy
    ten-minute series silently ended one occurrence after every re-arm.
    """

    scheduler = object.__new__(TaskScheduler)
    scheduler._validate_task_matches_provenance(
        task=_task("2026-07-14T15:10:00+00:00"),
        provenance=_provenance("2026-07-14T15:20:00+00:00"),
    )


def test_equivalent_offset_scheduled_activation_passes():
    scheduler = object.__new__(TaskScheduler)
    scheduler._validate_task_matches_provenance(
        task=_task("2026-07-27T13:17:00+05:00"),
        provenance=_provenance("2026-07-27T08:17:00Z"),
    )


def test_normalize_activation_datetime_treats_naive_values_as_utc():
    assert (
        TaskScheduler._normalize_activation_datetime(
            "2026-07-27T08:17:00",
        )
        == "2026-07-27T08:17:00+00:00"
    )


def test_normalize_activation_datetime_preserves_malformed_literal():
    assert TaskScheduler._normalize_activation_datetime("not-a-timestamp") == (
        "not-a-timestamp"
    )


# --------------------------------------------------------------------------- #
# ActiveTask.result finalization                                               #
# --------------------------------------------------------------------------- #


class _FailingHandle:
    async def result(self):
        raise RuntimeError("occurrence blew up")


class _SuccessfulHandle:
    async def result(self):
        return "completed inbox summary"


class _FakeScheduler:
    """Records the only definition write a finished run may perform."""

    def __init__(self, task: Task | None = None):
        self.one_shot_completions: list[int] = []
        self._task = task or _task("2026-07-14T15:10:00+00:00")

    def _get_task_or_raise(self, task_id):
        return self._task

    def _mark_one_shot_completed(self, task_id):
        # Mirrors the real helper: repeating and triggerable definitions are
        # never disarmed by a run finishing.
        if self._task.repeat is not None or self._task.trigger is not None:
            return
        self.one_shot_completions.append(task_id)


def _active_task(scheduler: _FakeScheduler) -> ActiveTask:
    task = object.__new__(ActiveTask)
    task._actor_handle = _FailingHandle()
    task._was_stopped = False
    task._scheduler = scheduler
    task._task_id = 10
    task._preserve_definition_status = False
    task._summary_scheduled = True

    async def _noop_persist(**kwargs):
        return None

    task._persist_task_run_terminal_state = _noop_persist
    return task


def _repeating_task() -> Task:
    return Task(
        task_id=10,
        name="tick",
        description="recurring tick",
        priority="normal",
        schedule={"start_at": "2026-07-14T15:10:00+00:00"},
        repeat=[{"frequency": "minutely", "interval": 10}],
    )


def test_failed_run_never_disarms_a_repeating_definition():
    """The regression that froze the SmartLead campaign runtime for 51 hours."""

    scheduler = _FakeScheduler(_repeating_task())
    task = _active_task(scheduler)
    with pytest.raises(RuntimeError, match="occurrence blew up"):
        asyncio.run(task.result())
    assert scheduler.one_shot_completions == []


def test_failed_one_shot_run_disarms_its_definition():
    scheduler = _FakeScheduler()
    task = _active_task(scheduler)
    with pytest.raises(RuntimeError, match="occurrence blew up"):
        asyncio.run(task.result())
    assert scheduler.one_shot_completions == [10]


def test_successful_result_survives_terminal_persistence_failure():
    scheduler = _FakeScheduler()
    task = _active_task(scheduler)
    task._actor_handle = _SuccessfulHandle()

    async def _failing_persist(**kwargs):
        raise RuntimeError("storage unavailable")

    task._persist_task_run_terminal_state = _failing_persist

    assert asyncio.run(task.result()) == "completed inbox summary"


# --------------------------------------------------------------------------- #
# offline_runner._mark_source_task_failed                                      #
# --------------------------------------------------------------------------- #


class _Row:
    def __init__(self, entries):
        self.id = 555
        self.entries = entries


class _FakeStoreScheduler:
    def __init__(self, entries):
        self._entries = entries
        self.writes: list[dict] = []

        class _Store:
            def __init__(inner, outer):
                inner._outer = outer

            def get_rows_by_log_ids(inner, *, log_ids):
                return [_Row(inner._outer._entries)]

        self._store = _Store(self)

    def _write_log_entries(self, *, logs, entries):
        self.writes.append(entries)
        return {}


def _config() -> OfflineTaskConfig:
    return OfflineTaskConfig(
        assistant_id="1406",
        run_key="offline:scheduled:1406:10:rev",
        task_id=10,
        function_id=1,
        request="run tick",
        wake="scheduled",
        source_task_log_id=555,
        revision="rev",
    )


def _run_mark_failed(entries: dict) -> _FakeStoreScheduler:
    fake = _FakeStoreScheduler(entries)
    with (
        patch(
            "unify.task_scheduler.offline_runner.TaskScheduler",
            lambda: fake,
        ),
        patch(
            "unify.task_scheduler.offline_runner.SESSION_DETAILS.populate_from_env",
            lambda: None,
        ),
        patch(
            "unify.task_scheduler.offline_runner.unify.ensure_initialised",
            lambda **kwargs: None,
        ),
    ):
        _mark_source_task_failed(_config(), "boom")
    return fake


def test_crash_leaves_a_recurring_definition_untouched():
    fake = _run_mark_failed({"repeat": ["every 10 minutes"], "trigger": None})
    assert fake.writes == []


def test_crash_leaves_a_triggerable_definition_untouched():
    fake = _run_mark_failed({"repeat": None, "trigger": {"event": "x"}})
    assert fake.writes == []


def test_crash_disarms_a_one_shot_definition():
    fake = _run_mark_failed({"repeat": None, "trigger": None})
    assert fake.writes
    written = fake.writes[0]
    assert written == {"enabled": False}


# --------------------------------------------------------------------------- #
# offline_runner.main — superseded activation is a benign no-op                #
# --------------------------------------------------------------------------- #


def test_main_skips_superseded_activation(monkeypatch):
    from unify.task_scheduler import offline_runner

    monkeypatch.setenv("ASSISTANT_ID", "1406")
    monkeypatch.setenv("UNITY_OFFLINE_RUN_KEY", "offline:scheduled:1406:10:rev")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_ID", "10")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_FUNCTION_ID", "777")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_REQUEST", "run tick")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_WAKE", "scheduled")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_SOURCE_TASK_LOG_ID", "555")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_REVISION", "rev")
    monkeypatch.setenv("ORCHESTRA_URL", "https://orchestra.test")
    monkeypatch.setenv("ORCHESTRA_ADMIN_KEY", "admin-key")

    updates = []
    marked = []

    async def _raise_superseded(config):
        raise StaleActivationSuperseded("schedule moved on")

    monkeypatch.setattr(offline_runner, "_execute_offline_task", _raise_superseded)
    monkeypatch.setattr(
        offline_runner,
        "_update_task_run",
        lambda assistant_id, run_key, **kwargs: updates.append(kwargs["updates"]),
    )
    monkeypatch.setattr(
        offline_runner,
        "_mark_source_task_failed",
        lambda *args, **kwargs: marked.append(args),
    )

    exit_code = offline_runner.main()

    assert exit_code == 0
    assert marked == []
    assert len(updates) == 1
    assert updates[0]["state"] == "completed"
    assert "superseded" in updates[0]["result_summary"].lower()
