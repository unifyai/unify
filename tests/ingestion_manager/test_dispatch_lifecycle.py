"""What happens to a dispatched run when the dispatch itself goes wrong.

The two-phase submit (stage, upload, publish) runs off the caller's thread and
can take minutes for multi-hundred-MB sources. Two failure shapes used to leave
a run `queued` forever — the one state whose next step is "keep polling", which
is exactly wrong for a run that will never start:

* the dispatch raised (unreadable file, refused publish, dead plane) and
  nothing recorded the failure on the run row;
* the submitting process died mid-upload, so no error was ever thrown anywhere
  a reader could see.

The first now lands on the run row like every other outcome. The second is
detected deterministically at read time: uploads only ever run in the
submitting process, so a dispatched row with no dispatch id and no upload in
flight in this process has no future — no timer, no grace period, no guess.
"""

from __future__ import annotations

import threading
from typing import Any, Dict, List

import pytest

from unify.ingestion_manager.ingestion_manager import IngestionManager
from unify.ingestion_manager.settings import IngestionSettings
from unify.ingestion_manager.types import FilesSource, IngestionRequest, TableTarget


class _Recorder:
    """Captures run-row updates and events the manager writes."""

    def __init__(self) -> None:
        self.updates: List[Dict[str, Any]] = []
        self.events: List[Dict[str, Any]] = []


@pytest.fixture()
def manager(monkeypatch) -> IngestionManager:
    instance = IngestionManager.__new__(IngestionManager)
    instance._lock = threading.RLock()
    instance._dispatching = set()
    instance._settings = IngestionSettings(PIPELINE_URL="http://plane.test")
    recorder = _Recorder()
    instance._recorder = recorder  # type: ignore[attr-defined]

    monkeypatch.setattr(
        IngestionManager,
        "_update_run",
        lambda self, run_key, runs_context, fields: recorder.updates.append(
            {"run_key": run_key, **fields},
        ),
        raising=False,
    )
    monkeypatch.setattr(
        IngestionManager,
        "_record_event",
        lambda self, run_key, **kwargs: recorder.events.append(
            {"run_key": run_key, **kwargs},
        ),
        raising=False,
    )
    return instance


def _request() -> IngestionRequest:
    return IngestionRequest(
        source=FilesSource(paths=["/tmp/a.csv"]),
        target=TableTarget(context="Data/Deals"),
    )


class TestDispatchFailureLandsOnTheRun:
    def test_a_raising_dispatch_marks_the_run_failed(self, manager, monkeypatch):
        monkeypatch.setattr(
            IngestionManager,
            "_dispatch",
            lambda self, run_key, runs_context, request: (_ for _ in ()).throw(
                RuntimeError("the control plane refused (503): no backends"),
            ),
            raising=False,
        )
        manager._dispatching.add("run1")

        manager._dispatch_guarded("run1", "u/1/Ingestion/Runs", _request())

        failed = [u for u in manager._recorder.updates if u.get("state") == "failed"]
        assert failed and "no backends" in failed[0]["error"]
        assert failed[0].get("finished_at")
        errors = [e for e in manager._recorder.events if e.get("level") == "error"]
        assert errors and "Dispatch to the worker fleet failed" in errors[0]["message"]

    def test_the_in_flight_marker_is_cleared_either_way(self, manager, monkeypatch):
        monkeypatch.setattr(
            IngestionManager,
            "_dispatch",
            lambda self, run_key, runs_context, request: None,
            raising=False,
        )
        manager._dispatching.add("run1")
        manager._dispatch_guarded("run1", "u/1/Ingestion/Runs", _request())
        assert "run1" not in manager._dispatching

        monkeypatch.setattr(
            IngestionManager,
            "_dispatch",
            lambda self, run_key, runs_context, request: (_ for _ in ()).throw(
                RuntimeError("boom"),
            ),
            raising=False,
        )
        manager._dispatching.add("run2")
        manager._dispatch_guarded("run2", "u/1/Ingestion/Runs", _request())
        assert "run2" not in manager._dispatching


class TestAPathlessDispatchIsRefused:
    def test_a_rows_request_cannot_reach_the_fleet(self, manager, monkeypatch):
        """The fleet executes staged files; a rows dispatch publishes zero jobs.

        A zero-job dispatch succeeds at every step and then folds to `queued`
        forever — the manifest exists, no worker ever picks anything up, and no
        retry scope can reach it. Refusing before the publish turns that hang
        into a failure the run row reports.
        """
        from unify.ingestion_manager.types import RowsSource

        request = IngestionRequest(
            source=RowsSource(rows=[{"a": 1}]),
            target=TableTarget(context="Data/Deals"),
        )
        manager._dispatching.add("run1")
        manager._dispatch_guarded("run1", "u/1/Ingestion/Runs", request)

        failed = [u for u in manager._recorder.updates if u.get("state") == "failed"]
        assert failed and "stages no files" in failed[0]["error"]


class TestOrphanedDispatchIsDetectedOnRead:
    def _row(self, **overrides) -> Dict[str, Any]:
        row = {
            "run_key": "run1",
            "state": "queued",
            "executed_as": "dispatched",
            "dispatch_id": None,
        }
        row.update(overrides)
        return row

    def test_an_orphaned_queued_dispatch_folds_to_failed(self, manager):
        folded = manager._fold_fleet_status(self._row(), "u/1/Ingestion/Runs")
        assert folded["state"] == "failed"
        assert "never reached the worker fleet" in folded["error"]
        # And the verdict is written back, so `wait` terminates and later
        # reads need not re-derive it.
        assert manager._recorder.updates
        assert manager._recorder.updates[-1]["state"] == "failed"

    def test_an_upload_still_in_flight_here_is_left_queued(self, manager):
        manager._dispatching.add("run1")
        folded = manager._fold_fleet_status(self._row(), "u/1/Ingestion/Runs")
        assert folded["state"] == "queued"
        assert manager._recorder.updates == []

    def test_an_inline_run_without_a_dispatch_id_is_untouched(self, manager):
        folded = manager._fold_fleet_status(
            self._row(executed_as="inline"),
            "u/1/Ingestion/Runs",
        )
        assert folded["state"] == "queued"
        assert manager._recorder.updates == []

    def test_a_terminal_row_is_never_revisited(self, manager):
        folded = manager._fold_fleet_status(
            self._row(state="failed", error="already settled"),
            "u/1/Ingestion/Runs",
        )
        assert folded["error"] == "already settled"
        assert manager._recorder.updates == []
