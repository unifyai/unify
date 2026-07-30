"""The decisions ingestion makes without asking anyone.

Two of them are load-bearing and neither is visible in a result, which is why they
are tested directly rather than through a run:

* **Which tier runs a request.** Getting it wrong is either a needless cloud
  dispatch that adds minutes to seconds of work, or a plan held open by an
  ingestion that should have been queued.
* **What a caller should do next.** A status that has to be interpreted eventually
  is, and the two ways that goes wrong are a retry that duplicates data and a
  failure nobody notices.

The validation tests cover the refusals that prevent data loss rather than merely
tidy input -- most sharply the collection name that could alias a file's own
namespace.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from unify.ingestion_manager.policy import choose_tier, next_step, stages_from_events
from unify.ingestion_manager.settings import IngestionSettings
from unify.ingestion_manager.types import (
    CollectionTarget,
    FilesSource,
    FolderSource,
    IngestionRequest,
    RowsSource,
    TableSource,
    TableTarget,
)

SETTINGS = IngestionSettings(
    MAX_INLINE_ROWS=100,
    MAX_INLINE_FILES=2,
    MAX_INLINE_BYTES=1024,
)


def request(source, target=None, mode="auto") -> IngestionRequest:
    return IngestionRequest(
        source=source,
        target=target or TableTarget(context="Data/Target"),
        mode=mode,
    )


class TestTierSelection:
    def test_a_small_pull_stays_in_process(self):
        # An API page or connected-app pull is seconds of work; dispatching it would
        # add queue latency for nothing.
        assert (
            choose_tier(request(RowsSource(rows=[{"a": 1}] * 10)), SETTINGS) == "inline"
        )

    def test_a_large_row_set_is_dispatched(self):
        assert (
            choose_tier(request(RowsSource(rows=[{"a": 1}] * 500)), SETTINGS)
            == "dispatched"
        )

    def test_a_folder_is_always_dispatched(self):
        # A folder states that the set is open-ended. Its size cannot be known
        # without walking it, and a plan must not be held open to find out.
        tier = choose_tier(
            request(FolderSource(path="/exports", pattern="*.xlsx")),
            SETTINGS,
        )
        assert tier == "dispatched"

    def test_file_count_decides_when_size_cannot_be_measured(self):
        # These paths do not exist, so bytes are unmeasurable. The count has to
        # decide alone rather than an assumed zero making everything look small.
        assert choose_tier(request(FilesSource(paths=["a.pdf"])), SETTINGS) == "inline"
        assert (
            choose_tier(request(FilesSource(paths=["a", "b", "c"])), SETTINGS)
            == "dispatched"
        )

    def test_size_dispatches_what_the_count_would_miss(self, tmp_path):
        # Two files sound small; two very large spreadsheets are dispatch work.
        big = tmp_path / "big.csv"
        big.write_bytes(b"x" * 4096)
        assert (
            choose_tier(request(FilesSource(paths=[str(big)])), SETTINGS)
            == "dispatched"
        )

    def test_a_stored_table_stays_in_process(self):
        # Read server-side in bounded pages, so the local cost does not scale with
        # how many rows match.
        assert (
            choose_tier(request(TableSource(context="Data/Source")), SETTINGS)
            == "inline"
        )

    @pytest.mark.parametrize("forced", ["inline", "dispatched"])
    def test_an_explicit_mode_wins(self, forced):
        # The caller sometimes knows what the shape cannot show -- a small file that
        # takes minutes to parse.
        source = (
            RowsSource(rows=[{"a": 1}] * 500)
            if forced == "inline"
            else RowsSource(rows=[{"a": 1}])
        )
        assert choose_tier(request(source, mode=forced), SETTINGS) == forced


class TestNextStep:
    """Every state has to name a concrete action, including 'none'."""

    def _step(self, state, *, parked=0, error=None, contexts=("Data/X",)):
        return next_step(
            state=state,
            parked=parked,
            error=error,
            executed_as="inline",
            contexts=list(contexts),
        )

    def test_a_failure_with_parked_items_names_the_safe_retry(self):
        step = self._step("failed", parked=3)
        assert 'retry(only="dlq")' in step
        assert "3" in step

    def test_a_failure_with_nothing_parked_does_not_suggest_retrying(self):
        # Retrying would fail identically, so offering the verb would send the
        # caller in a circle. Explaining *why not* is fine and wanted, so this
        # checks the call form is absent rather than banning the word.
        step = self._step("failed", error="column mismatch")
        assert "retry(" not in step
        assert "get_logs" in step
        assert "column mismatch" in step

    def test_success_with_parked_items_is_not_reported_as_clean(self):
        # The dangerous case: a run that finished with items parked looks fine in a
        # listing, and the missing rows are easy to miss.
        step = self._step("succeeded", parked=2)
        assert 'retry(only="dlq")' in step
        assert "not in the result" in step

    def test_a_clean_success_says_there_is_nothing_to_do(self):
        step = self._step("succeeded")
        assert step.startswith("Nothing")
        assert "Data/X" in step

    def test_paused_offers_resume_and_cancel_but_not_retry(self):
        step = self._step("paused")
        assert "resume()" in step
        assert "cancel()" in step
        assert "retry" not in step

    def test_running_offers_polling_or_waiting(self):
        step = self._step("running")
        assert "get_status" in step
        assert "wait()" in step

    @pytest.mark.parametrize(
        "state",
        ["queued", "running", "paused", "succeeded", "failed", "cancelled"],
    )
    def test_every_state_produces_an_action(self, state):
        assert self._step(state).strip()


class TestStageFolding:
    def test_progress_is_folded_from_events(self):
        stages = stages_from_events(
            [
                {"stage": "parse", "state": "succeeded", "done": 3, "total": 3},
                {"stage": "ingest", "state": "running", "done": 1, "total": 3},
            ],
        )
        assert [(s.stage, s.state, s.done) for s in stages] == [
            ("parse", "succeeded", 3),
            ("ingest", "running", 1),
        ]

    def test_a_later_event_supersedes_an_earlier_one(self):
        # Progress is derived rather than stored precisely so it cannot drift; the
        # last word on a stage has to win.
        stages = stages_from_events(
            [
                {"stage": "ingest", "state": "running", "done": 1},
                {"stage": "ingest", "state": "succeeded", "done": 9},
            ],
        )
        assert (stages[0].state, stages[0].done) == ("succeeded", 9)

    def test_an_error_event_attaches_to_its_stage(self):
        stages = stages_from_events(
            [
                {"stage": "ingest", "state": "failed"},
                {"stage": "ingest", "level": "error", "message": "row 2 rejected"},
            ],
        )
        assert stages[0].error == "row 2 rejected"

    def test_events_without_a_stage_are_ignored(self):
        assert stages_from_events([{"message": "queued"}]) == []


class TestRequestValidation:
    def test_rows_cannot_target_a_collection(self):
        # Rows have no documents to keep whole. The message has to name the
        # alternative, since this is the one pairing an actor might reasonably try.
        with pytest.raises(ValidationError, match="TableTarget"):
            IngestionRequest(
                source=RowsSource(rows=[{"a": 1}]),
                target=CollectionTarget(name="anything"),
            )

    def test_files_into_one_table_is_allowed(self):
        # Deliberately permitted: it is what the batch pipeline already does for
        # spreadsheets, and the whole point of exposing it.
        assert IngestionRequest(
            source=FilesSource(paths=["q1.csv", "q2.csv"]),
            target=TableTarget(context="Data/Sales"),
        )

    @pytest.mark.parametrize("name", ["42", "0", "007"])
    def test_an_all_digit_collection_name_is_refused(self, name):
        """This one prevents data loss, not untidiness.

        The storage layer distinguishes a shared collection from a single file's
        namespace by comparing the storage id to that file's record id, and that
        flag selects between deleting one file's rows and clearing the whole
        context. A collection named "42" holding the file whose id is 42 would
        therefore wipe every other file in the collection on re-ingest.
        """
        with pytest.raises(ValidationError, match="all digits"):
            CollectionTarget(name=name)

    @pytest.mark.parametrize("name", ["a/b", "../etc", "x/../y"])
    def test_a_collection_name_cannot_escape_its_segment(self, name):
        with pytest.raises(ValidationError, match="single safe path segment"):
            CollectionTarget(name=name)

    @pytest.mark.parametrize("name", ["Q4 Reports", "batch-42", "client_acme"])
    def test_ordinary_collection_names_are_accepted(self, name):
        assert CollectionTarget(name=name).name == name

    def test_an_unnamed_collection_is_valid(self):
        # Preserves the existing behaviour of giving each file its own namespace.
        assert CollectionTarget().name is None

    def test_a_context_cannot_contain_traversal(self):
        with pytest.raises(ValidationError, match=r"\.\."):
            TableTarget(context="Data/../secrets")

    def test_an_empty_row_set_is_refused(self):
        # Almost always an upstream call that returned nothing and went unnoticed.
        # Reporting a successful run that stored nothing would hide the real fault.
        with pytest.raises(ValidationError, match="empty"):
            RowsSource(rows=[])
