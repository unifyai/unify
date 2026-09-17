"""The decisions ingestion makes without asking anyone.

The one that matters most is not visible in a result, which is why it is tested
directly rather than through a run: **what a caller should do next.** A status
that has to be interpreted eventually is, and the two ways that goes wrong are a
retry that duplicates data and a failure nobody notices.

The validation tests cover the refusals that prevent data loss rather than merely
tidy input -- most sharply the collection name that could alias a file's own
namespace.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from unify.ingestion_manager.policy import next_step, stages_from_events
from unify.ingestion_manager.types import (
    CollectionTarget,
    FilesSource,
    IngestionRequest,
    RowsSource,
    TableTarget,
)


def request(source, target=None) -> IngestionRequest:
    return IngestionRequest(
        source=source,
        target=target or TableTarget(context="Data/Target"),
    )


class TestNextStep:
    """Every state has to name a concrete action, including 'none'."""

    def _step(self, state, *, parked=0, error=None, contexts=("Data/X",)):
        return next_step(
            state=state,
            parked=parked,
            error=error,
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
        # The dangerous case: a run that finished with items parked looks fine in
        # a listing, and the missing rows are easy to miss.
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

    def test_paused_says_a_resume_continues_rather_than_restarts(self):
        # The reason resume is safe to offer at all: it picks up from the
        # checkpoint, so a caller cannot read it as "start over".
        assert "checkpoint" in self._step("paused")

    def test_running_offers_polling_or_waiting(self):
        step = self._step("running")
        assert "get_status" in step
        assert "wait()" in step

    def test_queued_says_to_poll(self):
        assert "get_status" in next_step(
            state="queued",
            parked=0,
            error=None,
            contexts=[],
        )

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


class TestStagesOnATerminalRun:
    """A run that is over must not report a stage still running.

    Stage events are append-only and a stage that runs to completion records no
    closing event, so a failed file ingestion reported ``parse: running`` beside
    ``ingest: failed`` -- parsing had finished and the write was what failed, but
    anything reading the stage list saw a run still in flight. These pin the
    fold, which is where the answer has to come from: there is no stored counter
    to correct.
    """

    def test_a_stage_that_reached_its_total_closes_as_succeeded(self):
        # This is the live case: parse finished, ingest failed after it.
        stages = stages_from_events(
            [
                {"stage": "parse", "state": "running", "done": 1, "total": 1},
                {
                    "stage": "ingest",
                    "state": "failed",
                    "level": "error",
                    "message": "x",
                },
            ],
            run_state="failed",
        )
        by_stage = {s.stage: s for s in stages}
        assert by_stage["parse"].state == "succeeded"
        assert by_stage["ingest"].state == "failed"

    def test_an_unfinished_stage_ends_however_the_run_ended(self):
        # Short of its total, so it did not finish -- claiming otherwise would
        # hide exactly the partial work a reader needs to see.
        stages = stages_from_events(
            [{"stage": "parse", "state": "running", "done": 1, "total": 4}],
            run_state="cancelled",
        )
        assert stages[0].state == "cancelled"

    def test_a_stage_with_no_declared_total_ends_however_the_run_ended(self):
        stages = stages_from_events(
            [{"stage": "ingest", "state": "running", "done": 7}],
            run_state="failed",
        )
        assert stages[0].state == "failed"

    def test_a_live_run_leaves_its_stages_alone(self):
        stages = stages_from_events(
            [{"stage": "parse", "state": "running", "done": 1, "total": 1}],
            run_state="running",
        )
        assert stages[0].state == "running"

    def test_a_stage_that_already_closed_is_not_reopened_or_relabelled(self):
        stages = stages_from_events(
            [{"stage": "parse", "state": "succeeded", "done": 2, "total": 2}],
            run_state="cancelled",
        )
        assert stages[0].state == "succeeded"


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


class TestActorSurface:
    """The manager is only real if a plan can actually call it.

    Registering a ManagerSpec is not enough: the alias must also be canonical
    and mapped to a registry getter, or scoped collection drops the manager and
    `primitives.ingestion` simply does not exist on the surface the actor is
    handed. That is how a fully-built, fully-tested manager can be unreachable
    from every plan while every unit test still passes.
    """

    def test_the_alias_is_canonical(self):
        from unify.function_manager.primitives.scope import VALID_MANAGER_ALIASES

        assert "ingestion" in VALID_MANAGER_ALIASES

    def test_every_public_verb_is_collected(self):
        from unify.function_manager.primitives.registry import get_registry
        from unify.function_manager.primitives.scope import PrimitiveScope

        collected = get_registry().collect_primitives(
            PrimitiveScope.single("ingestion"),
        )
        names = {key.rsplit(".", 1)[-1] for key in collected}
        assert names == {
            "submit",
            "get_status",
            "get_logs",
            "wait",
            "list_runs",
            "retry",
            "cancel",
            "pause",
            "resume",
            # The closing step of a run: rows landed against rows expected, plus
            # the columns that came back blank. A count alone agreed with a run
            # that committed 449,287 valueless rows.
            "reconcile",
        }

    def test_a_default_primitives_instance_exposes_it(self):
        from unify.function_manager.primitives import Primitives

        primitives = Primitives()
        assert "ingestion" in primitives.primitive_scope.scoped_managers
        assert hasattr(primitives, "ingestion")
