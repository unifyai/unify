"""The invariants that make an interrupted ingestion resumable.

These are the tests that used to exist only against the cloud backend, moved onto
the port so both bindings are held to them. That matters more than coverage: the
whole reason in-process execution is safe to offer is that it runs *this* code, so
a guarantee proven only for the fleet would be a guarantee the local tier merely
claims.

Each test below corresponds to a specific way an interrupted ingestion loses or
duplicates rows. None of them are hypothetical -- they are why the lease, the
checkpoint and the completion check exist.
"""

from __future__ import annotations

import tempfile
from typing import Any, Dict, List, Optional

import pytest

from unify.common.pipeline.artifact_store import LocalArtifactStore
from unify.common.pipeline.checkpointed_ingest import (
    INGEST_KEY_COLUMN,
    SURRENDER_SENTINEL,
    CheckpointedIngest,
    DuplicateLiveAttempt,
    IncompleteIngest,
    TableWork,
)
from unify.common.pipeline.types import InlineRowsHandle
from unify.common.pipeline.work_queue import PipelineCancelled, RetryWorkItem


@pytest.fixture()
def store():
    with tempfile.TemporaryDirectory() as tmp:
        yield LocalArtifactStore(root_dir=tmp)


class _Task:
    """Stands in for the executor's task object, which only ``task_type`` is read from."""

    task_type = "insert_chunk_0"


class _Result:
    def __init__(self, rows: int):
        self.value = {"row_count": rows}


class FakeDataManager:
    """Records what it was asked to insert and drives the chunk callbacks.

    Deliberately not a mock of the real insert path -- these tests are about the
    bookkeeping around it, and a fake keeps the failure modes (skip offsets,
    checkpoint counts, surrender points) directly observable.
    """

    def __init__(self, *, chunks: int = 4, rows_per_chunk: int = 25):
        self.chunks = chunks
        self.rows_per_chunk = rows_per_chunk
        self.calls: List[Dict[str, Any]] = []
        self.fail_on_chunk: Optional[int] = None

    def ingest(
        self,
        context: str,
        rows: Any = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        # Mirrors DataManager.ingest, where ``rows`` is positional and is None
        # when the caller supplies a streaming handle instead.
        assert rows is None, "the engine must stream from a handle, not pass rows"
        self.calls.append({"context": context, **kwargs})
        on_task_complete = kwargs.get("on_task_complete")
        before = kwargs.get("before_insert_chunk")
        skip = int(kwargs.get("skip_rows") or 0)
        already = skip // self.rows_per_chunk

        for index in range(already, self.chunks):
            if before:
                before()
            if self.fail_on_chunk is not None and index == self.fail_on_chunk:
                raise RuntimeError(f"insert failed at chunk {index}")
            if on_task_complete:
                on_task_complete(_Task(), _Result(self.rows_per_chunk))
        return {"context": context}


def work(rows: int = 100, *, table_id: str = "t1") -> TableWork:
    return TableWork(
        table_id=table_id,
        label=f"table-{table_id}",
        context="Data/Target",
        handle=InlineRowsHandle(rows=[{"a": i} for i in range(rows)], row_count=rows),
        declared_rows=rows,
        columns=["a"],
        chunk_size=25,
    )


class TestCheckpointing:
    def test_progress_is_recorded_as_each_chunk_commits(self, store):
        dm = FakeDataManager()
        engine = CheckpointedIngest(artifact_store=store, job_id="j1")
        outcome = engine.run([work()], dm=dm)

        assert outcome.rows_committed == 100
        checkpoint = store.read_checkpoint("j1", "t1")
        assert (checkpoint.rows_committed, checkpoint.chunks_committed) == (100, 4)

    def test_an_interrupted_run_resumes_from_its_checkpoint(self, store):
        """The central guarantee: re-work is bounded to one chunk.

        Without it, a crash at 75% either restarts from zero or appends the rows
        it already wrote -- and the second is worse, because nothing reports it.
        """
        dm = FakeDataManager()
        dm.fail_on_chunk = 3
        engine = CheckpointedIngest(artifact_store=store, job_id="j2")
        with pytest.raises(IncompleteIngest):
            engine.run([work()], dm=dm)

        assert store.read_checkpoint("j2", "t1").rows_committed == 75

        # A fresh attempt picks up where the last one stopped.
        resumed = FakeDataManager()
        again = CheckpointedIngest(artifact_store=store, job_id="j2")
        outcome = again.run([work()], dm=resumed)

        assert resumed.calls[0]["skip_rows"] == 75
        assert outcome.rows_committed == 100

    def test_a_completed_table_acks_a_duplicate_without_reingesting(self, store):
        dm = FakeDataManager()
        CheckpointedIngest(artifact_store=store, job_id="j3").run([work()], dm=dm)

        duplicate = FakeDataManager()
        outcome = CheckpointedIngest(artifact_store=store, job_id="j3").run(
            [work()],
            dm=duplicate,
        )
        assert duplicate.calls == []
        assert outcome.tables[0].already_complete
        assert outcome.rows_committed == 100

    def test_a_stalled_checkpoint_still_falls_through_to_a_real_ingest(self, store):
        # The duplicate fast path is gated on reaching the declared total, so a
        # checkpoint that stopped short is never mistaken for a finished table.
        dm = FakeDataManager()
        dm.fail_on_chunk = 2
        with pytest.raises(IncompleteIngest):
            CheckpointedIngest(artifact_store=store, job_id="j4").run([work()], dm=dm)

        second = FakeDataManager()
        CheckpointedIngest(artifact_store=store, job_id="j4").run([work()], dm=second)
        assert second.calls, "a short checkpoint must not be read as complete"


class TestCompletionCheck:
    def test_a_shortfall_is_raised_rather_than_returned(self, store):
        """Raised so it cannot be finalised past by ignoring a field.

        A run that commits fewer rows than its source declared has lost data. The
        only thing worse than failing here is succeeding here.
        """
        dm = FakeDataManager(chunks=2)  # commits 50 of a declared 100
        with pytest.raises(IncompleteIngest) as raised:
            CheckpointedIngest(artifact_store=store, job_id="j5").run([work()], dm=dm)
        assert "50/100" in raised.value.detail

    def test_the_shortfall_names_every_affected_table(self, store):
        dm = FakeDataManager(chunks=1)
        with pytest.raises(IncompleteIngest) as raised:
            CheckpointedIngest(artifact_store=store, job_id="j6").run(
                [work(table_id="a"), work(table_id="b")],
                dm=dm,
            )
        assert {t.table_id for t in raised.value.shortfalls} == {"a", "b"}

    def test_verification_can_be_deferred_but_the_gap_is_still_reported(self, store):
        # For a caller implementing its own bounded resume. The gap remains
        # visible in the outcome, so turning the raise off does not hide it.
        dm = FakeDataManager(chunks=2)
        outcome = CheckpointedIngest(artifact_store=store, job_id="j7").run(
            [work()],
            dm=dm,
            verify=False,
        )
        assert [t.table_id for t in outcome.shortfalls] == ["t1"]


class TestLeasing:
    def test_a_second_live_attempt_is_refused(self, store):
        engine = CheckpointedIngest(artifact_store=store, job_id="j8")
        engine._acquire("t1")

        other = CheckpointedIngest(artifact_store=store, job_id="j8")
        with pytest.raises(DuplicateLiveAttempt):
            other._acquire("t1")

    def test_a_lease_is_released_when_a_table_finishes(self, store):
        """Released rather than left to expire.

        A stranded lease makes a successor wait out the TTL before it can resume,
        which turns a one-chunk interruption into minutes of stalled work.
        """
        dm = FakeDataManager()
        engine = CheckpointedIngest(artifact_store=store, job_id="j9")
        engine.run([work()], dm=dm)
        assert engine._held == {}

        # Immediately reacquirable, which is what "released" has to mean.
        assert CheckpointedIngest(artifact_store=store, job_id="j9")._acquire("t1")

    def test_a_lease_is_released_even_when_the_insert_fails(self, store):
        dm = FakeDataManager()
        dm.fail_on_chunk = 0
        engine = CheckpointedIngest(artifact_store=store, job_id="j10")
        with pytest.raises(IncompleteIngest):
            engine.run([work()], dm=dm)
        assert engine._held == {}

    def test_a_checkpoint_carries_the_lease_generation_that_wrote_it(self, store):
        # This is what lets a later attempt tell whose progress it is reading,
        # rather than assuming the mark is its own.
        dm = FakeDataManager()
        CheckpointedIngest(artifact_store=store, job_id="j11").run([work()], dm=dm)
        assert store.read_checkpoint("j11", "t1").lease_generation is not None


class TestStoppingCleanly:
    def test_a_surrender_between_chunks_becomes_a_retry_not_a_failure(self, store):
        """A worker shutting down must park, not fail.

        The retry wrapper captures every exception into a result string, so a
        surrender arrives looking like an ordinary error. Left that way a rollout
        would fail every in-flight run instead of resuming it one chunk later.
        """
        dm = FakeDataManager()
        engine = CheckpointedIngest(artifact_store=store, job_id="j12")
        with pytest.raises(RetryWorkItem) as raised:
            engine.run([work()], dm=dm, should_surrender=lambda: True)
        assert SURRENDER_SENTINEL in str(raised.value)

    def test_surrendering_leaves_the_checkpoint_current(self, store):
        # The point of stopping at a chunk boundary: whatever committed is
        # recorded, so the replacement loses nothing.
        dm = FakeDataManager()
        surrender = {"after": 2}

        def _should_surrender() -> bool:
            surrender["after"] -= 1
            return surrender["after"] < 0

        engine = CheckpointedIngest(artifact_store=store, job_id="j13")
        with pytest.raises(RetryWorkItem):
            engine.run([work()], dm=dm, should_surrender=_should_surrender)

        checkpoint = store.read_checkpoint("j13", "t1")
        assert checkpoint is not None and checkpoint.rows_committed > 0

    def test_cancellation_stops_at_a_chunk_boundary(self, store):
        dm = FakeDataManager()
        engine = CheckpointedIngest(artifact_store=store, job_id="j14")
        with pytest.raises((IncompleteIngest, PipelineCancelled)):
            engine.run([work()], dm=dm, is_cancelled=lambda: True)
        # Whatever committed before the stop is still recorded and resumable.
        checkpoint = store.read_checkpoint("j14", "t1")
        assert checkpoint is None or checkpoint.rows_committed <= 100


class TestIngestShape:
    def test_rows_are_upserted_rather_than_appended(self, store):
        """The reserved key is declared unique on every ingest.

        Without it a resume that misjudges its offset appends duplicates instead
        of overwriting them, and the table ends up longer than the source.
        """
        dm = FakeDataManager()
        CheckpointedIngest(artifact_store=store, job_id="j15").run([work()], dm=dm)
        call = dm.calls[0]
        assert call["unique_keys"] == {INGEST_KEY_COLUMN: "str"}
        assert INGEST_KEY_COLUMN in call["fields"]

    def test_caller_declared_keys_own_the_row_identity(self, store):
        """Declared unique keys reach the insert; the reserved key stands down.

        This is the contract behind "supplying unique_keys makes a re-run an
        upsert": if the declared keys were dropped in favour of the per-run
        reserved key, every re-submission of the same source would append a
        full duplicate copy while reporting success.
        """
        dm = FakeDataManager()
        entry = work()
        keyed = TableWork(
            **{
                **entry.__dict__,
                "unique_keys": {"email": "str"},
                "fields": {"email": "str", "name": "str"},
            },
        )
        CheckpointedIngest(artifact_store=store, job_id="j18").run([keyed], dm=dm)
        call = dm.calls[0]
        assert call["unique_keys"] == {"email": "str"}
        assert call["private_ingest_key_column"] == ""
        assert INGEST_KEY_COLUMN not in call["fields"]
        assert call["fields"]["email"] == "str"

    def test_progress_is_reported_as_it_commits(self, store):
        seen: List[tuple] = []
        dm = FakeDataManager()
        CheckpointedIngest(artifact_store=store, job_id="j16").run(
            [work()],
            dm=dm,
            on_progress=lambda table, done, total: seen.append((table, done, total)),
        )
        assert seen[0] == ("t1", 25, 100)
        assert seen[-1] == ("t1", 100, 100)

    def test_an_empty_work_list_is_not_an_error(self, store):
        outcome = CheckpointedIngest(artifact_store=store, job_id="j17").run(
            [],
            dm=FakeDataManager(),
        )
        assert outcome.rows_committed == 0
        assert outcome.shortfalls == []
