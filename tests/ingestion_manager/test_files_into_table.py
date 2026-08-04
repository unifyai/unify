"""What the in-process files tier hands the engine.

This tier is the fallback taken whenever no worker fleet is reachable, which
includes every self-host deployment and any hosted one whose control plane
cannot reach its backends. It built its own transport handles from
``table.rows`` instead of lowering through ``build_table_handles`` like the parse
worker does, and a parser only inlines rows below a bound -- above it the rows
are meant to be streamed from the source. So every table of consequence looked
empty, was skipped, and the run failed as "no tabular content": on staging,
three CSVs of 130-346 MB and 622k-1.27M rows, all of them perfectly tabular.

The engine is stubbed rather than run, because what went wrong was entirely in
the work handed to it: the handle it was given, and the row count it was told to
verify the durable checkpoint against.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, List

import pytest

from unify.common.pipeline import InlineRowsHandle, TableWork
from unify.common.pipeline.row_streaming import iter_table_input_rows
from unify.common.pipeline.types import CsvFileHandle
from unify.file_manager.file_parsers.settings import FILE_PARSER_SETTINGS
from unify.ingestion_manager.ingestion_manager import IngestionManager
from unify.ingestion_manager.types import FilesSource, IngestionRequest, TableTarget

INLINE_LIMIT = int(FILE_PARSER_SETTINGS.TABULAR_INLINE_ROW_LIMIT)


class CapturedEngine:
    """Stands in for the shared engine and keeps the work it was handed."""

    def __init__(self) -> None:
        self.work: List[TableWork] = []

    def __call__(self, run_key, work, *, request, control) -> Any:
        self.work = list(work)

        class Outcome:
            rows_committed = sum(item.declared_rows for item in work)
            contexts = [request.target.context]

        return Outcome()


@pytest.fixture
def manager(monkeypatch) -> IngestionManager:
    instance = IngestionManager.__new__(IngestionManager)
    monkeypatch.setattr(
        IngestionManager,
        "_record_event",
        lambda *args, **kwargs: None,
        raising=False,
    )
    return instance


@pytest.fixture
def engine(manager, monkeypatch) -> CapturedEngine:
    captured = CapturedEngine()
    monkeypatch.setattr(
        IngestionManager,
        "_run_engine",
        lambda self, run_key, work, *, request, control: captured(
            run_key,
            work,
            request=request,
            control=control,
        ),
        raising=False,
    )
    return captured


def write_csv(path: Path, rows: int, *, columns: int = 3, cell: str = "v") -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([f"col{index}" for index in range(columns)])
        for _ in range(rows):
            writer.writerow([cell] * columns)


def run(manager: IngestionManager, paths: List[Path], **target_kwargs) -> Any:
    request = IngestionRequest(
        source=FilesSource(paths=[str(path) for path in paths]),
        target=TableTarget(context="Data/Repairs", **target_kwargs),
    )
    return manager._files_into_table(
        "run-key",
        [str(path) for path in paths],
        request=request,
        control={"cancel": False},
    )


class TestATableAboveTheInlineBound:
    def test_it_is_ingested_at_all(self, manager, engine, tmp_path):
        # The whole defect in one assertion: this raised "Parsing found no
        # tables to store" for any spreadsheet worth ingesting.
        path = tmp_path / "FactAppointments.csv"
        write_csv(path, INLINE_LIMIT * 3)

        rows, contexts, parsed = run(manager, [path])

        assert parsed == 1
        assert contexts == ["Data/Repairs"]
        assert rows == INLINE_LIMIT * 3

    def test_it_arrives_as_a_streaming_handle(self, manager, engine, tmp_path):
        path = tmp_path / "FactAppointments.csv"
        write_csv(path, INLINE_LIMIT * 3)

        run(manager, [path])

        assert isinstance(engine.work[0].handle, CsvFileHandle)

    def test_the_handle_yields_every_row_the_parser_counted(
        self,
        manager,
        engine,
        tmp_path,
    ):
        path = tmp_path / "FactAppointments.csv"
        write_csv(path, INLINE_LIMIT * 3)

        run(manager, [path])

        streamed = list(iter_table_input_rows(engine.work[0].handle))
        assert len(streamed) == INLINE_LIMIT * 3
        assert set(streamed[0]) == {"col0", "col1", "col2"}

    def test_the_declared_count_is_the_parser_count(self, manager, engine, tmp_path):
        # Not the length of whatever was inlined. This is the number the
        # completion check holds the durable checkpoint against, so when it came
        # from the inlined slice the gate could not detect a shortfall it had
        # itself caused.
        path = tmp_path / "FactAppointments.csv"
        write_csv(path, INLINE_LIMIT * 3)

        run(manager, [path])

        assert engine.work[0].declared_rows == INLINE_LIMIT * 3


class TestATableBelowTheInlineBound:
    def test_its_rows_still_travel_inline(self, manager, engine, tmp_path):
        # Small tables must not start paying a file read; the point of the bound
        # is that they are cheap to carry.
        path = tmp_path / "dimContractors.csv"
        write_csv(path, 10)

        run(manager, [path])

        handle = engine.work[0].handle
        assert isinstance(handle, InlineRowsHandle)
        assert len(handle.rows) == 10
        assert engine.work[0].declared_rows == 10


class TestAWideTable:
    def test_it_streams_even_though_it_is_short(self, manager, engine, tmp_path):
        # Rows alone would wave this through: it is under the row limit, but
        # materialising it costs both the parsing process's memory and the
        # bandwidth of every write that carries it.
        path = tmp_path / "wide.csv"
        write_csv(path, 200, columns=60, cell="x" * 500)

        run(manager, [path])

        assert isinstance(engine.work[0].handle, CsvFileHandle)
        assert engine.work[0].declared_rows == 200
        assert len(list(iter_table_input_rows(engine.work[0].handle))) == 200


class TestSeveralFiles:
    def test_each_file_becomes_its_own_work_unit(self, manager, engine, tmp_path):
        big = tmp_path / "big.csv"
        small = tmp_path / "small.csv"
        write_csv(big, INLINE_LIMIT * 2)
        write_csv(small, 5)

        rows, _, parsed = run(manager, [big, small])

        assert parsed == 2
        assert rows == INLINE_LIMIT * 2 + 5
        assert sorted(item.declared_rows for item in engine.work) == [
            5,
            INLINE_LIMIT * 2,
        ]

    def test_checkpoint_ids_are_distinct_per_source_table(
        self,
        manager,
        engine,
        tmp_path,
    ):
        # Two files sharing a parser-local table_id must not share a checkpoint,
        # or a resume finds the wrong one.
        first = tmp_path / "a.csv"
        second = tmp_path / "b.csv"
        write_csv(first, 5)
        write_csv(second, 5)

        run(manager, [first, second])

        ids = [item.table_id for item in engine.work]
        assert len(set(ids)) == 2


class TestRefusals:
    def test_a_counted_table_with_no_reachable_rows_refuses_by_name(
        self,
        manager,
        engine,
        monkeypatch,
        tmp_path,
    ):
        # A handle that carries a count but neither inline rows nor a source to
        # read is a promise the transport cannot keep. Storing the reachable
        # part would under-ingest without reporting it, so this refuses -- the
        # same refusal the dispatched tier already makes.
        path = tmp_path / "unreachable.csv"
        write_csv(path, 50)

        # Patched at its source rather than on the manager's namespace: the
        # helper is imported inside the method, so a module-level rebind is
        # simply not seen.
        import unify.common.pipeline.transport as transport

        monkeypatch.setattr(
            transport,
            "build_table_handles",
            lambda result, **kwargs: {
                table.table_id: InlineRowsHandle(
                    rows=[],
                    columns=list(table.columns),
                    row_count=table.num_rows,
                )
                for table in result.tables
            },
        )

        with pytest.raises(RuntimeError, match="cannot read"):
            run(manager, [path])
        assert engine.work == []

    def test_a_file_with_no_tables_keeps_the_collection_advice(
        self,
        manager,
        engine,
        tmp_path,
    ):
        # The one case the original message was right about, and now the only
        # case it fires for.
        path = tmp_path / "notes.txt"
        path.write_text("no table here, just prose.\n", encoding="utf-8")

        with pytest.raises(RuntimeError, match="CollectionTarget"):
            run(manager, [path])

    def test_a_header_only_file_says_it_was_empty(self, manager, engine, tmp_path):
        # Tabular, parsed, simply has no rows. Distinct from both refusals
        # above, because the fix for it is different.
        path = tmp_path / "empty.csv"
        write_csv(path, 0)

        with pytest.raises(RuntimeError, match="every one was empty"):
            run(manager, [path])

    def test_an_unreadable_file_names_the_parse_failure(self, manager, engine):
        with pytest.raises(RuntimeError, match="failed to parse"):
            run(manager, [Path("/nonexistent/never.csv")])


class TestTheTargetsIntent:
    def test_declared_keys_reach_the_engine(self, manager, engine, tmp_path):
        # Without these a re-submit appends a duplicate, which is the upsert
        # guarantee the target's docstring makes.
        path = tmp_path / "FactAppointments.csv"
        write_csv(path, INLINE_LIMIT * 2)

        run(manager, [path], unique_keys={"col0": "str"})

        assert engine.work[0].unique_keys == {"col0": "str"}

    def test_the_targets_context_is_used_verbatim(self, manager, engine, tmp_path):
        path = tmp_path / "FactAppointments.csv"
        write_csv(path, 5)

        run(manager, [path])

        assert engine.work[0].context == "Data/Repairs"
