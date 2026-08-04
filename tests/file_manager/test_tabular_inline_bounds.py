"""When a parsed table's rows are carried inline rather than streamed.

Bounded on both axes because a table is only cheap to carry when it is short
*and* narrow, and the width axis was missing. A thousand rows two hundred wide
columns across is under any row limit and still costs twice: materialised whole
in the process that parsed it, then written in chunks whose payloads contend for
the bandwidth the assistant is already using. Neither cost appears in a row
count.

Declining to inline is never lossy -- the rows stream from their source instead
-- so the bounds are deliberately conservative in that direction.
"""

from __future__ import annotations

from unify.file_manager.file_parsers.implementations.native.spreadsheet_support import (
    measure_row_bytes,
    should_inline_tabular_rows,
)
from unify.file_manager.file_parsers.settings import FileParserSettings

NARROW = [{"id": "1", "trade": "Plumbing"}]
WIDE = [{f"col{index}": "x" * 500 for index in range(60)}]


def settings(**overrides) -> FileParserSettings:
    base = {"TABULAR_INLINE_ROW_LIMIT": 1000, "TABULAR_INLINE_MAX_BYTES": 4_000_000}
    return FileParserSettings(**{**base, **overrides})


class TestTheLengthBound:
    def test_a_short_table_inlines(self):
        assert should_inline_tabular_rows(
            row_count=10,
            settings=settings(),
            sample_rows=NARROW,
        )

    def test_a_long_table_does_not(self):
        assert not should_inline_tabular_rows(
            row_count=1_001,
            settings=settings(),
            sample_rows=NARROW,
        )

    def test_the_limit_is_inclusive(self):
        # Stated because an off-by-one moves the boundary silently.
        assert should_inline_tabular_rows(
            row_count=1_000,
            settings=settings(),
            sample_rows=NARROW,
        )


class TestTheWidthBound:
    def test_a_short_but_wide_table_does_not_inline(self):
        # The case a row limit alone waves through. 900 rows of ~30 KB each is
        # roughly 27 MB materialised, and every chunk written carries a slice of
        # it.
        assert not should_inline_tabular_rows(
            row_count=900,
            settings=settings(),
            sample_rows=WIDE,
        )

    def test_the_same_width_inlines_when_there_are_few_enough_rows(self):
        # The bound is on the product, not on width alone: a handful of wide
        # rows is still cheap.
        assert should_inline_tabular_rows(
            row_count=5,
            settings=settings(),
            sample_rows=WIDE,
        )

    def test_the_projection_is_measured_from_the_sample(self):
        per_row = measure_row_bytes(WIDE)
        ceiling = per_row * 10
        tight = settings(TABULAR_INLINE_MAX_BYTES=ceiling)
        assert should_inline_tabular_rows(
            row_count=10,
            settings=tight,
            sample_rows=WIDE,
        )
        assert not should_inline_tabular_rows(
            row_count=11,
            settings=tight,
            sample_rows=WIDE,
        )

    def test_without_a_sample_only_the_row_bound_applies(self):
        # A caller that has not collected a preview cannot be judged on width,
        # and inventing a number for it would be the guess this design avoids.
        assert should_inline_tabular_rows(row_count=900, settings=settings())

    def test_a_zero_ceiling_disables_the_width_bound(self):
        assert should_inline_tabular_rows(
            row_count=900,
            settings=settings(TABULAR_INLINE_MAX_BYTES=0),
            sample_rows=WIDE,
        )

    def test_an_empty_table_inlines_trivially(self):
        assert should_inline_tabular_rows(
            row_count=0,
            settings=settings(),
            sample_rows=WIDE,
        )


class TestMeasureRowBytes:
    def test_it_measures_the_serialised_form(self):
        # JSON length rather than an object-graph estimate, because that is the
        # form the rows travel in and the form the backend is handed, so one
        # number bounds memory and write payload alike.
        assert measure_row_bytes([{"a": "bb"}]) == len('[{"a": "bb"}]')

    def test_it_survives_values_json_does_not_know(self):
        # Parser output is normalised, but a bound that raises on an odd cell
        # would fail the parse rather than merely decline to inline.
        class Odd:
            def __str__(self) -> str:
                return "odd"

        assert measure_row_bytes([{"a": Odd()}]) > 0

    def test_it_grows_with_width(self):
        assert measure_row_bytes(WIDE) > measure_row_bytes(NARROW)
