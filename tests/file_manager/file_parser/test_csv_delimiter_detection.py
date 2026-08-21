"""A delimiter is measured from the content, never assumed.

``csv.Sniffer`` raises ``csv.Error`` on any sample it cannot read confidently,
and a 64 KB window cut mid-row is enough to do it. The fallback used to be a
comma. For a tab-delimited export that yields exactly one field per row, and a
single-column frame reads downstream as "no tabular content" -- so the file was
**dropped rather than visibly misparsed**, which is the worst of the three
possible outcomes.

Observed live: five files in one staging ingestion run were lost this way. The
assistant had itself reported "10 comma-delimited and 5 tab-delimited" from the
same bytes, so the information was available and the parser was the only thing
that did not use it.

There was no delimiter coverage at all before this file, which is why a comma
fallback could stand in for detection unnoticed.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from unify.file_manager.file_parsers.implementations.native.backends.csv_backend import (
    _detect_csv_dialect,
)


def _best_delimiter(sample_text: str, **kwargs):
    """Imported inside the call so the behaviour tests still collect without it.

    A module-level import of the helper would turn a missing fix into a
    collection error, and a file that cannot be collected reports nothing at all
    -- the behaviour assertions below would never run to catch the regression
    they exist for.
    """
    from unify.file_manager.file_parsers.implementations.native.backends import (
        csv_backend,
    )

    return csv_backend._best_delimiter(sample_text, **kwargs)


ROWS = [
    ["WorksOrderRef", "PropertyRef", "Trade", "Notes"],
    ["WH-WO-000001", "WH-PROP-0001", "Multitrade", "leak, kitchen"],
    ["WH-WO-000002", "WH-PROP-0002", "Electrician", "EICR, urgent"],
    ["WH-WO-000003", "WH-PROP-0003", "Multitrade", "door"],
]


def _write(path: Path, delimiter: str, rows=ROWS) -> Path:
    with path.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh, delimiter=delimiter).writerows(rows)
    return path


class TestTheBugThatLostFiles:
    def test_a_tab_file_is_not_read_as_one_column(self, tmp_path):
        # The regression, stated as its consequence: a comma fallback gives one
        # field per row, and one column is what made these files vanish.
        path = _write(tmp_path / "dimJobDetails.csv", "\t")

        info = _detect_csv_dialect(path)

        assert info["delimiter"] == "\t"

    def test_a_failing_sniffer_does_not_fall_back_to_a_comma(
        self,
        tmp_path,
        monkeypatch,
    ):
        # The actual trigger. The sniffer's opinion is unavailable, which is
        # common on a truncated sample, and the delimiter still has to be right.
        def _always_raises(self, *args, **kwargs):
            raise csv.Error("Could not determine delimiter")

        monkeypatch.setattr(csv.Sniffer, "sniff", _always_raises)
        monkeypatch.setattr(csv.Sniffer, "has_header", _always_raises)
        path = _write(tmp_path / "tabbed.csv", "\t")

        assert _detect_csv_dialect(path)["delimiter"] == "\t"

    def test_commas_inside_tab_separated_fields_do_not_win(self, tmp_path):
        # "leak, kitchen" and "EICR, urgent" mean commas outnumber nothing --
        # counting occurrences would pick the wrong delimiter, so consistency of
        # row shape is what decides.
        path = _write(tmp_path / "notes.csv", "\t")

        assert _detect_csv_dialect(path)["delimiter"] == "\t"

    def test_a_truncated_final_row_does_not_skew_the_choice(self):
        # A 64 KB read lands mid-row. The partial line must not cast a vote for
        # whichever delimiter happens to appear in it.
        sample = "a\tb\tc\n1\t2\t3\n4\t5\t6\n7,8,9,10,11"

        assert _best_delimiter(sample) == "\t"


class TestOtherRealFormats:
    @pytest.mark.parametrize(
        "delimiter,label",
        [
            (",", "comma"),
            ("\t", "tab"),
            (";", "semicolon, common in European exports"),
            ("|", "pipe"),
        ],
    )
    def test_each_supported_delimiter_is_detected(self, tmp_path, delimiter, label):
        path = _write(tmp_path / f"f_{label[:4]}.csv", delimiter)

        assert _detect_csv_dialect(path)["delimiter"] == delimiter, label

    def test_the_widest_consistent_split_wins_a_tie(self):
        # Both delimiters partition every row; the one finding more columns is
        # the real one, because a coincidental separator rarely splits evenly.
        assert _best_delimiter("a;b\tc\td\n1;2\t3\t4\n5;6\t7\t8\n") == "\t"


class TestDegradingSafely:
    def test_a_single_column_file_still_parses(self):
        # Nothing to split on is a legitimate shape, not a failure.
        assert _best_delimiter("value\n1\n2\n3\n") == ","

    def test_an_empty_sample_yields_a_usable_default(self):
        assert _best_delimiter("") == ","

    def test_blank_lines_are_ignored(self):
        assert _best_delimiter("a\tb\n\n1\t2\n\n3\t4\n") == "\t"

    def test_detection_reports_the_header_and_encoding_it_used(self, tmp_path):
        # The caller relies on these alongside the delimiter; a delimiter fix
        # must not quietly drop them.
        path = _write(tmp_path / "hdr.csv", "\t")

        info = _detect_csv_dialect(path)

        assert info["has_header"] is True
        assert info["encoding"] in ("utf-8", "utf-8-sig")
        assert info["quotechar"] == '"'
