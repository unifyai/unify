from __future__ import annotations

import pytest

from unify.common.federated_search import (
    FederatedSearchContext,
    SortSpec,
    federated_count,
    federated_filter,
    federated_reduce,
    federated_text_search,
    merge_sorted_batches,
    query_tokens,
    text_match,
)


def test_merge_sorted_batches_applies_explicit_global_sorting():
    assistant = FederatedSearchContext("assistant/Guidance", "assistant")
    builtins = FederatedSearchContext("Builtins/Guidance", "builtins")

    rows = merge_sorted_batches(
        [
            (
                assistant,
                [
                    {"name": "alpha", "priority": 10},
                    {"name": "delta", "priority": 40},
                ],
            ),
            (
                builtins,
                [
                    {"name": "bravo", "priority": 20},
                    {"name": "charlie", "priority": 30},
                ],
            ),
        ],
        sorting=[SortSpec("priority")],
        offset=1,
        limit=2,
    )

    assert [row["name"] for row in rows] == ["bravo", "charlie"]
    assert rows[0]["_federated_source"] == "builtins"


def test_merge_sorted_batches_supports_descending_and_missing_policy():
    assistant = FederatedSearchContext("assistant/Guidance", "assistant")
    builtins = FederatedSearchContext("Builtins/Guidance", "builtins")

    rows = merge_sorted_batches(
        [
            (
                assistant,
                [
                    {"name": "missing"},
                    {"name": "low", "priority": 1},
                ],
            ),
            (builtins, [{"name": "high", "priority": 10}]),
        ],
        sorting=[SortSpec("priority", direction="descending", missing="last")],
        limit=3,
    )

    assert [row["name"] for row in rows] == ["high", "low", "missing"]


def test_merge_sorted_batches_defaults_to_source_order_then_local_order():
    assistant = FederatedSearchContext("assistant/Guidance", "assistant")
    builtins = FederatedSearchContext("Builtins/Guidance", "builtins")

    rows = merge_sorted_batches(
        [
            (assistant, [{"name": "a1"}, {"name": "a2"}]),
            (builtins, [{"name": "b1"}]),
        ],
        limit=3,
    )

    assert [row["name"] for row in rows] == ["a1", "a2", "b1"]


def test_merge_sorted_batches_dedups_by_unique_id_in_source_order():
    first = FederatedSearchContext("ctx/first", "first")
    second = FederatedSearchContext("ctx/second", "second")

    rows = merge_sorted_batches(
        [
            (first, [{"id": 1, "name": "first-copy"}]),
            (second, [{"id": 1, "name": "second-copy"}, {"id": 2, "name": "only"}]),
        ],
        limit=10,
        unique_id_field="id",
    )

    assert [row["name"] for row in rows] == ["first-copy", "only"]


def test_federated_filter_fetches_offset_plus_limit_per_context_and_sorts():
    contexts = [
        FederatedSearchContext("assistant/Guidance", "assistant", row_filter="active"),
        FederatedSearchContext("Builtins/Guidance", "builtins"),
    ]
    calls: list[tuple[str, str | None, tuple[tuple[str, str], ...], int]] = []

    def fetcher(spec, filter, sorting, limit):
        calls.append(
            (
                spec.source,
                filter,
                tuple((item.field, item.direction) for item in sorting),
                limit,
            ),
        )
        if spec.source == "assistant":
            return [{"name": "assistant-mid", "priority": 20}]
        return [
            {"name": "builtin-low", "priority": 10},
            {"name": "builtin-high", "priority": 30},
        ]

    rows = federated_filter(
        contexts,
        filter="'github' in title",
        sorting=[SortSpec("priority", direction="descending")],
        offset=1,
        limit=2,
        fetcher=fetcher,
    )

    assert calls == [
        ("assistant", "'github' in title", (("priority", "descending"),), 3),
        ("builtins", "'github' in title", (("priority", "descending"),), 3),
    ]
    assert [row["name"] for row in rows] == ["assistant-mid", "builtin-low"]


def test_federated_filter_without_fetcher_delegates_to_server(monkeypatch):
    calls = []

    def fake_get_logs_federated(**kwargs):
        calls.append(kwargs)
        return {
            "logs": [{"name": "row", "_federated_source": "assistant"}],
            "count": 1,
            "counts": {"assistant": 1},
        }

    monkeypatch.setattr(
        "unify.common.federated_search.db.get_logs_federated",
        fake_get_logs_federated,
    )

    rows = federated_filter(
        [
            FederatedSearchContext(
                "assistant/Guidance",
                "assistant",
                row_filter="is_builtin == False",
                allowed_fields=["name", "priority"],
            ),
            FederatedSearchContext(
                "Builtins/Guidance",
                "builtins",
                excluded_fields=["implementation"],
                project="Builtins",
            ),
        ],
        filter="'github' in title",
        sorting=[SortSpec("priority", direction="descending", missing="first")],
        offset=1,
        limit=2,
        unique_id_field="guidance_id",
    )

    assert rows == [{"name": "row", "_federated_source": "assistant"}]
    assert calls == [
        {
            "contexts": [
                {
                    "context": "assistant/Guidance",
                    "source": "assistant",
                    "filter": "is_builtin == False",
                    "from_fields": ["name", "priority"],
                },
                {
                    "context": "Builtins/Guidance",
                    "source": "builtins",
                    "exclude_fields": ["implementation"],
                    "project_name": "Builtins",
                },
            ],
            "filter": "'github' in title",
            "sorting": [
                {
                    "field": "priority",
                    "direction": "descending",
                    "missing": "first",
                },
            ],
            "offset": 1,
            "limit": 2,
            "unique_id_field": "guidance_id",
            "annotate": True,
        },
    ]


def test_federated_count_delegates_to_server_count_only_read(monkeypatch):
    calls = []

    def fake_get_logs_federated(**kwargs):
        calls.append(kwargs)
        return {"logs": [], "count": 7, "counts": {"a": 3, "b": 4}}

    monkeypatch.setattr(
        "unify.common.federated_search.db.get_logs_federated",
        fake_get_logs_federated,
    )

    total = federated_count(
        [
            FederatedSearchContext("ctx/a", "a"),
            FederatedSearchContext("ctx/b", "b"),
        ],
        key="row_id",
        filter="status == 'open'",
    )

    assert total == 7
    assert calls[0]["limit"] == 0
    assert calls[0]["filter"] == "(status == 'open') and (exists(row_id))"
    assert federated_count([], key="row_id") == 0


def test_query_tokens_are_distinct_lowercase_words():
    assert query_tokens("Fill out PDF form-fields, fill PDF!") == [
        "fill",
        "out",
        "pdf",
        "form",
        "fields",
    ]


def test_text_match_counts_distinct_tokens_and_per_field_hits():
    row = {
        "name": "parse_csv_report",
        "docstring": "Parse a CSV report and summarise its rows.",
    }
    references = {"name": "parse csv totals", "docstring": "parse csv totals"}

    matched, hits = text_match(row, references)

    # ``parse`` and ``csv`` are found (``totals`` is not); each hits both
    # fields, so the row scores two distinct tokens and four hits.
    assert (matched, hits) == (2, 4)
    # A token matches on word prefix, never mid-word: ``port`` is inside
    # ``report`` but not at the start of any word.
    assert text_match(row, {"docstring": "port"}) == (0, 0)
    assert text_match(row, {"docstring": "summar"}) == (1, 1)
    assert text_match(row, {"missing": "parse"}) == (0, 0)


def _patch_rows(monkeypatch, rows):
    calls = []

    def fake_get_logs_federated(**kwargs):
        calls.append(kwargs)
        return {"logs": [dict(row) for row in rows], "count": len(rows), "counts": {}}

    monkeypatch.setattr(
        "unify.common.federated_search.db.get_logs_federated",
        fake_get_logs_federated,
    )
    return calls


def test_federated_text_search_ranks_by_tokens_matched_then_hits(monkeypatch):
    contexts = [
        FederatedSearchContext("assistant/Guidance", "assistant"),
        FederatedSearchContext("Builtins/Guidance", "builtins", project="Builtins"),
    ]
    calls = _patch_rows(
        monkeypatch,
        [
            {"guidance_id": 1, "title": "docx", "content": "Create Word documents."},
            {
                "guidance_id": 2,
                "title": "pptx",
                "content": "Create PowerPoint slide decks and presentations.",
            },
            {
                "guidance_id": 3,
                "title": "slides",
                "content": "Slide layout tips for any deck.",
            },
        ],
    )

    rows = federated_text_search(
        contexts,
        {"content": "create a powerpoint slide deck"},
        limit=2,
        unique_id_field="guidance_id",
    )

    assert [row["guidance_id"] for row in rows] == [2, 3]
    assert rows[0]["_federated_score"] == 0.8  # create, powerpoint, slide, deck
    assert rows[1]["_federated_score"] == 0.4  # slide, deck
    # One full read: no offset, no limit, every spec forwarded.
    assert len(calls) == 1
    assert calls[0]["limit"] is None
    assert calls[0]["offset"] == 0
    assert [spec["source"] for spec in calls[0]["contexts"]] == [
        "assistant",
        "builtins",
    ]
    assert calls[0]["unique_id_field"] == "guidance_id"


def test_federated_text_search_matches_across_reference_fields(monkeypatch):
    _patch_rows(
        monkeypatch,
        [
            {"guidance_id": 1, "title": "pdf", "content": "Read and split files."},
            {
                "guidance_id": 2,
                "title": "forms",
                "content": "Fill out pdf form fields.",
            },
        ],
    )

    rows = federated_text_search(
        [FederatedSearchContext("ctx", "source")],
        {"content": "fill out pdf form fields", "title": "pdf"},
        limit=5,
        annotate=False,
    )

    # Row 2 matches every content token; row 1 matches ``pdf`` via its
    # title only. Without annotation no score field is written.
    assert [row["guidance_id"] for row in rows] == [2, 1]
    assert "_federated_score" not in rows[0]


def test_federated_text_search_backfills_newest_unmatched_rows(monkeypatch):
    _patch_rows(
        monkeypatch,
        [
            {"function_id": 1, "name": "alpha"},
            {"function_id": 3, "name": "gamma"},
            {"function_id": 2, "name": "beta_match"},
        ],
    )
    contexts = [FederatedSearchContext("ctx", "source")]

    assert federated_text_search(contexts, {"name": "match"}, limit=5) == [
        {"function_id": 2, "name": "beta_match", "_federated_score": 1.0},
    ]

    rows = federated_text_search(
        contexts,
        {"name": "match"},
        limit=5,
        unique_id_field="function_id",
        backfill=True,
    )
    assert [(row["function_id"], row["_federated_score"]) for row in rows] == [
        (2, 1.0),
        (3, 0.0),
        (1, 0.0),
    ]
    # Without references the search is a plain newest-first sample.
    assert federated_text_search(contexts, None, limit=2) == []
    sample = federated_text_search(
        contexts,
        None,
        limit=2,
        unique_id_field="function_id",
        backfill=True,
    )
    assert [row["function_id"] for row in sample] == [3, 2]


def test_federated_text_search_returns_empty_without_contexts_or_limit(monkeypatch):
    calls = _patch_rows(monkeypatch, [{"name": "row"}])

    assert federated_text_search([], {"name": "row"}) == []
    assert (
        federated_text_search(
            [FederatedSearchContext("ctx", "source")],
            {"name": "row"},
            limit=0,
        )
        == []
    )
    assert calls == []


def test_federated_filter_validates_offset():
    with pytest.raises(ValueError, match="offset"):
        federated_filter(
            [FederatedSearchContext("ctx", "source")],
            offset=-1,
            fetcher=lambda *_args: [],
        )


def test_federated_reduce_combines_decomposable_metrics():
    contexts = [
        FederatedSearchContext("ctx/a", "a"),
        FederatedSearchContext("ctx/b", "b"),
    ]
    metrics = {
        ("ctx/a", "count"): 3,
        ("ctx/b", "count"): 2,
        ("ctx/a", "sum"): 30.0,
        ("ctx/b", "sum"): 20.0,
        ("ctx/a", "min"): 1,
        ("ctx/b", "min"): 4,
        ("ctx/a", "max"): 10,
        ("ctx/b", "max"): 7,
    }

    def metric_fetcher(spec, metric, keys, filter, group_by):
        assert group_by is None
        return metrics[(spec.context, metric)]

    def reduce(metric):
        return federated_reduce(
            contexts,
            metric=metric,
            columns="amount",
            metric_fetcher=metric_fetcher,
        )

    assert reduce("count") == 5
    assert reduce("sum") == 50.0
    assert reduce("min") == 1
    assert reduce("max") == 10
    assert reduce("mean") == 10.0  # 50 / 5


def test_federated_reduce_falls_back_to_rows_for_grouped_and_exotic_metrics(
    monkeypatch,
):
    contexts = [
        FederatedSearchContext("ctx/a", "a"),
        FederatedSearchContext("ctx/b", "b"),
    ]

    def fake_get_logs_federated(**kwargs):
        assert kwargs["annotate"] is False
        if kwargs["offset"]:
            return {"logs": [], "count": 3, "counts": {}}
        return {
            "logs": [
                {"status": "open", "amount": 1},
                {"status": "closed", "amount": 3},
                {"status": "open", "amount": 5},
            ],
            "count": 3,
            "counts": {"a": 2, "b": 1},
        }

    monkeypatch.setattr(
        "unify.common.federated_search.db.get_logs_federated",
        fake_get_logs_federated,
    )

    def metric_fetcher(*args):
        raise AssertionError("decomposable path must not be used")

    median = federated_reduce(
        contexts,
        metric="median",
        columns="amount",
        metric_fetcher=metric_fetcher,
    )
    assert median == 3

    grouped = federated_reduce(
        contexts,
        metric="sum",
        columns="amount",
        group_by="status",
        metric_fetcher=metric_fetcher,
    )
    assert grouped == {"open": 6, "closed": 3}


def test_federated_reduce_single_context_delegates_to_server():
    calls = []

    def metric_fetcher(spec, metric, keys, filter, group_by):
        calls.append((spec.context, metric, keys, filter, group_by))
        return {"open": 4}

    result = federated_reduce(
        [FederatedSearchContext("ctx/a", "a")],
        metric="median",
        columns="amount",
        group_by="status",
        metric_fetcher=metric_fetcher,
    )

    assert result == {"open": 4}
    assert calls == [("ctx/a", "median", "amount", None, "status")]


def test_federated_reduce_validates_metric_and_contexts():
    with pytest.raises(ValueError, match="Unsupported reduction metric"):
        federated_reduce(
            [FederatedSearchContext("ctx", "source")],
            metric="bogus",
            columns="amount",
        )
    with pytest.raises(ValueError, match="at least one context"):
        federated_reduce([], metric="count", columns="amount")
