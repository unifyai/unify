from __future__ import annotations

import pytest

from unity.common.federated_search import (
    FederatedSearchContext,
    SortSpec,
    default_filter_fetcher,
    federated_count,
    federated_filter,
    federated_ranked_search,
    federated_reduce,
    merge_ranked_batches,
    merge_sorted_batches,
)


def _row(name: str, score: float) -> dict:
    return {"name": name, "_score": score}


def test_merge_ranked_batches_applies_global_offset_and_limit():
    assistant = FederatedSearchContext("assistant/Guidance", "assistant")
    builtins = FederatedSearchContext("Builtins/Guidance", "builtins")

    rows = merge_ranked_batches(
        [
            (assistant, [_row("a1", 0.1), _row("a2", 0.3), _row("a3", 0.5)], "_score"),
            (builtins, [_row("b1", 0.2), _row("b2", 0.4), _row("b3", 0.6)], "_score"),
        ],
        offset=1,
        limit=3,
    )

    assert [row["name"] for row in rows] == ["b1", "a2", "b2"]
    assert [row["_federated_score"] for row in rows] == [0.2, 0.3, 0.4]
    assert rows[0]["_federated_source"] == "builtins"
    assert rows[0]["_federated_context"] == "Builtins/Guidance"
    assert "_score" not in rows[0]


def test_merge_ranked_batches_uses_source_and_local_order_as_tie_breakers():
    first = FederatedSearchContext("ctx/first", "first")
    second = FederatedSearchContext("ctx/second", "second")

    rows = merge_ranked_batches(
        [
            (first, [_row("first-1", 0.1), _row("first-2", 0.1)], "_score"),
            (second, [_row("second-1", 0.1)], "_score"),
        ],
        limit=3,
    )

    assert [row["name"] for row in rows] == ["first-1", "first-2", "second-1"]


def test_merge_ranked_batches_dedups_by_unique_id_keeping_best_score():
    first = FederatedSearchContext("ctx/first", "first")
    second = FederatedSearchContext("ctx/second", "second")

    rows = merge_ranked_batches(
        [
            (first, [{"id": 1, "_score": 0.3}, {"id": 2, "_score": 0.5}], "_score"),
            (second, [{"id": 1, "_score": 0.1}, {"id": 3, "_score": 0.4}], "_score"),
        ],
        limit=10,
        unique_id_field="id",
    )

    assert [(row["id"], row["_federated_score"]) for row in rows] == [
        (1, 0.1),
        (3, 0.4),
        (2, 0.5),
    ]
    assert rows[0]["_federated_source"] == "second"


def test_merge_ranked_batches_without_annotation_returns_clean_rows():
    spec = FederatedSearchContext("ctx", "source")

    rows = merge_ranked_batches(
        [(spec, [_row("a", 0.1)], "_score")],
        limit=1,
        annotate=False,
    )

    assert rows == [{"name": "a"}]


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


def test_default_filter_fetcher_combines_context_and_caller_filters(monkeypatch):
    calls = []

    class Row:
        def __init__(self, entries):
            self.entries = entries

    def fake_get_logs(**kwargs):
        calls.append(kwargs)
        return [Row({"name": "row"})]

    monkeypatch.setattr("unity.common.federated_search.unisdk.get_logs", fake_get_logs)

    rows = default_filter_fetcher(
        FederatedSearchContext(
            "assistant/Guidance",
            "assistant",
            row_filter="is_builtin == False",
            allowed_fields=["name", "priority"],
        ),
        "'github' in title",
        [SortSpec("priority", direction="descending")],
        5,
    )

    assert rows == [{"name": "row"}]
    assert calls == [
        {
            "context": "assistant/Guidance",
            "project": None,
            "filter": "('github' in title) and (is_builtin == False)",
            "sorting": {"priority": "descending"},
            "limit": 5,
            "offset": 0,
            "from_fields": ["name", "priority"],
        },
    ]


def test_default_filter_fetcher_fetches_all_rows_when_missing_first(monkeypatch):
    calls = []

    class Row:
        def __init__(self, entries):
            self.entries = entries

    def fake_get_logs(**kwargs):
        calls.append(kwargs)
        return [Row({"name": f"row-{kwargs['offset']}"})]

    monkeypatch.setattr("unity.common.federated_search.unisdk.get_logs", fake_get_logs)

    rows = default_filter_fetcher(
        FederatedSearchContext("ctx", "source"),
        None,
        [SortSpec("priority", missing="first")],
        2,
    )

    # The backend sorts NULLs last, so missing="first" must ignore the window
    # limit and page through everything (page size 1000).
    assert rows == [{"name": "row-0"}]
    assert calls[0]["limit"] == 1000


def test_federated_ranked_search_fetches_offset_plus_limit_per_context():
    contexts = [
        FederatedSearchContext("assistant/Guidance", "assistant"),
        FederatedSearchContext("Builtins/Guidance", "builtins"),
    ]
    calls: list[tuple[str, dict, int]] = []

    def fetcher(spec, references, limit):
        calls.append((spec.source, dict(references), limit))
        if spec.source == "assistant":
            return [_row("assistant-best", 0.1), _row("assistant-next", 0.4)], "_score"
        return [_row("builtin-best", 0.2), _row("builtin-next", 0.3)], "_score"

    rows = federated_ranked_search(
        contexts,
        {"content": "how to use GitHub"},
        offset=1,
        limit=2,
        fetcher=fetcher,
    )

    assert calls == [
        ("assistant", {"content": "how to use GitHub"}, 3),
        ("builtins", {"content": "how to use GitHub"}, 3),
    ]
    assert [row["name"] for row in rows] == ["builtin-best", "builtin-next"]


def test_federated_ranked_search_backfills_to_limit_across_contexts(monkeypatch):
    contexts = [
        FederatedSearchContext("ctx/a", "a", row_filter="active"),
        FederatedSearchContext("ctx/b", "b"),
    ]

    def fetcher(spec, references, limit):
        if spec.source == "a":
            return [{"id": 1, "_score": 0.1}], "_score"
        return [], "_score"

    backfill_calls = []

    def fake_backfill(
        context,
        initial_rows,
        k,
        *,
        row_filter,
        unique_id_field,
        allowed_fields,
        project=None,
    ):
        backfill_calls.append((context, len(initial_rows), k, row_filter))
        if context == "ctx/a":
            # id 1 already present; contributes one new row.
            return list(initial_rows) + [{"id": 5}]
        return list(initial_rows) + [{"id": 9}]

    monkeypatch.setattr(
        "unity.common.federated_search.backfill_rows",
        fake_backfill,
    )

    rows = federated_ranked_search(
        contexts,
        {"content": "x"},
        limit=3,
        fetcher=fetcher,
        unique_id_field="id",
        backfill=True,
    )

    assert [row["id"] for row in rows] == [1, 5, 9]
    assert rows[1]["_federated_source"] == "a"
    assert rows[2]["_federated_source"] == "b"
    assert backfill_calls == [("ctx/a", 1, 3, "active"), ("ctx/b", 2, 3, None)]


def test_federated_ranked_search_without_references_backfills_only(monkeypatch):
    contexts = [FederatedSearchContext("ctx/a", "a")]

    monkeypatch.setattr(
        "unity.common.federated_search.backfill_rows",
        lambda context, initial_rows, k, **kwargs: [{"id": 1}, {"id": 2}],
    )

    assert federated_ranked_search(contexts, None) == []
    rows = federated_ranked_search(contexts, None, limit=2, backfill=True)
    assert [row["id"] for row in rows] == [1, 2]


def test_federated_ranked_search_returns_empty_without_contexts_or_references():
    calls = []

    def fetcher(spec, references, limit):
        calls.append((spec, references, limit))
        return [], "_score"

    assert federated_ranked_search([], {"content": "x"}, fetcher=fetcher) == []
    assert (
        federated_ranked_search(
            [FederatedSearchContext("ctx", "source")],
            {},
            fetcher=fetcher,
        )
        == []
    )
    assert calls == []


def test_federated_ranked_search_and_filter_validate_offset():
    with pytest.raises(ValueError, match="offset"):
        federated_ranked_search(
            [FederatedSearchContext("ctx", "source")],
            {"content": "x"},
            offset=-1,
            fetcher=lambda *_args: ([], "_score"),
        )
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


def test_federated_reduce_falls_back_to_rows_for_grouped_and_exotic_metrics():
    contexts = [
        FederatedSearchContext("ctx/a", "a"),
        FederatedSearchContext("ctx/b", "b"),
    ]
    rows_by_context = {
        "ctx/a": [
            {"status": "open", "amount": 1},
            {"status": "closed", "amount": 3},
        ],
        "ctx/b": [{"status": "open", "amount": 5}],
    }

    def row_fetcher(spec, filter):
        return rows_by_context[spec.context]

    def metric_fetcher(*args):
        raise AssertionError("decomposable path must not be used")

    median = federated_reduce(
        contexts,
        metric="median",
        columns="amount",
        metric_fetcher=metric_fetcher,
        row_fetcher=row_fetcher,
    )
    assert median == 3

    grouped = federated_reduce(
        contexts,
        metric="sum",
        columns="amount",
        group_by="status",
        metric_fetcher=metric_fetcher,
        row_fetcher=row_fetcher,
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


def test_federated_count_sums_per_context_counts():
    counts = {"ctx/a": 3, "ctx/b": None, "ctx/c": 4}

    def metric_fetcher(spec, metric, keys, filter, group_by):
        assert metric == "count"
        assert keys == "row_id"
        return counts[spec.context]

    total = federated_count(
        [
            FederatedSearchContext("ctx/a", "a"),
            FederatedSearchContext("ctx/b", "b"),
            FederatedSearchContext("ctx/c", "c"),
        ],
        key="row_id",
        metric_fetcher=metric_fetcher,
    )

    assert total == 7
    assert federated_count([], key="row_id") == 0
