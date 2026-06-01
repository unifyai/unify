from __future__ import annotations

import pytest

from unity.common.federated_search import (
    FederatedSearchContext,
    federated_ranked_search,
    merge_ranked_batches,
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


def test_federated_ranked_search_validates_offset():
    with pytest.raises(ValueError, match="offset"):
        federated_ranked_search(
            [FederatedSearchContext("ctx", "source")],
            {"content": "x"},
            offset=-1,
            fetcher=lambda *_args: ([], "_score"),
        )
