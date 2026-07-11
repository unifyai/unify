"""Unit tests for read-only multi-term ranked search (client-side combination).

Multi-term searches against a foreign project (public-read contexts such as
the builtins catalogue) cannot create derived mean-of-cosines columns, so
``fetch_top_k_by_terms_with_score`` combines per-term read-only queries
client-side instead. These tests stub the unify read calls and verify the
combination semantics mirror ``ensure_mean_cosine_column``: mean over terms
whose embedding exists for the row, maximal distance 2 when none exist.
"""

from __future__ import annotations

import re
from types import SimpleNamespace

import unisdk
import unify.common.semantic_search as semantic_search
from unify.common.semantic_search import (
    COMBINED_COSINE_KEY,
    fetch_top_k_by_terms_combined_client_side,
    fetch_top_k_by_terms_with_score,
)

# Per-row cosine distance per embedding column; None means the row has no
# embedding for that term (e.g. NULL source column).
_DISTANCES = {
    "_a_emb": {1: 0.1, 2: 0.5, 3: 0.9, 4: None},
    "_b_emb": {1: 0.4, 2: None, 3: 0.2, 4: None},
}


def _fake_get_logs(
    *,
    context,
    project=None,
    filter=None,
    sorting=None,
    limit=None,
    from_fields=None,
    return_sort_distance=False,
    **kwargs,
):
    assert return_sort_distance, "read-only path must rank via sort distance"
    sort_expr = next(iter(sorting))
    embed_col = next(col for col in _DISTANCES if col in sort_expr)
    distances = _DISTANCES[embed_col]

    row_ids = list(distances)
    if filter:
        in_match = re.search(r"guidance_id in \[([^\]]*)\]", filter)
        assert in_match, f"unexpected filter {filter!r}"
        wanted = {int(value) for value in in_match.group(1).split(",")}
        row_ids = [row_id for row_id in row_ids if row_id in wanted]

    # Backend orders by ascending distance with NULLs last.
    row_ids.sort(key=lambda rid: (distances[rid] is None, distances[rid] or 0.0))
    logs = []
    for row_id in row_ids[:limit]:
        entries = {"guidance_id": row_id, "title": f"row-{row_id}"}
        if distances[row_id] is not None:
            entries["_sort_distance"] = distances[row_id]
        logs.append(SimpleNamespace(entries=entries))
    return logs


def _fake_get_context(context, *, project=None, **kwargs):
    return {"unique_keys": {"guidance_id": "int"}}


def test_combined_client_side_mean_with_missing_penalty(monkeypatch):
    monkeypatch.setattr(unisdk, "get_logs", _fake_get_logs)
    monkeypatch.setattr(unisdk, "get_context", _fake_get_context)

    rows, score_key = fetch_top_k_by_terms_combined_client_side(
        "Guidance",
        [("_a_emb", "ref a"), ("_b_emb", "ref b")],
        k=10,
        allowed_fields=["guidance_id", "title"],
        project="Builtins",
    )

    assert score_key == COMBINED_COSINE_KEY
    by_id = {row["guidance_id"]: row[COMBINED_COSINE_KEY] for row in rows}
    # Row 1: both terms present -> mean(0.1, 0.4).
    assert by_id[1] == (0.1 + 0.4) / 2
    # Row 2: term b embedding missing -> mean over the present term only.
    assert by_id[2] == 0.5
    # Row 3: outside term a's top-k window but exact score backfilled.
    assert by_id[3] == (0.9 + 0.2) / 2
    # Row 4: no embeddings at all -> maximal distance 2.
    assert by_id[4] == 2.0
    assert [row["guidance_id"] for row in rows] == [1, 2, 3, 4]


def test_combined_client_side_applies_k_window(monkeypatch):
    monkeypatch.setattr(unisdk, "get_logs", _fake_get_logs)
    monkeypatch.setattr(unisdk, "get_context", _fake_get_context)

    rows, _ = fetch_top_k_by_terms_combined_client_side(
        "Guidance",
        [("_a_emb", "ref a"), ("_b_emb", "ref b")],
        k=2,
        allowed_fields=["guidance_id", "title"],
        project="Builtins",
    )
    assert [row["guidance_id"] for row in rows] == [1, 2]


def test_multi_term_foreign_project_routes_to_client_side(monkeypatch):
    captured = {}

    def fake_combined(context, terms, *, k, row_filter, allowed_fields, project):
        captured.update(
            context=context,
            terms=terms,
            k=k,
            project=project,
        )
        return [{"guidance_id": 1, COMBINED_COSINE_KEY: 0.0}], COMBINED_COSINE_KEY

    monkeypatch.setattr(
        semantic_search,
        "fetch_top_k_by_terms_combined_client_side",
        fake_combined,
    )

    rows, score_key = fetch_top_k_by_terms_with_score(
        "Guidance",
        [("_a_emb", "x"), ("_b_emb", "y")],
        k=5,
        project="Builtins",
    )

    assert score_key == COMBINED_COSINE_KEY
    assert rows == [{"guidance_id": 1, COMBINED_COSINE_KEY: 0.0}]
    assert captured == {
        "context": "Guidance",
        "terms": [("_a_emb", "x"), ("_b_emb", "y")],
        "k": 5,
        "project": "Builtins",
    }


def test_single_term_foreign_project_stays_on_sort_distance_path(monkeypatch):
    monkeypatch.setattr(unisdk, "get_logs", _fake_get_logs)

    rows, score_key = fetch_top_k_by_terms_with_score(
        "Guidance",
        [("_a_emb", "ref a")],
        k=2,
        allowed_fields=["guidance_id", "title"],
        project="Builtins",
    )
    assert score_key == "_sort_distance"
    assert [row["guidance_id"] for row in rows] == [1, 2]
