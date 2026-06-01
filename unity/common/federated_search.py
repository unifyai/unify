from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping, Optional, Sequence

from .semantic_search import ensure_vector_for_source, fetch_top_k_by_terms_with_score


@dataclass(frozen=True)
class FederatedSearchContext:
    """One context participating in a federated semantic search."""

    context: str
    source: str
    row_filter: Optional[str] = None
    allowed_fields: Optional[Sequence[str]] = None


RankedFetcher = Callable[
    [FederatedSearchContext, Mapping[str, str], int],
    tuple[list[dict], str],
]


def default_ranked_fetcher(
    spec: FederatedSearchContext,
    references: Mapping[str, str],
    limit: int,
) -> tuple[list[dict], str]:
    """Fetch ranked rows from one context and expose the score column."""
    terms = [
        (ensure_vector_for_source(spec.context, source_expr), str(ref_text))
        for source_expr, ref_text in references.items()
    ]
    return fetch_top_k_by_terms_with_score(
        spec.context,
        terms,
        k=limit,
        row_filter=spec.row_filter,
        allowed_fields=list(spec.allowed_fields) if spec.allowed_fields else None,
    )


def merge_ranked_batches(
    batches: Sequence[tuple[FederatedSearchContext, list[dict], str]],
    *,
    offset: int = 0,
    limit: int = 10,
    score_field: str = "_federated_score",
    source_field: str = "_federated_source",
    context_field: str = "_federated_context",
) -> list[dict]:
    """Merge per-context ranked result batches into one globally ranked window.

    Each batch is already sorted by ascending distance within its own context.
    The merge is exact when every context was fetched with at least
    ``offset + limit`` rows: a row outside that local window cannot appear in
    the global window because its own context already has ``offset + limit``
    better rows ahead of it.
    """
    if offset < 0:
        raise ValueError("offset must be >= 0")
    if limit <= 0:
        return []

    annotated: list[tuple[float, int, int, dict]] = []
    for source_order, (spec, rows, raw_score_field) in enumerate(batches):
        for local_order, row in enumerate(rows):
            merged = dict(row)
            try:
                score = float(merged.get(raw_score_field, float("inf")))
            except (TypeError, ValueError):
                score = float("inf")

            if raw_score_field and raw_score_field != score_field:
                merged.pop(raw_score_field, None)
            merged[score_field] = score
            merged[source_field] = spec.source
            merged[context_field] = spec.context
            annotated.append((score, source_order, local_order, merged))

    annotated.sort(key=lambda item: (item[0], item[1], item[2]))
    return [row for *_unused, row in annotated[offset : offset + limit]]


def federated_ranked_search(
    contexts: Sequence[FederatedSearchContext],
    references: Optional[Mapping[str, str]],
    *,
    offset: int = 0,
    limit: int = 10,
    fetcher: RankedFetcher = default_ranked_fetcher,
    score_field: str = "_federated_score",
    source_field: str = "_federated_source",
    context_field: str = "_federated_context",
) -> list[dict]:
    """Run an exact federated top-k semantic search across multiple contexts.

    The helper fans out to every context with ``offset + limit`` as the local
    fetch size, then globally merges by ascending score and applies the final
    window once. It intentionally handles semantic/ranked search only; plain
    filtered list pagination needs an explicit global ordering policy from the
    caller.
    """
    if offset < 0:
        raise ValueError("offset must be >= 0")
    if limit <= 0 or not contexts or not references:
        return []

    window = offset + limit
    batches = [(spec, *fetcher(spec, references, window)) for spec in contexts]
    return merge_ranked_batches(
        batches,
        offset=offset,
        limit=limit,
        score_field=score_field,
        source_field=source_field,
        context_field=context_field,
    )


__all__ = [
    "FederatedSearchContext",
    "RankedFetcher",
    "default_ranked_fetcher",
    "federated_ranked_search",
    "merge_ranked_batches",
]
