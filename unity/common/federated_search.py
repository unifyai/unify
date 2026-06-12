from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from functools import cmp_to_key
from typing import Any, Callable, Literal, Mapping, Optional, Sequence, Union

import unify
from unify.utils.http import RequestError as _UnifyRequestError

from .metrics_utils import SUPPORTED_REDUCTION_METRICS, reduce_logs
from .semantic_search import (
    backfill_rows,
    ensure_vector_for_source,
    fetch_top_k_by_terms_with_score,
)

SOURCE_FIELD = "_federated_source"
CONTEXT_FIELD = "_federated_context"
SCORE_FIELD = "_federated_score"

_PAGE_SIZE = 1000

# Metrics whose global value can be combined exactly from per-context
# server-side results, without fetching rows client-side.
_DECOMPOSABLE_METRICS = {"count", "sum", "min", "max", "mean"}


@dataclass(frozen=True)
class FederatedSearchContext:
    """One context participating in a federated read."""

    context: str
    source: str
    row_filter: Optional[str] = None
    allowed_fields: Optional[Sequence[str]] = None


@dataclass(frozen=True)
class SortSpec:
    """One global sort key for a federated filtered read.

    ``missing`` controls where rows lacking the field (or with a ``None``
    value) are placed. The backend always sorts NULLs last, so
    ``missing="last"`` permits exact server-side windowing while
    ``missing="first"`` forces a full per-context fetch.
    """

    field: str
    direction: Literal["ascending", "descending"] = "ascending"
    missing: Literal["first", "last"] = "last"


RankedFetcher = Callable[
    [FederatedSearchContext, Mapping[str, str], int],
    tuple[list[dict], str],
]
FilterFetcher = Callable[
    [FederatedSearchContext, Optional[str], Sequence[SortSpec], int],
    list[dict],
]
MetricFetcher = Callable[
    [
        FederatedSearchContext,
        str,
        Union[str, Sequence[str]],
        Optional[str],
        Optional[Union[str, Sequence[str]]],
    ],
    Any,
]
RowFetcher = Callable[[FederatedSearchContext, Optional[str]], list[dict]]


def _is_missing_context_error(exc: Exception) -> bool:
    if not isinstance(exc, _UnifyRequestError):
        return False
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status == 404


def _combine_filters(left: Optional[str], right: Optional[str]) -> Optional[str]:
    parts = [part for part in (left, right) if part]
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return " and ".join(f"({part})" for part in parts)


def _id_key(value: Any) -> Any:
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


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


def _backend_sorting(sorting: Sequence[SortSpec]) -> dict[str, str] | None:
    backend = {
        spec.field: spec.direction
        for spec in sorting
        if not spec.field.startswith("_federated_")
    }
    return backend or None


def default_filter_fetcher(
    spec: FederatedSearchContext,
    filter: Optional[str],
    sorting: Sequence[SortSpec],
    limit: Optional[int],
) -> list[dict]:
    """Fetch filtered rows from one context for a federated filtered read.

    Missing contexts (404) yield an empty batch so fan-out reads tolerate
    roots where the table has not been provisioned yet.

    The backend always sorts NULLs last. When any pushed-down sort key asks
    for ``missing="first"``, the per-context window under backend ordering is
    no longer a superset of the global window, so the fetch ignores ``limit``
    and pages through every matching row, leaving the exact ordering to the
    client-side merge.
    """
    backend = _backend_sorting(sorting)
    exact_window = all(
        spec_item.missing == "last"
        for spec_item in sorting
        if not spec_item.field.startswith("_federated_")
    )
    combined_filter = _combine_filters(filter, spec.row_filter)

    rows: list[dict] = []
    offset = 0
    while True:
        if limit is not None and exact_window:
            page_limit = min(_PAGE_SIZE, limit - len(rows))
            if page_limit <= 0:
                break
        else:
            page_limit = _PAGE_SIZE
        kwargs: dict[str, Any] = {
            "context": spec.context,
            "filter": combined_filter,
            "sorting": backend,
            "limit": page_limit,
            "offset": offset,
        }
        if spec.allowed_fields:
            kwargs["from_fields"] = list(spec.allowed_fields)
        try:
            page = [row.entries for row in unify.get_logs(**kwargs)]
        except Exception as exc:
            if _is_missing_context_error(exc):
                break
            raise
        rows.extend(page)
        if len(page) < page_limit:
            break
        offset += page_limit
    return rows


def _compare_present_values(left: object, right: object) -> int:
    try:
        if left < right:  # type: ignore[operator]
            return -1
        if left > right:  # type: ignore[operator]
            return 1
        return 0
    except TypeError:
        left_repr = repr(left)
        right_repr = repr(right)
        if left_repr < right_repr:
            return -1
        if left_repr > right_repr:
            return 1
        return 0


def _compare_by_sorting(left: dict, right: dict, sorting: Sequence[SortSpec]) -> int:
    for spec in sorting:
        left_missing = spec.field not in left or left.get(spec.field) is None
        right_missing = spec.field not in right or right.get(spec.field) is None
        if left_missing or right_missing:
            if left_missing and right_missing:
                continue
            left_first = spec.missing == "first"
            return -1 if (left_missing == left_first) else 1

        cmp = _compare_present_values(left.get(spec.field), right.get(spec.field))
        if cmp != 0:
            return -cmp if spec.direction == "descending" else cmp
    return 0


def _annotate(row: dict, spec: FederatedSearchContext) -> dict:
    annotated = dict(row)
    annotated[SOURCE_FIELD] = spec.source
    annotated[CONTEXT_FIELD] = spec.context
    return annotated


def _dedup_rows(rows: list[dict], unique_id_field: Optional[str]) -> list[dict]:
    if not unique_id_field:
        return rows
    seen: set = set()
    deduped: list[dict] = []
    for row in rows:
        value = row.get(unique_id_field)
        if value is None:
            deduped.append(row)
            continue
        key = _id_key(value)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def merge_ranked_batches(
    batches: Sequence[tuple[FederatedSearchContext, list[dict], str]],
    *,
    offset: int = 0,
    limit: int = 10,
    unique_id_field: Optional[str] = None,
    annotate: bool = True,
) -> list[dict]:
    """Merge per-context ranked result batches into one globally ranked window.

    Each batch is already sorted by ascending distance within its own context.
    The merge is exact when every context was fetched with at least
    ``offset + limit`` rows: a row outside that local window cannot appear in
    the global window because its own context already has ``offset + limit``
    better rows ahead of it. Cross-context deduplication (``unique_id_field``)
    keeps the best-scoring instance; exactness then assumes the same logical
    row is absent from (or equally ranked in) other contexts.
    """
    if offset < 0:
        raise ValueError("offset must be >= 0")
    if limit <= 0:
        return []

    annotated: list[tuple[float, int, int, dict]] = []
    for source_order, (spec, batch_rows, raw_score_field) in enumerate(batches):
        for local_order, row in enumerate(batch_rows):
            merged = _annotate(row, spec) if annotate else dict(row)
            try:
                score = float(merged.get(raw_score_field, float("inf")))
            except (TypeError, ValueError):
                score = float("inf")

            if raw_score_field and raw_score_field != SCORE_FIELD:
                merged.pop(raw_score_field, None)
            if annotate:
                merged[SCORE_FIELD] = score
            annotated.append((score, source_order, local_order, merged))

    annotated.sort(key=lambda item: (item[0], item[1], item[2]))
    rows = [row for *_unused, row in annotated]
    rows = _dedup_rows(rows, unique_id_field)
    return rows[offset : offset + limit]


def merge_sorted_batches(
    batches: Sequence[tuple[FederatedSearchContext, list[dict]]],
    *,
    sorting: Optional[Sequence[SortSpec]] = None,
    offset: int = 0,
    limit: int = 100,
    unique_id_field: Optional[str] = None,
    annotate: bool = True,
) -> list[dict]:
    """Merge per-context filtered batches into one globally ordered window.

    Without ``sorting``, rows preserve source order then each context's local
    fetch order. With ``sorting``, rows are globally re-ordered; the sort is
    stable, so equal rows keep source-then-local order as the tie-breaker.
    """
    if offset < 0:
        raise ValueError("offset must be >= 0")
    if limit <= 0:
        return []

    rows: list[dict] = []
    for spec, batch_rows in batches:
        for row in batch_rows:
            rows.append(_annotate(row, spec) if annotate else dict(row))

    if sorting:
        effective_sorting = tuple(sorting)
        rows.sort(
            key=cmp_to_key(
                lambda left, right: _compare_by_sorting(
                    left,
                    right,
                    effective_sorting,
                ),
            ),
        )
    rows = _dedup_rows(rows, unique_id_field)
    return rows[offset : offset + limit]


def federated_filter(
    contexts: Sequence[FederatedSearchContext],
    *,
    filter: Optional[str] = None,
    sorting: Optional[Sequence[SortSpec]] = None,
    offset: int = 0,
    limit: int = 100,
    fetcher: FilterFetcher = default_filter_fetcher,
    unique_id_field: Optional[str] = None,
    annotate: bool = True,
) -> list[dict]:
    """Run an exact federated filtered read across multiple contexts.

    The helper fans out to every context with ``offset + limit`` as the local
    fetch size, then merges into one globally ordered window and applies the
    final slice once — as though all rows lived in a single context. Pass
    ``sorting`` for single-table-style global ordering; when omitted, rows
    preserve source order and each context's local fetch order, which is
    likewise exact under windowed fetching.
    """
    if offset < 0:
        raise ValueError("offset must be >= 0")
    if limit <= 0 or not contexts:
        return []

    window = offset + limit
    effective_sorting = tuple(sorting or ())
    batches = [
        (spec, fetcher(spec, filter, effective_sorting, window)) for spec in contexts
    ]
    return merge_sorted_batches(
        batches,
        sorting=effective_sorting or None,
        offset=offset,
        limit=limit,
        unique_id_field=unique_id_field,
        annotate=annotate,
    )


def federated_ranked_search(
    contexts: Sequence[FederatedSearchContext],
    references: Optional[Mapping[str, str]],
    *,
    offset: int = 0,
    limit: int = 10,
    fetcher: RankedFetcher = default_ranked_fetcher,
    unique_id_field: Optional[str] = None,
    backfill: bool = False,
    annotate: bool = True,
) -> list[dict]:
    """Run an exact federated top-k semantic search across multiple contexts.

    The helper fans out to every context with ``offset + limit`` as the local
    fetch size, then globally merges by ascending score and applies the final
    window once. With ``backfill=True``, results short of the window are
    topped up with deterministic recent rows (``unique_id_field`` descending)
    drawn from each context in order, mirroring single-context backfill.
    """
    if offset < 0:
        raise ValueError("offset must be >= 0")
    if limit <= 0 or not contexts:
        return []

    window = offset + limit
    if references:
        batches = [(spec, *fetcher(spec, references, window)) for spec in contexts]
        rows = merge_ranked_batches(
            batches,
            offset=0,
            limit=window,
            unique_id_field=unique_id_field,
            annotate=annotate,
        )
    else:
        if not backfill:
            return []
        rows = []

    if backfill and len(rows) < window:
        for spec in contexts:
            before = len(rows)
            filled = backfill_rows(
                spec.context,
                rows,
                window,
                row_filter=spec.row_filter,
                unique_id_field=unique_id_field,
                allowed_fields=(
                    list(spec.allowed_fields) if spec.allowed_fields else None
                ),
            )
            if annotate:
                rows = filled[:before] + [
                    _annotate(row, spec) for row in filled[before:]
                ]
            else:
                rows = filled
            if len(rows) >= window:
                break

    return rows[offset : offset + limit]


def default_metric_fetcher(
    spec: FederatedSearchContext,
    metric: str,
    keys: Union[str, Sequence[str]],
    filter: Optional[str],
    group_by: Optional[Union[str, Sequence[str]]],
) -> Any:
    """Compute one server-side metric for a single context.

    Missing contexts contribute nothing: ``0`` for counts, ``None`` otherwise
    (``{}`` when grouped). The existence probe is required because the metric
    endpoint silently drops an unknown context from its scope instead of
    returning 404, which would otherwise aggregate over the whole project.
    """

    def _empty() -> Any:
        if group_by is not None:
            return {}
        empty = 0 if metric == "count" else None
        if isinstance(keys, (list, tuple)):
            return {key: empty for key in keys}
        return empty

    try:
        unify.get_context(spec.context)
    except Exception as exc:
        if _is_missing_context_error(exc):
            return _empty()
        raise

    try:
        return reduce_logs(
            context=spec.context,
            metric=metric,
            keys=list(keys) if isinstance(keys, (list, tuple)) else keys,
            filter=_combine_filters(filter, spec.row_filter),
            group_by=(
                list(group_by) if isinstance(group_by, (list, tuple)) else group_by
            ),
        )
    except Exception as exc:
        if not _is_missing_context_error(exc):
            raise
        return _empty()


def default_row_fetcher(
    spec: FederatedSearchContext,
    filter: Optional[str],
) -> list[dict]:
    """Fetch every matching row from one context for client-side reductions."""
    return default_filter_fetcher(spec, filter, (), None)


def reduce_rows(
    rows: list[dict],
    *,
    metric: str,
    columns: Union[str, list[str]],
) -> Any:
    """Compute ungrouped reductions over already-fetched merged rows."""
    metric_norm = metric.strip().lower()
    column_names = columns if isinstance(columns, list) else [columns]

    def values_for(column_name: str) -> list[Any]:
        return [
            row[column_name]
            for row in rows
            if column_name in row and row[column_name] is not None
        ]

    def reduce_one(column_name: str) -> Any:
        values = values_for(column_name)
        # COUNT(column) on the backend counts non-null values; mirror that
        # here so client-side fallbacks agree with server-side reductions.
        if metric_norm == "count":
            return len(values)
        if not values:
            return None
        if metric_norm == "sum":
            return sum(value or 0 for value in values)
        if metric_norm == "min":
            return min(values)
        if metric_norm == "max":
            return max(values)
        if metric_norm == "mean":
            return sum(values) / len(values)
        if metric_norm == "median":
            ordered = sorted(values)
            mid = len(ordered) // 2
            if len(ordered) % 2:
                return ordered[mid]
            return (ordered[mid - 1] + ordered[mid]) / 2
        if metric_norm == "mode":
            return Counter(values).most_common(1)[0][0]
        if metric_norm in {"var", "std"}:
            mean = sum(values) / len(values)
            variance = sum((value - mean) ** 2 for value in values) / len(values)
            return variance if metric_norm == "var" else variance**0.5
        raise ValueError(f"Unsupported reduction metric {metric!r}.")

    if isinstance(columns, list):
        return {column_name: reduce_one(column_name) for column_name in column_names}
    return reduce_one(column_names[0])


def reduce_grouped_rows(
    rows: list[dict],
    *,
    metric: str,
    columns: Union[str, list[str]],
    group_by: Union[str, list[str]],
) -> dict[Any, Any]:
    """Compute grouped reductions over merged rows."""
    group_columns = group_by if isinstance(group_by, list) else [group_by]

    def reduce_group(group_rows: list[dict], depth: int) -> Any:
        if depth >= len(group_columns):
            return reduce_rows(group_rows, metric=metric, columns=columns)
        grouped: dict[Any, list[dict]] = defaultdict(list)
        group_column = group_columns[depth]
        for row in group_rows:
            grouped[row.get(group_column)].append(row)
        return {
            group_value: reduce_group(child_rows, depth + 1)
            for group_value, child_rows in grouped.items()
        }

    return reduce_group(rows, 0)


def _combine_metric(metric: str, values: list[Any]) -> Any:
    present = [value for value in values if value is not None]
    if metric == "count":
        return sum(int(value) for value in present)
    if not present:
        return None
    if metric == "sum":
        return sum(present)
    if metric == "min":
        return min(present)
    if metric == "max":
        return max(present)
    raise ValueError(f"Metric {metric!r} is not decomposable.")


def federated_reduce(
    contexts: Sequence[FederatedSearchContext],
    *,
    metric: str,
    columns: Union[str, list[str]],
    filter: Optional[str] = None,
    group_by: Optional[Union[str, list[str]]] = None,
    metric_fetcher: MetricFetcher = default_metric_fetcher,
    row_fetcher: RowFetcher = default_row_fetcher,
) -> Any:
    """Compute one reduction metric across multiple contexts.

    A single context delegates wholly to the server. With multiple contexts,
    decomposable ungrouped metrics (count, sum, min, max, mean) are pushed
    down per context and combined exactly; grouped or non-decomposable
    metrics (median, mode, var, std) fetch the merged rows and reduce
    client-side, matching single-context semantics.
    """
    metric_norm = metric.strip().lower()
    if metric_norm not in SUPPORTED_REDUCTION_METRICS:
        raise ValueError(
            f"Unsupported reduction metric {metric!r}. "
            f"Supported metrics are: {sorted(SUPPORTED_REDUCTION_METRICS)}",
        )
    if not contexts:
        raise ValueError("federated_reduce requires at least one context")

    if len(contexts) == 1:
        return metric_fetcher(contexts[0], metric_norm, columns, filter, group_by)

    if group_by is not None or metric_norm not in _DECOMPOSABLE_METRICS:
        rows: list[dict] = []
        for spec in contexts:
            rows.extend(row_fetcher(spec, filter))
        if group_by is not None:
            return reduce_grouped_rows(
                rows,
                metric=metric_norm,
                columns=columns,
                group_by=group_by,
            )
        return reduce_rows(rows, metric=metric_norm, columns=columns)

    column_names = columns if isinstance(columns, list) else [columns]

    def combine_one(column_name: str) -> Any:
        if metric_norm == "mean":
            sums = [
                metric_fetcher(spec, "sum", column_name, filter, None)
                for spec in contexts
            ]
            counts = [
                metric_fetcher(spec, "count", column_name, filter, None)
                for spec in contexts
            ]
            total_count = _combine_metric("count", counts)
            if not total_count:
                return None
            total_sum = _combine_metric("sum", sums)
            if total_sum is None:
                return None
            return total_sum / total_count
        values = [
            metric_fetcher(spec, metric_norm, column_name, filter, None)
            for spec in contexts
        ]
        return _combine_metric(metric_norm, values)

    if isinstance(columns, list):
        return {column_name: combine_one(column_name) for column_name in column_names}
    return combine_one(column_names[0])


def federated_count(
    contexts: Sequence[FederatedSearchContext],
    *,
    key: str,
    filter: Optional[str] = None,
    metric_fetcher: MetricFetcher = default_metric_fetcher,
) -> int:
    """Count rows (by non-null ``key``) summed across every context."""
    if not contexts:
        return 0
    values = [metric_fetcher(spec, "count", key, filter, None) for spec in contexts]
    return int(_combine_metric("count", values))


__all__ = [
    "CONTEXT_FIELD",
    "FederatedSearchContext",
    "FilterFetcher",
    "MetricFetcher",
    "RankedFetcher",
    "RowFetcher",
    "SCORE_FIELD",
    "SOURCE_FIELD",
    "SortSpec",
    "default_filter_fetcher",
    "default_metric_fetcher",
    "default_ranked_fetcher",
    "default_row_fetcher",
    "federated_count",
    "federated_filter",
    "federated_ranked_search",
    "federated_reduce",
    "merge_ranked_batches",
    "merge_sorted_batches",
    "reduce_grouped_rows",
    "reduce_rows",
]
