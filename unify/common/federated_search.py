"""Reads that span several contexts as if they were one table.

The skill libraries keep a user's own rows in one context and the read-only
builtins catalogue in another; every list, count, reduction and search here
merges the two. Search is a plain text match: the libraries hold tens to
hundreds of entries, so every candidate row is fetched and ranked in Python
by how many query tokens it contains.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from functools import cmp_to_key
from typing import Any, Callable, Literal, Mapping, Optional, Sequence, Union

from unify import db
from unify.db import InvalidExpression, NotFound

from .metrics_utils import SUPPORTED_REDUCTION_METRICS, reduce_logs
from .tool_outcome import ToolErrorException

SOURCE_FIELD = "_federated_source"
CONTEXT_FIELD = "_federated_context"
SCORE_FIELD = "_federated_score"

_PAGE_SIZE = 1000

_TOKEN = re.compile(r"[a-z0-9]+")

# Metrics whose global value can be combined exactly from per-context
# server-side results, without fetching rows client-side.
_DECOMPOSABLE_METRICS = {"count", "sum", "min", "max", "mean"}


@dataclass(frozen=True)
class FederatedSearchContext:
    """One context participating in a federated read.

    ``project`` addresses contexts living outside the active project (for
    example the public-read builtins catalogue); ``None`` means the active
    project.
    """

    context: str
    source: str
    row_filter: Optional[str] = None
    allowed_fields: Optional[Sequence[str]] = None
    excluded_fields: Optional[Sequence[str]] = None
    project: Optional[str] = None

    def to_request_spec(self) -> dict:
        """Serialize into a ``POST /logs/federated`` context spec."""
        spec: dict = {"context": self.context, "source": self.source}
        if self.row_filter:
            spec["filter"] = self.row_filter
        if self.allowed_fields:
            spec["from_fields"] = list(self.allowed_fields)
        if self.excluded_fields:
            spec["exclude_fields"] = list(self.excluded_fields)
        if self.project:
            spec["project_name"] = self.project
        return spec


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


def is_missing_context_error(exc: Exception) -> bool:
    return isinstance(exc, NotFound)


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


def query_tokens(text: str) -> list[str]:
    """Distinct lower-case alphanumeric tokens of ``text``, in first-seen order."""
    return list(dict.fromkeys(_TOKEN.findall(str(text).lower())))


def text_match(
    row: Mapping[str, Any],
    references: Mapping[str, str],
) -> tuple[int, int]:
    """Count how many query tokens ``row`` contains.

    ``references`` maps a field name to the text to look for in it. A token
    hits a field when some word of the field's value starts with it, so
    ``slide`` finds ``slides`` and ``deploy`` finds ``deploying``; a token
    of one or two characters must match a whole word, so ``a`` does not
    hit ``and``. Returns ``(matched, hits)``: the number of distinct tokens
    found in at least one field, and the total number of (token, field)
    hits, which ranks a row whose name and docstring both carry a token
    above one where only the docstring does.
    """
    matched: set[str] = set()
    hits = 0
    for field, text in references.items():
        value = row.get(field)
        if value is None:
            continue
        words = set(_TOKEN.findall(str(value).lower()))
        for token in query_tokens(text):
            if len(token) < 3:
                found = token in words
            else:
                found = any(word.startswith(token) for word in words)
            if found:
                matched.add(token)
                hits += 1
    return len(matched), hits


def federated_text_search(
    contexts: Sequence[FederatedSearchContext],
    references: Optional[Mapping[str, str]],
    *,
    limit: int = 10,
    unique_id_field: Optional[str] = None,
    backfill: bool = False,
    annotate: bool = True,
) -> list[dict]:
    """Rank every row across ``contexts`` by how many query tokens it contains.

    All candidate rows are read in one federated round trip (each spec's
    ``row_filter`` and field projection apply) and scored with
    :func:`text_match`. Rows containing at least one token come first, by
    distinct tokens matched, then total hits, then source order. With
    ``backfill=True`` the window is topped up with the remaining rows,
    newest first by ``unique_id_field``, so a vague query still returns a
    sample of the library. When ``annotate`` is set each row carries
    ``_federated_score``: the fraction of distinct query tokens it matched.
    """
    if limit <= 0 or not contexts:
        return []
    references = {
        field: text for field, text in (references or {}).items() if str(text).strip()
    }
    if not references and not backfill:
        return []

    response = _server_federated_read(
        contexts,
        filter=None,
        sorting=(),
        offset=0,
        limit=None,
        unique_id_field=unique_id_field,
        annotate=annotate,
    )
    rows = response["logs"]
    total_tokens = len(
        {token for text in references.values() for token in query_tokens(text)},
    )

    scored: list[tuple[int, int, int, dict]] = []
    rest: list[tuple[Any, int, dict]] = []
    for order, row in enumerate(rows):
        matched, hits = text_match(row, references)
        if matched:
            scored.append((matched, hits, order, row))
        else:
            recency = _id_key(row.get(unique_id_field)) if unique_id_field else None
            rest.append((recency, order, row))
    scored.sort(key=lambda item: (-item[0], -item[1], item[2]))

    ranked: list[dict] = []
    for matched, _hits, _order, row in scored:
        if annotate:
            row[SCORE_FIELD] = matched / total_tokens
        ranked.append(row)
    if backfill and len(ranked) < limit:
        rest.sort(
            key=lambda item: (
                item[0] is None,
                -item[0] if isinstance(item[0], int) else 0,
                item[1],
            ),
        )
        for _recency, _order, row in rest:
            if annotate:
                row[SCORE_FIELD] = 0.0
            ranked.append(row)
    return ranked[:limit]


def _sorting_payload(sorting: Sequence[SortSpec]) -> list[dict]:
    return [
        {
            "field": spec.field,
            "direction": spec.direction,
            "missing": spec.missing,
        }
        for spec in sorting
        if not spec.field.startswith("_federated_")
    ]


FILTER_GRAMMAR_HINT = (
    "comparisons (==, !=, <, <=, >, >=), membership tests (in / not in), and "
    "boolean combinators (and, or, not) over field names and literal values, "
    "plus a fixed set of helpers (len(), string methods like .lower() / "
    ".startswith()). Arbitrary Python calls outside that set, e.g. "
    "' '.join(x) or a list comprehension, are rejected."
)


def _invalid_filter_error(
    exc: Exception,
    filter: Optional[str],
) -> ToolErrorException:
    """Translate a rejected filter expression into an actionable payload."""
    return ToolErrorException(
        {
            "error_kind": "invalid_filter",
            "message": (
                f"filter {filter!r} was rejected: {exc} "
                f"Supported filter grammar: {FILTER_GRAMMAR_HINT}"
            ),
            "details": {"filter": filter},
        },
    )


def _server_federated_read(
    contexts: Sequence[FederatedSearchContext],
    *,
    filter: Optional[str],
    sorting: Sequence[SortSpec],
    offset: int,
    limit: int,
    unique_id_field: Optional[str],
    annotate: bool,
) -> dict:
    """Run one federated read on the backend and return its raw response.

    The server executes every context through the ordinary single-context
    read pipeline (per-context field types, partition pruning) and performs
    the exact global merge, so a single round trip replaces the previous
    per-context paged fan-out. Missing contexts contribute nothing.

    A 4xx here means the caller-supplied ``filter`` itself was rejected (bad
    grammar) — raised as a :class:`ToolErrorException` carrying an actionable
    message instead of the raw HTTP error, so it survives whatever generic
    exception handling sits above the calling tool. 5xx/transport errors are
    left to propagate unchanged.
    """
    try:
        return db.get_logs_federated(
            contexts=[spec.to_request_spec() for spec in contexts],
            filter=filter,
            sorting=_sorting_payload(sorting),
            offset=offset,
            limit=limit,
            unique_id_field=unique_id_field,
            annotate=annotate,
        )
    except InvalidExpression as exc:
        raise _invalid_filter_error(exc, filter) from exc


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
    fetcher: Optional[FilterFetcher] = None,
    unique_id_field: Optional[str] = None,
    annotate: bool = True,
) -> list[dict]:
    """Run an exact federated filtered read across multiple contexts.

    The result is one globally ordered window, exactly as though all rows
    lived in a single context. Pass ``sorting`` for single-table-style global
    ordering; when omitted, rows preserve source order and each context's
    local fetch order.

    Without a ``fetcher`` the read is delegated wholly to the backend's
    federated endpoint in one round trip. A ``fetcher`` forces the
    client-side path — for rows that do not come from the logs API (local
    stores, impl-specific reads) — fanning out with ``offset + limit`` as
    the local fetch size and merging here.
    """
    if offset < 0:
        raise ValueError("offset must be >= 0")
    if limit <= 0 or not contexts:
        return []

    effective_sorting = tuple(sorting or ())
    if fetcher is None:
        response = _server_federated_read(
            contexts,
            filter=filter,
            sorting=effective_sorting,
            offset=offset,
            limit=limit,
            unique_id_field=unique_id_field,
            annotate=annotate,
        )
        return response["logs"]

    window = offset + limit
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
        db.get_context(spec.context, project=spec.project)
    except Exception as exc:
        if is_missing_context_error(exc):
            return _empty()
        raise

    try:
        return reduce_logs(
            context=spec.context,
            project=spec.project,
            metric=metric,
            keys=list(keys) if isinstance(keys, (list, tuple)) else keys,
            filter=_combine_filters(filter, spec.row_filter),
            group_by=(
                list(group_by) if isinstance(group_by, (list, tuple)) else group_by
            ),
        )
    except Exception as exc:
        if not is_missing_context_error(exc):
            raise
        return _empty()


def _fetch_merged_rows(
    contexts: Sequence[FederatedSearchContext],
    filter: Optional[str],
) -> list[dict]:
    """Fetch every matching merged row for client-side reductions."""
    rows: list[dict] = []
    offset = 0
    while True:
        response = _server_federated_read(
            contexts,
            filter=filter,
            sorting=(),
            offset=offset,
            limit=_PAGE_SIZE,
            unique_id_field=None,
            annotate=False,
        )
        page = response["logs"]
        rows.extend(page)
        if len(page) < _PAGE_SIZE:
            break
        offset += _PAGE_SIZE
    return rows


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
) -> Any:
    """Compute one reduction metric across multiple contexts.

    A single context delegates wholly to the server. With multiple contexts,
    decomposable ungrouped metrics (count, sum, min, max, mean) are pushed
    down per context and combined exactly; grouped or non-decomposable
    metrics (median, mode, var, std) fetch the merged rows through the
    federated endpoint and reduce client-side, matching single-context
    semantics.
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
        rows = _fetch_merged_rows(contexts, filter)
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
) -> int:
    """Count rows (by non-null ``key``) summed across every context.

    Delegates to the backend's federated endpoint as a count-only read
    (``limit=0``), which computes every per-context total in one round trip.
    """
    if not contexts:
        return 0
    response = _server_federated_read(
        contexts,
        filter=_combine_filters(filter, f"exists({key})"),
        sorting=(),
        offset=0,
        limit=0,
        unique_id_field=None,
        annotate=False,
    )
    return int(response["count"])


__all__ = [
    "CONTEXT_FIELD",
    "FederatedSearchContext",
    "FilterFetcher",
    "MetricFetcher",
    "SCORE_FIELD",
    "SOURCE_FIELD",
    "SortSpec",
    "default_metric_fetcher",
    "federated_count",
    "federated_filter",
    "federated_text_search",
    "federated_reduce",
    "is_missing_context_error",
    "merge_sorted_batches",
    "reduce_grouped_rows",
    "reduce_rows",
    "query_tokens",
    "text_match",
]
