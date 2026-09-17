from __future__ import annotations

from typing import Any, List, Union, Optional

from unify import db
from .filter_utils import normalize_filter_expr

SUPPORTED_REDUCTION_METRICS: set[str] = {
    "count",
    "sum",
    "mean",
    "var",
    "std",
    "min",
    "max",
    "median",
    "mode",
}


def _normalize_filter(flt: Optional[str]) -> Optional[str]:
    """Normalise a filter expression with the shared helper so reduction
    tools accept the same syntax as every other filter-capable tool."""
    if flt is None:
        return None
    return normalize_filter_expr(flt)


def reduce_logs(
    *,
    context: str,
    metric: str,
    keys: Union[str, List[str]],
    filter: Optional[str] = None,
    group_by: Optional[Union[str, List[str]]] = None,
    project: Optional[str] = None,
) -> Any:
    """
    Compute one or more reduction metrics over a Unify context.

    This is a thin convenience wrapper around :func:`db.get_logs_metric`
    that enforces a common contract for manager-level ``reduce`` tools.

    Parameters
    ----------
    context : str
        Fully-qualified Unify context to aggregate over.
    metric : str
        Reduction metric to compute. Supported values (case-insensitive) are:
        ``\"sum\"``, ``\"mean\"``, ``\"var\"``, ``\"std\"``, ``\"min\"``,
        ``\"max\"``, ``\"median\"``, ``\"mode\"``, and ``\"count\"``.
    keys : str | list[str]
        Field name(s) to compute the metric for. A single column name (string)
        produces a scalar result (when ``group_by`` is not used); a list of
        column names computes the same metric independently for each key and
        returns a ``{key -> value}`` mapping.
    filter : str | None, default None
        Optional filter expression restricting the rows aggregated, normalised
        via :func:`normalize_filter_expr` for consistency with other tools.
    group_by : str | list[str] | None, default None
        Optional field(s) to group by. Use a single column name for a single
        grouping level, or a list like ``[\"status\", \"priority\"]`` to group
        hierarchically in the given order. When provided, the return value
        follows the grouped forms described in the Unify ``get_logs_metric``
        docs (nested ``dict`` structures keyed by group values).

    Returns
    -------
    Any
        The metric value(s) produced by :func:`db.get_logs_metric`:

        * Single key, no grouping  → scalar (float/int/str/bool).
        * Multiple keys, no grouping → ``dict[key -> scalar]``.
        * With grouping             → nested ``dict`` keyed by group values.

    Raises
    ------
    ValueError
        If ``metric`` is not one of the supported reduction metrics.
    """
    metric_norm = metric.strip().lower()
    if metric_norm not in SUPPORTED_REDUCTION_METRICS:
        raise ValueError(
            f"Unsupported reduction metric {metric!r}. "
            f"Supported metrics are: {sorted(SUPPORTED_REDUCTION_METRICS)}",
        )

    normalized_filter = _normalize_filter(filter)

    return db.get_logs_metric(
        metric=metric_norm,
        key=keys,
        filter=normalized_filter,
        context=context,
        project=project,
        group_by=group_by,
    )
