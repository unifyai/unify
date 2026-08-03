"""Resolve and validate canvas data bindings.

A binding is written against a manager and a logical table. Before it is stored
it is resolved to a fully qualified context path and dry-run against the real
DataManager, so a bad filter or an unknown column fails while the assistant is
still authoring rather than in front of a viewer.

Resolution happens exactly once, at author time. The resolved path is persisted
and is what executes on every subsequent view, which means a stored canvas
cannot later resolve to a different context than the one its bindings were
validated against.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence

from unify.canvas_manager.types.binding import (
    MAX_BINDINGS_PER_CANVAS,
    PrimitiveBinding,
)


class BindingError(ValueError):
    """A binding could not be resolved or did not execute cleanly."""


def coerce_bindings(bindings: Sequence[object] | None) -> List[PrimitiveBinding]:
    """Accept dicts or models and return validated bindings."""
    if not bindings:
        return []

    coerced: List[PrimitiveBinding] = []
    for entry in bindings:
        if isinstance(entry, PrimitiveBinding):
            coerced.append(entry)
        elif isinstance(entry, dict):
            coerced.append(PrimitiveBinding.model_validate(entry))
        else:
            raise BindingError(
                f"Binding must be a PrimitiveBinding or dict, got {type(entry).__name__}.",
            )

    if len(coerced) > MAX_BINDINGS_PER_CANVAS:
        raise BindingError(
            f"A canvas may declare at most {MAX_BINDINGS_PER_CANVAS} bindings; got {len(coerced)}. "
            f"Consider a join instead of several separate reads.",
        )

    seen: set[str] = set()
    for binding in coerced:
        if binding.alias in seen:
            raise BindingError(
                f"Duplicate binding alias {binding.alias!r}; aliases must be unique.",
            )
        seen.add(binding.alias)

    return coerced


def check_bindable(bindings: Sequence[PrimitiveBinding]) -> None:
    """Reject any binding a canvas is not allowed to display.

    The policy lives in ``canvas_manager.policy`` because it is about what may
    be projected onto a shareable surface, not about what a manager owns; the
    table names themselves come from each manager's own context declaration.
    """
    from unify.canvas_manager.policy import check_readable

    for binding in bindings:
        problem = check_readable(binding.manager, binding.table)
        if problem:
            raise BindingError(f"Binding {binding.alias!r}: {problem}")


def resolve_binding_contexts(
    bindings: Sequence[PrimitiveBinding],
    *,
    root_context: str,
) -> List[PrimitiveBinding]:
    """Attach the fully qualified context path each binding will execute against."""
    resolved: List[PrimitiveBinding] = []
    base = root_context.strip("/")

    for binding in bindings:
        path = f"{base}/{binding.table.strip('/')}"
        updated = binding.model_copy(update={"resolved_context": path})

        args = binding.args
        if getattr(args, "operation", None) in {"join", "join_reduce"}:
            tables = [
                f"{base}/{table.strip('/')}" for table in getattr(args, "tables", [])
            ]
            updated = updated.model_copy(update={"resolved_tables": tables})

        resolved.append(updated)

    return resolved


def verify_bindings(
    bindings: Sequence[PrimitiveBinding],
    *,
    data_manager,
) -> Dict[str, List[Any]]:
    """Dry-run every binding, raising on the first that fails.

    Runs with a tiny row limit: the point is to prove the query is well formed
    against the real schema, not to fetch anything.

    Returns the sample rows per alias. They are worth keeping rather than
    discarding, because the author-time render gate replays them in place of the
    parent: rendering against real column names catches a canvas that reads a
    field its query never returns, which rendering against no data cannot.
    """
    samples: Dict[str, List[Any]] = {}

    for binding in bindings:
        args = binding.args
        operation = getattr(args, "operation", "")
        context = binding.resolved_context or binding.table

        try:
            if operation == "filter":
                rows = data_manager.filter(
                    context,
                    filter=args.filter,
                    columns=args.columns,
                    exclude_columns=args.exclude_columns,
                    order_by=args.order_by,
                    descending=args.descending,
                    limit=5,
                )
            elif operation == "reduce":
                rows = data_manager.reduce(
                    context,
                    metric=args.metric,
                    columns=args.columns,
                    filter=args.filter,
                    group_by=args.group_by,
                )
            elif operation == "join":
                rows = data_manager.filter_join(
                    tables=binding.resolved_tables or args.tables,
                    join_expr=args.join_expr,
                    select=args.select,
                    mode=args.mode,
                    left_where=args.left_where,
                    right_where=args.right_where,
                    result_where=args.result_where,
                    result_limit=5,
                )
            elif operation == "join_reduce":
                rows = data_manager.reduce_join(
                    tables=binding.resolved_tables or args.tables,
                    join_expr=args.join_expr,
                    select=args.select,
                    metric=args.metric,
                    columns=args.columns,
                    mode=args.mode,
                    left_where=args.left_where,
                    right_where=args.right_where,
                    result_where=args.result_where,
                    group_by=args.group_by,
                )
        except Exception as error:  # noqa: BLE001 - surfaced verbatim to the author
            raise BindingError(
                f"Binding {binding.alias!r} ({operation} on {context}) failed to execute: "
                f"{error}\n"
                f"If this table does not exist yet, materialise it before binding to it. "
                f"Data from connected apps in particular has to be fetched and stored "
                f"first -- call the integration tools, store the rows with "
                f"`primitives.ingestion.submit(RowsSource(rows=...), "
                f"TableTarget(context=...))` and wait for the run, schedule the refresh "
                f"with `primitives.tasks`, then bind the canvas to that table.",
            ) from error

        samples[binding.alias] = _as_rows(rows)

    return samples


def _as_rows(result: Any) -> List[Any]:
    """Normalise a dry-run result into the row list the frame is handed.

    Reductions come back as a scalar or a grouped mapping rather than rows, and
    the runtime gives every alias to the canvas as an array. Normalising here
    keeps that one shape, so an authored canvas never has to branch on which
    operation produced its alias.
    """
    if result is None:
        return []
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return [result]
    return [{"value": result}]


def serialize_bindings(bindings: Sequence[object]) -> str:
    """Serialise resolved bindings for storage on the canvas record."""
    return json.dumps(
        [binding.model_dump(exclude_none=True) for binding in bindings],
        separators=(",", ":"),
    )


def deserialize_bindings(payload: str | None) -> List[PrimitiveBinding]:
    """Read bindings back off a stored canvas record."""
    if not payload:
        return []
    return [PrimitiveBinding.model_validate(entry) for entry in json.loads(payload)]


def binding_contexts(bindings: Sequence[object]) -> str:
    """Comma-joined resolved contexts, for auditing what a canvas can read.

    Accepts both binding kinds. An integration binding resolves to the single
    canvas-owned context its sync fills, and has no join tables, so the lookups
    below are tolerant rather than assuming one shape.
    """
    seen: Dict[str, None] = {}
    for binding in bindings:
        paths = [
            getattr(binding, "resolved_context", None),
            *(getattr(binding, "resolved_tables", None) or []),
        ]
        for path in paths:
            if path:
                seen.setdefault(path, None)
    return ",".join(seen)
