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
from typing import Dict, List, Sequence

from unify.canvas_manager.types.binding import (
    CANVAS_MAX_BINDINGS,
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

    if len(coerced) > CANVAS_MAX_BINDINGS:
        raise BindingError(
            f"A canvas may declare at most {CANVAS_MAX_BINDINGS} bindings; got {len(coerced)}. "
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
    """Reject any binding naming a manager or table not declared bindable.

    The allowlist lives on each manager's registry spec, so a manager opts its
    tables in rather than everything being reachable by default.
    """
    from unify.function_manager.primitives.registry import get_registry

    registry = get_registry()
    for binding in bindings:
        allowed = registry.bindable_tables(binding.manager)
        if not allowed:
            raise BindingError(
                f"Manager {binding.manager!r} exposes no bindable tables, so a canvas "
                f"cannot read from it.",
            )
        if binding.table not in allowed:
            # Dynamic sub-tables (``Data/Sales``) are allowed when their root is.
            root = binding.table.split("/", 1)[0]
            if root not in allowed:
                raise BindingError(
                    f"Table {binding.table!r} is not bindable on manager {binding.manager!r}. "
                    f"Available: {', '.join(sorted(allowed))}.",
                )


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


def verify_bindings(bindings: Sequence[PrimitiveBinding], *, data_manager) -> None:
    """Dry-run every binding, raising on the first that fails.

    Runs with a tiny row limit: the point is to prove the query is well formed
    against the real schema, not to fetch anything.
    """
    for binding in bindings:
        args = binding.args
        operation = getattr(args, "operation", "")
        context = binding.resolved_context or binding.table

        try:
            if operation == "filter":
                data_manager.filter(
                    context,
                    filter=args.filter,
                    columns=args.columns,
                    exclude_columns=args.exclude_columns,
                    order_by=args.order_by,
                    descending=args.descending,
                    limit=5,
                )
            elif operation == "reduce":
                data_manager.reduce(
                    context,
                    metric=args.metric,
                    columns=args.columns,
                    filter=args.filter,
                    group_by=args.group_by,
                )
            elif operation == "join":
                data_manager.filter_join(
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
                data_manager.reduce_join(
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
                f"Binding {binding.alias!r} ({operation} on {context}) failed to execute: {error}",
            ) from error


def serialize_bindings(bindings: Sequence[PrimitiveBinding]) -> str:
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


def binding_contexts(bindings: Sequence[PrimitiveBinding]) -> str:
    """Comma-joined resolved contexts, for auditing what a canvas can read."""
    seen: Dict[str, None] = {}
    for binding in bindings:
        for path in [binding.resolved_context, *(binding.resolved_tables or [])]:
            if path:
                seen.setdefault(path, None)
    return ",".join(seen)
