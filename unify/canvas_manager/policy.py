"""What a canvas is allowed to read.

A canvas is different from a chat answer in one way that matters here: it is a
rendered surface with a URL, and that URL can be shared. "The owner could read
this themselves" is therefore not sufficient justification for projecting
something into one — the question is whether it is safe to *display*, not
whether it is safe to *access*.

So display is opt-in per manager. The list below names managers whose data a
canvas may show; the tables themselves are read from each manager's own
``Config.required_contexts``, so a table name is declared once, by the manager
that owns it, and cannot drift out of step with this file.
"""

from __future__ import annotations

from typing import Optional

# Managers whose tables a canvas may bind to.
#
# Absent on purpose, and to stay absent:
#   secrets    — a shareable surface must never be able to project credentials
#   blacklist  — moderation state, not something to render
#   comms      — an action surface with no rows worth displaying
#   canvas     — a canvas rendering the canvas catalogue is a loop, not a view
#
# `knowledge` and `guidance` are absent for a different reason: they are exposed
# as top-level JSON tools rather than primitives, so they have no primitives
# spec to resolve a class from. Adding them means giving them a spec first.
CANVAS_READABLE_MANAGERS: frozenset[str] = frozenset(
    {
        "tasks",
        "contacts",
        "data",
        "transcripts",
        "files",
    },
)


def readable_tables(manager_alias: str) -> frozenset[str]:
    """Tables a canvas may bind to on one manager.

    Empty for any manager not opted in, so an unlisted manager is refused rather
    than silently resolved.
    """
    if manager_alias not in CANVAS_READABLE_MANAGERS:
        return frozenset()

    from unify.common.context_registry import ContextRegistry
    from unify.function_manager.primitives.registry import get_registry

    registry = get_registry()
    spec = registry.get_manager_spec(manager_alias)
    if spec is None:
        return frozenset()

    manager = registry._load_manager_class(spec.primitive_class_path)
    if manager is None:
        return frozenset()

    # `*/Meta` tables hold sync hashes and other bookkeeping a manager keeps for
    # itself. They contain nothing a viewer would want and are excluded so the
    # readable surface reflects actual content.
    return frozenset(
        table
        for table in ContextRegistry.declared_tables(manager)
        if not table.endswith("/Meta")
    )


def check_readable(manager_alias: str, table: str) -> Optional[str]:
    """Return a diagnostic if a canvas may not read this table, else ``None``.

    Returns rather than raises so the caller can decide whether a rejected
    binding is an error or a warning, and so the message can be surfaced to the
    author verbatim.
    """
    allowed = readable_tables(manager_alias)

    if not allowed:
        return (
            f"Manager {manager_alias!r} cannot be displayed on a canvas. A canvas has a "
            f"shareable URL, so what it may show is opted into per manager. "
            f"Readable managers: {', '.join(sorted(CANVAS_READABLE_MANAGERS))}."
        )

    # Data tables are created per user (``Data/Sales``), so a sub-table is
    # readable whenever its declared root is.
    if table in allowed or table.split("/", 1)[0] in allowed:
        return None

    return (
        f"Table {table!r} is not declared by manager {manager_alias!r}. "
        f"It declares: {', '.join(sorted(allowed))}."
    )
