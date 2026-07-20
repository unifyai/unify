"""Load EventBus trees for a durable ``Tasks/Runs`` row.

``Tasks/Runs`` does not embed event payloads. Join is by ``run_key`` value:

- Runs may live at ``Teams/{id}/Tasks/Runs`` (or ``{user}/{assistant}/Tasks/Runs``)
- Events live under the **executing assistant's** ``{…}/Events/{Type}`` contexts

Filter ``Events/ManagerMethod`` and ``Events/ToolLoop`` with
``run_key == "<key>"`` (server-side via DataManager). Reconstruct nesting from
``hierarchy`` / ``hierarchy_label`` — there is no ``parent_event_id``.

Not yet exposed on ``primitives.tasks.*``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from unify.data_manager.data_manager import DataManager

__all__ = [
    "TaskRunEventTree",
    "build_run_key_filter",
    "fetch_task_run_events",
    "normalize_events_base_context",
]

_TREE_EVENT_TYPES: tuple[str, ...] = ("ManagerMethod", "ToolLoop")


def normalize_events_base_context(events_base_context: str) -> str:
    """Return an Events root path without a trailing slash."""

    base = (events_base_context or "").strip().rstrip("/")
    if not base:
        raise ValueError("events_base_context must be a non-empty Events root path")
    if base == "Events" or base.endswith("/Events"):
        return base
    # Caller passed a typed context (e.g. ``…/Events/ManagerMethod``).
    marker = "/Events/"
    if marker in base:
        return base.split(marker, 1)[0] + "/Events"
    # Assistant write root that owns Events as a child.
    return f"{base}/Events"


def build_run_key_filter(run_key: str) -> str:
    """Build a server-side Orchestra filter for ``run_key`` equality."""

    key = str(run_key or "").strip()
    if not key:
        raise ValueError("run_key must be a non-empty string")
    escaped = key.replace("\\", "\\\\").replace('"', '\\"')
    return f'run_key == "{escaped}"'


@dataclass(frozen=True)
class TaskRunEventTree:
    """ManagerMethod + ToolLoop rows attributed to one ``Tasks/Runs.run_key``."""

    run_key: str
    events_base_context: str
    manager_methods: list[dict[str, Any]] = field(default_factory=list)
    tool_loops: list[dict[str, Any]] = field(default_factory=list)

    @property
    def rows(self) -> list[dict[str, Any]]:
        """All fetched rows (ManagerMethod then ToolLoop), unsorted."""

        return [*self.manager_methods, *self.tool_loops]

    def hierarchy_roots(self) -> list[str]:
        """Unique first hierarchy segments across fetched rows."""

        roots: list[str] = []
        seen: set[str] = set()
        for row in self.rows:
            hierarchy = row.get("hierarchy")
            if isinstance(hierarchy, list) and hierarchy:
                root = str(hierarchy[0])
            else:
                label = row.get("hierarchy_label")
                root = str(label).split("->", 1)[0] if label else ""
            if root and root not in seen:
                seen.add(root)
                roots.append(root)
        return roots


def _paginate_filter(
    dm: DataManager,
    context: str,
    *,
    filter_expr: str,
    limit: int,
    order_by: str | None,
    descending: bool,
) -> list[dict[str, Any]]:
    """Paginate DataManager.filter past the 1000-row cap when needed."""

    page_size = min(max(int(limit), 1), 1000)
    rows: list[dict[str, Any]] = []
    offset = 0
    while len(rows) < limit:
        batch = dm.filter(
            context,
            filter=filter_expr,
            limit=min(page_size, limit - len(rows)),
            offset=offset,
            order_by=order_by,
            descending=descending,
        )
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += len(batch)
    return rows


def fetch_task_run_events(
    run_key: str,
    *,
    events_base_context: str,
    data_manager: DataManager | None = None,
    limit_per_type: int = 1000,
    order_by: str = "event_timestamp",
    descending: bool = False,
    event_types: Iterable[str] | None = None,
) -> TaskRunEventTree:
    """Fetch the ManagerMethod + ToolLoop tree for one ``Tasks/Runs.run_key``.

    Parameters
    ----------
    run_key:
        Unique key on the ``Tasks/Runs`` row (and stamped on Event payloads).
    events_base_context:
        Assistant Events root (``{user}/{assistant}/Events``) or write-context
        parent that owns ``Events``. Cross-context vs ``Teams/…/Tasks/Runs`` is
        expected; join is by ``run_key`` value only.
    """

    events_root = normalize_events_base_context(events_base_context)
    filter_expr = build_run_key_filter(run_key)
    dm = data_manager if data_manager is not None else DataManager()
    types = tuple(event_types) if event_types is not None else _TREE_EVENT_TYPES

    by_type: dict[str, list[dict[str, Any]]] = {t: [] for t in types}
    for event_type in types:
        ctx = f"{events_root}/{event_type}"
        by_type[event_type] = _paginate_filter(
            dm,
            ctx,
            filter_expr=filter_expr,
            limit=int(limit_per_type),
            order_by=order_by,
            descending=descending,
        )

    return TaskRunEventTree(
        run_key=str(run_key),
        events_base_context=events_root,
        manager_methods=by_type.get("ManagerMethod", []),
        tool_loops=by_type.get("ToolLoop", []),
    )
