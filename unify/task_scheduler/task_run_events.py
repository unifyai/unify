"""Load EventBus trees for a durable ``Tasks/Executions`` row.

``Tasks/Executions`` does not embed event payloads. Join is by ``run_key`` value:

- Executions may live at ``Teams/{id}/Tasks/Executions`` (or ``{user}/{assistant}/Tasks/Executions``)
- Events live under the **executing assistant's** ``{…}/Events/{Type}`` contexts

Filter ``Events/ManagerMethod`` and ``Events/ToolLoop`` with
``run_key == "<key>"`` (server-side via DataManager). Reconstruct nesting from
``hierarchy`` / ``hierarchy_label`` — there is no ``parent_event_id``.

Actor-facing access is depth-1 via
``primitives.tasks.get_run_event_children`` / ``get_run_event`` (never dump the
full forest in one primitive return).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from unify.data_manager.data_manager import DataManager
from unify.events.task_run_lineage import parse_task_run_lineage_segment

__all__ = [
    "TaskRunEventTree",
    "build_hierarchy_prefix_filter",
    "build_run_key_filter",
    "fetch_task_run_events",
    "find_rows_at_node",
    "hierarchy_segments",
    "join_hierarchy_prefix",
    "normalize_events_base_context",
    "project_immediate_children",
    "resolve_task_run_root_segment",
]

_TREE_EVENT_TYPES: tuple[str, ...] = ("ManagerMethod", "ToolLoop")
_ERROR_TRUNCATE = 200
_DEFAULT_CHILD_LIMIT = 50


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


def build_hierarchy_prefix_filter(prefix: str) -> str:
    """Build a server-side ``hierarchy_label.startswith(...)`` filter fragment."""

    value = str(prefix or "").strip()
    if not value:
        raise ValueError("hierarchy prefix must be a non-empty string")
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'hierarchy_label.startswith("{escaped}")'


def hierarchy_segments(row: Mapping[str, Any]) -> list[str]:
    """Return hierarchy path segments for one Event row."""

    hierarchy = row.get("hierarchy")
    if isinstance(hierarchy, list) and hierarchy:
        return [str(s) for s in hierarchy if str(s)]
    label = row.get("hierarchy_label")
    if not label:
        return []
    return [part for part in str(label).split("->") if part]


def join_hierarchy_prefix(segments: Iterable[str]) -> str:
    """Join hierarchy segments into a ``hierarchy_label``-style prefix."""

    return "->".join(str(s) for s in segments if str(s))


def resolve_task_run_root_segment(
    rows: Iterable[Mapping[str, Any]],
    *,
    run_key: str,
) -> str | None:
    """Return the ``Task.run(...)`` root segment for this ``run_key``, if any."""

    key = str(run_key or "").strip()
    fallback: str | None = None
    for row in rows:
        segs = hierarchy_segments(row)
        if not segs:
            continue
        parsed = parse_task_run_lineage_segment(segs[0])
        if parsed is None:
            continue
        if key and parsed.run_key == key:
            return segs[0]
        if fallback is None:
            fallback = segs[0]
    return fallback


def _row_event_type(row: Mapping[str, Any]) -> str | None:
    explicit = row.get("_event_type") or row.get("type")
    if isinstance(explicit, str) and explicit:
        return explicit
    payload_cls = row.get("payload_cls")
    if isinstance(payload_cls, str) and payload_cls:
        if "ManagerMethod" in payload_cls:
            return "ManagerMethod"
        if "ToolLoop" in payload_cls:
            return "ToolLoop"
    return None


def _truncate_error(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) <= _ERROR_TRUNCATE:
        return text
    return text[:_ERROR_TRUNCATE] + "…"


def _event_id(row: Mapping[str, Any]) -> str | None:
    eid = row.get("event_id")
    if eid is None:
        return None
    text = str(eid).strip()
    return text or None


def project_immediate_children(
    rows: Iterable[Mapping[str, Any]],
    *,
    run_key: str,
    parent_prefix: str | None = None,
    limit: int = _DEFAULT_CHILD_LIMIT,
) -> list[dict[str, Any]]:
    """Project depth-1 child stubs under ``parent_prefix`` (or Task.run root).

    Never returns descendant payloads — only compact stubs for the next level.
    """

    material = [dict(r) for r in rows]
    root = resolve_task_run_root_segment(material, run_key=run_key)
    if parent_prefix is None:
        if root is None:
            return []
        parent_segs = [root]
    else:
        parent_segs = [p for p in str(parent_prefix).split("->") if p]
        if not parent_segs:
            return []

    groups: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    for row in material:
        segs = hierarchy_segments(row)
        if len(segs) <= len(parent_segs):
            continue
        if segs[: len(parent_segs)] != parent_segs:
            continue
        child_seg = segs[len(parent_segs)]
        node_id = join_hierarchy_prefix([*parent_segs, child_seg])
        if child_seg not in groups:
            groups[child_seg] = {
                "node_id": node_id,
                "segment": child_seg,
                "event_type": None,
                "_event_types": set(),
                "method": None,
                "kind": None,
                "phase": None,
                "status": None,
                "error": None,
                "has_children": False,
                "event_ids": [],
            }
            order.append(child_seg)
        stub = groups[child_seg]
        if len(segs) > len(parent_segs) + 1:
            stub["has_children"] = True
            continue

        # Row is exactly at this child node.
        et = _row_event_type(row)
        if et:
            stub["_event_types"].add(et)
        method = row.get("method")
        if isinstance(method, str) and method and stub["method"] is None:
            stub["method"] = method
        kind = row.get("kind")
        if isinstance(kind, str) and kind and stub["kind"] is None:
            stub["kind"] = kind
        phase = row.get("phase")
        if isinstance(phase, str) and phase:
            # Prefer outgoing when both phases exist.
            if stub["phase"] is None or phase == "outgoing":
                stub["phase"] = phase
        status = row.get("status")
        if status is not None and str(status).strip():
            if stub["status"] is None or str(status).lower() in {
                "failed",
                "error",
                "cancelled",
            }:
                stub["status"] = str(status)
        err = _truncate_error(row.get("error"))
        if err:
            stub["error"] = err
        eid = _event_id(row)
        if eid and eid not in stub["event_ids"]:
            stub["event_ids"].append(eid)

    children: list[dict[str, Any]] = []
    for child_seg in order[: max(int(limit), 0)]:
        stub = groups[child_seg]
        types = stub.pop("_event_types")
        if len(types) == 0:
            stub["event_type"] = None
        elif len(types) == 1:
            stub["event_type"] = next(iter(types))
        else:
            stub["event_type"] = "mixed"
        children.append(stub)
    return children


def find_rows_at_node(
    rows: Iterable[Mapping[str, Any]],
    *,
    node_id: str,
    event_id: str | None = None,
) -> list[dict[str, Any]]:
    """Return near-raw rows whose hierarchy equals ``node_id`` (not descendants)."""

    target = [p for p in str(node_id or "").split("->") if p]
    if not target:
        raise ValueError("node_id must be a non-empty hierarchy prefix")
    want_id = str(event_id).strip() if event_id else None
    out: list[dict[str, Any]] = []
    for row in rows:
        segs = hierarchy_segments(row)
        if segs != target:
            continue
        if want_id is not None and _event_id(row) != want_id:
            continue
        cleaned = dict(row)
        cleaned.pop("_event_type", None)
        out.append(cleaned)
    return out


@dataclass(frozen=True)
class TaskRunEventTree:
    """ManagerMethod + ToolLoop rows attributed to one ``Tasks/Executions.run_key``."""

    run_key: str
    events_base_context: str
    manager_methods: list[dict[str, Any]] = field(default_factory=list)
    tool_loops: list[dict[str, Any]] = field(default_factory=list)

    @property
    def rows(self) -> list[dict[str, Any]]:
        """All fetched rows annotated with ``_event_type`` for projection."""

        out: list[dict[str, Any]] = []
        for row in self.manager_methods:
            item = dict(row)
            item.setdefault("_event_type", "ManagerMethod")
            out.append(item)
        for row in self.tool_loops:
            item = dict(row)
            item.setdefault("_event_type", "ToolLoop")
            out.append(item)
        return out

    def hierarchy_roots(self) -> list[str]:
        """Unique first hierarchy segments across fetched rows."""

        roots: list[str] = []
        seen: set[str] = set()
        for row in self.rows:
            segs = hierarchy_segments(row)
            if not segs:
                continue
            root = segs[0]
            if root not in seen:
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
    hierarchy_prefix: str | None = None,
) -> TaskRunEventTree:
    """Fetch the ManagerMethod + ToolLoop tree for one ``Tasks/Executions.run_key``.

    Parameters
    ----------
    run_key:
        Unique key on the ``Tasks/Executions`` row (and stamped on Event payloads).
    events_base_context:
        Assistant Events root (``{user}/{assistant}/Events``) or write-context
        parent that owns ``Events``. Cross-context vs ``Teams/…/Tasks/Executions`` is
        expected; join is by ``run_key`` value only.
    hierarchy_prefix:
        Optional ``hierarchy_label`` prefix to narrow the Orchestra fetch
        (descendants of a parent node).
    """

    events_root = normalize_events_base_context(events_base_context)
    filter_expr = build_run_key_filter(run_key)
    if hierarchy_prefix:
        filter_expr = (
            f"({filter_expr}) and ({build_hierarchy_prefix_filter(hierarchy_prefix)})"
        )
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
