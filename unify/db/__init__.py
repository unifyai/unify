"""The assistant's persistence layer: typed, queryable tables in local SQLite.

Managers keep their state as rows in named *contexts* (tables) inside a
*project*. Contexts are addressed by path (``Contacts``,
``Personal/12/Contacts``), can declare unique keys, auto-counted ids and
derived columns, and are read back with expressions in the row language
(:mod:`unify.db.expressions`).

An active context path scopes every read and write that does not name one
explicitly, so a manager can bind itself once and address its tables by
relative name. Tests bind a fresh root per test the same way.

The engine (:mod:`unify.db.engine`) is opened lazily at
``UNIFY_STORE_PATH`` (default ``~/.unify/db.sqlite``); every function here
is a thin, keyword-only wrapper around it.
"""

from __future__ import annotations

import os
import posixpath
from contextvars import ContextVar
from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence

from . import embeddings
from .engine import Store, get_store, reset_store, store_home, store_path
from .errors import (
    AlreadyExists,
    Conflict,
    DuplicateKey,
    InvalidExpression,
    NotFound,
    StoreError,
    SyncLeaseHeldError,
)
from .expressions import compile_expression

__all__ = [
    "AlreadyExists",
    "CONTEXT_READ",
    "CONTEXT_WRITE",
    "Conflict",
    "DEFAULT_PROJECT",
    "DuplicateKey",
    "InvalidExpression",
    "Log",
    "NotFound",
    "Store",
    "StoreError",
    "SyncLeaseHeldError",
    "acquire_sync_lease",
    "activate",
    "active_project",
    "claim_logs",
    "commit_context",
    "commit_project",
    "compile_expression",
    "create_assistant",
    "create_context",
    "create_contexts",
    "create_derived_logs",
    "create_fields",
    "create_logs",
    "delete_assistant",
    "delete_context",
    "delete_fields",
    "delete_logs",
    "delete_project",
    "embeddings",
    "get_active_context",
    "get_context",
    "get_context_commits",
    "get_contexts",
    "get_fields",
    "get_groups",
    "get_logs",
    "get_logs_federated",
    "get_logs_metric",
    "get_project_commits",
    "get_store",
    "join_logs",
    "join_query",
    "list_assistants",
    "list_projects",
    "log",
    "release_sync_lease",
    "rename_context",
    "rename_field",
    "reset_store",
    "rollback_context",
    "rollback_project",
    "set_context",
    "store_home",
    "store_path",
    "unset_context",
    "update_assistant_config",
    "update_logs",
]

DEFAULT_PROJECT = "Assistants"

CONTEXT_READ: ContextVar[str] = ContextVar("UNIFY_CONTEXT_READ", default="")
CONTEXT_WRITE: ContextVar[str] = ContextVar("UNIFY_CONTEXT_WRITE", default="")

_PROJECT: str | None = None


# ---------------------------------------------------------------------------
# Projects and the active context
# ---------------------------------------------------------------------------


def active_project() -> str | None:
    """The activated project, else ``UNIFY_PROJECT`` from the environment."""
    if _PROJECT is not None:
        return _PROJECT
    return os.environ.get("UNIFY_PROJECT") or None


def _project(project: str | None) -> str:
    name = project or active_project() or DEFAULT_PROJECT
    store = get_store()
    if not store.project_exists(name):
        store.create_project(name)
    return name


def activate(project: str, overwrite: bool | str = False) -> None:
    """Make ``project`` the default for every call that names none."""
    global _PROJECT
    create_project(project, exist_ok=True, overwrite=overwrite)
    _PROJECT = project


def create_project(
    name: str,
    exist_ok: bool = True,
    *,
    overwrite: bool | str = False,
    is_public_read: bool = False,
) -> bool:
    store = get_store()
    if overwrite == "contexts":
        if store.project_exists(name):
            store.delete_project_contexts(name)
    elif overwrite:
        store.delete_project(name)
    created = store.create_project(name, is_public_read=is_public_read)
    if not created and not exist_ok:
        raise AlreadyExists(f"Project {name!r} already exists")
    return created


def delete_project(name: str) -> bool:
    global _PROJECT
    deleted = get_store().delete_project(name)
    if _PROJECT == name:
        _PROJECT = None
    return deleted


def list_projects() -> list[dict[str, Any]]:
    return get_store().list_projects()


def commit_project(name: str, commit_message: str | None = None) -> dict[str, Any]:
    return get_store().commit_project(_project(name), commit_message)


def rollback_project(name: str, commit_hash: str) -> None:
    get_store().rollback_project(_project(name), commit_hash)


def get_project_commits(name: str) -> list[dict[str, Any]]:
    return get_store().list_project_commits(_project(name))


def _join_path(base: str, context: str) -> str:
    if not base:
        return posixpath.normpath(context)
    return posixpath.normpath(posixpath.join(base, context))


def set_context(
    context: str,
    mode: str = "both",
    overwrite: bool = False,
    relative: bool = True,
    skip_create: bool = False,
    *,
    project: str | None = None,
) -> str:
    """Bind the active read and/or write context, creating it unless told not to."""
    if mode not in ("both", "read", "write"):
        raise ValueError(f"mode must be 'both', 'read' or 'write', not {mode!r}")
    if mode in ("both", "write"):
        base = CONTEXT_WRITE.get()
        write_path = _join_path(base, context) if relative else context
        CONTEXT_WRITE.set(write_path)
        context_path = write_path
    if mode in ("both", "read"):
        base = CONTEXT_READ.get()
        read_path = _join_path(base, context) if relative else context
        CONTEXT_READ.set(read_path)
        context_path = read_path
    if skip_create:
        return context_path
    project_name = _project(project)
    store = get_store()
    exists = store.context_exists(project_name, context_path)
    if overwrite and exists:
        if mode == "read":
            raise StoreError("Cannot overwrite a context bound in read mode")
        store.delete_context(project_name, context_path)
        exists = False
    if not exists:
        store.create_context(project_name, context_path)
    return context_path


def unset_context() -> None:
    CONTEXT_WRITE.set("")
    CONTEXT_READ.set("")


def get_active_context() -> dict[str, str]:
    return {"read": CONTEXT_READ.get(), "write": CONTEXT_WRITE.get()}


def _read_context(context: str | Mapping[str, Any] | None) -> str:
    return _resolve_context(context, CONTEXT_READ.get())


def _write_context(context: str | Mapping[str, Any] | None) -> str:
    return _resolve_context(context, CONTEXT_WRITE.get())


def _resolve_context(context: str | Mapping[str, Any] | None, active: str) -> str:
    if isinstance(context, Mapping):
        context = context.get("name")
    if context:
        return str(context)
    if active:
        return active
    raise StoreError("No context given and no active context is bound")


# ---------------------------------------------------------------------------
# Contexts
# ---------------------------------------------------------------------------


def create_context(
    name: str,
    description: str | None = None,
    is_versioned: bool = True,
    allow_duplicates: bool = True,
    unique_keys: Mapping[str, str] | None = None,
    auto_counting: Mapping[str, str | None] | None = None,
    foreign_keys: Sequence[Mapping[str, Any]] | None = None,
    exist_ok: bool = True,
    *,
    project: str | None = None,
) -> bool:
    """Create a table; return False when it already existed (with ``exist_ok``)."""
    return get_store().create_context(
        _project(project),
        name,
        description=description,
        is_versioned=is_versioned,
        allow_duplicates=allow_duplicates,
        unique_keys=unique_keys,
        auto_counting=auto_counting,
        foreign_keys=foreign_keys,
        exist_ok=exist_ok,
    )


def create_contexts(
    contexts: Sequence[Mapping[str, Any]],
    *,
    exist_ok: bool = True,
    project: str | None = None,
) -> dict[str, Any]:
    created, existing = [], []
    for spec in contexts:
        spec = dict(spec)
        name = spec.pop("name")
        if create_context(name, exist_ok=exist_ok, project=project, **spec):
            created.append(name)
        else:
            existing.append(name)
    return {"created": created, "existing": existing}


def get_context(name: str, *, project: str | None = None) -> dict[str, Any]:
    return get_store().get_context(_project(project), name)


def get_contexts(
    project: str | None = None,
    *,
    prefix: str | None = None,
) -> dict[str, str | None]:
    rows = get_store().list_contexts(_project(project), prefix=prefix)
    return {row["name"]: row["description"] for row in rows}


def delete_context(
    name: str,
    *,
    delete_children: bool = True,
    project: str | None = None,
) -> int | None:
    """Delete a context tree; ``None`` when nothing by that name existed."""
    try:
        return get_store().delete_context(
            _project(project),
            name,
            delete_children=delete_children,
        )
    except NotFound:
        return None


def rename_context(name: str, new_name: str, *, project: str | None = None) -> None:
    get_store().rename_context(_project(project), name, new_name)


def commit_context(
    name: str,
    commit_message: str | None = None,
    *,
    project: str | None = None,
) -> dict[str, Any]:
    return get_store().commit_context(_project(project), name, commit_message)


def rollback_context(
    name: str,
    commit_hash: str,
    *,
    project: str | None = None,
) -> None:
    get_store().rollback_context(_project(project), name, commit_hash)


def get_context_commits(
    name: str,
    *,
    project: str | None = None,
) -> list[dict[str, Any]]:
    return get_store().list_context_commits(_project(project), name)


# ---------------------------------------------------------------------------
# Fields
# ---------------------------------------------------------------------------


def get_fields(
    *,
    context: str | None = None,
    project: str | None = None,
) -> dict[str, dict[str, Any]]:
    return get_store().get_fields(_project(project), _read_context(context))


def create_fields(
    fields: Mapping[str, Mapping[str, Any] | str],
    *,
    context: str | None = None,
    project: str | None = None,
) -> None:
    get_store().create_fields(_project(project), _write_context(context), fields)


def delete_fields(
    *,
    context: str | None = None,
    fields: Sequence[str],
    project: str | None = None,
) -> None:
    get_store().delete_fields(_project(project), _write_context(context), fields)


def rename_field(
    name: str,
    new_name: str,
    *,
    context: str | None = None,
    project: str | None = None,
) -> None:
    get_store().rename_field(_project(project), _write_context(context), name, new_name)


def create_derived_logs(
    *,
    key: str,
    equation: str,
    context: str | None = None,
    from_ids: Sequence[int] | None = None,
    project: str | None = None,
) -> dict[str, Any]:
    """Declare a derived column and materialise it for every (or the given) row."""
    touched = get_store().create_derived_field(
        _project(project),
        _write_context(context),
        key,
        equation,
        from_ids=from_ids,
    )
    return {"key": key, "rows": touched}


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------


class Log:
    """One row: its id, creation timestamp, context and entries."""

    __slots__ = ("_id", "_ts", "_project", "_context", "_entries")

    def __init__(
        self,
        *,
        id: int | None = None,
        ts: str | datetime | None = None,
        project: str | None = None,
        context: str | None = None,
        entries: Mapping[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        self._id = id
        self._ts = ts
        self._project = project
        self._context = context
        self._entries = {**(entries or {}), **extra}

    @property
    def id(self) -> int | None:
        return self._id

    @property
    def ts(self) -> str | datetime | None:
        return self._ts

    @property
    def context(self) -> str | None:
        return self._context

    @property
    def project(self) -> str | None:
        return self._project

    @property
    def entries(self) -> dict[str, Any]:
        return self._entries

    def set_id(self, id: int) -> None:
        self._id = id

    def update_entries(self, **entries: Any) -> None:
        update_logs(
            logs=self._id,
            context=self._context,
            entries=entries,
            overwrite=True,
        )
        self._entries = {**self._entries, **entries}

    def delete(self) -> None:
        delete_logs(logs=self._id, context=self._context, project=self._project)

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self._id,
            "ts": self._ts,
            "project_name": self._project,
            "context": self._context,
            "entries": self._entries,
        }

    def __eq__(self, other: object) -> bool:
        if isinstance(other, dict):
            other = Log(id=other.get("id"), entries=other.get("entries", {}))
        if not isinstance(other, Log):
            return NotImplemented
        if self._id is not None and other._id is not None:
            return self._id == other._id
        return self._entries == other._entries

    def __hash__(self) -> int:
        return hash(self._id)

    def __len__(self) -> int:
        return len(self._entries)

    def __repr__(self) -> str:
        return f"Log(id={self._id})"


def _entries_with_mutability(
    mutable: bool | Mapping[str, bool] | None,
    entries: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for entry in entries:
        item = dict(entry)
        if mutable is None or mutable is True:
            prepared.append(item)
            continue
        explicit = dict(item.get("explicit_types") or {})
        targets = (
            mutable.items()
            if isinstance(mutable, Mapping)
            else (
                (k, mutable)
                for k in item
                if k not in ("explicit_types", "infer_untyped_fields")
            )
        )
        for field, flag in targets:
            if field in item:
                spec = dict(explicit.get(field) or {})
                spec["mutable"] = bool(flag)
                explicit[field] = spec
        item["explicit_types"] = explicit
        prepared.append(item)
    return prepared


def _to_ids(logs: int | Log | Iterable[int | Log] | None) -> list[int]:
    if logs is None:
        raise StoreError("logs must be given")
    if isinstance(logs, (int, Log)):
        logs = [logs]
    ids: list[int] = []
    for item in logs:
        if isinstance(item, Log):
            if item.id is None:
                raise StoreError("Log has no id yet")
            ids.append(int(item.id))
        else:
            ids.append(int(item))
    return ids


def _log_from_row(row: Mapping[str, Any], project: str, context: str) -> Log:
    return Log(
        id=row["id"],
        ts=row.get("ts"),
        project=project,
        context=context,
        entries=row["entries"],
    )


def log(
    *,
    project: str | None = None,
    context: str | Mapping[str, Any] | None = None,
    mutable: bool | Mapping[str, bool] | None = True,
    **entries: Any,
) -> Log:
    """Insert one row and return it."""
    created = create_logs(
        project=project,
        context=context,
        entries=[entries],
        mutable=mutable,
    )
    return created[0]


def create_logs(
    *,
    project: str | None = None,
    context: str | Mapping[str, Any] | None = None,
    entries: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    mutable: bool | Mapping[str, bool] | None = True,
    on_duplicate: str | None = None,
) -> list[Log]:
    """Insert rows and return them with their ids and auto-counted keys filled in.

    ``on_duplicate="skip"`` drops rows that collide on a unique key instead of
    failing the batch; the surviving rows are returned in order.
    """
    if entries is None:
        return []
    if isinstance(entries, Mapping):
        entries = [entries]
    project_name = _project(project)
    context_name = _write_context(context)
    prepared = _entries_with_mutability(mutable, entries)
    ids, failed = get_store().create_rows(
        project_name,
        context_name,
        prepared,
        on_duplicate=on_duplicate or "error",
    )
    failed_indices = {f["index"] for f in failed}
    surviving = [e for i, e in enumerate(prepared) if i not in failed_indices]
    for original, filled in zip(
        (e for i, e in enumerate(entries) if i not in failed_indices),
        surviving,
    ):
        if isinstance(original, dict):
            for key, value in filled.items():
                if (
                    key not in ("explicit_types", "infer_untyped_fields")
                    and key not in original
                ):
                    original[key] = value
    return [
        Log(
            id=row_id,
            project=project_name,
            context=context_name,
            entries={
                k: v
                for k, v in entry.items()
                if k not in ("explicit_types", "infer_untyped_fields")
            },
        )
        for entry, row_id in zip(surviving, ids)
    ]


def get_logs(
    *,
    project: str | None = None,
    context: str | Mapping[str, Any] | None = None,
    filter: str | None = None,
    limit: int | None = 1000,
    offset: int = 0,
    sorting: Mapping[str, str] | None = None,
    from_ids: Sequence[int] | None = None,
    exclude_ids: Sequence[int] | None = None,
    from_fields: Sequence[str] | None = None,
    exclude_fields: Sequence[str] | None = None,
    return_ids_only: bool = False,
    return_sort_distance: bool = False,
) -> list[Log] | list[int]:
    """Read rows from a context, filtered, sorted and windowed."""
    project_name = _project(project)
    context_name = _read_context(context)
    rows = get_store().get_rows(
        project_name,
        context_name,
        filter=filter,
        limit=limit,
        offset=offset,
        sorting=sorting,
        from_ids=from_ids,
        exclude_ids=exclude_ids,
        from_fields=from_fields,
        exclude_fields=exclude_fields,
        return_ids_only=return_ids_only,
        return_sort_distance=return_sort_distance,
    )
    if return_ids_only:
        return rows
    return [_log_from_row(row, project_name, context_name) for row in rows]


def get_logs_federated(
    *,
    contexts: Sequence[Mapping[str, Any]],
    filter: str | None = None,
    sorting: Sequence[Mapping[str, Any]] | None = None,
    offset: int = 0,
    limit: int | None = None,
    unique_id_field: str | None = None,
    annotate: bool = True,
    project: str | None = None,
) -> dict[str, Any]:
    """Read several contexts as one table; entries carry their source label."""
    result = get_store().federated(
        _project(project),
        contexts,
        filter=filter,
        sorting=sorting,
        offset=offset,
        limit=limit,
        unique_id_field=unique_id_field,
        annotate=annotate,
    )
    return {
        "logs": [row["entries"] for row in result["logs"]],
        "count": result["count"],
        "counts": result["counts"],
    }


def get_logs_metric(
    *,
    metric: str,
    key: str | Sequence[str],
    filter: str | None = None,
    context: str | None = None,
    project: str | None = None,
    group_by: str | Sequence[str] | None = None,
    from_ids: Sequence[int] | None = None,
    exclude_ids: Sequence[int] | None = None,
) -> Any:
    return get_store().metric(
        _project(project),
        _read_context(context),
        metric,
        key,
        filter=filter,
        group_by=group_by,
        from_ids=from_ids,
        exclude_ids=exclude_ids,
    )


def get_groups(
    *,
    key: str,
    context: str | None = None,
    filter: str | None = None,
    project: str | None = None,
) -> dict[str, Any]:
    return get_store().groups(
        _project(project),
        _read_context(context),
        key,
        filter=filter,
    )


def update_logs(
    *,
    logs: int | Log | Iterable[int | Log] | None = None,
    context: str | Mapping[str, Any] | None = None,
    entries: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None,
    overwrite: bool = True,
    on_duplicate: str | None = None,
    project: str | None = None,
) -> dict[str, Any]:
    """Merge entries into rows. ``overwrite=False`` refuses to change set values."""
    if not logs or entries is None:
        return {"updated": [], "failed": []}
    context_name = context.get("name") if isinstance(context, Mapping) else context
    return get_store().update_rows(
        _project(project),
        context_name,
        _to_ids(logs),
        entries,
        overwrite=overwrite,
        on_duplicate=on_duplicate or "error",
    )


def delete_logs(
    *,
    logs: int | Log | Iterable[int | Log] | None = None,
    context: str | None = None,
    project: str | None = None,
) -> dict[str, Any]:
    ids = _to_ids(logs) if logs is not None else []
    if not ids:
        return {"deleted": 0}
    deleted = get_store().delete_rows(_project(project), context, ids)
    return {"deleted": deleted}


def claim_logs(
    *,
    context: str | None = None,
    expect: Mapping[str, Any],
    updates: Mapping[str, Any],
    limit: int | None = None,
    project: str | None = None,
) -> dict[str, Any]:
    return get_store().claim_rows(
        _project(project),
        _write_context(context),
        expect=expect,
        updates=updates,
        limit=limit,
    )


def join_logs(
    *,
    pair_of_args: Sequence[Mapping[str, Any]],
    join_expr: str,
    mode: str = "inner",
    new_context: str,
    columns: Mapping[str, str] | Sequence[str] | None = None,
    project: str | None = None,
) -> dict[str, Any]:
    """Materialise a join of two contexts into ``new_context``."""
    left, right = pair_of_args
    rows = get_store().join(
        _project(project),
        left,
        right,
        join_expr=join_expr,
        mode=mode,
        columns=columns,
        new_context=new_context,
    )
    return {"context": new_context, "count": len(rows)}


def join_query(
    *,
    pair_of_args: Sequence[Mapping[str, Any]],
    join_expr: str,
    mode: str = "inner",
    columns: Mapping[str, str] | Sequence[str] | None = None,
    filter: str | None = None,
    sorting: Mapping[str, str] | None = None,
    limit: int | None = None,
    offset: int = 0,
    group_by: str | Sequence[str] | None = None,
    metric: str | None = None,
    key: str | Sequence[str] | None = None,
    project: str | None = None,
) -> dict[str, Any]:
    """Query a join without storing it: rows, or a reduction when ``metric`` is set."""
    left, right = pair_of_args
    store = get_store()
    rows = store.join(
        _project(project),
        left,
        right,
        join_expr=join_expr,
        mode=mode,
        columns=columns,
        filter=filter,
        sorting=sorting,
        limit=None if metric else limit,
        offset=0 if metric else offset,
    )
    if not metric:
        return {"logs": [row["entries"] for row in rows], "count": len(rows)}
    from .engine import _group_label, _reduce

    if key is None:
        raise StoreError("join_query needs a key when metric is set")
    names = [key] if isinstance(key, str) else list(key)
    groups = [group_by] if isinstance(group_by, str) else list(group_by or [])

    def grouped(subset: list[dict[str, Any]], name: str, depth: int) -> Any:
        if depth == len(groups):
            values = [
                r["entries"].get(name)
                for r in subset
                if r["entries"].get(name) is not None
            ]
            return _reduce(metric, values)
        buckets: dict[Any, list[dict[str, Any]]] = {}
        for r in subset:
            buckets.setdefault(
                _group_label(r["entries"].get(groups[depth])),
                [],
            ).append(r)
        return {
            label: grouped(items, name, depth + 1) for label, items in buckets.items()
        }

    results = {name: grouped(rows, name, 0) for name in names}
    return {
        "metric": results[names[0]] if isinstance(key, str) else results,
        "count": len(rows),
    }


# ---------------------------------------------------------------------------
# Sync leases
# ---------------------------------------------------------------------------


def acquire_sync_lease(
    lease_key: str,
    holder: str,
    *,
    ttl_seconds: float = 300.0,
    project: str | None = None,
) -> dict[str, Any]:
    return get_store().acquire_lease(
        _project(project),
        lease_key,
        holder,
        ttl_seconds=ttl_seconds,
    )


def release_sync_lease(
    lease_key: str,
    holder: str,
    *,
    project: str | None = None,
) -> bool:
    return get_store().release_lease(_project(project), lease_key, holder)


# ---------------------------------------------------------------------------
# Assistants
# ---------------------------------------------------------------------------


def list_assistants(*, agent_id: int | None = None) -> list[dict[str, Any]]:
    return get_store().list_assistants(agent_id=agent_id)


def create_assistant(**fields: Any) -> dict[str, Any]:
    return get_store().create_assistant(**fields)


def delete_assistant(agent_id: int) -> bool:
    return get_store().delete_assistant(agent_id)


def update_assistant_config(agent_id: int, config: Mapping[str, Any]) -> dict[str, Any]:
    return get_store().update_assistant(agent_id, **dict(config))
