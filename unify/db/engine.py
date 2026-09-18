"""The SQLite engine behind :mod:`unify.db`.

One file holds every project, context, row, field definition, derived-column
equation, commit snapshot and assistant record for an install. A
context is a named, hierarchically-addressed table (``Guidance``,
``tests/foo/Functions/Compositional``) whose rows are JSON documents typed by the fields
declared on it. Rows carry a global integer id, so a row id alone identifies a
row anywhere in the store.

Filters, sort keys and derived columns are expressions in the row language of
:mod:`unify.db.expressions`, evaluated in Python over the context's rows.
Derived values are materialised into the row on write so reads never compute.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import statistics
import threading
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from .errors import (
    AlreadyExists,
    Conflict,
    DuplicateKey,
    InvalidExpression,
    NotFound,
    StoreError,
)
from .expressions import (
    ROW_ID_NAMES,
    TIMESTAMP_NAMES,
    Expression,
    compile_expression,
)

RESERVED_ENTRY_KEYS = ("explicit_types", "infer_untyped_fields")
JOIN_MODES = ("inner", "left", "right", "outer")
METRICS = ("count", "sum", "mean", "var", "std", "min", "max", "median", "mode")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    is_public_read INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS contexts (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    is_versioned INTEGER NOT NULL DEFAULT 1,
    allow_duplicates INTEGER NOT NULL DEFAULT 1,
    unique_keys TEXT NOT NULL DEFAULT '{}',
    auto_counting TEXT NOT NULL DEFAULT '{}',
    foreign_keys TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT,
    UNIQUE (project_id, name)
);
CREATE TABLE IF NOT EXISTS fields (
    id INTEGER PRIMARY KEY,
    context_id INTEGER NOT NULL REFERENCES contexts(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    data_type TEXT NOT NULL,
    field_type TEXT NOT NULL DEFAULT 'entry',
    mutable INTEGER NOT NULL DEFAULT 1,
    is_unique INTEGER NOT NULL DEFAULT 0,
    description TEXT,
    equation TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (context_id, name)
);
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    context_id INTEGER NOT NULL REFERENCES contexts(id) ON DELETE CASCADE,
    data TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_logs_context ON logs(context_id, id);
CREATE TABLE IF NOT EXISTS counters (
    context_id INTEGER NOT NULL REFERENCES contexts(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    scope TEXT NOT NULL,
    next_value INTEGER NOT NULL,
    PRIMARY KEY (context_id, key, scope)
);
CREATE TABLE IF NOT EXISTS commits (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    context_id INTEGER REFERENCES contexts(id) ON DELETE CASCADE,
    hash TEXT NOT NULL,
    message TEXT,
    created_at TEXT NOT NULL,
    snapshot TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS assistants (
    agent_id INTEGER PRIMARY KEY AUTOINCREMENT,
    data TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


def store_home() -> Path:
    """Directory holding the store file and the workspace."""
    raw = os.environ.get("UNIFY_HOME", "").strip()
    return Path(raw).expanduser() if raw else Path.home() / ".unify"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if hasattr(value, "model_dump"):
        return value.model_dump()
    return str(value)


def dumps(value: Any) -> str:
    return json.dumps(value, default=_json_default, ensure_ascii=False)


def _infer_type(value: Any) -> str:
    if value is None:
        return "Any"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, datetime):
        return "datetime"
    if isinstance(value, date):
        return "date"
    if isinstance(value, Mapping):
        return "dict"
    if isinstance(value, (list, tuple, set)):
        return "list"
    return "str"


def _canonical(value: Any) -> Any:
    """JSON round-trip so stored and in-memory values compare equal."""
    return json.loads(dumps(value))


def _scope_key(value: Any) -> str:
    return dumps(value)


class _Ctx:
    """A context row loaded from the database."""

    __slots__ = (
        "id",
        "project_id",
        "name",
        "description",
        "is_versioned",
        "allow_duplicates",
        "unique_keys",
        "auto_counting",
        "foreign_keys",
    )

    def __init__(self, row: sqlite3.Row) -> None:
        self.id = row["id"]
        self.project_id = row["project_id"]
        self.name = row["name"]
        self.description = row["description"]
        self.is_versioned = bool(row["is_versioned"])
        self.allow_duplicates = bool(row["allow_duplicates"])
        self.unique_keys = json.loads(row["unique_keys"])
        self.auto_counting = json.loads(row["auto_counting"])
        self.foreign_keys = json.loads(row["foreign_keys"])

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "is_versioned": self.is_versioned,
            "allow_duplicates": self.allow_duplicates,
            "unique_keys": dict(self.unique_keys),
            "auto_counting": dict(self.auto_counting),
            "foreign_keys": list(self.foreign_keys),
        }


class _Field:
    __slots__ = (
        "name",
        "data_type",
        "field_type",
        "mutable",
        "is_unique",
        "description",
        "equation",
        "created_at",
    )

    def __init__(self, row: sqlite3.Row) -> None:
        self.name = row["name"]
        self.data_type = row["data_type"]
        self.field_type = row["field_type"]
        self.mutable = bool(row["mutable"])
        self.is_unique = bool(row["is_unique"])
        self.description = row["description"]
        self.equation = row["equation"]
        self.created_at = row["created_at"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "data_type": self.data_type,
            "field_type": self.field_type,
            "mutable": self.mutable,
            "unique": self.is_unique,
            "description": self.description or "",
            "artifacts": self.equation or "",
            "created_at": self.created_at,
        }


class Store:
    """All persistent state for one install, in one SQLite file."""

    def __init__(self, path: str | os.PathLike[str] = ":memory:") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            self.path,
            check_same_thread=False,
            isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._depth = 0
        if self.path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # Transactions
    # ------------------------------------------------------------------

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            outermost = self._depth == 0
            if outermost:
                self._conn.execute("BEGIN IMMEDIATE")
            self._depth += 1
            try:
                yield self._conn
            except BaseException:
                self._depth -= 1
                if outermost:
                    self._conn.execute("ROLLBACK")
                raise
            else:
                self._depth -= 1
                if outermost:
                    self._conn.execute("COMMIT")

    # ------------------------------------------------------------------
    # Projects
    # ------------------------------------------------------------------

    def create_project(self, name: str, *, is_public_read: bool = False) -> bool:
        """Create a project; return False when it already existed."""
        with self._tx() as conn:
            if conn.execute(
                "SELECT 1 FROM projects WHERE name = ?",
                (name,),
            ).fetchone():
                if is_public_read:
                    conn.execute(
                        "UPDATE projects SET is_public_read = 1 WHERE name = ?",
                        (name,),
                    )
                return False
            conn.execute(
                "INSERT INTO projects (name, is_public_read, created_at) VALUES (?, ?, ?)",
                (name, int(is_public_read), _now_iso()),
            )
            return True

    def delete_project(self, name: str) -> bool:
        with self._tx() as conn:
            row = conn.execute(
                "SELECT id FROM projects WHERE name = ?",
                (name,),
            ).fetchone()
            if row is None:
                return False
            conn.execute("DELETE FROM logs WHERE project_id = ?", (row["id"],))
            conn.execute("DELETE FROM projects WHERE id = ?", (row["id"],))
            return True

    def delete_project_contexts(self, name: str) -> None:
        with self._tx() as conn:
            project_id = self._project_id(name)
            conn.execute("DELETE FROM logs WHERE project_id = ?", (project_id,))
            conn.execute("DELETE FROM contexts WHERE project_id = ?", (project_id,))

    def list_projects(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT name, is_public_read, created_at FROM projects ORDER BY id",
            ).fetchall()
        return [
            {
                "name": r["name"],
                "is_public_read": bool(r["is_public_read"]),
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    def project_exists(self, name: str) -> bool:
        with self._lock:
            return (
                self._conn.execute(
                    "SELECT 1 FROM projects WHERE name = ?",
                    (name,),
                ).fetchone()
                is not None
            )

    def _project_id(self, name: str) -> int:
        row = self._conn.execute(
            "SELECT id FROM projects WHERE name = ?",
            (name,),
        ).fetchone()
        if row is None:
            raise NotFound(f"Project {name!r} not found")
        return row["id"]

    # ------------------------------------------------------------------
    # Contexts
    # ------------------------------------------------------------------

    def create_context(
        self,
        project: str,
        name: str,
        *,
        description: str | None = None,
        is_versioned: bool = True,
        allow_duplicates: bool = True,
        unique_keys: Mapping[str, str] | None = None,
        auto_counting: Mapping[str, str | None] | None = None,
        foreign_keys: Sequence[Mapping[str, Any]] | None = None,
        exist_ok: bool = True,
    ) -> bool:
        """Create a context; return False when it already existed."""
        with self._tx() as conn:
            project_id = self._project_id(project)
            existing = conn.execute(
                "SELECT id FROM contexts WHERE project_id = ? AND name = ?",
                (project_id, name),
            ).fetchone()
            if existing is not None:
                if exist_ok:
                    return False
                raise AlreadyExists(f"Context {name!r} already exists in {project!r}")
            conn.execute(
                "INSERT INTO contexts (project_id, name, description, is_versioned,"
                " allow_duplicates, unique_keys, auto_counting, foreign_keys, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    project_id,
                    name,
                    description,
                    int(is_versioned),
                    int(allow_duplicates),
                    dumps(dict(unique_keys or {})),
                    dumps(dict(auto_counting or {})),
                    dumps(list(foreign_keys or [])),
                    _now_iso(),
                ),
            )
            ctx = self._ctx(project, name)
            for key, key_type in (unique_keys or {}).items():
                self._ensure_field(ctx, key, key_type, is_unique=True)
            for key in auto_counting or {}:
                if key not in (unique_keys or {}):
                    self._ensure_field(ctx, key, "int")
            return True

    def _ctx(self, project: str, name: str) -> _Ctx:
        row = self._conn.execute(
            "SELECT c.* FROM contexts c JOIN projects p ON p.id = c.project_id"
            " WHERE p.name = ? AND c.name = ?",
            (project, name),
        ).fetchone()
        if row is None:
            raise NotFound(f"Context {name!r} not found in project {project!r}")
        return _Ctx(row)

    def _ctx_by_id(self, context_id: int) -> _Ctx:
        row = self._conn.execute(
            "SELECT * FROM contexts WHERE id = ?",
            (context_id,),
        ).fetchone()
        if row is None:
            raise NotFound(f"Context id {context_id} not found")
        return _Ctx(row)

    def context_exists(self, project: str, name: str) -> bool:
        with self._lock:
            try:
                self._ctx(project, name)
            except NotFound:
                return False
            return True

    def get_context(self, project: str, name: str) -> dict[str, Any]:
        with self._lock:
            return self._ctx(project, name).to_dict()

    def list_contexts(
        self,
        project: str,
        prefix: str | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            project_id = self._project_id(project)
            rows = self._conn.execute(
                "SELECT name, description FROM contexts WHERE project_id = ? ORDER BY id",
                (project_id,),
            ).fetchall()
        result = [{"name": r["name"], "description": r["description"]} for r in rows]
        if prefix:
            result = [r for r in result if r["name"].startswith(prefix)]
        return result

    def delete_context(
        self,
        project: str,
        name: str,
        *,
        delete_children: bool = True,
    ) -> int:
        """Delete a context (and, by default, every context nested under it)."""
        with self._tx() as conn:
            project_id = self._project_id(project)
            if delete_children:
                rows = conn.execute(
                    "SELECT id FROM contexts WHERE project_id = ? AND (name = ? OR name LIKE ?)",
                    (project_id, name, name.rstrip("/") + "/%"),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id FROM contexts WHERE project_id = ? AND name = ?",
                    (project_id, name),
                ).fetchall()
            if not rows:
                raise NotFound(f"Context {name!r} not found in project {project!r}")
            for row in rows:
                conn.execute("DELETE FROM logs WHERE context_id = ?", (row["id"],))
                conn.execute("DELETE FROM contexts WHERE id = ?", (row["id"],))
            return len(rows)

    def rename_context(self, project: str, name: str, new_name: str) -> None:
        with self._tx() as conn:
            ctx = self._ctx(project, name)
            if conn.execute(
                "SELECT 1 FROM contexts WHERE project_id = ? AND name = ?",
                (ctx.project_id, new_name),
            ).fetchone():
                raise AlreadyExists(f"Context {new_name!r} already exists")
            conn.execute(
                "UPDATE contexts SET name = ?, updated_at = ? WHERE id = ?",
                (new_name, _now_iso(), ctx.id),
            )

    # ------------------------------------------------------------------
    # Fields
    # ------------------------------------------------------------------

    def _fields(self, ctx: _Ctx) -> dict[str, _Field]:
        rows = self._conn.execute(
            "SELECT * FROM fields WHERE context_id = ? ORDER BY id",
            (ctx.id,),
        ).fetchall()
        return {r["name"]: _Field(r) for r in rows}

    def _ensure_field(
        self,
        ctx: _Ctx,
        name: str,
        data_type: str,
        *,
        mutable: bool = True,
        is_unique: bool = False,
        description: str | None = None,
        field_type: str = "entry",
        equation: str | None = None,
    ) -> None:
        existing = self._conn.execute(
            "SELECT id, data_type FROM fields WHERE context_id = ? AND name = ?",
            (ctx.id, name),
        ).fetchone()
        if existing is None:
            self._conn.execute(
                "INSERT INTO fields (context_id, name, data_type, field_type, mutable,"
                " is_unique, description, equation, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ctx.id,
                    name,
                    data_type,
                    field_type,
                    int(mutable),
                    int(is_unique),
                    description,
                    equation,
                    _now_iso(),
                ),
            )
        elif existing["data_type"] == "Any" and data_type != "Any":
            self._conn.execute(
                "UPDATE fields SET data_type = ? WHERE id = ?",
                (data_type, existing["id"]),
            )

    def get_fields(self, project: str, context: str) -> dict[str, dict[str, Any]]:
        with self._lock:
            ctx = self._ctx(project, context)
            return {name: f.to_dict() for name, f in self._fields(ctx).items()}

    def create_fields(
        self,
        project: str,
        context: str,
        fields: Mapping[str, Mapping[str, Any] | str],
    ) -> None:
        with self._tx():
            ctx = self._ctx(project, context)
            for name, spec in fields.items():
                if isinstance(spec, str):
                    spec = {"type": spec}
                self._ensure_field(
                    ctx,
                    name,
                    str(spec.get("type") or spec.get("data_type") or "Any"),
                    mutable=bool(spec.get("mutable", True)),
                    is_unique=bool(spec.get("unique", False)),
                    description=spec.get("description"),
                )

    def delete_fields(self, project: str, context: str, names: Sequence[str]) -> None:
        with self._tx() as conn:
            ctx = self._ctx(project, context)
            for name in names:
                conn.execute(
                    "DELETE FROM fields WHERE context_id = ? AND name = ?",
                    (ctx.id, name),
                )
            rows = conn.execute(
                "SELECT id, data FROM logs WHERE context_id = ?",
                (ctx.id,),
            ).fetchall()
            for row in rows:
                data = json.loads(row["data"])
                if any(n in data for n in names):
                    for name in names:
                        data.pop(name, None)
                    conn.execute(
                        "UPDATE logs SET data = ?, updated_at = ? WHERE id = ?",
                        (dumps(data), _now_iso(), row["id"]),
                    )

    def rename_field(
        self,
        project: str,
        context: str,
        name: str,
        new_name: str,
    ) -> None:
        with self._tx() as conn:
            ctx = self._ctx(project, context)
            fields = self._fields(ctx)
            if name not in fields:
                raise NotFound(f"Field {name!r} not found in {context!r}")
            if new_name in fields:
                raise AlreadyExists(f"Field {new_name!r} already exists in {context!r}")
            conn.execute(
                "UPDATE fields SET name = ? WHERE context_id = ? AND name = ?",
                (new_name, ctx.id, name),
            )
            rows = conn.execute(
                "SELECT id, data FROM logs WHERE context_id = ?",
                (ctx.id,),
            ).fetchall()
            for row in rows:
                data = json.loads(row["data"])
                if name in data:
                    data[new_name] = data.pop(name)
                    conn.execute(
                        "UPDATE logs SET data = ?, updated_at = ? WHERE id = ?",
                        (dumps(data), _now_iso(), row["id"]),
                    )

    # ------------------------------------------------------------------
    # Derived columns
    # ------------------------------------------------------------------

    def create_derived_field(
        self,
        project: str,
        context: str,
        key: str,
        equation: str,
        *,
        from_ids: Sequence[int] | None = None,
    ) -> int:
        """Register a derived column and materialise it; return rows touched."""
        expression = compile_expression(equation)
        with self._tx() as conn:
            ctx = self._ctx(project, context)
            existing = self._fields(ctx).get(key)
            if existing is None:
                self._ensure_field(
                    ctx,
                    key,
                    "Any",
                    field_type="derived",
                    equation=equation,
                )
            elif existing.equation != equation:
                conn.execute(
                    "UPDATE fields SET equation = ?, field_type = 'derived'"
                    " WHERE context_id = ? AND name = ?",
                    (equation, ctx.id, key),
                )
            if from_ids is not None:
                rows = self._rows_by_ids(ctx, list(from_ids))
            else:
                rows = self._all_rows(ctx)
            return self._materialise(ctx, rows, {key: expression})

    def _derived_expressions(self, ctx: _Ctx) -> dict[str, Expression]:
        return {
            name: compile_expression(f.equation)
            for name, f in self._fields(ctx).items()
            if f.field_type == "derived" and f.equation
        }

    def _materialise(
        self,
        ctx: _Ctx,
        rows: list[dict[str, Any]],
        derived: Mapping[str, Expression],
    ) -> int:
        """Compute derived values into ``rows`` (in place) and persist them."""
        if not rows or not derived:
            return 0
        for key, expression in derived.items():
            for row in rows:
                scope = _row_scope(row)
                row["data"][key] = _canonical(expression.evaluate(scope))
        now = _now_iso()
        self._conn.executemany(
            "UPDATE logs SET data = ?, updated_at = ? WHERE id = ?",
            [(dumps(r["data"]), now, r["id"]) for r in rows],
        )
        return len(rows)

    # ------------------------------------------------------------------
    # Rows: create
    # ------------------------------------------------------------------

    def create_rows(
        self,
        project: str,
        context: str,
        entries: Sequence[Mapping[str, Any]],
        *,
        on_duplicate: str = "error",
    ) -> tuple[list[int], list[dict[str, Any]]]:
        """Insert rows; return ``(ids, failed)``.

        ``entries`` is updated in place with any auto-counted keys the store
        assigned. ``failed`` lists ``{"index", "error"}`` for rows skipped under
        ``on_duplicate="skip"``.
        """
        if on_duplicate not in ("error", "skip"):
            raise StoreError(
                f"on_duplicate must be 'error' or 'skip', not {on_duplicate!r}",
            )
        with self._tx() as conn:
            ctx = self._ctx(project, context)
            fields = self._fields(ctx)
            derived = self._derived_expressions(ctx)
            unique_names = [k for k in ctx.unique_keys] + [
                n for n, f in fields.items() if f.is_unique and n not in ctx.unique_keys
            ]
            seen: dict[str, set[str]] = {n: set() for n in unique_names}
            ids: list[int] = []
            failed: list[dict[str, Any]] = []
            inserted: list[dict[str, Any]] = []
            now = _now_iso()
            for index, entry in enumerate(entries):
                explicit = (
                    entry.get("explicit_types") if isinstance(entry, Mapping) else None
                )
                data = {
                    k: _canonical(v)
                    for k, v in entry.items()
                    if k not in RESERVED_ENTRY_KEYS and k not in derived
                }
                self._assign_counters(ctx, data)
                if isinstance(entry, dict):
                    for key in ctx.auto_counting:
                        if key in data:
                            entry[key] = data[key]
                collision = self._find_collision(ctx, unique_names, data, seen)
                if collision is not None:
                    if on_duplicate == "error":
                        raise DuplicateKey(*collision)
                    failed.append(
                        {
                            "index": index,
                            "error": f"Duplicate entry for unique field {collision[0]!r}",
                        },
                    )
                    continue
                if not ctx.allow_duplicates and self._identical_exists(ctx, data):
                    if on_duplicate == "error":
                        raise Conflict(f"Duplicate row in context {context!r}")
                    failed.append({"index": index, "error": "Duplicate row"})
                    continue
                self._register_fields(ctx, fields, data, explicit)
                cursor = conn.execute(
                    "INSERT INTO logs (project_id, context_id, data, created_at)"
                    " VALUES (?, ?, ?, ?)",
                    (ctx.project_id, ctx.id, dumps(data), now),
                )
                row_id = int(cursor.lastrowid)
                ids.append(row_id)
                inserted.append({"id": row_id, "ts": now, "data": data})
                for name in unique_names:
                    if data.get(name) is not None:
                        seen[name].add(_scope_key(data[name]))
            self._materialise(ctx, inserted, derived)
            return ids, failed

    def _assign_counters(self, ctx: _Ctx, data: dict[str, Any]) -> None:
        pending = dict(ctx.auto_counting)
        # Independent counters first so dependants can scope on their values.
        ordered = sorted(pending.items(), key=lambda kv: kv[1] is not None)
        for key, parent in ordered:
            if data.get(key) is not None:
                continue
            scope = _scope_key(data.get(parent)) if parent else ""
            row = self._conn.execute(
                "SELECT next_value FROM counters WHERE context_id = ? AND key = ? AND scope = ?",
                (ctx.id, key, scope),
            ).fetchone()
            value = (
                int(row["next_value"])
                if row
                else self._seed_counter(ctx, key, parent, data)
            )
            self._conn.execute(
                "INSERT INTO counters (context_id, key, scope, next_value) VALUES (?, ?, ?, ?)"
                " ON CONFLICT(context_id, key, scope) DO UPDATE SET next_value = excluded.next_value",
                (ctx.id, key, scope, value + 1),
            )
            data[key] = value

    def _seed_counter(
        self,
        ctx: _Ctx,
        key: str,
        parent: str | None,
        data: dict[str, Any],
    ) -> int:
        """First value for a counter: one past the largest existing value."""
        rows = self._conn.execute(
            "SELECT data FROM logs WHERE context_id = ?",
            (ctx.id,),
        ).fetchall()
        best = -1
        for row in rows:
            payload = json.loads(row["data"])
            if parent and payload.get(parent) != data.get(parent):
                continue
            value = payload.get(key)
            if isinstance(value, int) and not isinstance(value, bool) and value > best:
                best = value
        return best + 1

    def _find_collision(
        self,
        ctx: _Ctx,
        unique_names: Sequence[str],
        data: Mapping[str, Any],
        seen: Mapping[str, set[str]],
        *,
        exclude_id: int | None = None,
    ) -> tuple[str, Any] | None:
        for name in unique_names:
            value = data.get(name)
            if value is None:
                continue
            if _scope_key(value) in seen.get(name, ()):
                return name, value
            if self._value_exists(ctx, name, value, exclude_id=exclude_id):
                return name, value
        return None

    def _value_exists(
        self,
        ctx: _Ctx,
        name: str,
        value: Any,
        *,
        exclude_id: int | None = None,
    ) -> bool:
        if isinstance(value, (str, int, float)) and not isinstance(value, bool):
            query = (
                "SELECT id FROM logs WHERE context_id = ? AND json_extract(data, ?) = ?"
            )
            params: list[Any] = [ctx.id, f"$.{name}", value]
            if exclude_id is not None:
                query += " AND id != ?"
                params.append(exclude_id)
            return self._conn.execute(query + " LIMIT 1", params).fetchone() is not None
        target = _scope_key(value)
        for row in self._conn.execute(
            "SELECT id, data FROM logs WHERE context_id = ?",
            (ctx.id,),
        ):
            if exclude_id is not None and row["id"] == exclude_id:
                continue
            payload = json.loads(row["data"])
            if name in payload and _scope_key(payload[name]) == target:
                return True
        return False

    def _identical_exists(self, ctx: _Ctx, data: Mapping[str, Any]) -> bool:
        target = dumps(dict(sorted(data.items())))
        for row in self._conn.execute(
            "SELECT data FROM logs WHERE context_id = ?",
            (ctx.id,),
        ):
            if dumps(dict(sorted(json.loads(row["data"]).items()))) == target:
                return True
        return False

    def _register_fields(
        self,
        ctx: _Ctx,
        fields: dict[str, _Field],
        data: Mapping[str, Any],
        explicit: Mapping[str, Any] | None,
    ) -> None:
        for key, value in data.items():
            spec = (explicit or {}).get(key) if isinstance(explicit, Mapping) else None
            spec = spec if isinstance(spec, Mapping) else {}
            data_type = str(spec.get("type") or _infer_type(value))
            current = fields.get(key)
            if current is None or (current.data_type == "Any" and data_type != "Any"):
                self._ensure_field(
                    ctx,
                    key,
                    data_type,
                    mutable=bool(spec.get("mutable", True)),
                    is_unique=bool(spec.get("unique", False)),
                )
                fields.update(self._fields(ctx))

    # ------------------------------------------------------------------
    # Rows: read
    # ------------------------------------------------------------------

    def _all_rows(self, ctx: _Ctx) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT id, data, created_at FROM logs WHERE context_id = ? ORDER BY id",
            (ctx.id,),
        ).fetchall()
        return [
            {"id": r["id"], "ts": r["created_at"], "data": json.loads(r["data"])}
            for r in rows
        ]

    def _rows_by_ids(self, ctx: _Ctx, ids: Sequence[int]) -> list[dict[str, Any]]:
        if not ids:
            return []
        result: list[dict[str, Any]] = []
        for chunk_start in range(0, len(ids), 500):
            chunk = list(ids[chunk_start : chunk_start + 500])
            marks = ",".join("?" * len(chunk))
            rows = self._conn.execute(
                f"SELECT id, data, created_at FROM logs WHERE context_id = ? AND id IN ({marks})"
                " ORDER BY id",
                [ctx.id, *chunk],
            ).fetchall()
            result.extend(
                {"id": r["id"], "ts": r["created_at"], "data": json.loads(r["data"])}
                for r in rows
            )
        return result

    def get_rows(
        self,
        project: str,
        context: str,
        *,
        filter: str | None = None,
        limit: int | None = 1000,
        offset: int = 0,
        sorting: Mapping[str, str] | Sequence[Mapping[str, Any]] | None = None,
        from_ids: Sequence[int] | None = None,
        exclude_ids: Sequence[int] | None = None,
        from_fields: Sequence[str] | None = None,
        exclude_fields: Sequence[str] | None = None,
        return_ids_only: bool = False,
        return_sort_distance: bool = False,
    ) -> list[Any]:
        """Read rows as ``{"id", "ts", "entries"}`` dicts (or bare ids)."""
        with self._lock:
            ctx = self._ctx(project, context)
            rows = self._query(
                ctx,
                filter=filter,
                sorting=sorting,
                from_ids=from_ids,
                exclude_ids=exclude_ids,
                return_sort_distance=return_sort_distance,
            )
        rows = rows[offset:]
        if limit is not None and limit > 0:
            rows = rows[:limit]
        if return_ids_only:
            return [r["id"] for r in rows]
        return [
            {
                "id": r["id"],
                "ts": r["ts"],
                "entries": _project(r["data"], from_fields, exclude_fields),
            }
            for r in rows
        ]

    def _query(
        self,
        ctx: _Ctx,
        *,
        filter: str | None = None,
        sorting: Mapping[str, str] | Sequence[Mapping[str, Any]] | None = None,
        from_ids: Sequence[int] | None = None,
        exclude_ids: Sequence[int] | None = None,
        return_sort_distance: bool = False,
    ) -> list[dict[str, Any]]:
        rows = (
            self._rows_by_ids(ctx, list(from_ids))
            if from_ids is not None
            else self._all_rows(ctx)
        )
        if exclude_ids:
            excluded = set(exclude_ids)
            rows = [r for r in rows if r["id"] not in excluded]
        if filter:
            expression = compile_expression(filter)
            rows = [r for r in rows if expression.matches(_row_scope(r))]
        if sorting:
            rows = _sort_rows(rows, sorting, attach_distance=return_sort_distance)
        return rows

    # ------------------------------------------------------------------
    # Rows: update / delete
    # ------------------------------------------------------------------

    def update_rows(
        self,
        project: str,
        context: str | None,
        ids: Sequence[int],
        entries: Mapping[str, Any] | Sequence[Mapping[str, Any]],
        *,
        overwrite: bool = True,
        on_duplicate: str = "error",
    ) -> dict[str, Any]:
        """Merge ``entries`` into the rows with ``ids``."""
        if isinstance(entries, Mapping):
            per_row = [entries] * len(ids)
        else:
            per_row = list(entries)
            if len(per_row) != len(ids):
                raise StoreError("entries must be one mapping or one per log id")
        updated: list[int] = []
        failed: list[dict[str, Any]] = []
        with self._tx() as conn:
            for row_id, entry in zip(ids, per_row):
                row = conn.execute(
                    "SELECT context_id, data FROM logs WHERE id = ?",
                    (row_id,),
                ).fetchone()
                if row is None:
                    raise NotFound(f"Log {row_id} not found")
                ctx = self._ctx_by_id(row["context_id"])
                if context and ctx.name != context:
                    raise NotFound(f"Log {row_id} is not in context {context!r}")
                fields = self._fields(ctx)
                derived = self._derived_expressions(ctx)
                data = json.loads(row["data"])
                explicit = entry.get("explicit_types")
                changes = {
                    k: _canonical(v)
                    for k, v in entry.items()
                    if k not in RESERVED_ENTRY_KEYS and k not in derived
                }
                if not overwrite:
                    clash = [k for k in changes if k in data and data[k] is not None]
                    if clash:
                        raise Conflict(
                            f"Existing values cannot be overwritten because overwrite is"
                            f" False: {clash}",
                        )
                for key in changes:
                    field = fields.get(key)
                    if (
                        field is not None
                        and not field.mutable
                        and key in data
                        and data[key] != changes[key]
                    ):
                        raise Conflict(f"Field {key!r} is immutable")
                unique_names = [k for k in ctx.unique_keys] + [
                    n
                    for n, f in fields.items()
                    if f.is_unique and n not in ctx.unique_keys
                ]
                collision = self._find_collision(
                    ctx,
                    [
                        n
                        for n in unique_names
                        if n in changes and changes[n] != data.get(n)
                    ],
                    changes,
                    {},
                    exclude_id=row_id,
                )
                if collision is not None:
                    if on_duplicate == "skip":
                        failed.append(
                            {
                                "log_event_id": row_id,
                                "error": f"Duplicate entry for unique field {collision[0]!r}",
                            },
                        )
                        continue
                    raise DuplicateKey(*collision)
                data.update(changes)
                self._register_fields(ctx, fields, changes, explicit)
                now = _now_iso()
                conn.execute(
                    "UPDATE logs SET data = ?, updated_at = ? WHERE id = ?",
                    (dumps(data), now, row_id),
                )
                if derived:
                    self._materialise(
                        ctx,
                        [{"id": row_id, "ts": now, "data": data}],
                        derived,
                    )
                updated.append(row_id)
        return {"updated": updated, "failed": failed}

    def delete_rows(self, project: str, context: str | None, ids: Sequence[int]) -> int:
        with self._tx() as conn:
            deleted = 0
            for row_id in ids:
                row = conn.execute(
                    "SELECT context_id, data FROM logs WHERE id = ?",
                    (row_id,),
                ).fetchone()
                if row is None:
                    continue
                ctx = self._ctx_by_id(row["context_id"])
                self._cascade_foreign_keys(ctx, json.loads(row["data"]))
                conn.execute("DELETE FROM logs WHERE id = ?", (row_id,))
                deleted += 1
            return deleted

    def _cascade_foreign_keys(self, ctx: _Ctx, data: Mapping[str, Any]) -> None:
        """Apply ``on_delete`` rules of contexts referencing ``ctx``."""
        referrers = self._conn.execute(
            "SELECT * FROM contexts WHERE project_id = ? AND foreign_keys != '[]'",
            (ctx.project_id,),
        ).fetchall()
        short_name = ctx.name.rsplit("/", 1)[-1]
        for row in referrers:
            other = _Ctx(row)
            for fk in other.foreign_keys:
                ref = str(fk.get("references", ""))
                if "." not in ref:
                    continue
                ref_ctx, ref_col = ref.rsplit(".", 1)
                if ref_ctx not in (ctx.name, short_name):
                    continue
                value = data.get(ref_col)
                if value is None:
                    continue
                action = str(fk.get("on_delete", "")).upper()
                column = str(fk.get("name", ""))
                if not column or action not in ("CASCADE", "SET NULL"):
                    continue
                segments = column.split(".")
                # A reference held inside a list (``ids[*]`` or
                # ``items[*].ref.id``) is one element among many, so both
                # rules act on that element: CASCADE pops it and SET NULL
                # clears it. Only a scalar reference makes CASCADE delete
                # the referring row itself.
                row_cascade = action == "CASCADE" and "[*]" not in column
                for target in self._all_rows(other):
                    if not _fk_walk(target["data"], segments, value, action, False):
                        continue
                    if row_cascade:
                        self._cascade_foreign_keys(other, target["data"])
                        self._conn.execute(
                            "DELETE FROM logs WHERE id = ?",
                            (target["id"],),
                        )
                    else:
                        _fk_walk(target["data"], segments, value, action, True)
                        self._conn.execute(
                            "UPDATE logs SET data = ?, updated_at = ? WHERE id = ?",
                            (dumps(target["data"]), _now_iso(), target["id"]),
                        )

    def claim_rows(
        self,
        project: str,
        context: str,
        *,
        expect: Mapping[str, Any],
        updates: Mapping[str, Any],
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Atomically move rows matching ``expect`` to ``updates``."""
        with self._tx():
            ctx = self._ctx(project, context)
            claimed: list[dict[str, Any]] = []
            for row in self._all_rows(ctx):
                if all(row["data"].get(k) == v for k, v in expect.items()):
                    row["data"].update(_canonical(dict(updates)))
                    self._conn.execute(
                        "UPDATE logs SET data = ?, updated_at = ? WHERE id = ?",
                        (dumps(row["data"]), _now_iso(), row["id"]),
                    )
                    claimed.append({"id": row["id"], "data": row["data"]})
                    if limit and len(claimed) >= limit:
                        break
            return {"claimed": claimed, "count": len(claimed)}

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def metric(
        self,
        project: str,
        context: str,
        metric: str,
        keys: str | Sequence[str],
        *,
        filter: str | None = None,
        group_by: str | Sequence[str] | None = None,
        from_ids: Sequence[int] | None = None,
        exclude_ids: Sequence[int] | None = None,
    ) -> Any:
        """Reduce one or more keys over the matching rows."""
        metric = metric.strip().lower()
        if metric not in METRICS:
            raise StoreError(
                f"Unsupported metric {metric!r}; expected one of {METRICS}",
            )
        single = isinstance(keys, str)
        names = [keys] if single else list(keys)
        with self._lock:
            ctx = self._ctx(project, context)
            rows = self._query(
                ctx,
                filter=filter,
                from_ids=from_ids,
                exclude_ids=exclude_ids,
            )
        groups = [group_by] if isinstance(group_by, str) else list(group_by or [])

        def reduce_rows(subset: list[dict[str, Any]], key: str) -> Any:
            values = [
                r["data"].get(key) for r in subset if r["data"].get(key) is not None
            ]
            return _reduce(metric, values)

        def grouped(subset: list[dict[str, Any]], key: str, depth: int) -> Any:
            if depth == len(groups):
                return reduce_rows(subset, key)
            buckets: dict[Any, list[dict[str, Any]]] = {}
            for r in subset:
                buckets.setdefault(
                    _group_label(r["data"].get(groups[depth])),
                    [],
                ).append(r)
            return {
                label: grouped(items, key, depth + 1)
                for label, items in buckets.items()
            }

        results = {name: grouped(rows, name, 0) for name in names}
        return results[names[0]] if single else results

    def groups(
        self,
        project: str,
        context: str,
        key: str,
        *,
        filter: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            ctx = self._ctx(project, context)
            rows = self._query(ctx, filter=filter)
        seen: dict[str, Any] = {}
        for row in rows:
            value = row["data"].get(key)
            if value is None:
                continue
            seen.setdefault(_scope_key(value), value)
        return {str(i): v for i, v in enumerate(seen.values())}

    # ------------------------------------------------------------------
    # Federated reads
    # ------------------------------------------------------------------

    def federated(
        self,
        project: str,
        contexts: Sequence[Mapping[str, Any]],
        *,
        filter: str | None = None,
        sorting: Sequence[Mapping[str, Any]] | None = None,
        offset: int = 0,
        limit: int | None = None,
        unique_id_field: str | None = None,
        annotate: bool = True,
    ) -> dict[str, Any]:
        """Read several contexts as if they were one table.

        Each spec names a ``context`` and a ``source`` label, with optional
        ``filter``, ``from_fields``, ``exclude_fields`` and ``project_name``.
        Rows are annotated with ``_federated_source`` / ``_federated_context``
        and, when ``unique_id_field`` is given, deduplicated on that field in
        source order.
        """
        merged: list[dict[str, Any]] = []
        counts: dict[str, int] = {}
        for spec in contexts:
            spec_project = spec.get("project_name") or project
            spec_filter = spec.get("filter")
            combined = _and(filter, spec_filter)
            with self._lock:
                try:
                    ctx = self._ctx(spec_project, spec["context"])
                except NotFound:
                    counts[spec["source"]] = 0
                    continue
                rows = self._query(ctx, filter=combined)
            counts[spec["source"]] = len(rows)
            for row in rows:
                entries = _project(
                    row["data"],
                    spec.get("from_fields"),
                    spec.get("exclude_fields"),
                )
                if annotate:
                    entries["_federated_source"] = spec["source"]
                    entries["_federated_context"] = ctx.name
                merged.append({"id": row["id"], "ts": row["ts"], "entries": entries})
        if unique_id_field:
            seen: set[str] = set()
            deduped = []
            for row in merged:
                key = row["entries"].get(unique_id_field)
                token = _scope_key(key)
                if key is not None and token in seen:
                    continue
                seen.add(token)
                deduped.append(row)
            merged = deduped
        if sorting:
            merged = _sort_federated(merged, sorting)
        total = len(merged)
        window = merged[offset:]
        if limit:
            window = window[:limit]
        return {"logs": window, "count": total, "counts": counts}

    # ------------------------------------------------------------------
    # Joins
    # ------------------------------------------------------------------

    def join(
        self,
        project: str,
        left: Mapping[str, Any],
        right: Mapping[str, Any],
        *,
        join_expr: str,
        mode: str = "inner",
        columns: Mapping[str, str] | Sequence[str] | None = None,
        new_context: str | None = None,
        filter: str | None = None,
        sorting: Mapping[str, str] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Join two contexts; materialise into ``new_context`` when given."""
        if mode not in JOIN_MODES:
            raise StoreError(
                f"Invalid join mode {mode!r}; expected one of {JOIN_MODES}",
            )
        with self._tx():
            left_ctx = self._ctx(left.get("project_name") or project, left["context"])
            right_ctx = self._ctx(
                right.get("project_name") or project,
                right["context"],
            )
            left_rows = self._query(left_ctx, filter=left.get("filter"))
            right_rows = self._query(right_ctx, filter=right.get("filter"))
            aliases = _JoinAliases(left_ctx.name, right_ctx.name)
            condition = compile_expression(aliases.rewrite(join_expr), aliases.names)
            selected = aliases.columns(columns)
            joined: list[dict[str, Any]] = []
            matched_right: set[int] = set()
            for lrow in left_rows:
                hit = False
                lscope = _row_scope(lrow)
                for rrow in right_rows:
                    rscope = _row_scope(rrow)
                    if condition.evaluate(
                        {},
                        {aliases.left: lscope, aliases.right: rscope},
                    ):
                        hit = True
                        matched_right.add(rrow["id"])
                        joined.append(aliases.project(selected, lscope, rscope))
                if not hit and mode in ("left", "outer"):
                    joined.append(aliases.project(selected, lscope, None))
            if mode in ("right", "outer"):
                for rrow in right_rows:
                    if rrow["id"] not in matched_right:
                        joined.append(aliases.project(selected, None, _row_scope(rrow)))
            if filter:
                expression = compile_expression(filter)
                joined = [r for r in joined if expression.matches(r)]
            if sorting:
                shaped = [
                    {"id": i, "ts": None, "data": r} for i, r in enumerate(joined)
                ]
                joined = [r["data"] for r in _sort_rows(shaped, sorting)]
            if new_context:
                self.create_context(project, new_context)
                ids, _ = self.create_rows(project, new_context, joined)
                return [{"id": i, "entries": r} for i, r in zip(ids, joined)]
            window = joined[offset:]
            if limit:
                window = window[:limit]
            return [{"id": i, "entries": r} for i, r in enumerate(window)]

    # ------------------------------------------------------------------
    # Commits
    # ------------------------------------------------------------------

    def commit_context(
        self,
        project: str,
        context: str,
        message: str | None = None,
    ) -> dict[str, Any]:
        with self._tx() as conn:
            ctx = self._ctx(project, context)
            snapshot = self._snapshot(ctx)
            digest = hashlib.sha1(snapshot.encode("utf-8")).hexdigest()
            now = _now_iso()
            conn.execute(
                "INSERT INTO commits (project_id, context_id, hash, message, created_at, snapshot)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (ctx.project_id, ctx.id, digest, message, now, snapshot),
            )
            return {"commit_hash": digest, "context": context, "created_at": now}

    def rollback_context(self, project: str, context: str, commit_hash: str) -> None:
        with self._tx() as conn:
            ctx = self._ctx(project, context)
            row = conn.execute(
                "SELECT snapshot FROM commits WHERE context_id = ? AND hash = ?"
                " ORDER BY id DESC LIMIT 1",
                (ctx.id, commit_hash),
            ).fetchone()
            if row is None:
                raise NotFound(
                    f"Commit {commit_hash!r} not found for context {context!r}",
                )
            self._restore(ctx, json.loads(row["snapshot"]))

    def list_context_commits(self, project: str, context: str) -> list[dict[str, Any]]:
        with self._lock:
            ctx = self._ctx(project, context)
            rows = self._conn.execute(
                "SELECT hash, message, created_at FROM commits WHERE context_id = ? ORDER BY id",
                (ctx.id,),
            ).fetchall()
        return [
            {
                "commit_hash": r["hash"],
                "message": r["message"],
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    def commit_project(
        self,
        project: str,
        message: str | None = None,
    ) -> dict[str, Any]:
        with self._tx() as conn:
            project_id = self._project_id(project)
            snapshots = {
                ctx.name: json.loads(self._snapshot(ctx))
                for ctx in (
                    _Ctx(r)
                    for r in conn.execute(
                        "SELECT * FROM contexts WHERE project_id = ?",
                        (project_id,),
                    ).fetchall()
                )
            }
            payload = dumps(snapshots)
            digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
            now = _now_iso()
            conn.execute(
                "INSERT INTO commits (project_id, context_id, hash, message, created_at, snapshot)"
                " VALUES (?, NULL, ?, ?, ?, ?)",
                (project_id, digest, message, now, payload),
            )
            return {"commit_hash": digest, "project": project, "created_at": now}

    def rollback_project(self, project: str, commit_hash: str) -> None:
        with self._tx() as conn:
            project_id = self._project_id(project)
            row = conn.execute(
                "SELECT snapshot FROM commits WHERE project_id = ? AND context_id IS NULL"
                " AND hash = ? ORDER BY id DESC LIMIT 1",
                (project_id, commit_hash),
            ).fetchone()
            if row is None:
                raise NotFound(
                    f"Commit {commit_hash!r} not found for project {project!r}",
                )
            snapshots = json.loads(row["snapshot"])
            for name, snapshot in snapshots.items():
                self.create_context(project, name)
                self._restore(self._ctx(project, name), snapshot)

    def list_project_commits(self, project: str) -> list[dict[str, Any]]:
        with self._lock:
            project_id = self._project_id(project)
            rows = self._conn.execute(
                "SELECT hash, message, created_at FROM commits"
                " WHERE project_id = ? AND context_id IS NULL ORDER BY id",
                (project_id,),
            ).fetchall()
        return [
            {
                "commit_hash": r["hash"],
                "message": r["message"],
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    def _snapshot(self, ctx: _Ctx) -> str:
        rows = self._conn.execute(
            "SELECT id, data, created_at, updated_at FROM logs WHERE context_id = ? ORDER BY id",
            (ctx.id,),
        ).fetchall()
        counters = self._conn.execute(
            "SELECT key, scope, next_value FROM counters WHERE context_id = ?",
            (ctx.id,),
        ).fetchall()
        fields = self._conn.execute(
            "SELECT name, data_type, field_type, mutable, is_unique, description, equation,"
            " created_at FROM fields WHERE context_id = ? ORDER BY id",
            (ctx.id,),
        ).fetchall()
        return dumps(
            {
                "rows": [dict(r) for r in rows],
                "counters": [dict(r) for r in counters],
                "fields": [dict(r) for r in fields],
            },
        )

    def _restore(self, ctx: _Ctx, snapshot: Mapping[str, Any]) -> None:
        conn = self._conn
        conn.execute("DELETE FROM logs WHERE context_id = ?", (ctx.id,))
        conn.execute("DELETE FROM counters WHERE context_id = ?", (ctx.id,))
        conn.execute("DELETE FROM fields WHERE context_id = ?", (ctx.id,))
        for f in snapshot.get("fields", []):
            conn.execute(
                "INSERT INTO fields (context_id, name, data_type, field_type, mutable,"
                " is_unique, description, equation, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ctx.id,
                    f["name"],
                    f["data_type"],
                    f["field_type"],
                    f["mutable"],
                    f["is_unique"],
                    f["description"],
                    f["equation"],
                    f["created_at"],
                ),
            )
        for c in snapshot.get("counters", []):
            conn.execute(
                "INSERT INTO counters (context_id, key, scope, next_value) VALUES (?, ?, ?, ?)",
                (ctx.id, c["key"], c["scope"], c["next_value"]),
            )
        for r in snapshot.get("rows", []):
            conn.execute(
                "INSERT INTO logs (id, project_id, context_id, data, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (
                    r["id"],
                    ctx.project_id,
                    ctx.id,
                    r["data"],
                    r["created_at"],
                    r["updated_at"],
                ),
            )

    # ------------------------------------------------------------------
    # Assistants
    # ------------------------------------------------------------------

    def create_assistant(self, **fields: Any) -> dict[str, Any]:
        with self._tx() as conn:
            requested = fields.pop("agent_id", None)
            now = _now_iso()
            payload = dumps(_canonical(fields))
            if requested is not None:
                conn.execute(
                    "INSERT INTO assistants (agent_id, data, created_at) VALUES (?, ?, ?)",
                    (int(requested), payload, now),
                )
                agent_id = int(requested)
            else:
                cursor = conn.execute(
                    "INSERT INTO assistants (data, created_at) VALUES (?, ?)",
                    (payload, now),
                )
                agent_id = int(cursor.lastrowid)
            return self._assistant(agent_id)

    def _assistant(self, agent_id: int) -> dict[str, Any]:
        row = self._conn.execute(
            "SELECT agent_id, data, created_at FROM assistants WHERE agent_id = ?",
            (agent_id,),
        ).fetchone()
        if row is None:
            raise NotFound(f"Assistant {agent_id} not found")
        record = json.loads(row["data"])
        record["agent_id"] = row["agent_id"]
        record["created_at"] = row["created_at"]
        return record

    def list_assistants(self, agent_id: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if agent_id is not None:
                try:
                    return [self._assistant(int(agent_id))]
                except NotFound:
                    return []
            rows = self._conn.execute(
                "SELECT agent_id FROM assistants ORDER BY agent_id",
            ).fetchall()
            return [self._assistant(r["agent_id"]) for r in rows]

    def update_assistant(self, agent_id: int, **fields: Any) -> dict[str, Any]:
        with self._tx() as conn:
            record = self._assistant(int(agent_id))
            record.pop("agent_id", None)
            record.pop("created_at", None)
            record.update(_canonical(fields))
            conn.execute(
                "UPDATE assistants SET data = ? WHERE agent_id = ?",
                (dumps(record), int(agent_id)),
            )
            return self._assistant(int(agent_id))

    def delete_assistant(self, agent_id: int) -> bool:
        with self._tx() as conn:
            cursor = conn.execute(
                "DELETE FROM assistants WHERE agent_id = ?",
                (int(agent_id),),
            )
            return cursor.rowcount > 0


# ----------------------------------------------------------------------
# Row helpers
# ----------------------------------------------------------------------


class _RowScope(dict):
    """A row's fields plus id and timestamp pseudo-fields."""

    __slots__ = ()


def _row_scope(row: Mapping[str, Any]) -> _RowScope:
    scope = _RowScope(row["data"])
    for name in ROW_ID_NAMES:
        scope.setdefault(name, row["id"])
    for name in TIMESTAMP_NAMES:
        scope.setdefault(name, row.get("ts"))
    return scope


def _project(
    data: Mapping[str, Any],
    from_fields: Sequence[str] | None,
    exclude_fields: Sequence[str] | None,
) -> dict[str, Any]:
    if from_fields is not None:
        wanted = list(from_fields)
        return {k: data[k] for k in wanted if k in data}
    if exclude_fields:
        excluded = set(exclude_fields)
        return {k: v for k, v in data.items() if k not in excluded}
    return dict(data)


def _and(*filters: str | None) -> str | None:
    parts = [f"({f})" for f in filters if f]
    return " and ".join(parts) if parts else None


def _group_label(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return dumps(value)
    return value


def _reduce(metric: str, values: list[Any]) -> Any:
    if metric == "count":
        return len(values)
    numbers = [
        float(v)
        for v in values
        if isinstance(v, (int, float)) and not isinstance(v, bool)
    ]
    if metric in ("min", "max"):
        pool = numbers or values
        if not pool:
            return None
        try:
            return min(pool) if metric == "min" else max(pool)
        except TypeError:
            return None
    if metric == "mode":
        if not values:
            return None
        try:
            return statistics.mode(values)
        except statistics.StatisticsError:
            return values[0]
    if not numbers:
        return None
    if metric == "sum":
        return sum(numbers)
    if metric == "mean":
        return statistics.fmean(numbers)
    if metric == "median":
        return statistics.median(numbers)
    if metric == "var":
        return statistics.pvariance(numbers)
    if metric == "std":
        return statistics.pstdev(numbers)
    raise StoreError(f"Unsupported metric {metric!r}")


def _sort_rows(
    rows: list[dict[str, Any]],
    sorting: Mapping[str, str] | Sequence[Mapping[str, Any]],
    *,
    attach_distance: bool = False,
) -> list[dict[str, Any]]:
    """Stable multi-key sort; ``None`` keys sort last regardless of direction."""
    specs: list[tuple[Expression, bool, bool]] = []
    if isinstance(sorting, Mapping):
        items = [(k, v, "last") for k, v in sorting.items()]
    else:
        items = [
            (s["field"], s.get("direction", "ascending"), s.get("missing", "last"))
            for s in sorting
        ]
    for key, direction, missing in items:
        descending = str(direction).lower().startswith("desc")
        specs.append(
            (compile_expression(key), descending, str(missing).lower() == "first"),
        )
    keyed = []
    for row in rows:
        scope = _row_scope(row)
        values = []
        for index, (expression, descending, _missing_first) in enumerate(specs):
            value = expression.evaluate(scope)
            if attach_distance and index == 0:
                row["data"]["_sort_distance"] = value
            values.append(value)
        keyed.append((values, row))
    from functools import cmp_to_key

    from .expressions import _order

    def compare(a: tuple[list[Any], Any], b: tuple[list[Any], Any]) -> int:
        for index, (_expr, descending, missing_first) in enumerate(specs):
            va, vb = a[0][index], b[0][index]
            if va is None and vb is None:
                continue
            if va is None:
                return -1 if missing_first else 1
            if vb is None:
                return 1 if missing_first else -1
            ordered = _order(va, vb)
            if ordered is None or ordered == 0:
                continue
            return -ordered if descending else ordered
        return 0

    keyed.sort(key=cmp_to_key(compare))
    return [row for _values, row in keyed]


def _sort_federated(
    rows: list[dict[str, Any]],
    sorting: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    shaped = [{"id": r["id"], "ts": r["ts"], "data": r["entries"]} for r in rows]
    ordered = _sort_rows(shaped, sorting)
    return [{"id": r["id"], "ts": r["ts"], "entries": r["data"]} for r in ordered]


class _JoinAliases:
    """Rewrites ``<context>.<column>`` references into alias subscripts."""

    def __init__(self, left: str, right: str) -> None:
        self.left = "__left__"
        self.right = "__right__"
        self.left_name = left
        self.right_name = right
        self.names = (self.left, self.right)

    def _prefixes(self) -> list[tuple[str, str]]:
        pairs = [
            (self.left_name, self.left),
            (self.right_name, self.right),
            ("A", self.left),
            ("B", self.right),
        ]
        return sorted(pairs, key=lambda p: -len(p[0]))

    def rewrite(self, expr: str) -> str:
        import re

        for name, alias in self._prefixes():
            pattern = re.compile(re.escape(name) + r"\.([A-Za-z_]\w*)")
            expr = pattern.sub(lambda m, a=alias: f'{a}["{m.group(1)}"]', expr)
        return expr.replace(" = ", " == ")

    def split(self, reference: str) -> tuple[str, str]:
        for name, alias in self._prefixes():
            if reference.startswith(name + "."):
                return alias, reference[len(name) + 1 :]
        raise InvalidExpression(
            f"Column reference {reference!r} names no joined context",
        )

    def columns(
        self,
        columns: Mapping[str, str] | Sequence[str] | None,
    ) -> list[tuple[str, str, str]]:
        if columns is None:
            return []
        if isinstance(columns, Mapping):
            return [(*self.split(src), out) for src, out in columns.items()]
        return [(*self.split(src), src.rsplit(".", 1)[-1]) for src in columns]

    def project(
        self,
        selected: list[tuple[str, str, str]],
        left: Mapping[str, Any] | None,
        right: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        sides = {self.left: left, self.right: right}
        if not selected:
            merged: dict[str, Any] = {}
            for side in (left, right):
                if side:
                    merged.update(
                        {
                            k: v
                            for k, v in side.items()
                            if k not in ROW_ID_NAMES + TIMESTAMP_NAMES
                        },
                    )
            return merged
        return {
            out: (sides[alias] or {}).get(col) if sides[alias] is not None else None
            for alias, col, out in selected
        }


def _fk_walk(
    node: Any,
    segments: Sequence[str],
    value: Any,
    on_delete: str,
    mutate: bool,
) -> bool:
    """Whether any leaf under a foreign-key path equals ``value``.

    A path is dot-separated; a segment ending in ``[*]`` names a list and
    applies the rest of the path to every element. With ``mutate`` the
    ``on_delete`` rule is applied in place: CASCADE drops a matching list
    element, SET NULL replaces a matching scalar (in a list or not) with
    ``None`` and, when the path continues into a list element, clears the
    matching leaf inside it.
    """
    segment, rest = segments[0], segments[1:]
    listwise = segment.endswith("[*]")
    key = segment[:-3] if listwise else segment
    if not isinstance(node, dict) or key not in node:
        return False
    current = node[key]
    if listwise:
        if not isinstance(current, list):
            return False
        if not rest:
            hit = any(item == value for item in current)
            if hit and mutate:
                node[key] = [
                    item for item in current if item != value or on_delete != "CASCADE"
                ]
                if on_delete != "CASCADE":
                    node[key] = [None if item == value else item for item in current]
            return hit
        hit = False
        kept = []
        for item in current:
            matched = _fk_walk(item, rest, value, on_delete, mutate)
            hit = hit or matched
            if mutate and matched and on_delete == "CASCADE":
                continue
            kept.append(item)
        if mutate:
            node[key] = kept
        return hit
    if not rest:
        hit = current == value
        if hit and mutate:
            node[key] = None
        return hit
    return _fk_walk(current, rest, value, on_delete, mutate)


# ----------------------------------------------------------------------
# Process-wide store
# ----------------------------------------------------------------------

_STORE: Store | None = None
_STORE_LOCK = threading.Lock()


def store_path() -> str:
    explicit = os.environ.get("UNIFY_STORE_PATH", "").strip()
    if explicit:
        return explicit
    return str(store_home() / "store.sqlite")


def get_store() -> Store:
    """The process-wide store, opened on first use at :func:`store_path`."""
    global _STORE
    with _STORE_LOCK:
        if _STORE is None or _STORE.path != store_path():
            if _STORE is not None:
                _STORE.close()
            _STORE = Store(store_path())
        return _STORE


def reset_store() -> None:
    """Close the process-wide store so the next call reopens it."""
    global _STORE
    with _STORE_LOCK:
        if _STORE is not None:
            _STORE.close()
            _STORE = None
