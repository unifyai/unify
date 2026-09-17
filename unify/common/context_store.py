from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from unify import db
from unify.common.authorship import fields_with_authoring, is_shared_authored_context

logger = logging.getLogger(__name__)


class ContextIdentityError(RuntimeError):
    """A live context is missing identity configuration its manager declares."""


def _verify_context_identity(
    name: str,
    *,
    unique_keys: Dict[str, str],
    auto_counting: Dict[str, Optional[str]],
    project: Optional[str],
) -> None:
    """Raise when the live context lacks the declared identity configuration.

    ``create_context`` reports success both for a fresh create and for a
    pre-existing context, and a pre-existing context keeps whatever
    configuration it was first created with — including none at all when a
    row write reached the store first and auto-created it bare. A bare
    context assigns no ids to inserted rows, so provisioning time is the only
    place the corruption is caught before unaddressable rows accumulate.
    """
    live = db.get_context(name, project=project)
    missing_keys = set(unique_keys) - set(live.get("unique_keys") or [])
    missing_counters = set(auto_counting) - set(live.get("auto_counting") or {})
    if missing_keys or missing_counters:
        raise ContextIdentityError(
            f"Context {name!r} is live without its declared identity "
            f"configuration (missing unique_keys: {sorted(missing_keys)}, "
            f"missing auto_counting: {sorted(missing_counters)}). It was "
            "first created without them — typically implicitly, by a row "
            "write that reached the store before provisioning. Delete and "
            "re-provision the context to repair.",
        )


def create_context_checked(
    name: str,
    *,
    unique_keys: Optional[Dict[str, str]] = None,
    auto_counting: Optional[Dict[str, Optional[str]]] = None,
    description: Optional[str] = None,
    foreign_keys: Optional[list[Dict[str, Any]]] = None,
    project: Optional[str] = None,
) -> None:
    """Create a context idempotently and verify its declared identity."""
    db.create_context(
        name,
        unique_keys=unique_keys,
        auto_counting=auto_counting,
        description=description,
        foreign_keys=foreign_keys,
        project=project,
    )
    if unique_keys or auto_counting:
        _verify_context_identity(
            name,
            unique_keys=unique_keys or {},
            auto_counting=auto_counting or {},
            project=project,
        )


class TableStore:
    """
    Idempotent context/field provisioner with safe accessors.

    Guarantees that a given ``(project, context)`` exists with the required
    fields before read/write operations.
    """

    # Process-local memo to avoid repeated ensures in the same run
    _ENSURED: set[tuple[str, str]] = set()

    def __init__(
        self,
        context: str,
        *,
        unique_keys: Optional[Dict[str, str]] = None,
        auto_counting: Optional[Dict[str, Optional[str]]] = None,
        description: Optional[str] = None,
        fields: Optional[Dict[str, Any]] = None,
        foreign_keys: Optional[list[Dict[str, Any]]] = None,
    ) -> None:
        self._ctx = context
        self._project = db.active_project()
        self._unique_keys = dict(unique_keys or {})
        self._auto_counting = dict(auto_counting or {})
        self._description = description or ""
        self._fields = dict(fields or {})
        if is_shared_authored_context(context):
            self._fields = fields_with_authoring(self._fields)
        self._foreign_keys = list(foreign_keys or [])

    def ensure_context(self) -> None:
        """Create the context (and its fields) in the store."""
        key = (self._project, self._ctx)
        if key in self._ENSURED:
            return

        create_context_checked(
            self._ctx,
            unique_keys=self._unique_keys or None,
            auto_counting=self._auto_counting or None,
            description=self._description,
            foreign_keys=self._foreign_keys or None,
        )

        if self._fields:
            db.create_fields(fields=self._fields, context=self._ctx)

        self._ENSURED.add(key)

    def get_columns(self) -> Dict[str, str]:
        """Return {column_name: column_type} for this context.

        Provisions the context first when it is missing. Normalises to a
        single string label per field.
        """
        try:
            data = db.get_fields(project=self._project, context=self._ctx)
        except db.NotFound:
            self.ensure_context()
            data = db.get_fields(project=self._project, context=self._ctx)
        out: Dict[str, str] = {}
        for k, v in data.items():
            if isinstance(v, dict):
                out[str(k)] = (
                    str(v.get("data_type") or v.get("type") or "")
                ).strip() or "unknown"
            else:
                out[str(k)] = str(v)
        return out
