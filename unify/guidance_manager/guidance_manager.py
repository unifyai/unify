from __future__ import annotations

from typing import FrozenSet, List, Dict, Optional, Any, Tuple
import functools
import logging

from unify import db
from ..common.log_utils import (
    assigned_row_id,
    log as write_log,
)
from ..common.tool_outcome import ToolErrorException, ToolOutcome
from ..common.model_to_fields import model_to_fields
from ..common.context_store import TableStore
from ..common.federated_search import (
    FederatedSearchContext,
    federated_count,
    federated_filter,
    federated_ranked_search,
    is_missing_context_error,
)
from ..common.builtins import builtins_project
from .base import BaseGuidanceManager
from .builtins_catalog import BUILTINS_GUIDANCE_CONTEXT
from .types.guidance import Guidance
from ..common.embed_utils import ensure_vector_column, list_private_fields
from ..common.filter_utils import normalize_filter_expr
from ..common.context_registry import TableContext, ContextRegistry
from ..common.stale_reason import StaleReason, merge_stale_reasons

GUIDANCE_TABLE = "Guidance"
FUNCTIONS_COMPOSITIONAL_TABLE = "Functions/Compositional"

logger = logging.getLogger(__name__)

# Content cap for search/filter result payloads. Entries (notably imported
# builtin skills) can run to 100KB+; returning them wholesale from list-style
# reads floods the caller's context window. Reads above this cap return a
# preview and the full text is fetched per entry via ``get_guidance``.
GUIDANCE_PREVIEW_CHARS = 2000


class GuidanceManager(BaseGuidanceManager):
    """
    Concrete Guidance manager backed by Unify contexts and fields.
    """

    class Config:
        required_contexts = [
            TableContext(
                name=GUIDANCE_TABLE,
                description="Table of procedural guidance entries.",
                fields=model_to_fields(Guidance),
                unique_keys={"guidance_id": "int"},
                auto_counting={"guidance_id": None},
                foreign_keys=[
                    {
                        "name": "function_ids[*]",
                        "references": f"{FUNCTIONS_COMPOSITIONAL_TABLE}.function_id",
                        "on_delete": "CASCADE",  # pop on function deletion
                        "on_update": "CASCADE",
                    },
                ],
            ),
        ]

    def __init__(
        self,
        *,
        rolling_summary_in_prompts: bool = True,
        filter_scope: Optional[str] = None,
        exclude_ids: Optional[FrozenSet[int]] = None,
    ) -> None:
        super().__init__()
        self._ctx = ContextRegistry.get_context(self, GUIDANCE_TABLE)

        self._filter_scope = filter_scope
        self._exclude_ids = frozenset(exclude_ids) if exclude_ids else None

        # Built-in fields derived from Guidance model
        self._BUILTIN_FIELDS: Tuple[str, ...] = tuple(Guidance.model_fields.keys())

        self._rolling_summary_in_prompts = rolling_summary_in_prompts

        # Ensure context/schema exist
        self._provision_storage()

    def _read_spec(
        self,
        *,
        row_filter: Optional[str] = None,
        allowed_fields: Optional[List[str]] = None,
    ) -> FederatedSearchContext:
        """Return the federated source for this assistant's own Guidance table."""
        return FederatedSearchContext(
            context=self._ctx,
            source=self._ctx,
            row_filter=row_filter,
            allowed_fields=allowed_fields,
        )

    def _builtins_read_spec(
        self,
        *,
        row_filter: Optional[str] = None,
        allowed_fields: Optional[List[str]] = None,
    ) -> FederatedSearchContext:
        """Return the federated source for the global builtins guidance catalogue."""
        return FederatedSearchContext(
            context=BUILTINS_GUIDANCE_CONTEXT,
            source="builtins",
            row_filter=row_filter,
            allowed_fields=allowed_fields,
            project=builtins_project(),
        )

    def _functions_context(self) -> str:
        """Return the compositional functions context guidance links into."""
        from ..function_manager.function_manager import (
            FUNCTIONS_COMPOSITIONAL_TABLE as FUNCTION_MANAGER_COMPOSITIONAL_TABLE,
            FunctionManager,
        )

        return ContextRegistry.get_context(
            FunctionManager,
            FUNCTION_MANAGER_COMPOSITIONAL_TABLE,
        )

    def _available_functions_by_id(self) -> dict[int, str]:
        """Return currently resolvable compositional functions keyed by id."""
        available: dict[int, str] = {}
        for log in db.get_logs(
            context=self._functions_context(),
            from_fields=["function_id", "name"],
        ):
            function_id = log.entries.get("function_id")
            if function_id is not None:
                available[int(function_id)] = str(log.entries.get("name") or "")
        return available

    @staticmethod
    def _missing_function_reasons(
        guidance: Guidance,
        *,
        available: dict[int, str],
        preserve_historical: bool,
    ) -> list[StaleReason]:
        preserved = [
            reason for reason in guidance.stale_reasons if reason.dep_kind != "function"
        ]
        candidates: dict[int, str | None] = {
            int(function_id): None for function_id in guidance.function_ids
        }
        if preserve_historical:
            for reason in guidance.stale_reasons:
                if reason.dep_kind == "function" and reason.id is not None:
                    candidates.setdefault(int(reason.id), reason.name)
        missing = [
            StaleReason(
                dep_kind="function",
                id=function_id,
                name=name,
                message=(
                    f"missing function_id={function_id}"
                    + (f" name={name}" if name else "")
                ),
            )
            for function_id, name in candidates.items()
            if function_id not in available
        ]
        return merge_stale_reasons(preserved, *missing)

    # ------------------------------- Helpers ---------------------------------

    # -- Scope / exclusion properties ----------------------------------------

    @property
    def filter_scope(self) -> Optional[str]:
        """A boolean expression permanently applied to all read queries."""
        return self._filter_scope

    @filter_scope.setter
    def filter_scope(self, value: Optional[str]) -> None:
        self._filter_scope = value

    @property
    def exclude_ids(self) -> Optional[FrozenSet[int]]:
        """Guidance IDs excluded from all read queries."""
        return self._exclude_ids

    @exclude_ids.setter
    def exclude_ids(self, value: Optional[FrozenSet[int]]) -> None:
        self._exclude_ids = frozenset(value) if value else None

    @staticmethod
    def _build_id_exclusion(ids: Optional[FrozenSet[int]]) -> Optional[str]:
        """Build a filter clause excluding a set of guidance IDs."""
        if not ids:
            return None
        sorted_ids = sorted(ids)
        if len(sorted_ids) == 1:
            return f"guidance_id != {sorted_ids[0]}"
        joined_ids = ", ".join(str(gid) for gid in sorted_ids)
        return f"guidance_id not in [{joined_ids}]"

    def _scoped_filter(self, caller_filter: Optional[str]) -> Optional[str]:
        """Compose *caller_filter* with ``_filter_scope`` and id exclusions.

        Returns ``None`` when all parts are absent, meaning "no filter".
        """
        parts = [
            p
            for p in [
                caller_filter,
                self._filter_scope,
                self._build_id_exclusion(self._exclude_ids),
            ]
            if p
        ]
        if not parts:
            return None
        if len(parts) == 1:
            return parts[0]
        return " and ".join(f"({p})" for p in parts)

    def _is_builtin_guidance(self, guidance_id: int) -> bool:
        """Return whether an id refers to a row in the builtins catalogue."""
        try:
            rows = db.get_logs(
                context=BUILTINS_GUIDANCE_CONTEXT,
                project=builtins_project(),
                filter=f"guidance_id == {int(guidance_id)}",
                limit=1,
                from_fields=["guidance_id"],
            )
        except Exception as exc:
            if is_missing_context_error(exc):
                return False
            raise
        return bool(rows)

    def _raise_if_builtin(self, guidance_id: int, action: str) -> None:
        """Refuse mutations of builtins entries with an actionable error."""
        if self._is_builtin_guidance(guidance_id):
            raise ValueError(
                f"guidance_id {guidance_id} is a built-in platform guidance "
                f"entry and cannot be {action}. Built-in guidance is "
                "read-only for everyone. To tailor it, create your own "
                "entry with add_guidance (optionally adapting the built-in "
                "content); that copy can then be updated or deleted freely.",
            )

    @staticmethod
    def _with_content_preview(row: Guidance) -> Guidance:
        """Return *row* with content truncated to the list-read preview cap."""
        if len(row.content) <= GUIDANCE_PREVIEW_CHARS:
            return row
        preview = (
            row.content[:GUIDANCE_PREVIEW_CHARS]
            + f"\n\n… [content preview truncated at {GUIDANCE_PREVIEW_CHARS:,} "
            f"of {len(row.content):,} chars — fetch the full entry with "
            f"get_guidance(guidance_id={row.guidance_id})]"
        )
        return row.model_copy(update={"content": preview})

    def _num_items(self) -> int:
        return federated_count(
            [self._read_spec(), self._builtins_read_spec()],
            key="guidance_id",
            filter=self._scoped_filter(None),
        )

    @functools.wraps(BaseGuidanceManager.clear, updated=())
    def clear(self) -> None:
        db.delete_context(self._ctx)

        # Ensure the schema exists again via shared provisioning helper
        self._ctx = ContextRegistry.refresh(self, GUIDANCE_TABLE) or self._ctx
        self._provision_storage()

        # Verify the context is visible before attempting reads
        try:
            import time as _time  # local import to avoid polluting module namespace

            for _ in range(3):
                try:
                    db.get_fields(context=self._ctx)
                    break
                except Exception:
                    _time.sleep(0.05)
        except Exception:
            pass

    def warm_embeddings(self) -> None:
        try:
            ensure_vector_column(
                self._ctx,
                embed_column="_content_emb",
                source_column="content",
            )
        except Exception:
            pass

    def _provision_storage(self) -> None:
        """Ensure Guidance context and schema exist (idempotent)."""
        self._store = TableStore(
            self._ctx,
            unique_keys={"guidance_id": "int"},
            auto_counting={"guidance_id": None},
            description="Table of procedural guidance entries.",
            fields=model_to_fields(Guidance),
        )

    def _get_columns(self) -> Dict[str, str]:
        return self._store.get_columns()

    def _list_columns(
        self,
        *,
        include_types: bool = True,
    ) -> Dict[str, Any] | List[str]:
        """List available columns in the Guidance table.

        Parameters
        ----------
        include_types : bool, default True
            When True, return a mapping of column_name → type information as
            stored in the backing context. When False, return a simple list of
            column names. This is useful for building prompts or validating
            filter expressions without exposing the full schema payload.

        Returns
        -------
        Dict[str, Any] | List[str]
            Either a dict of column metadata or a list of column names,
            depending on ``include_types``.
        """
        cols = self._get_columns()
        return cols if include_types else list(cols)

    @functools.wraps(BaseGuidanceManager.add_guidance, updated=())
    def add_guidance(
        self,
        *,
        title: Optional[str] = None,
        content: Optional[str] = None,
        function_ids: Optional[List[int]] = None,
    ) -> ToolOutcome:
        if not title and not content:
            raise ValueError(
                "At least one field (title/content) must be provided.",
            )
        g = Guidance(
            title=title or "",
            content=content or "",
            function_ids=function_ids or [],
        )
        payload = g.to_post_json()
        log = write_log(
            context=self._ctx,
            **payload,
            new=True,
            mutable=True,
        )
        return {
            "outcome": "guidance created successfully",
            "details": {
                "guidance_id": assigned_row_id(log, "guidance_id", context=self._ctx),
            },
        }

    @functools.wraps(BaseGuidanceManager.update_guidance, updated=())
    def update_guidance(
        self,
        *,
        guidance_id: int,
        title: Optional[str] = None,
        content: Optional[str] = None,
        function_ids: Optional[List[int]] = None,
    ) -> ToolOutcome:
        updates: Dict[str, Any] = {}
        if title is not None:
            updates["title"] = title
        if content is not None:
            updates["content"] = content
        if function_ids is not None:
            # Validate via model validator
            _g = Guidance(
                title=title or "tmp",
                content=content or "tmp",
                function_ids=function_ids,
            )
            updates["function_ids"] = _g.function_ids
        if not updates:
            raise ValueError("At least one field must be provided for an update.")

        self._raise_if_builtin(guidance_id, "updated")
        logs = db.get_logs(
            context=self._ctx,
            filter=f"guidance_id == {int(guidance_id)}",
            limit=2,
            exclude_fields=list_private_fields(self._ctx),
        )
        if not logs:
            raise ValueError(
                f"No guidance found with guidance_id {guidance_id} to update.",
            )
        if len(logs) > 1:
            raise RuntimeError(
                f"Multiple rows found with guidance_id {guidance_id}. Data integrity issue.",
            )
        if function_ids is not None:
            candidate = Guidance(**{**logs[0].entries, **updates})
            updates["stale_reasons"] = [
                reason.model_dump(mode="json")
                for reason in self._missing_function_reasons(
                    candidate,
                    available=self._available_functions_by_id(),
                    preserve_historical=False,
                )
            ]
        db.update_logs(
            logs=[logs[0].id],
            context=self._ctx,
            entries=updates,
            overwrite=True,
        )
        return {"outcome": "guidance updated", "details": {"guidance_id": guidance_id}}

    # ─────────────────────────── Functions helpers ───────────────────────────
    def _get_functions_for_guidance(
        self,
        *,
        guidance_id: int,
        include_implementations: bool = False,
    ) -> List[Dict[str, Any]]:
        """Return metadata for functions linked to a guidance entry.

        Parameters
        ----------
        guidance_id : int
            Identifier of the guidance row whose related functions to fetch.
        include_implementations : bool, default False
            When True, include the function implementation source in the
            payload; otherwise only surface metadata useful for selection.

        Returns
        -------
        list[dict]
            One item per related function, including ``function_id``, ``name``,
            ``argspec``, ``docstring``, ``calls``, and ``precondition`` fields.
        """
        rows = self.filter(filter=f"guidance_id == {int(guidance_id)}", limit=1)
        if not rows:
            return []
        fids = list(dict.fromkeys(int(fid) for fid in (rows[0].function_ids or [])))
        if not fids:
            return []

        # Build a safe filter like: (function_id == 1) or (function_id == 2)
        filt = " or ".join(f"function_id == {int(fid)}" for fid in fids)
        context = self._functions_context()
        funcs = db.get_logs(
            context=context,
            filter=filt,
            exclude_fields=list_private_fields(context),
        )

        out: List[Dict[str, Any]] = []
        for lg in funcs:
            ent = lg.entries
            item: Dict[str, Any] = {
                "function_id": ent.get("function_id"),
                "name": ent.get("name"),
                "argspec": ent.get("argspec"),
                "docstring": ent.get("docstring"),
                "calls": ent.get("calls"),
                "precondition": ent.get("precondition"),
            }
            if include_implementations:
                item["implementation"] = ent.get("implementation")
            out.append(item)
        return out

    def _attach_functions_for_guidance_to_context(
        self,
        *,
        guidance_id: int,
        include_implementations: bool = False,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Attach related functions into the loop context as structured data.

        Returns a dict with keys:
            attached_count: int
            functions: list of function dicts (see _get_functions_for_guidance)
        """
        funcs = self._get_functions_for_guidance(
            guidance_id=guidance_id,
            include_implementations=include_implementations,
        )
        if limit is not None:
            try:
                limit = int(limit)
            except Exception:
                limit = None
            if isinstance(limit, int) and limit >= 0:
                funcs = funcs[:limit]
        return {"attached_count": len(funcs), "functions": funcs}

    @functools.wraps(BaseGuidanceManager.delete_guidance, updated=())
    def delete_guidance(
        self,
        *,
        guidance_id: int,
    ) -> ToolOutcome:
        self._raise_if_builtin(guidance_id, "deleted")
        ids = db.get_logs(
            context=self._ctx,
            filter=f"guidance_id == {int(guidance_id)}",
            limit=2,
            return_ids_only=True,
        )
        if not ids:
            raise ValueError(
                f"No guidance found with guidance_id {guidance_id} to delete.",
            )
        if len(ids) > 1:
            raise RuntimeError(
                f"Multiple rows found with guidance_id {guidance_id}. Data integrity issue.",
            )
        db.delete_logs(context=self._ctx, logs=ids[0])
        return {"outcome": "guidance deleted", "details": {"guidance_id": guidance_id}}

    @functools.wraps(BaseGuidanceManager.reconcile_dependencies, updated=())
    def reconcile_dependencies(
        self,
        *,
        guidance_ids: Optional[List[int]] = None,
    ) -> ToolOutcome:
        filter_expr = (
            " or ".join(
                f"guidance_id == {int(guidance_id)}" for guidance_id in guidance_ids
            )
            if guidance_ids
            else None
        )
        logs = db.get_logs(
            context=self._ctx,
            filter=filter_expr,
            limit=1000,
            exclude_fields=list_private_fields(self._ctx),
        )
        available = self._available_functions_by_id()
        stale_guidance_ids: list[int] = []
        for log in logs:
            guidance = Guidance(**log.entries)
            refreshed = self._missing_function_reasons(
                guidance,
                available=available,
                preserve_historical=True,
            )
            if refreshed:
                stale_guidance_ids.append(int(guidance.guidance_id))
            serialized = [reason.model_dump(mode="json") for reason in refreshed]
            if serialized == [
                reason.model_dump(mode="json") for reason in guidance.stale_reasons
            ]:
                continue
            db.update_logs(
                logs=[log.id],
                context=self._ctx,
                entries={"stale_reasons": serialized},
                overwrite=True,
            )
        return {
            "outcome": "dependencies reconciled",
            "details": {
                "checked": len(logs),
                "stale_guidance_ids": stale_guidance_ids,
                "stale_count": len(stale_guidance_ids),
            },
        }

    @functools.wraps(BaseGuidanceManager.search, updated=())
    def search(
        self,
        *,
        references: Optional[Dict[str, str]] = None,
        k: int = 10,
    ) -> List[Guidance]:
        allowed_fields = list(self._BUILTIN_FIELDS)
        rows = federated_ranked_search(
            [
                self._read_spec(
                    row_filter=self._scoped_filter(None),
                    allowed_fields=allowed_fields,
                ),
                self._builtins_read_spec(
                    row_filter=self._scoped_filter(None),
                    allowed_fields=allowed_fields,
                ),
            ],
            references,
            limit=k,
            backfill=True,
            annotate=False,
        )
        return [self._with_content_preview(Guidance(**r)) for r in rows]

    @functools.wraps(BaseGuidanceManager.filter, updated=())
    def filter(
        self,
        *,
        filter: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> List[Guidance]:
        from_fields = list(self._BUILTIN_FIELDS)
        try:
            rows = federated_filter(
                [
                    self._read_spec(allowed_fields=from_fields),
                    self._builtins_read_spec(allowed_fields=from_fields),
                ],
                filter=self._scoped_filter(normalize_filter_expr(filter)),
                offset=offset,
                limit=limit,
                annotate=False,
            )
        except ToolErrorException as exc:
            return exc.payload
        return [self._with_content_preview(Guidance(**row)) for row in rows]

    @functools.wraps(BaseGuidanceManager.get_guidance, updated=())
    def get_guidance(
        self,
        *,
        guidance_id: int,
    ) -> Guidance:
        from_fields = list(self._BUILTIN_FIELDS)
        rows = federated_filter(
            [
                self._read_spec(allowed_fields=from_fields),
                self._builtins_read_spec(allowed_fields=from_fields),
            ],
            filter=self._scoped_filter(f"guidance_id == {int(guidance_id)}"),
            limit=1,
            annotate=False,
        )
        if not rows:
            raise ValueError(f"No guidance found with guidance_id {guidance_id}.")
        return Guidance(**rows[0])
