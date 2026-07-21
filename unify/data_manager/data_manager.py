"""
Concrete DataManager implementation.

This module provides the concrete DataManager class that implements
BaseDataManager. It delegates to ops/ functions for implementation
and stays thin (orchestration only).

Docstrings are inherited from BaseDataManager via @functools.wraps.
"""

from __future__ import annotations

import functools
import logging
from contextlib import contextmanager
from threading import RLock
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import unisdk


from unify.data_manager.base import BaseDataManager
from unify.data_manager.types.table import TableDescription
from unify.data_manager.types.ingest import (
    IngestExecutionConfig,
    IngestResult,
    PostIngestConfig,
)
from unify.data_manager.ops.table_ops import (
    create_table_impl,
    describe_table_impl,
    get_columns_impl,
    get_table_impl,
    list_tables_impl,
    delete_table_impl,
    rename_table_impl,
    create_column_impl,
    delete_column_impl,
    rename_column_impl,
    create_derived_column_impl,
    create_external_column_impl,
)
from unify.data_manager.ops.query_ops import (
    filter_impl,
    search_impl,
)
from unify.data_manager.ops.mutation_ops import (
    insert_rows_impl,
    update_rows_impl,
    update_by_ids_impl,
    delete_rows_impl,
)
from unify.data_manager.ops.join_ops import (
    join_tables_impl,
    filter_join_impl,
    search_join_impl,
    filter_multi_join_impl,
    search_multi_join_impl,
)
from unify.common.embed_utils import ensure_vector_column as _ensure_vector_column
from unify.common.federated_search import (
    FederatedSearchContext,
    SortSpec,
    default_ranked_fetcher,
    federated_filter,
    federated_ranked_search,
    federated_reduce,
    reduce_grouped_rows,
    reduce_rows,
)
from unify.common.embed_utils import list_private_fields
from unify.common.filter_utils import normalize_filter_expr
from unify.common.join_utils import rewrite_join_paths
from unify.data_manager.ops.ingest_ops import run_ingest
from unify.common.context_registry import (
    TEAM_CONTEXT_PREFIX,
    ContextRegistry,
    TableContext,
)
from unify.common.log_utils import create_logs as unity_create_logs
from unify.common.model_to_fields import model_to_fields
from unify.common.tool_outcome import ToolErrorException
from unify.data_manager.custom_data import compute_custom_data_hash
from unify.data_manager.types.meta import DataMeta
from unify.session_details import SESSION_DETAILS

logger = logging.getLogger(__name__)

DATA_META_TABLE = "Data/Meta"


# Known absolute prefixes that indicate a path should not be resolved
_ABSOLUTE_PREFIXES = (
    "Data/",
    "Dashboards/",
    "Files/",
    "FileRecords/",
    "Teams/",
    "Contacts",
    "Knowledge/",
    "Tasks",
    "Messages",
    "Exchanges",
    TEAM_CONTEXT_PREFIX,
)


class DataManager(BaseDataManager):
    """
    Canonical implementation of data operations for any Unify context.

    See BaseDataManager for full API documentation.
    """

    class Config:
        """Context registration for DataManager's owned namespace."""

        required_contexts = [
            TableContext(
                name="Data",
                description=(
                    "Root namespace for pipeline/API-derived datasets. "
                    "Sub-contexts are created dynamically as Data/project/table paths."
                ),
                fields=None,  # No fixed schema - tables created dynamically
                unique_keys=None,
                auto_counting=None,
            ),
            TableContext(
                name=DATA_META_TABLE,
                description="Metadata for source-defined custom data sync state.",
                fields=model_to_fields(DataMeta),
                unique_keys={"meta_id": "int"},
            ),
        ]

    def __init__(self) -> None:
        """Initialize DataManager with context registration."""
        super().__init__()
        self._base_ctx = ContextRegistry.get_context(self, "Data")
        self._meta_ctx = ContextRegistry.get_context(self, DATA_META_TABLE)
        self._custom_data_synced = False
        self._custom_data_synced_contexts: set[str] = set()
        self._destination_context_lock = RLock()
        self._destination_write_scoped = False

        logger.debug("DataManager initialized with base context: %s", self._base_ctx)

    def _resolve_unique_keys_and_auto_counting(
        self,
        resolved: str,
        unique_keys: Optional[Dict[str, str]],
        auto_counting: Optional[Dict[str, Optional[str]]],
    ) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, Optional[str]]]]:
        """Apply DM-owned defaults for unique keys and auto-counting.

        When *resolved* lives under DataManager's own namespace and the
        caller hasn't supplied explicit values, defaults to a global
        auto-incrementing ``row_id`` column — matching the convention used
        by every other state manager.  For foreign contexts the caller's
        values pass through unchanged.
        """
        if self._data_context_suffix(resolved) is not None:
            if unique_keys is None:
                unique_keys = {"row_id": "int"}
            if auto_counting is None:
                auto_counting = {"row_id": None}
        return unique_keys, auto_counting

    def _resolve_context(self, context: str) -> str:
        """
        Resolve a context path, handling relative and absolute paths.

        Parameters
        ----------
        context : str
            Context path. Can be:
            - Relative: "projects/housing" → resolved to "{base_ctx}/projects/housing"
            - Short-form absolute: "Data/examplehousing/arrears", "Contacts" → used as-is
            - Fully-qualified: "org123/42/Contacts", "org123/42/Data/foo" → as-is

        Returns
        -------
        str
            Fully resolved context path.
        """
        context = context.lstrip("/")
        if not context:
            raise ValueError("Empty context path")

        # Short-form absolute: starts with a known context root name
        if any(context.startswith(p) for p in _ABSOLUTE_PREFIXES):
            return context

        # Fully-qualified: shares the org/assistant scope with our base context.
        # _base_ctx = "org/42/Data" → scope = "org/42/" →
        # "org/42/Contacts" is recognised as already-qualified.
        if self._base_ctx and "/" in self._base_ctx:
            scope = self._base_ctx.rsplit("/", 1)[0] + "/"
            if context.startswith(scope):
                return context

        # Relative path: prepend base context
        return f"{self._base_ctx}/{context}" if self._base_ctx else context

    def _data_root_from_registry_root(self, root_context: str) -> str:
        """Return the concrete Data namespace for a registry root."""

        return f"{root_context.strip('/')}/Data"

    def _data_context_suffix(self, context: str) -> str | None:
        """Return a Data-relative suffix when a context belongs to DataManager."""

        context = context.lstrip("/")
        if not context:
            raise ValueError("Empty context path")

        if context == "Data":
            return ""
        if context.startswith("Data/"):
            return context[len("Data/") :]

        base = (self._base_ctx or "").strip("/")
        if base:
            if context == base:
                return ""
            if context.startswith(base + "/"):
                return context[len(base) + 1 :]

        if context.startswith(TEAM_CONTEXT_PREFIX):
            parts = context.split("/", 3)
            if len(parts) >= 3 and parts[2] == "Data":
                return parts[3] if len(parts) == 4 else ""
            return None

        if any(context.startswith(p) for p in _ABSOLUTE_PREFIXES):
            return None

        if base and "/" in base:
            scope = base.rsplit("/", 1)[0] + "/"
            if context.startswith(scope):
                return None

        return context

    def _context_under_data_root(self, root_context: str, suffix: str) -> str:
        data_root = self._data_root_from_registry_root(root_context)
        return f"{data_root}/{suffix}" if suffix else data_root

    def _is_exact_data_context(self, context: str) -> bool:
        """Return whether a Data context should be read exactly as supplied."""

        context = context.lstrip("/")
        base = (self._base_ctx or "").strip("/")
        return context.startswith(TEAM_CONTEXT_PREFIX) or bool(
            base and (context == base or context.startswith(base + "/")),
        )

    def _resolve_context_for_write(
        self,
        context: str,
        *,
        destination: str | None = None,
    ) -> str:
        """Resolve a write target, routing Data-owned contexts when requested."""

        suffix = self._data_context_suffix(context)
        if suffix is None:
            if destination is None:
                return self._resolve_context(context)
            raise ContextRegistry._invalid_destination(
                "Data",
                destination,
                "Destination can only be used with Data-owned contexts.",
            )

        root_context = ContextRegistry.write_root(
            self,
            "Data",
            destination=destination,
        )
        return self._context_under_data_root(root_context, suffix)

    def _resolve_contexts_for_read(self, context: str) -> list[str]:
        """Return ordered readable contexts for Data-owned reads."""

        suffix = self._data_context_suffix(context)
        if suffix is None or self._is_exact_data_context(context):
            return [self._resolve_context(context)]

        try:
            root_contexts = ContextRegistry.read_roots(self, "Data")
        except RuntimeError as exc:
            if "no base context available" not in str(exc):
                raise
            root_contexts = [
                f"{TEAM_CONTEXT_PREFIX}{team_id}"
                for team_id in SESSION_DETAILS.team_ids
            ]
            if not root_contexts:
                return [self._resolve_context(context)]
        contexts = [
            self._context_under_data_root(root, suffix) for root in root_contexts
        ]
        return list(dict.fromkeys(contexts))

    def _first_successful_read_context(self, context: str) -> str:
        """Return the first readable context that exists for metadata operations."""

        last_error: Exception | None = None
        for resolved in self._resolve_contexts_for_read(context):
            try:
                get_table_impl(resolved)
                return resolved
            except Exception as exc:
                last_error = exc
        if last_error:
            raise last_error
        return self._resolve_context(context)

    def _resolve_join_context_groups(self, tables: list[str]) -> list[list[str]]:
        """Resolve table names into root-aligned context groups for join reads."""

        context_options = [self._resolve_contexts_for_read(table) for table in tables]
        group_count = max(len(options) for options in context_options)
        if group_count == 1:
            return [[options[0] for options in context_options]]

        groups: list[list[str]] = []
        for index in range(group_count):
            group: list[str] = []
            for options in context_options:
                if len(options) == 1:
                    group.append(options[0])
                elif len(options) == group_count:
                    group.append(options[index])
                else:
                    raise RuntimeError("Mismatched Data read roots for join inputs.")
            groups.append(group)
        return groups

    def _rewrite_join_inputs(
        self,
        tables: list[str],
        resolved_tables: list[str],
        join_expr: str,
        select: Dict[str, str],
    ) -> tuple[str, Dict[str, str]]:
        """Rewrite a join expression and selected columns for one root group."""

        return rewrite_join_paths(tables, resolved_tables, join_expr, select)

    def _collect_join_rows(
        self,
        *,
        tables: list[str],
        join_expr: str,
        select: Dict[str, str],
        mode: str,
        left_where: Optional[str],
        right_where: Optional[str],
        result_where: Optional[str],
        limit: int | None,
    ) -> list[dict[str, Any]]:
        """Collect joined rows from every readable root group."""

        rows: list[dict[str, Any]] = []
        last_error: Exception | None = None
        for resolved_tables in self._resolve_join_context_groups(tables):
            rewritten_expr, rewritten_select = self._rewrite_join_inputs(
                tables,
                resolved_tables,
                join_expr,
                select,
            )
            try:
                offset = 0
                context_rows: list[dict[str, Any]] = []
                while True:
                    page_limit = (
                        1000 if limit is None else min(1000, limit - len(context_rows))
                    )
                    if page_limit <= 0:
                        break
                    page = filter_join_impl(
                        tables=resolved_tables,
                        join_expr=rewritten_expr,
                        select=rewritten_select,
                        mode=mode,
                        left_where=left_where,
                        right_where=right_where,
                        result_where=result_where,
                        result_limit=page_limit,
                        result_offset=offset,
                    )
                    context_rows.extend(page)
                    if len(page) < page_limit or (
                        limit is not None and len(context_rows) >= limit
                    ):
                        break
                    offset += page_limit
                rows.extend(context_rows)
            except Exception as exc:
                last_error = exc
                continue
        if not rows and last_error is not None:
            raise last_error
        return rows

    def _multi_join_table_names(self, joins: List[Dict[str, Any]]) -> list[str]:
        """Return stable table references used by a multi-join plan."""

        table_names: list[str] = []
        for step in joins:
            raw_tables = step.get("tables")
            raw_tables = [raw_tables] if isinstance(raw_tables, str) else raw_tables
            if not isinstance(raw_tables, list):
                continue
            for table in raw_tables:
                if table in {"$prev", "__prev__", "_"}:
                    continue
                if isinstance(table, str) and table not in table_names:
                    table_names.append(table)
        return table_names

    def _multi_join_context_resolvers(
        self,
        joins: List[Dict[str, Any]],
    ) -> list[Callable[[str], str]]:
        """Return root-aligned context resolvers for multi-join reads."""

        table_names = self._multi_join_table_names(joins)
        if not table_names:
            return [self._first_successful_read_context]

        groups = self._resolve_join_context_groups(table_names)
        resolvers = []
        for resolved_group in groups:
            table_map = dict(zip(table_names, resolved_group))

            def resolve(table_name: str, table_map: dict[str, str] = table_map) -> str:
                return table_map.get(table_name) or self._first_successful_read_context(
                    table_name,
                )

            resolvers.append(resolve)
        return resolvers

    def _rewrite_multi_join_for_resolver(
        self,
        joins: List[Dict[str, Any]],
        resolver: Callable[[str], str],
    ) -> List[Dict[str, Any]]:
        """Rewrite multi-join table references for one readable root."""

        rewritten_joins: list[dict[str, Any]] = []
        for step in joins:
            rewritten_step = step.copy()
            raw_tables = rewritten_step.get("tables")
            raw_tables = [raw_tables] if isinstance(raw_tables, str) else raw_tables
            if not isinstance(raw_tables, list):
                rewritten_joins.append(rewritten_step)
                continue

            originals: list[str] = []
            resolved: list[str] = []
            rewritten_tables: list[str] = []
            for table in raw_tables:
                if table in {"$prev", "__prev__", "_"} or not isinstance(table, str):
                    rewritten_tables.append(table)
                    continue
                resolved_table = resolver(table)
                originals.append(table)
                resolved.append(resolved_table)
                rewritten_tables.append(resolved_table)

            rewritten_step["tables"] = rewritten_tables
            join_expr = rewritten_step.get("join_expr")
            select = rewritten_step.get("select")
            if isinstance(join_expr, str) and isinstance(select, dict):
                rewritten_expr, rewritten_select = rewrite_join_paths(
                    originals,
                    resolved,
                    join_expr,
                    select,
                )
                rewritten_step["join_expr"] = rewritten_expr
                rewritten_step["select"] = rewritten_select
            for where_key in ("left_where", "right_where"):
                where_expr = rewritten_step.get(where_key)
                if isinstance(where_expr, str):
                    for original, resolved_table in zip(originals, resolved):
                        where_expr = where_expr.replace(original, resolved_table)
                    rewritten_step[where_key] = where_expr
            rewritten_joins.append(rewritten_step)
        return rewritten_joins

    @staticmethod
    def _tool_error(exc: ToolErrorException) -> Dict[str, Any]:
        """Return the structured tool-error payload carried by *exc*."""

        return dict(exc.payload)

    # ──────────────────────────────────────────────────────────────────────────
    # Table Management
    # ──────────────────────────────────────────────────────────────────────────

    @functools.wraps(BaseDataManager.create_table, updated=())
    def create_table(
        self,
        context: str,
        *,
        description: Optional[str] = None,
        fields: Optional[Dict[str, Any]] = None,
        unique_keys: Optional[Dict[str, str]] = None,
        auto_counting: Optional[Dict[str, Optional[str]]] = None,
        destination: str | None = None,
    ) -> str:
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        unique_keys, auto_counting = self._resolve_unique_keys_and_auto_counting(
            resolved,
            unique_keys,
            auto_counting,
        )
        return create_table_impl(
            resolved,
            description=description,
            fields=fields,
            unique_keys=unique_keys,
            auto_counting=auto_counting,
        )

    @functools.wraps(BaseDataManager.describe_table, updated=())
    def describe_table(self, context: str) -> TableDescription:
        resolved = self._first_successful_read_context(context)
        return describe_table_impl(resolved)

    @functools.wraps(BaseDataManager.get_columns, updated=())
    def get_columns(self, table: str) -> Dict[str, Any]:
        resolved = self._first_successful_read_context(table)
        return get_columns_impl(resolved)

    @functools.wraps(BaseDataManager.get_table, updated=())
    def get_table(self, context: str) -> Dict[str, Any]:
        resolved = self._first_successful_read_context(context)
        return get_table_impl(resolved)

    @functools.wraps(BaseDataManager.list_tables, updated=())
    def list_tables(
        self,
        *,
        prefix: Optional[str] = None,
        include_column_info: bool = True,
    ) -> Union[List[str], Dict[str, Any]]:
        resolved_prefixes = (
            self._resolve_contexts_for_read(prefix) if prefix else [None]
        )
        merged: Union[List[str], Dict[str, Any]]
        merged = {} if include_column_info else []
        for resolved_prefix in resolved_prefixes:
            result = list_tables_impl(
                prefix=resolved_prefix,
                include_column_info=include_column_info,
            )
            if include_column_info:
                assert isinstance(merged, dict)
                if isinstance(result, dict):
                    merged.update(result)
            else:
                assert isinstance(merged, list)
                if isinstance(result, list):
                    merged.extend(result)
        if isinstance(merged, list):
            return sorted(dict.fromkeys(merged))
        return merged

    @functools.wraps(BaseDataManager.delete_table, updated=())
    def delete_table(
        self,
        context: str,
        *,
        dangerous_ok: bool = False,
        destination: str | None = None,
    ) -> None:
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        delete_table_impl(resolved, dangerous_ok=dangerous_ok)

    @functools.wraps(BaseDataManager.rename_table, updated=())
    def rename_table(
        self,
        old_context: str,
        new_context: str,
        *,
        destination: str | None = None,
    ) -> Dict[str, str]:
        try:
            resolved_old = self._resolve_context_for_write(
                old_context,
                destination=destination,
            )
            resolved_new = self._resolve_context_for_write(
                new_context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        return rename_table_impl(resolved_old, resolved_new)

    # ──────────────────────────────────────────────────────────────────────────
    # Column Operations
    # ──────────────────────────────────────────────────────────────────────────

    @functools.wraps(BaseDataManager.create_column, updated=())
    def create_column(
        self,
        context: str,
        *,
        column_name: str,
        column_type: str,
        mutable: bool = True,
        backfill_logs: bool = False,
        destination: str | None = None,
    ) -> Dict[str, str]:
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        return create_column_impl(
            resolved,
            column_name=column_name,
            column_type=column_type,
            mutable=mutable,
            backfill_logs=backfill_logs,
        )

    @functools.wraps(BaseDataManager.delete_column, updated=())
    def delete_column(
        self,
        context: str,
        *,
        column_name: str,
        destination: str | None = None,
    ) -> Dict[str, str]:
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        return delete_column_impl(resolved, column_name=column_name)

    @functools.wraps(BaseDataManager.rename_column, updated=())
    def rename_column(
        self,
        context: str,
        *,
        old_name: str,
        new_name: str,
        destination: str | None = None,
    ) -> Dict[str, str]:
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        return rename_column_impl(resolved, old_name=old_name, new_name=new_name)

    @functools.wraps(BaseDataManager.create_derived_column, updated=())
    def create_derived_column(
        self,
        context: str,
        *,
        column_name: str,
        equation: str,
        destination: str | None = None,
    ) -> Dict[str, str]:
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        return create_derived_column_impl(
            resolved,
            column_name=column_name,
            equation=equation,
        )

    @functools.wraps(BaseDataManager.create_external_column, updated=())
    def create_external_column(
        self,
        context: str,
        *,
        column_name: str,
        connector_id: str,
        binding: Dict[str, Any],
        column_type: str = "Any",
        destination: str | None = None,
    ) -> Dict[str, Any]:
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        return create_external_column_impl(
            resolved,
            column_name=column_name,
            connector_id=connector_id,
            binding=binding,
            column_type=column_type,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Query Operations
    # ──────────────────────────────────────────────────────────────────────────

    @functools.wraps(BaseDataManager.filter, updated=())
    def filter(
        self,
        context: str,
        *,
        filter: Optional[str] = None,
        columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: Optional[str] = None,
        descending: bool = False,
        return_ids_only: bool = False,
        include_ids: bool = False,
        hydrate: Optional[str] = None,
        hydrate_fields: Optional[List[str]] = None,
        materialize: Optional[bool] = None,
    ) -> Union[List[Dict[str, Any]], List[int]]:
        if return_ids_only and include_ids:
            raise ValueError("return_ids_only and include_ids are mutually exclusive")
        resolved_contexts = self._resolve_contexts_for_read(context)
        if include_ids and len(resolved_contexts) != 1:
            raise ValueError(
                "include_ids requires a single resolved context; "
                "pass a fully-qualified path rather than a federated name",
            )
        if len(resolved_contexts) == 1 or return_ids_only:
            return filter_impl(
                resolved_contexts[0],
                filter=filter,
                columns=columns,
                exclude_columns=exclude_columns,
                limit=limit,
                offset=offset,
                order_by=order_by,
                descending=descending,
                return_ids_only=return_ids_only,
                include_ids=include_ids,
                hydrate=hydrate,
                hydrate_fields=hydrate_fields,
                materialize=materialize,
            )

        sorting = None
        if order_by:
            sorting = [
                SortSpec(
                    order_by,
                    direction="descending" if descending else "ascending",
                ),
            ]

        def _excluded(resolved: str) -> Optional[List[str]]:
            # Mirror filter_impl: auto-hide private fields unless the caller
            # picked columns or supplied an explicit exclusion list.
            if exclude_columns is not None:
                return exclude_columns
            if columns is None:
                return list_private_fields(resolved)
            return None

        return federated_filter(
            [
                FederatedSearchContext(
                    context=resolved,
                    source=resolved,
                    allowed_fields=columns,
                    excluded_fields=_excluded(resolved),
                )
                for resolved in resolved_contexts
            ],
            filter=normalize_filter_expr(filter),
            sorting=sorting,
            offset=offset,
            limit=limit,
            annotate=False,
        )

    @functools.wraps(BaseDataManager.search, updated=())
    def search(
        self,
        context: str,
        *,
        references: Optional[Dict[str, str]] = None,
        k: int = 10,
        filter: Optional[str] = None,
        columns: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        resolved_contexts = self._resolve_contexts_for_read(context)
        if len(resolved_contexts) == 1:
            return search_impl(
                resolved_contexts[0],
                references=references,
                k=k,
                filter=filter,
                columns=columns,
            )

        if k < 1 or k > 1000:
            raise ValueError("k must be between 1 and 1000")

        errors: list[Exception] = []

        def fetcher(spec, refs, fetch_limit):
            try:
                return default_ranked_fetcher(spec, refs, fetch_limit)
            except Exception as exc:
                errors.append(exc)
                return [], ""

        rows = federated_ranked_search(
            [
                FederatedSearchContext(
                    context=resolved,
                    source=resolved,
                    row_filter=normalize_filter_expr(filter),
                    allowed_fields=columns,
                )
                for resolved in resolved_contexts
            ],
            references,
            limit=k,
            fetcher=fetcher,
            backfill=True,
            annotate=False,
        )
        if not rows and errors:
            raise errors[-1]
        return rows

    @functools.wraps(BaseDataManager.reduce, updated=())
    def reduce(
        self,
        context: str,
        *,
        metric: str,
        columns: Union[str, List[str]],
        filter: Optional[str] = None,
        group_by: Optional[Union[str, List[str]]] = None,
    ) -> Any:
        return federated_reduce(
            [
                FederatedSearchContext(context=resolved, source=resolved)
                for resolved in self._resolve_contexts_for_read(context)
            ],
            metric=metric,
            columns=columns,
            filter=normalize_filter_expr(filter),
            group_by=group_by,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Join Operations
    # ──────────────────────────────────────────────────────────────────────────

    @functools.wraps(BaseDataManager.join_tables, updated=())
    def join_tables(
        self,
        *,
        left_table: str,
        right_table: str,
        join_expr: str,
        dest_table: str,
        select: Dict[str, str],
        mode: str = "inner",
        left_where: Optional[str] = None,
        right_where: Optional[str] = None,
        destination: str | None = None,
    ) -> str:
        try:
            resolved_dest = self._resolve_context_for_write(
                dest_table,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        resolved_left = self._first_successful_read_context(left_table)
        resolved_right = self._first_successful_read_context(right_table)
        join_expr, select = rewrite_join_paths(
            [left_table, right_table],
            [resolved_left, resolved_right],
            join_expr,
            select,
        )
        return join_tables_impl(
            left_table=resolved_left,
            right_table=resolved_right,
            join_expr=join_expr,
            dest_table=resolved_dest,
            select=select,
            mode=mode,
            left_where=left_where,
            right_where=right_where,
        )

    @functools.wraps(BaseDataManager.filter_join, updated=())
    def filter_join(
        self,
        *,
        tables: Union[str, List[str]],
        join_expr: str,
        select: Dict[str, str],
        mode: str = "inner",
        left_where: Optional[str] = None,
        right_where: Optional[str] = None,
        result_where: Optional[str] = None,
        result_limit: int = 100,
        result_offset: int = 0,
    ) -> List[Dict[str, Any]]:
        # Resolve table contexts
        if isinstance(tables, str):
            tables = [tables]
        rows = self._collect_join_rows(
            tables=tables,
            join_expr=join_expr,
            select=select,
            mode=mode,
            left_where=left_where,
            right_where=right_where,
            result_where=result_where,
            limit=result_offset + result_limit,
        )
        return rows[result_offset : result_offset + result_limit]

    @functools.wraps(BaseDataManager.reduce_join, updated=())
    def reduce_join(
        self,
        *,
        tables: Union[str, List[str]],
        join_expr: str,
        select: Dict[str, str],
        metric: str,
        columns: Union[str, List[str]],
        mode: str = "inner",
        left_where: Optional[str] = None,
        right_where: Optional[str] = None,
        result_where: Optional[str] = None,
        group_by: Optional[Union[str, List[str]]] = None,
    ) -> Any:
        if isinstance(tables, str):
            tables = [tables]
        rows = self._collect_join_rows(
            tables=tables,
            join_expr=join_expr,
            select=select,
            mode=mode,
            left_where=left_where,
            right_where=right_where,
            result_where=result_where,
            limit=None,
        )
        if group_by is not None:
            return reduce_grouped_rows(
                rows,
                metric=metric,
                columns=columns,
                group_by=group_by,
            )
        return reduce_rows(rows, metric=metric, columns=columns)

    @functools.wraps(BaseDataManager.search_join, updated=())
    def search_join(
        self,
        *,
        tables: Union[str, List[str]],
        join_expr: str,
        select: Dict[str, str],
        mode: str = "inner",
        left_where: Optional[str] = None,
        right_where: Optional[str] = None,
        references: Optional[Dict[str, str]] = None,
        k: int = 10,
        filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        # Resolve table contexts
        if isinstance(tables, str):
            tables = [tables]
        rows: list[dict[str, Any]] = []
        last_error: Exception | None = None
        for resolved_tables in self._resolve_join_context_groups(tables):
            rewritten_expr, rewritten_select = self._rewrite_join_inputs(
                tables,
                resolved_tables,
                join_expr,
                select,
            )
            try:
                rows.extend(
                    search_join_impl(
                        tables=resolved_tables,
                        join_expr=rewritten_expr,
                        select=rewritten_select,
                        mode=mode,
                        left_where=left_where,
                        right_where=right_where,
                        references=references,
                        k=k,
                        filter=filter,
                        tmp_context_prefix=self._base_ctx,
                    ),
                )
            except Exception as exc:
                last_error = exc
                continue
        if not rows and last_error is not None:
            raise last_error
        return rows[:k]

    @functools.wraps(BaseDataManager.filter_multi_join, updated=())
    def filter_multi_join(
        self,
        *,
        joins: List[Dict[str, Any]],
        result_where: Optional[str] = None,
        result_limit: int = 100,
        result_offset: int = 0,
    ) -> List[Dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        target_count = result_offset + result_limit
        last_error: Exception | None = None
        for resolver in self._multi_join_context_resolvers(joins):
            resolved_joins = self._rewrite_multi_join_for_resolver(joins, resolver)
            try:
                rows.extend(
                    filter_multi_join_impl(
                        joins=resolved_joins,
                        context_resolver=lambda table_name: table_name,
                        result_where=result_where,
                        result_limit=target_count,
                        result_offset=0,
                        tmp_context_prefix=self._base_ctx,
                    ),
                )
            except Exception as exc:
                last_error = exc
                continue
        if not rows and last_error is not None:
            raise last_error
        return rows[result_offset : result_offset + result_limit]

    @functools.wraps(BaseDataManager.search_multi_join, updated=())
    def search_multi_join(
        self,
        *,
        joins: List[Dict[str, Any]],
        references: Optional[Dict[str, str]] = None,
        k: int = 10,
        filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        last_error: Exception | None = None
        for resolver in self._multi_join_context_resolvers(joins):
            resolved_joins = self._rewrite_multi_join_for_resolver(joins, resolver)
            try:
                rows.extend(
                    search_multi_join_impl(
                        joins=resolved_joins,
                        context_resolver=lambda table_name: table_name,
                        references=references,
                        k=k,
                        filter=filter,
                        tmp_context_prefix=self._base_ctx,
                    ),
                )
            except Exception as exc:
                last_error = exc
                continue
        if not rows and last_error is not None:
            raise last_error
        return rows[:k]

    # ──────────────────────────────────────────────────────────────────────────
    # Mutation Operations
    # ──────────────────────────────────────────────────────────────────────────

    @functools.wraps(BaseDataManager.insert_rows, updated=())
    def insert_rows(
        self,
        context: str,
        rows: List[Dict[str, Any]],
        *,
        batched: bool = True,
        on_duplicate: Optional[str] = None,
        destination: str | None = None,
    ) -> List[int]:
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        return insert_rows_impl(
            resolved,
            rows,
            batched=batched,
            on_duplicate=on_duplicate,
        )

    @functools.wraps(BaseDataManager.update_rows, updated=())
    def update_rows(
        self,
        context: str,
        updates: Dict[str, Any],
        *,
        filter: Optional[str] = None,
        log_ids: Optional[List[int]] = None,
        overwrite: bool = False,
        destination: str | None = None,
    ) -> int:
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        return update_rows_impl(
            resolved,
            updates,
            filter=filter,
            log_ids=log_ids,
            overwrite=overwrite,
        )

    @functools.wraps(BaseDataManager.update_by_ids, updated=())
    def update_by_ids(
        self,
        log_ids: List[int],
        updates: Dict[str, Any],
        *,
        overwrite: bool = True,
        context: Optional[str] = None,
    ) -> int:
        resolved = None
        if context is not None:
            try:
                resolved = self._resolve_context_for_write(context)
            except ToolErrorException as exc:
                return self._tool_error(exc)  # type: ignore[return-value]
        return update_by_ids_impl(
            log_ids,
            updates,
            overwrite=overwrite,
            context=resolved,
        )

    @functools.wraps(BaseDataManager.delete_rows, updated=())
    def delete_rows(
        self,
        context: str,
        *,
        filter: Optional[str] = None,
        log_ids: Optional[List[int]] = None,
        dangerous_ok: bool = False,
        delete_empty_rows: bool = False,
        destination: str | None = None,
    ) -> int:
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        return delete_rows_impl(
            resolved,
            filter=filter,
            log_ids=log_ids,
            dangerous_ok=dangerous_ok,
            delete_empty_rows=delete_empty_rows,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # High-Level Ingestion
    # ──────────────────────────────────────────────────────────────────────────

    @functools.wraps(BaseDataManager.ingest, updated=())
    def ingest(
        self,
        context: str,
        rows: Optional[List[Dict[str, Any]]] = None,
        *,
        table_input_handle=None,
        description: Optional[str] = None,
        fields: Optional[Dict[str, Any]] = None,
        unique_keys: Optional[Dict[str, str]] = None,
        embed_columns: Optional[List[str]] = None,
        embed_strategy: str = "along",
        chunk_size: int = 1000,
        auto_counting: Optional[Dict[str, Optional[str]]] = None,
        infer_untyped_fields: bool = False,
        execution: Optional["IngestExecutionConfig"] = None,
        post_ingest: Optional["PostIngestConfig"] = None,
        on_task_complete=None,
        coerce_types: bool = True,
        storage_client=None,
        skip_rows: int = 0,
        destination: str | None = None,
        expected_total_rows: int | None = None,
        private_ingest_key_column: str = "",
        private_ingest_key_prefix: str = "",
        before_insert_chunk=None,
    ) -> "IngestResult":
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        unique_keys, auto_counting = self._resolve_unique_keys_and_auto_counting(
            resolved,
            unique_keys,
            auto_counting,
        )
        return run_ingest(
            self,
            resolved,
            rows,
            table_input_handle=table_input_handle,
            description=description,
            fields=fields,
            unique_keys=unique_keys,
            embed_columns=embed_columns,
            embed_strategy=embed_strategy,
            chunk_size=chunk_size,
            auto_counting=auto_counting,
            infer_untyped_fields=infer_untyped_fields,
            execution=execution,
            post_ingest=post_ingest,
            on_task_complete=on_task_complete,
            coerce_types=coerce_types,
            storage_client=storage_client,
            skip_rows=skip_rows,
            expected_total_rows=expected_total_rows,
            private_ingest_key_column=private_ingest_key_column,
            private_ingest_key_prefix=private_ingest_key_prefix,
            before_insert_chunk=before_insert_chunk,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Embedding Operations
    # ──────────────────────────────────────────────────────────────────────────

    @functools.wraps(BaseDataManager.ensure_vector_column, updated=())
    def ensure_vector_column(
        self,
        context: str,
        *,
        source_column: str,
        target_column: Optional[str] = None,
        async_embeddings: bool = False,
        destination: str | None = None,
    ) -> str:
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        target = target_column or f"_{source_column}_emb"
        _ensure_vector_column(
            context=resolved,
            embed_column=target,
            source_column=source_column,
            derived_expr=None,
            from_ids=None,
            async_embeddings=async_embeddings,
        )
        return target

    @functools.wraps(BaseDataManager.vectorize_rows, updated=())
    def vectorize_rows(
        self,
        context: str,
        *,
        source_column: str,
        target_column: Optional[str] = None,
        row_ids: Optional[List[int]] = None,
        batch_size: int = 100,
        async_embeddings: bool = False,
        destination: str | None = None,
    ) -> int:
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
        except ToolErrorException as exc:
            return self._tool_error(exc)  # type: ignore[return-value]
        target = target_column or f"_{source_column}_emb"
        _ensure_vector_column(
            context=resolved,
            embed_column=target,
            source_column=source_column,
            derived_expr=None,
            from_ids=row_ids,
            async_embeddings=async_embeddings,
        )
        return len(row_ids) if row_ids else 0

    def _meta_context_for_destination(self, destination: str | None) -> str:
        """Resolve a public destination into one concrete Data/Meta context."""
        root_context = ContextRegistry.write_root(
            self,
            DATA_META_TABLE,
            destination=destination,
        )
        return f"{root_context.strip('/')}/{DATA_META_TABLE}"

    @contextmanager
    def _temporary_meta_context(self, context: str):
        """Temporarily bind meta storage to a resolved destination context."""
        with self._destination_context_lock:
            original = self._meta_ctx
            was_write_scoped = self._destination_write_scoped
            self._meta_ctx = context
            self._destination_write_scoped = True
            try:
                yield
            finally:
                self._meta_ctx = original
                self._destination_write_scoped = was_write_scoped

    def _sync_destination_contexts(
        self,
        destination: str | None,
    ) -> tuple[str, bool]:
        """Return destination-scoped meta context and personal flag."""
        meta_context = self._meta_context_for_destination(destination)
        return meta_context, destination in (None, "personal")

    def _get_stored_custom_data_hash(self) -> str:
        try:
            logs = unisdk.get_logs(
                context=self._meta_ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                return logs[0].entries.get("custom_data_hash", "") or ""
        except Exception as exc:
            logger.warning("Failed to read custom data hash: %s", exc)
        return ""

    def _store_custom_data_hash(self, hash_value: str) -> None:
        try:
            logs = unisdk.get_logs(
                context=self._meta_ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                unisdk.update_logs(
                    context=self._meta_ctx,
                    logs=[logs[0].id],
                    entries={"custom_data_hash": hash_value},
                    overwrite=True,
                )
            else:
                unity_create_logs(
                    context=self._meta_ctx,
                    entries=[{"meta_id": 1, "custom_data_hash": hash_value}],
                    stamp_authoring=True,
                )
        except Exception as exc:
            logger.warning("Failed to store custom data hash: %s", exc)

    def _table_exists(
        self,
        context: str,
        destination: str | None,
    ) -> bool:
        try:
            resolved = self._resolve_context_for_write(
                context,
                destination=destination,
            )
            get_table_impl(resolved)
            return True
        except Exception:
            return False

    def _table_key_info(
        self,
        context: str,
        destination: str | None,
    ) -> tuple[str, bool]:
        """Return the table's primary unique key and whether it is auto-counted.

        Auto-counted keys (the synthetic ``row_id`` default) are assigned by
        the server and must be stripped from writes. Value keys (``slug``,
        ``campaign_slug``, …) are real data columns that form the row's
        composite identity and must be preserved on insert.
        """

        resolved = self._first_successful_read_context(context)
        ctx_info = get_table_impl(resolved)
        keys = ctx_info.get("unique_keys")
        if isinstance(keys, list) and keys:
            key = str(keys[0])
        elif isinstance(keys, str) and keys:
            key = keys
        elif isinstance(keys, dict) and keys:
            key = next(iter(keys))
        else:
            key = "row_id"
        auto_counting = ctx_info.get("auto_counting")
        auto_counted_keys = (
            set(auto_counting.keys()) if isinstance(auto_counting, dict) else set()
        )
        return key, key in auto_counted_keys or key == "row_id"

    def _get_custom_rows_for_table(
        self,
        context: str,
        destination: str | None,
    ) -> Dict[str, Dict[str, Any]]:
        resolved = self._resolve_context_for_write(
            context,
            destination=destination,
        )
        rows = filter_impl(
            resolved,
            filter="custom_hash != None",
            limit=1000,
        )
        unique_key, _ = self._table_key_info(context, destination)
        indexed: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            custom_key = row.get("custom_key")
            if not custom_key:
                continue
            indexed[str(custom_key)] = row
            if unique_key in row:
                indexed[str(custom_key)][unique_key] = row[unique_key]
        return indexed

    def _find_unmanaged_row_by_seed(
        self,
        *,
        context: str,
        seed_key: str,
        seed_value: str,
        destination: str | None,
    ) -> Optional[Dict[str, Any]]:
        resolved = self._resolve_context_for_write(
            context,
            destination=destination,
        )
        rows = filter_impl(
            resolved,
            filter=f"{seed_key} == '{seed_value}' and custom_hash == None",
            limit=1,
        )
        return rows[0] if rows else None

    def _insert_custom_row(
        self,
        *,
        context: str,
        row_data: Dict[str, Any],
        destination: str | None,
    ) -> None:
        unique_key, auto_counted = self._table_key_info(context, destination)
        clean = {
            k: v for k, v in row_data.items() if not (auto_counted and k == unique_key)
        }
        self.insert_rows(context, [clean], destination=destination)

    def _update_custom_row(
        self,
        *,
        context: str,
        row_filter: str,
        row_data: Dict[str, Any],
        destination: str | None,
    ) -> None:
        """Update one sync-managed row addressed by a caller-supplied filter.

        Sync rows are identified by their sync identity (``custom_key`` for
        managed rows, the seed column for adoption), never by the storage
        key: tables may have value unique keys, auto-counted keys, or no
        unique keys at all.
        """

        unique_key, auto_counted = self._table_key_info(context, destination)
        clean = {
            k: v for k, v in row_data.items() if not (auto_counted and k == unique_key)
        }
        self.update_rows(
            context,
            updates=clean,
            filter=row_filter,
            destination=destination,
        )

    def _delete_custom_row_by_key(
        self,
        *,
        context: str,
        custom_key: str,
        destination: str | None,
    ) -> None:
        self.delete_rows(
            context,
            filter=f"custom_key == '{custom_key}' and custom_hash != None",
            destination=destination,
        )

    def sync_custom_data(
        self,
        *,
        source_tables: Optional[Dict[str, Dict[str, Any]]] = None,
        destination: str | None = None,
    ) -> bool:
        """Ensure deployment-defined data rows match source definitions."""
        try:
            meta_context, is_personal = self._sync_destination_contexts(destination)
        except ToolErrorException as exc:
            logger.warning(
                "Skipping custom data sync for destination %r: %s",
                destination,
                exc.payload,
            )
            return False
        with self._temporary_meta_context(meta_context):
            if source_tables is None:
                source_tables = {}
            expected_hash = compute_custom_data_hash(
                source_tables=source_tables,
            )
            current_hash = self._get_stored_custom_data_hash()
            already_synced = (
                self._custom_data_synced
                if is_personal
                else meta_context in self._custom_data_synced_contexts
            )

            if already_synced and current_hash == expected_hash:
                return False

            if current_hash == expected_hash:
                logger.debug("Custom data hash matches, skipping sync")
                if is_personal:
                    self._custom_data_synced = True
                else:
                    self._custom_data_synced_contexts.add(meta_context)
                return False

            logger.info(
                "Custom data hash mismatch " "(current=%s, expected=%s), syncing...",
                current_hash,
                expected_hash,
            )

            processed_keys_by_context: Dict[str, Set[str]] = {}

            for context, table_spec in source_tables.items():
                rows = table_spec.get("rows", [])
                if not rows:
                    continue
                seed_key = table_spec.get("seed_key")
                if not seed_key:
                    logger.warning(
                        "Data table %s has no seed_key, skipping",
                        context,
                    )
                    continue

                if not self._table_exists(context, destination):
                    unique_keys = table_spec.get("unique_keys")
                    auto_counting = table_spec.get("auto_counting")
                    self.create_table(
                        context,
                        description=table_spec.get("description"),
                        fields=table_spec.get("fields"),
                        unique_keys=unique_keys,
                        auto_counting=auto_counting,
                        destination=destination,
                    )

                db_rows = self._get_custom_rows_for_table(context, destination)
                processed_keys: Set[str] = set()
                processed_keys_by_context[context] = processed_keys

                for row in rows:
                    custom_key = str(row.get("custom_key", ""))
                    if not custom_key:
                        continue
                    processed_keys.add(custom_key)
                    seed_value = str(row.get(seed_key, ""))
                    row_data = {
                        k: v for k, v in row.items() if k not in {"destination"}
                    }

                    if custom_key in db_rows:
                        db_entry = db_rows[custom_key]
                        if db_entry.get("custom_hash") != row_data.get("custom_hash"):
                            logger.info(
                                "Updating custom data row: %s",
                                custom_key,
                            )
                            self._update_custom_row(
                                context=context,
                                row_filter=(
                                    f"custom_key == {custom_key!r} "
                                    "and custom_hash != None"
                                ),
                                row_data=row_data,
                                destination=destination,
                            )
                    else:
                        unmanaged = self._find_unmanaged_row_by_seed(
                            context=context,
                            seed_key=seed_key,
                            seed_value=seed_value,
                            destination=destination,
                        )
                        if unmanaged is not None:
                            logger.info(
                                "Adopting unmanaged data row: %s",
                                custom_key,
                            )
                            self._update_custom_row(
                                context=context,
                                row_filter=(
                                    f"{seed_key} == {seed_value!r} "
                                    "and custom_hash == None"
                                ),
                                row_data=row_data,
                                destination=destination,
                            )
                        else:
                            logger.info(
                                "Inserting custom data row: %s",
                                custom_key,
                            )
                            self._insert_custom_row(
                                context=context,
                                row_data=row_data,
                                destination=destination,
                            )

            for context in processed_keys_by_context:
                db_rows = self._get_custom_rows_for_table(context, destination)
                processed_keys = processed_keys_by_context[context]
                for custom_key in db_rows:
                    if custom_key not in processed_keys:
                        logger.info(
                            "Deleting removed custom data row: %s",
                            custom_key,
                        )
                        self._delete_custom_row_by_key(
                            context=context,
                            custom_key=custom_key,
                            destination=destination,
                        )

            self._store_custom_data_hash(expected_hash)
            if is_personal:
                self._custom_data_synced = True
            else:
                self._custom_data_synced_contexts.add(meta_context)
            return True

    def sync_custom(
        self,
        *,
        source_tables: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> bool:
        """Sync custom data from pre-collected sources across destinations."""
        if source_tables is None:
            source_tables = {}

        by_destination: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for context, table_spec in source_tables.items():
            destination = table_spec.get("destination") or "personal"
            by_destination.setdefault(destination, {})[context] = table_spec

        changed = False
        for destination, tables in by_destination.items():
            destination_arg = None if destination == "personal" else destination
            changed |= self.sync_custom_data(
                source_tables=tables,
                destination=destination_arg,
            )
        return changed
