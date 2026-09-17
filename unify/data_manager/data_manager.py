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
from typing import Any, Dict, List, Optional, Tuple, Union

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
)
from unify.data_manager.ops.query_ops import (
    filter_impl,
    search_impl,
    reduce_impl,
)
from unify.data_manager.ops.mutation_ops import (
    insert_rows_impl,
    update_rows_impl,
    update_by_ids_impl,
    claim_impl,
    reclaim_impl,
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
from unify.common.federated_search import reduce_grouped_rows, reduce_rows
from unify.common.join_utils import rewrite_join_paths
from unify.data_manager.ops.ingest_ops import run_ingest
from unify.common.context_registry import (
    ContextRegistry,
    TableContext,
)

logger = logging.getLogger(__name__)


# Short-form roots of other managers' contexts, used as supplied.
_ABSOLUTE_PREFIXES = (
    "Files/",
    "FileRecords/",
    "Contacts",
    "Knowledge/",
    "Messages",
    "Exchanges",
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
        ]

    def __init__(self) -> None:
        """Initialize DataManager with context registration."""
        super().__init__()
        self._base_ctx = ContextRegistry.get_context(self, "Data")

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

    def _data_context_suffix(self, context: str) -> str | None:
        """Return the Data-relative suffix of *context*.

        ``None`` means the context belongs to another manager and is used
        as supplied.
        """

        context = context.lstrip("/")
        if not context:
            raise ValueError("Empty context path")

        if context == "Data":
            return ""
        if context.startswith("Data/"):
            return context[len("Data/") :]

        base = self._base_ctx
        if context == base:
            return ""
        if context.startswith(base + "/"):
            return context[len(base) + 1 :]

        if any(context.startswith(p) for p in _ABSOLUTE_PREFIXES):
            return None

        scope = base.rsplit("/", 1)[0] + "/"
        if context.startswith(scope):
            return None

        return context

    def _resolve_context(self, context: str) -> str:
        """
        Resolve a context path to its fully-qualified form.

        Parameters
        ----------
        context : str
            Context path. Can be:
            - Relative: "projects/housing" → "{base_ctx}/projects/housing"
            - Data short-form: "Data/housing/arrears" → "{base_ctx}/housing/arrears"
            - Foreign short-form: "Contacts", "Files/x" → used as-is
            - Fully-qualified: "{root}/Contacts", "{base_ctx}/foo" → used as-is

        Returns
        -------
        str
            Fully resolved context path.
        """
        suffix = self._data_context_suffix(context)
        if suffix is None:
            return context.lstrip("/")
        return f"{self._base_ctx}/{suffix}" if suffix else self._base_ctx

    def _resolve_multi_join(
        self,
        joins: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Rewrite multi-join table references to fully-qualified contexts."""

        resolved_joins: list[dict[str, Any]] = []
        for step in joins:
            resolved_step = step.copy()
            raw_tables = resolved_step.get("tables")
            raw_tables = [raw_tables] if isinstance(raw_tables, str) else raw_tables
            if not isinstance(raw_tables, list):
                resolved_joins.append(resolved_step)
                continue

            originals: list[str] = []
            resolved: list[str] = []
            resolved_tables: list[str] = []
            for table in raw_tables:
                if table in {"$prev", "__prev__", "_"} or not isinstance(table, str):
                    resolved_tables.append(table)
                    continue
                resolved_table = self._resolve_context(table)
                originals.append(table)
                resolved.append(resolved_table)
                resolved_tables.append(resolved_table)

            resolved_step["tables"] = resolved_tables
            join_expr = resolved_step.get("join_expr")
            select = resolved_step.get("select")
            if isinstance(join_expr, str) and isinstance(select, dict):
                resolved_expr, resolved_select = rewrite_join_paths(
                    originals,
                    resolved,
                    join_expr,
                    select,
                )
                resolved_step["join_expr"] = resolved_expr
                resolved_step["select"] = resolved_select
            for where_key in ("left_where", "right_where"):
                where_expr = resolved_step.get(where_key)
                if isinstance(where_expr, str):
                    for original, resolved_table in zip(originals, resolved):
                        where_expr = where_expr.replace(original, resolved_table)
                    resolved_step[where_key] = where_expr
            resolved_joins.append(resolved_step)
        return resolved_joins

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
    ) -> str:
        resolved = self._resolve_context(context)
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
        resolved = self._resolve_context(context)
        return describe_table_impl(resolved)

    @functools.wraps(BaseDataManager.get_columns, updated=())
    def get_columns(self, table: str) -> Dict[str, Any]:
        resolved = self._resolve_context(table)
        return get_columns_impl(resolved)

    @functools.wraps(BaseDataManager.get_table, updated=())
    def get_table(self, context: str) -> Dict[str, Any]:
        resolved = self._resolve_context(context)
        return get_table_impl(resolved)

    @functools.wraps(BaseDataManager.list_tables, updated=())
    def list_tables(
        self,
        *,
        prefix: Optional[str] = None,
        include_column_info: bool = True,
    ) -> Union[List[str], Dict[str, Any]]:
        return list_tables_impl(
            prefix=self._resolve_context(prefix) if prefix else None,
            include_column_info=include_column_info,
        )

    @functools.wraps(BaseDataManager.delete_table, updated=())
    def delete_table(
        self,
        context: str,
        *,
        dangerous_ok: bool = False,
    ) -> None:
        resolved = self._resolve_context(context)
        delete_table_impl(resolved, dangerous_ok=dangerous_ok)

    @functools.wraps(BaseDataManager.rename_table, updated=())
    def rename_table(
        self,
        old_context: str,
        new_context: str,
    ) -> Dict[str, str]:
        resolved_old = self._resolve_context(old_context)
        resolved_new = self._resolve_context(new_context)
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
    ) -> Dict[str, str]:
        resolved = self._resolve_context(context)
        return create_column_impl(
            resolved,
            column_name=column_name,
            column_type=column_type,
            mutable=mutable,
        )

    @functools.wraps(BaseDataManager.delete_column, updated=())
    def delete_column(
        self,
        context: str,
        *,
        column_name: str,
    ) -> Dict[str, str]:
        resolved = self._resolve_context(context)
        return delete_column_impl(resolved, column_name=column_name)

    @functools.wraps(BaseDataManager.rename_column, updated=())
    def rename_column(
        self,
        context: str,
        *,
        old_name: str,
        new_name: str,
    ) -> Dict[str, str]:
        resolved = self._resolve_context(context)
        return rename_column_impl(resolved, old_name=old_name, new_name=new_name)

    @functools.wraps(BaseDataManager.create_derived_column, updated=())
    def create_derived_column(
        self,
        context: str,
        *,
        column_name: str,
        equation: str,
    ) -> Dict[str, str]:
        resolved = self._resolve_context(context)
        return create_derived_column_impl(
            resolved,
            column_name=column_name,
            equation=equation,
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
    ) -> Union[List[Dict[str, Any]], List[int]]:
        if return_ids_only and include_ids:
            raise ValueError("return_ids_only and include_ids are mutually exclusive")
        return filter_impl(
            self._resolve_context(context),
            filter=filter,
            columns=columns,
            exclude_columns=exclude_columns,
            limit=limit,
            offset=offset,
            order_by=order_by,
            descending=descending,
            return_ids_only=return_ids_only,
            include_ids=include_ids,
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
        return search_impl(
            self._resolve_context(context),
            references=references,
            k=k,
            filter=filter,
            columns=columns,
        )

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
        return reduce_impl(
            self._resolve_context(context),
            metric=metric,
            columns=columns,
            filter=filter,
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
    ) -> str:
        resolved_dest = self._resolve_context(dest_table)
        resolved_left = self._resolve_context(left_table)
        resolved_right = self._resolve_context(right_table)
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
        if isinstance(tables, str):
            tables = [tables]
        resolved_tables = [self._resolve_context(table) for table in tables]
        join_expr, select = rewrite_join_paths(
            tables,
            resolved_tables,
            join_expr,
            select,
        )
        return filter_join_impl(
            tables=resolved_tables,
            join_expr=join_expr,
            select=select,
            mode=mode,
            left_where=left_where,
            right_where=right_where,
            result_where=result_where,
            result_limit=result_limit,
            result_offset=result_offset,
        )

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
        resolved_tables = [self._resolve_context(table) for table in tables]
        join_expr, select = rewrite_join_paths(
            tables,
            resolved_tables,
            join_expr,
            select,
        )
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = filter_join_impl(
                tables=resolved_tables,
                join_expr=join_expr,
                select=select,
                mode=mode,
                left_where=left_where,
                right_where=right_where,
                result_where=result_where,
                result_limit=1000,
                result_offset=offset,
            )
            rows.extend(page)
            if len(page) < 1000:
                break
            offset += 1000
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
        if isinstance(tables, str):
            tables = [tables]
        resolved_tables = [self._resolve_context(table) for table in tables]
        join_expr, select = rewrite_join_paths(
            tables,
            resolved_tables,
            join_expr,
            select,
        )
        return search_join_impl(
            tables=resolved_tables,
            join_expr=join_expr,
            select=select,
            mode=mode,
            left_where=left_where,
            right_where=right_where,
            references=references,
            k=k,
            filter=filter,
            tmp_context_prefix=self._base_ctx,
        )

    @functools.wraps(BaseDataManager.filter_multi_join, updated=())
    def filter_multi_join(
        self,
        *,
        joins: List[Dict[str, Any]],
        result_where: Optional[str] = None,
        result_limit: int = 100,
        result_offset: int = 0,
    ) -> List[Dict[str, Any]]:
        return filter_multi_join_impl(
            joins=self._resolve_multi_join(joins),
            context_resolver=lambda table_name: table_name,
            result_where=result_where,
            result_limit=result_limit,
            result_offset=result_offset,
            tmp_context_prefix=self._base_ctx,
        )

    @functools.wraps(BaseDataManager.search_multi_join, updated=())
    def search_multi_join(
        self,
        *,
        joins: List[Dict[str, Any]],
        references: Optional[Dict[str, str]] = None,
        k: int = 10,
        filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return search_multi_join_impl(
            joins=self._resolve_multi_join(joins),
            context_resolver=lambda table_name: table_name,
            references=references,
            k=k,
            filter=filter,
            tmp_context_prefix=self._base_ctx,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Mutation Operations
    # ──────────────────────────────────────────────────────────────────────────

    @functools.wraps(BaseDataManager.insert_rows, updated=())
    def insert_rows(
        self,
        context: str,
        rows: List[Dict[str, Any]],
        *,
        on_duplicate: Optional[str] = None,
    ) -> List[int]:
        resolved = self._resolve_context(context)
        return insert_rows_impl(
            resolved,
            rows,
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
    ) -> int:
        resolved = self._resolve_context(context)
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
        resolved = None if context is None else self._resolve_context(context)
        return update_by_ids_impl(
            log_ids,
            updates,
            overwrite=overwrite,
            context=resolved,
        )

    @functools.wraps(BaseDataManager.claim, updated=())
    def claim(
        self,
        context: str,
        *,
        expect: Dict[str, Any],
        updates: Dict[str, Any],
        limit: int = 1,
    ) -> List[Dict[str, Any]]:
        resolved = self._resolve_context(context)
        return claim_impl(
            resolved,
            expect=expect,
            updates=updates,
            limit=limit,
        )

    def reclaim(
        self,
        context: str,
        *,
        claimed: Dict[str, Any],
        updates: Dict[str, Any],
        older_than_seconds: float,
        timestamp_field: str = "updated_at",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Requeue rows whose claim lease has expired.

        :meth:`claim` hands a row to exactly one worker but grants no
        lease, so a worker that dies mid-claim strands the row in the
        claimed state forever. ``reclaim`` moves rows matching *claimed*
        whose *timestamp_field* predates the lease window back via
        *updates*, using a per-row compare-and-set on the stale timestamp
        so a live worker can never lose its row. Queues built on
        :meth:`claim` call this at poll time for crash recovery.
        """
        resolved = self._resolve_context(context)
        return reclaim_impl(
            resolved,
            claimed=claimed,
            updates=updates,
            older_than_seconds=older_than_seconds,
            timestamp_field=timestamp_field,
            limit=limit,
        )

    @functools.wraps(BaseDataManager.delete_rows, updated=())
    def delete_rows(
        self,
        context: str,
        *,
        filter: Optional[str] = None,
        log_ids: Optional[List[int]] = None,
        dangerous_ok: bool = False,
    ) -> int:
        resolved = self._resolve_context(context)
        return delete_rows_impl(
            resolved,
            filter=filter,
            log_ids=log_ids,
            dangerous_ok=dangerous_ok,
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
        skip_rows: int = 0,
        expected_total_rows: int | None = None,
        private_ingest_key_column: str = "",
        private_ingest_key_prefix: str = "",
        before_insert_chunk=None,
    ) -> "IngestResult":
        resolved = self._resolve_context(context)
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
    ) -> str:
        resolved = self._resolve_context(context)
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
    ) -> int:
        resolved = self._resolve_context(context)
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
