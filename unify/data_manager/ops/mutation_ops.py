"""
Mutation operations for DataManager.

Implementation functions for insert_rows, update_rows, delete_rows.
These are called by DataManager methods and should not be used directly.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import unisdk
from unisdk.utils.http import RequestError

from unify.common.filter_utils import normalize_filter_expr
from unify.common.authorship import (
    is_shared_authored_context,
    strip_authoring_assistant_id,
)
from unify.common.log_utils import create_logs as unify_create_logs

logger = logging.getLogger(__name__)


def insert_rows_impl(
    context: str,
    rows: List[Dict[str, Any]],
    *,
    batched: bool = True,
    ignore_duplicate_composite_key_errors: bool = False,
) -> List[int]:
    """Insert rows into a context via bulk creation.

    Uniqueness enforcement belongs at the **schema level** (``unique_keys``
    on the context), not at insert time.  Use ``create_table(unique_keys=…)``
    or ``ingest(unique_keys=…)`` to declare which columns form the natural
    key; the backend rejects duplicate keys server-side.

    Parameters
    ----------
    context : str
        Fully-qualified Unify context path.
    rows : list[dict[str, Any]]
        Row dictionaries to insert.
    batched : bool, default True
        When True, uses batched log creation for better performance.
    ignore_duplicate_composite_key_errors : bool, default False
        Treat backend duplicate-key rejections as already-committed rows.
        This is reserved for deployment ingest paths that populate a
        deterministic private idempotency key and must tolerate replay after
        a crash.

    Returns
    -------
    list[int]
        Log IDs of inserted rows.
    """
    if not rows:
        return []

    logger.debug("Inserting %d rows into %s (batched=%s)", len(rows), context, batched)

    try:
        result = unify_create_logs(
            context=context,
            entries=rows,
            stamp_authoring=is_shared_authored_context(context),
            batched=batched,
        )
    except RequestError as exc:
        if (
            ignore_duplicate_composite_key_errors
            and "Duplicate composite key already exists" in str(exc)
        ):
            logger.info(
                "Treating duplicate composite key response as idempotent replay "
                "for %d rows in %s",
                len(rows),
                context,
            )
            return []
        raise
    if isinstance(result, list):
        return [lg.id for lg in result if hasattr(lg, "id")]
    return []


def update_rows_impl(
    context: str,
    updates: Dict[str, Any],
    *,
    filter: Optional[str] = None,
    log_ids: Optional[List[int]] = None,
    overwrite: bool = False,
) -> int:
    """
    Implementation of update_rows operation.

    Updates rows matching a filter expression and/or specific log IDs.
    Uses in-place Orchestra updates so log ids stay stable.

    Parameters
    ----------
    context : str
        Fully-qualified Unify context path.
    updates : dict[str, Any]
        Column values to update.
    filter : str | None
        Filter expression to select rows.
    log_ids : list[int] | None
        Specific log IDs to update. When set without ``filter``, updates
        those rows only.
    overwrite : bool, default False
        When False (default), merge ``updates`` into each row's existing
        entries (filter path) or pass ``updates`` through with Orchestra's
        non-overwrite merge (log_ids path). When True, replace entries with
        ``updates`` for log_ids path; for filter path, still merge then
        write with overwrite=True so unspecified columns are preserved.

    Returns
    -------
    int
        Number of rows updated.

    Raises
    ------
    ValueError
        If neither filter nor log_ids is provided.
    Exception
        If update fails.
    """
    if filter is None and log_ids is None:
        raise ValueError("Either filter or log_ids must be provided for update_rows")

    cleaned_updates = (
        strip_authoring_assistant_id(updates)
        if is_shared_authored_context(context)
        else dict(updates)
    )

    if log_ids is not None and filter is None:
        if not log_ids:
            return 0
        logger.debug(
            "Updating %d rows in %s by log_ids (overwrite=%s)",
            len(log_ids),
            context,
            overwrite,
        )
        unisdk.update_logs(
            logs=log_ids,
            context=context,
            entries=cleaned_updates,
            overwrite=overwrite,
        )
        return len(log_ids)

    filter_expr = normalize_filter_expr(filter)
    logger.debug("Updating rows in %s where %s", context, filter_expr)

    logs = unisdk.get_logs(context=context, filter=filter_expr)
    if not logs:
        return 0

    updated = 0
    for log in logs:
        log_id = log.id if hasattr(log, "id") else None
        if log_id is None:
            continue
        if hasattr(log, "entries") and isinstance(log.entries, dict):
            existing = dict(log.entries)
        elif isinstance(log, dict):
            existing = dict(log)
        else:
            continue
        new_entries = {**existing, **cleaned_updates}
        unisdk.update_logs(
            logs=[log_id],
            context=context,
            entries=new_entries,
            overwrite=True,
        )
        updated += 1

    return updated


def update_by_ids_impl(
    log_ids: List[int],
    updates: Dict[str, Any],
    *,
    overwrite: bool = True,
    context: Optional[str] = None,
) -> int:
    """In-place update of known log ids (stable ids; no delete+reinsert)."""
    if not log_ids:
        return 0
    unisdk.update_logs(
        logs=log_ids,
        context=context,
        entries=updates,
        overwrite=overwrite,
    )
    return len(log_ids)


def delete_rows_impl(
    context: str,
    *,
    filter: Optional[str] = None,
    log_ids: Optional[List[int]] = None,
    dangerous_ok: bool = False,
    delete_empty_rows: bool = False,
) -> int:
    """
    Implementation of delete_rows operation.

    Deletes rows matching a filter expression or specific log IDs.

    Parameters
    ----------
    context : str
        Fully-qualified Unify context path.
    filter : str | None
        Filter expression to select rows to delete.
    log_ids : list[int] | None
        Specific log IDs to delete. More efficient than filter when IDs are known.
    dangerous_ok : bool
        Safety flag; must be True to confirm destructive operation.
    delete_empty_rows : bool, default False
        When True, also deletes rows with no data (empty logs).

    Returns
    -------
    int
        Number of rows deleted.

    Raises
    ------
    ValueError
        If neither filter nor log_ids is provided.
    Exception
        If deletion fails.
    """
    if filter is None and log_ids is None:
        raise ValueError("Either filter or log_ids must be provided for delete_rows")

    logger.info(
        "Deleting rows from %s (filter=%s, log_ids=%s, delete_empty_rows=%s)",
        context,
        filter,
        f"{len(log_ids)} ids" if log_ids else None,
        delete_empty_rows,
    )

    ids_to_delete: List[int] = []

    if log_ids is not None:
        # Use provided log IDs directly
        ids_to_delete = list(log_ids)
    elif filter is not None:
        # Get log IDs using return_ids_only for efficiency
        filter_expr = normalize_filter_expr(filter)
        result = unisdk.get_logs(
            context=context,
            filter=filter_expr,
            return_ids_only=True,
        )
        if isinstance(result, list):
            ids_to_delete = result

    if not ids_to_delete:
        return 0

    # Delete the logs
    unisdk.delete_logs(
        context=context,
        logs=ids_to_delete,
        delete_empty_logs=delete_empty_rows,
    )

    return len(ids_to_delete)
