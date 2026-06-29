"""Global builtin-primitives catalogue: idempotent seeding.

The catalogue lives in the public-read builtins project (see
``unity.common.builtins``) owned by the platform admin account. It stores
exactly one copy of the static primitive rows for every manager;
deployments scope at read time via ``primitive_row_filter`` and never
write to it.

Seeding runs in bootstrap/admin processes (deploy hooks, self-host install,
the test harness) whose API key owns the catalogue project. It is
hash-guarded per manager — mirroring the per-assistant sync it replaces —
so repeated runs are cheap and idempotent.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

import unisdk
from unisdk.utils.http import RequestError

from ..common.builtins import (
    builtins_project,
    ensure_builtins_project,
    read_seed_hashes,
    write_seed_hashes,
)
from ..common.embed_utils import ensure_vector_column, list_private_fields
from .primitives.registry import get_registry
from .primitives.scope import PrimitiveScope
from .types.function import Function

logger = logging.getLogger(__name__)

BUILTINS_PRIMITIVES_CONTEXT = "Functions/Primitives"
BUILTINS_META_CONTEXT = "Functions/Meta"
_HASH_MAP_KEY = "primitives_hash_by_manager"


def _ensure_catalog_storage(project: str) -> None:
    """Create the catalogue project and contexts (idempotent).

    Field types are inferred from the first inserted rows, matching how
    manager-owned contexts behave in practice.
    """
    ensure_builtins_project(project)
    unisdk.create_context(
        BUILTINS_PRIMITIVES_CONTEXT,
        description="Builtin system action primitives with stable explicit IDs.",
        unique_keys={"function_id": "int"},
        project=project,
    )
    unisdk.create_context(
        BUILTINS_META_CONTEXT,
        description="Seeding state for the builtin primitives catalogue.",
        unique_keys={"meta_id": "int"},
        project=project,
    )


def _read_existing_rows(project: str) -> List[Any]:
    """Return all stored primitive rows (entries + log id) for reconciliation."""
    return unisdk.get_logs(
        project=project,
        context=BUILTINS_PRIMITIVES_CONTEXT,
        exclude_fields=list_private_fields(
            BUILTINS_PRIMITIVES_CONTEXT,
            project=project,
        ),
    )


def _name_prefix(manager_alias: str) -> str:
    """Stable, brand-independent name prefix for a manager's primitives.

    Primitive names are ``primitives.{alias}.{method}`` and the alias never
    changes when the implementing package or class path is renamed/moved.
    Using this prefix to scope deletes keeps reconciliation robust to such
    refactors (e.g. a package rename that shifts ``primitive_class``).
    """
    return f"primitives.{manager_alias}."


def _replace_rows(
    project: str,
    rows: List[Dict[str, Any]],
    *,
    existing: List[Any],
    name_prefixes: List[str],
) -> None:
    """Upsert *rows* keyed on the stable ``function_id`` unique key.

    Deletes any stored row that either collides on a ``function_id`` we are
    about to write or belongs to a pending manager (matched by the stable
    ``name`` prefix), then inserts the fresh rows. Keying the replace on
    ``function_id`` -- the context's unique key -- rather than the branded
    ``primitive_class`` path makes seeding idempotent across package renames:
    a stale row written under an old class path is still removed before the
    new row is inserted, so the insert never trips the unique-key constraint.
    """
    if not rows:
        return
    entries = [
        Function.model_validate(data).model_dump(include=set(data.keys()))
        for data in rows
    ]
    desired_ids = {entry["function_id"] for entry in entries}
    stale_ids = [
        log.id
        for log in existing
        if log.entries.get("function_id") in desired_ids
        or any(
            str(log.entries.get("name") or "").startswith(prefix)
            for prefix in name_prefixes
        )
    ]
    if stale_ids:
        unisdk.delete_logs(
            project=project,
            context=BUILTINS_PRIMITIVES_CONTEXT,
            logs=stale_ids,
        )
    unisdk.create_logs(
        project=project,
        context=BUILTINS_PRIMITIVES_CONTEXT,
        entries=entries,
        recompute_derived=True,
    )


def _compute_pending(
    project: str,
    registry: Any,
    manager_aliases: List[str],
) -> Tuple[List[Tuple[str, List[Dict[str, Any]], str]], List[Any], Dict[str, str]]:
    """Read-only diff of the stored catalogue against the live registry.

    Returns ``(pending, existing_rows, current_hashes)`` where *pending* lists
    the managers whose rows must be (re)written. Performs only reads, so it runs
    for any principal that can read the public Builtins project. Raises when the
    catalogue storage is absent (fresh project), signalling that the owner seed
    path must create it first.
    """
    current_hashes = read_seed_hashes(
        project,
        meta_context=BUILTINS_META_CONTEXT,
        key=_HASH_MAP_KEY,
    )
    existing_rows = _read_existing_rows(project)
    existing_by_id = {log.entries.get("function_id"): log for log in existing_rows}

    pending: List[Tuple[str, List[Dict[str, Any]], str]] = []
    for manager_alias in manager_aliases:
        expected_hash = registry.compute_hash_for_manager(manager_alias)
        rows = list(
            registry.collect_primitives(PrimitiveScope.single(manager_alias)).values(),
        )
        # Re-materialize when the public surface changed (hash) OR when stored
        # rows have drifted: missing, or stored under a stale ``primitive_class``
        # (e.g. after a package rename). The hash omits ``primitive_class``, so
        # class-path drift is detected here to keep the read-time scoping filter
        # accurate.
        needs_seed = current_hashes.get(manager_alias) != expected_hash
        if not needs_seed:
            for row in rows:
                stored = existing_by_id.get(row["function_id"])
                if stored is None or stored.entries.get("primitive_class") != row.get(
                    "primitive_class",
                ):
                    needs_seed = True
                    break
        if needs_seed:
            pending.append((manager_alias, rows, expected_hash))
    return pending, existing_rows, current_hashes


def seed_builtin_primitives(*, project: str | None = None) -> bool:
    """Seed the global builtins catalogue with every manager's primitives.

    Idempotent and reconciled per manager. A manager is re-materialized when
    its public surface hash (name/argspec/docstring) changed, or when its
    stored rows have drifted from the current shape -- missing, or written
    under a stale ``primitive_class`` (e.g. after a package rename). Rows are
    upserted on the stable ``function_id`` unique key, so re-seeding never
    collides even when the branded class path changes. Always ensures the
    ``embedding_text`` vector column exists so read-only consumers can run
    ranked semantic search without any write access.

    Returns True when any rows were written, False when already up to date.
    """
    project = project or builtins_project()
    registry = get_registry()
    manager_aliases = sorted(PrimitiveScope.all_managers().scoped_managers)

    logger.info("Starting builtins primitive catalogue seed project=%s", project)

    # Read-only convergence probe. The Builtins catalogue is public-read, so any
    # principal can verify convergence without write access; an already-seeded
    # catalogue is a pure no-op and never attempts an owner-guarded write. A
    # missing catalogue (fresh project) raises here and falls through to the
    # owner seed path below, which creates the storage before writing.
    try:
        pending, existing_rows, current_hashes = _compute_pending(
            project,
            registry,
            manager_aliases,
        )
        storage_ready = True
    except RequestError:
        storage_ready = False
        pending = None

    if storage_ready and not pending:
        logger.info(
            "Builtins primitive catalogue already converged project=%s managers=%d; "
            "read-only no-op",
            project,
            len(manager_aliases),
        )
        return False

    _ensure_catalog_storage(project)
    if not storage_ready:
        pending, existing_rows, current_hashes = _compute_pending(
            project,
            registry,
            manager_aliases,
        )

    if pending:
        all_rows: List[Dict[str, Any]] = []
        name_prefixes: List[str] = []
        for manager_alias, rows, _ in pending:
            name_prefixes.append(_name_prefix(manager_alias))
            all_rows.extend(rows)
        _replace_rows(
            project,
            all_rows,
            existing=existing_rows,
            name_prefixes=name_prefixes,
        )

        new_hashes = dict(current_hashes)
        for manager_alias, _, expected_hash in pending:
            new_hashes[manager_alias] = expected_hash
        write_seed_hashes(
            project,
            new_hashes,
            meta_context=BUILTINS_META_CONTEXT,
            key=_HASH_MAP_KEY,
        )
        logger.info(
            "Seeded builtins catalogue project=%s managers=%s rows=%d",
            project,
            [alias for alias, _, _ in pending],
            len(all_rows),
        )
    else:
        logger.info(
            "Builtins primitive catalogue already up to date project=%s managers=%d; "
            "skipping row rewrites",
            project,
            len(manager_aliases),
        )

    logger.info("Ensuring builtins primitive embedding column project=%s", project)
    ensure_vector_column(
        BUILTINS_PRIMITIVES_CONTEXT,
        embed_column="_embedding_text_emb",
        source_column="embedding_text",
        project=project,
    )
    return bool(pending)


__all__ = [
    "BUILTINS_META_CONTEXT",
    "BUILTINS_PRIMITIVES_CONTEXT",
    "seed_builtin_primitives",
]
