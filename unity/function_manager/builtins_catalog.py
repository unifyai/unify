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

import unify

from ..common.builtins import builtins_project, read_seed_hashes, write_seed_hashes
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
    unify.create_project(project, exist_ok=True, is_public_read=True)
    unify.create_context(
        BUILTINS_PRIMITIVES_CONTEXT,
        description="Builtin system action primitives with stable explicit IDs.",
        unique_keys={"function_id": "int"},
        project=project,
    )
    unify.create_context(
        BUILTINS_META_CONTEXT,
        description="Seeding state for the builtin primitives catalogue.",
        unique_keys={"meta_id": "int"},
        project=project,
    )


def _delete_rows_for_managers(project: str, class_paths: List[str]) -> None:
    if not class_paths:
        return
    filter_expr = " or ".join(
        f'primitive_class == "{class_path}"' for class_path in class_paths
    )
    logs = unify.get_logs(
        project=project,
        context=BUILTINS_PRIMITIVES_CONTEXT,
        filter=filter_expr,
        exclude_fields=list_private_fields(
            BUILTINS_PRIMITIVES_CONTEXT,
            project=project,
        ),
    )
    if logs:
        unify.delete_logs(
            project=project,
            context=BUILTINS_PRIMITIVES_CONTEXT,
            logs=[log.id for log in logs],
        )


def _insert_rows(project: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    entries = [
        Function.model_validate(data).model_dump(include=set(data.keys()))
        for data in rows
    ]
    unify.create_logs(
        project=project,
        context=BUILTINS_PRIMITIVES_CONTEXT,
        entries=entries,
        recompute_derived=True,
    )


def seed_builtin_primitives(*, project: str | None = None) -> bool:
    """Seed the global builtins catalogue with every manager's primitives.

    Idempotent and hash-guarded per manager: only managers whose primitive
    surface (name/argspec/docstring) changed are deleted and re-inserted.
    Always ensures the ``embedding_text`` vector column exists so read-only
    consumers can run ranked semantic search without any write access.

    Returns True when any rows were written, False when already up to date.
    """
    project = project or builtins_project()
    registry = get_registry()

    _ensure_catalog_storage(project)

    current_hashes = read_seed_hashes(
        project,
        meta_context=BUILTINS_META_CONTEXT,
        key=_HASH_MAP_KEY,
    )
    pending: List[Tuple[str, List[Dict[str, Any]], str]] = []
    for manager_alias in sorted(PrimitiveScope.all_managers().scoped_managers):
        expected_hash = registry.compute_hash_for_manager(manager_alias)
        if current_hashes.get(manager_alias) == expected_hash:
            continue
        rows = list(
            registry.collect_primitives(PrimitiveScope.single(manager_alias)).values(),
        )
        pending.append((manager_alias, rows, expected_hash))

    if pending:
        changed_class_paths = []
        for manager_alias, _, _ in pending:
            spec = registry.get_manager_spec(manager_alias)
            if spec:
                changed_class_paths.append(spec.primitive_class_path)
        _delete_rows_for_managers(project, changed_class_paths)

        all_rows: List[Dict[str, Any]] = []
        for _, rows, _ in pending:
            all_rows.extend(rows)
        _insert_rows(project, all_rows)

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
