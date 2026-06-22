"""Public-read Builtins catalogue for integration apps and provider tools."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, Iterable, List, Optional

import unify

from droid.common.builtins import (
    builtins_project,
    ensure_builtins_project,
    read_seed_hashes,
    write_seed_hashes,
)
from droid.common.embed_utils import ensure_vector_column, list_private_fields
from droid.common.semantic_search import fetch_top_k_by_terms_combined_client_side
from droid.function_manager.hash_utils import stable_hash_for_rows
from droid.function_manager.types.function import Function
from droid.integrations.embedding_text import (
    humanize_auth_modes,
    normalize_embedding_text,
)

logger = logging.getLogger(__name__)

BUILTINS_INTEGRATION_APPS_CONTEXT = "Integrations/Apps"
BUILTINS_INTEGRATION_TOOLS_CONTEXT = "Integrations/Tools"
BUILTINS_INTEGRATION_META_CONTEXT = "Integrations/Meta"
_HASH_MAP_KEY = "integration_catalog_hash_by_unit"
_DELETE_FILTER_BATCH_SIZE = 500
_LOG_PAGE_SIZE = 500
_INSERT_ROW_BATCH_SIZE = 500
_ENSURED_STORAGE_PROJECTS: set[str] = set()
_ENSURED_EMBEDDING_PROJECTS: set[str] = set()


def _stable_int_id(namespace: str, value: str) -> int:
    digest = hashlib.sha256(f"{namespace}:{value}".encode()).digest()
    return int.from_bytes(digest[:4], "big") & 0x7FFFFFFF


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _normalize_app_slug(value: Any) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _ensure_catalog_storage(project: str) -> None:
    logger.info("Ensuring integration catalogue storage project=%s", project)
    ensure_builtins_project(project)
    unify.create_context(
        BUILTINS_INTEGRATION_APPS_CONTEXT,
        description="Public integration app catalogue.",
        unique_keys={"app_id": "int"},
        project=project,
    )
    unify.create_context(
        BUILTINS_INTEGRATION_TOOLS_CONTEXT,
        description="Public integration tool catalogue.",
        unique_keys={"function_id": "int"},
        project=project,
    )
    unify.create_context(
        BUILTINS_INTEGRATION_META_CONTEXT,
        description="Seeding state for the integration catalogue.",
        unique_keys={"meta_id": "int"},
        project=project,
    )


def _ensure_catalog_storage_once(project: str) -> None:
    if project in _ENSURED_STORAGE_PROJECTS:
        logger.info("Integration catalogue storage already ensured project=%s", project)
        return
    _ensure_catalog_storage(project)
    _ENSURED_STORAGE_PROJECTS.add(project)


def _ensure_catalog_embeddings(project: str) -> None:
    for context in (
        BUILTINS_INTEGRATION_APPS_CONTEXT,
        BUILTINS_INTEGRATION_TOOLS_CONTEXT,
    ):
        logger.info(
            "Ensuring integration catalogue embedding column project=%s context=%s",
            project,
            context,
        )
        ensure_vector_column(
            context,
            embed_column="_embedding_text_emb",
            source_column="embedding_text",
            project=project,
        )


def _ensure_catalog_embeddings_once(project: str) -> None:
    if project in _ENSURED_EMBEDDING_PROJECTS:
        logger.info(
            "Integration catalogue embeddings already ensured project=%s",
            project,
        )
        return
    _ensure_catalog_embeddings(project)
    _ENSURED_EMBEDDING_PROJECTS.add(project)


def _harvest_category_names(
    *,
    category: Any,
    raw_metadata: Dict[str, Any],
) -> List[str]:
    """Collect category names from the canonical field plus raw provider metadata.

    The canonical ``category`` is the primary; additional names are recovered
    best-effort from the provider's own catalog metadata so multi-category apps
    contribute richer retrieval signal. Provider-neutral: it probes the common
    metadata containers and tolerates both ``{"name": ...}`` dicts and strings.
    """

    names: List[str] = []
    lowered: set[str] = set()

    def add(value: Any) -> None:
        text = str(value or "").strip()
        if text and text.lower() not in lowered:
            lowered.add(text.lower())
            names.append(text)

    add(category)
    if isinstance(raw_metadata, dict):
        for container_key in ("raw_toolkit_detail", "raw_toolkit", "raw_app"):
            container = raw_metadata.get(container_key)
            if not isinstance(container, dict):
                continue
            meta = container.get("meta")
            categories = (
                meta.get("categories") if isinstance(meta, dict) else None
            ) or container.get("categories")
            if not isinstance(categories, list):
                continue
            for entry in categories:
                if isinstance(entry, dict):
                    add(entry.get("name") or entry.get("slug") or entry.get("id"))
                else:
                    add(entry)
    return names


def app_catalog_row(
    app: Dict[str, Any],
    *,
    default_backend_id: str | None = None,
) -> Dict[str, Any]:
    slug = _normalize_app_slug(app.get("canonical_app_slug") or app.get("app_slug"))
    backend_id = str(app.get("backend_id") or default_backend_id or "provider")
    display_name = app.get("display_name") or app.get("app_display_name") or slug
    description = app.get("description") or ""
    source_type = app.get("source_type") or (
        "native" if backend_id == "droid_native" else "third_party"
    )
    auth_modes = app.get("auth_modes") or []
    available_scopes = (
        app.get("available_scopes") or app.get("available_scopes_json") or []
    )
    recommended_scopes = app.get("recommended_scopes") or []
    api_key_schema = app.get("api_key_schema") or app.get("api_key_schema_json")
    category = app.get("category")
    raw_metadata = (
        app.get("raw_provider_metadata") or app.get("raw_provider_metadata_json") or {}
    )
    categories_text = ", ".join(
        _harvest_category_names(category=category, raw_metadata=raw_metadata),
    )
    embedding_text = normalize_embedding_text(
        [
            display_name,
            description,
            categories_text,
            humanize_auth_modes(auth_modes),
            slug,
        ],
    )
    return {
        "app_id": _stable_int_id("integration_app", f"{backend_id}:{slug}"),
        "backend_id": backend_id,
        "provider_app_id": app.get("provider_app_id") or slug,
        "canonical_app_slug": slug,
        "display_name": display_name,
        "description": description,
        "category": category,
        "icon_url": app.get("icon_url") or app.get("app_icon_url"),
        "auth_modes": auth_modes,
        "available_scopes": available_scopes,
        "recommended_scopes": recommended_scopes,
        "api_key_schema": api_key_schema,
        "tool_count": int(app.get("tool_count") or 0),
        "source_type": source_type,
        "source_label": app.get("source_label")
        or ("Native" if source_type == "native" else "Third-party"),
        "supported": bool(app.get("supported", True)),
        "raw_provider_metadata": app.get("raw_provider_metadata")
        or app.get("raw_provider_metadata_json")
        or {},
        "embedding_text": embedding_text,
    }


def tool_catalog_row(
    tool: Dict[str, Any],
    *,
    default_backend_id: str | None = None,
) -> Dict[str, Any]:
    from droid.function_manager.function_manager import FunctionManager

    tool = dict(tool)
    backend_id = str(tool.get("backend_id") or default_backend_id or "provider")
    app_slug = _normalize_app_slug(
        tool.get("app_slug")
        or tool.get("canonical_app_slug")
        or tool.get("provider_app_id"),
    )
    tool_name = _normalize_app_slug(
        tool.get("name") or tool.get("canonical_name") or tool.get("provider_tool_id"),
    )
    tool["backend_id"] = backend_id
    tool["app_slug"] = app_slug
    tool.setdefault("tool_id", f"{backend_id}:{app_slug}:{tool_name}")
    tool.setdefault("canonical_name", f"primitives.integrations.{app_slug}.{tool_name}")
    tool.setdefault(
        "function_manager_name",
        f"primitives_integrations__{app_slug}__{tool_name}",
    )
    if "display_name" in tool and "tool_display_name" not in tool:
        tool["tool_display_name"] = tool["display_name"]
    return FunctionManager.__new__(
        FunctionManager,
    )._integration_tool_to_function_row(tool)


def _materialize_tool_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Validate a catalog tool row, preserving catalog-only extra fields.

    ``description`` is a catalog-row field (mirroring the Orchestra tool row) that
    has no representation on the universal ``Function`` model, so it is re-applied
    after validation rather than widening the shared model for an
    integration-only concern.
    """

    validated = Function.model_validate(row).model_dump(include=set(row.keys()))
    if "description" in row:
        validated["description"] = row["description"]
    return validated


def _unit_hash(rows: Iterable[Dict[str, Any]], *, fields: tuple[str, ...]) -> str:
    return stable_hash_for_rows(rows, fields=fields)


def _delete_filter_expressions(
    context: str,
    backend_to_slugs: dict[str, set[str]],
) -> list[str]:
    filters: list[str] = []
    for backend_id, slugs in sorted(backend_to_slugs.items()):
        sorted_slugs = sorted(slug for slug in slugs if slug)
        for index in range(0, len(sorted_slugs), _DELETE_FILTER_BATCH_SIZE):
            slug_batch = sorted_slugs[index : index + _DELETE_FILTER_BATCH_SIZE]
            if context == BUILTINS_INTEGRATION_APPS_CONTEXT:
                filters.append(
                    f"backend_id == {json.dumps(backend_id)} and "
                    f"canonical_app_slug in {json.dumps(slug_batch)}",
                )
            elif context == BUILTINS_INTEGRATION_TOOLS_CONTEXT:
                filters.append(
                    'metadata["source"] == "provider_backed" and '
                    f'metadata["integration"]["backend_id"] == {json.dumps(backend_id)} and '
                    f'metadata["integration"]["app_slug"] in {json.dumps(slug_batch)}',
                )
            else:
                raise ValueError(
                    f"Unsupported integration catalogue context: {context}",
                )
    return filters


def _delete_units(project: str, context: str, filters: list[str]) -> None:
    if not filters:
        return
    for index, filter_expr in enumerate(filters):
        page = 1
        while True:
            logs = unify.get_logs(
                project=project,
                context=context,
                filter=filter_expr,
                limit=_LOG_PAGE_SIZE,
                offset=0,
                return_ids_only=True,
            )
            if not logs:
                break
            logger.info(
                "Deleting integration catalogue page project=%s context=%s "
                "filter_batch=%d/%d page=%d rows=%d",
                project,
                context,
                index + 1,
                len(filters),
                page,
                len(logs),
            )
            unify.delete_logs(
                project=project,
                context=context,
                logs=logs,
            )
            page += 1


def _delete_function_ids(project: str, function_ids: Iterable[int]) -> None:
    unique_ids = sorted({int(function_id) for function_id in function_ids})
    for index in range(0, len(unique_ids), _DELETE_FILTER_BATCH_SIZE):
        batch = unique_ids[index : index + _DELETE_FILTER_BATCH_SIZE]
        page = 1
        while True:
            logs = unify.get_logs(
                project=project,
                context=BUILTINS_INTEGRATION_TOOLS_CONTEXT,
                filter=f"function_id in {json.dumps(batch)}",
                limit=_LOG_PAGE_SIZE,
                offset=0,
                return_ids_only=True,
            )
            if not logs:
                break
            logger.info(
                "Deleting integration catalogue function-id page project=%s "
                "batch=%d/%d page=%d rows=%d",
                project,
                index // _DELETE_FILTER_BATCH_SIZE + 1,
                (len(unique_ids) + _DELETE_FILTER_BATCH_SIZE - 1)
                // _DELETE_FILTER_BATCH_SIZE,
                page,
                len(logs),
            )
            unify.delete_logs(
                project=project,
                context=BUILTINS_INTEGRATION_TOOLS_CONTEXT,
                logs=logs,
            )
            page += 1


def _insert_rows(project: str, context: str, rows: List[Dict[str, Any]]) -> None:
    total_batches = (len(rows) + _INSERT_ROW_BATCH_SIZE - 1) // _INSERT_ROW_BATCH_SIZE
    for index in range(0, len(rows), _INSERT_ROW_BATCH_SIZE):
        batch = rows[index : index + _INSERT_ROW_BATCH_SIZE]
        logger.info(
            "Writing integration catalogue batch project=%s context=%s batch=%d/%d rows=%d",
            project,
            context,
            index // _INSERT_ROW_BATCH_SIZE + 1,
            total_batches,
            len(batch),
        )
        unify.create_logs(
            project=project,
            context=context,
            entries=batch,
        )


def _dedupe_tool_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_function_id: dict[int, Dict[str, Any]] = {}
    for row in rows:
        by_function_id[int(row["function_id"])] = row
    duplicates = len(rows) - len(by_function_id)
    if duplicates:
        logger.info(
            "Deduplicated integration catalogue tool rows duplicate_function_ids=%d",
            duplicates,
        )
    return [by_function_id[key] for key in sorted(by_function_id)]


def seed_builtin_integrations(
    *,
    apps: Optional[List[Dict[str, Any]]] = None,
    tools: Optional[List[Dict[str, Any]]] = None,
    backend_id: str | None = None,
    app_slugs: Optional[Iterable[str]] = None,
    prune_unlisted_apps: bool = False,
    project: str | None = None,
) -> bool:
    """Seed the public integration app/tool catalogue.

    Deploy/bootstrap jobs pass explicit rows from provider/native catalogue
    fetches. Omitting rows seeds an empty catalogue, keeping generic test
    setup cheap and side-effect-free.
    """

    project = project or builtins_project()
    logger.info(
        "Starting integration catalogue seed project=%s backend=%s apps=%s tools=%s "
        "prune_unlisted_apps=%s",
        project,
        backend_id,
        "omitted" if apps is None else len(apps),
        "omitted" if tools is None else len(tools),
        prune_unlisted_apps,
    )
    _ensure_catalog_storage_once(project)
    reconcile_apps = apps is not None
    reconcile_tools = tools is not None
    if apps is None and tools is None:
        _ensure_catalog_embeddings_once(project)
        return False
    apps = apps or []
    tools = tools or []
    seed_backend_id = str(backend_id) if backend_id else None
    logger.info(
        "Normalizing integration catalogue rows project=%s apps=%d tools=%d",
        project,
        len(apps),
        len(tools),
    )
    app_rows = [
        app_catalog_row(app, default_backend_id=seed_backend_id) for app in apps
    ]
    tool_rows = [
        _materialize_tool_row(row)
        for row in (
            tool_catalog_row(tool, default_backend_id=seed_backend_id) for tool in tools
        )
    ]
    tool_rows = _dedupe_tool_rows(tool_rows)
    logger.info(
        "Normalized integration catalogue rows project=%s app_rows=%d tool_rows=%d",
        project,
        len(app_rows),
        len(tool_rows),
    )
    scope_backends = {str(backend_id)} if backend_id else set()
    scope_slugs = {
        _normalize_app_slug(slug)
        for slug in (app_slugs or [])
        if _normalize_app_slug(slug)
    }
    by_unit: dict[str, tuple[str, str, str, list[dict[str, Any]], str]] = {}
    for row in app_rows:
        slug = row["canonical_app_slug"]
        scope_backends.add(str(row["backend_id"]))
        scope_slugs.add(slug)
        key = f'app:{row["backend_id"]}:{slug}'
        by_unit[key] = (
            BUILTINS_INTEGRATION_APPS_CONTEXT,
            str(row["backend_id"]),
            slug,
            [row],
            _unit_hash([row], fields=tuple(sorted(row))),
        )
    tools_by_app: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in tool_rows:
        integration = (row.get("metadata") or {}).get("integration") or {}
        backend_id = integration.get("backend_id") or "provider"
        app_slug = integration.get("app_slug") or ""
        scope_backends.add(str(backend_id))
        if app_slug:
            scope_slugs.add(str(app_slug))
        tools_by_app.setdefault((backend_id, app_slug), []).append(row)
    for (backend_id, app_slug), rows in tools_by_app.items():
        key = f"tools:{backend_id}:{app_slug}"
        by_unit[key] = (
            BUILTINS_INTEGRATION_TOOLS_CONTEXT,
            str(backend_id),
            str(app_slug),
            rows,
            _unit_hash(
                rows,
                fields=(
                    "function_id",
                    "name",
                    "description",
                    "argspec",
                    "docstring",
                    "embedding_text",
                    "metadata",
                    "verify",
                ),
            ),
        )
    if prune_unlisted_apps and not scope_backends:
        raise ValueError(
            "backend_id or catalog rows are required for prune_unlisted_apps",
        )

    logger.info("Reading integration catalogue seed hashes project=%s", project)
    current_hashes = read_seed_hashes(
        project,
        meta_context=BUILTINS_INTEGRATION_META_CONTEXT,
        key=_HASH_MAP_KEY,
    )
    logger.info(
        "Read integration catalogue seed hashes project=%s units=%d",
        project,
        len(current_hashes),
    )
    next_hashes = dict(current_hashes)
    changed = False
    delete_scopes_by_context: dict[str, dict[str, set[str]]] = {}
    insert_rows_by_context: dict[str, list[dict[str, Any]]] = {}
    changed_unit_count = 0
    for key, (context, unit_backend_id, app_slug, rows, row_hash) in by_unit.items():
        if current_hashes.get(key) == row_hash:
            continue
        delete_scopes_by_context.setdefault(context, {}).setdefault(
            unit_backend_id,
            set(),
        ).add(app_slug)
        insert_rows_by_context.setdefault(context, []).extend(rows)
        next_hashes[key] = row_hash
        changed_unit_count += 1
        changed = True

    stale_keys = sorted(set(current_hashes) - set(by_unit))
    removed_unit_count = 0
    for key in stale_keys:
        prefix, stale_backend_id, app_slug = key.split(":", 2)
        if scope_backends and stale_backend_id not in scope_backends:
            continue
        if prune_unlisted_apps:
            should_delete = True
        else:
            should_delete = app_slug in scope_slugs
        if prefix == "app" and reconcile_apps and should_delete:
            delete_scopes_by_context.setdefault(
                BUILTINS_INTEGRATION_APPS_CONTEXT,
                {},
            ).setdefault(stale_backend_id, set()).add(app_slug)
            next_hashes.pop(key, None)
            removed_unit_count += 1
            changed = True
        elif prefix == "tools" and reconcile_tools and should_delete:
            delete_scopes_by_context.setdefault(
                BUILTINS_INTEGRATION_TOOLS_CONTEXT,
                {},
            ).setdefault(stale_backend_id, set()).add(app_slug)
            next_hashes.pop(key, None)
            removed_unit_count += 1
            changed = True

    logger.info(
        "Computed integration catalogue changes project=%s total_units=%d "
        "changed_units=%d removed_units=%d",
        project,
        len(by_unit),
        changed_unit_count,
        removed_unit_count,
    )

    for context, backend_to_slugs in delete_scopes_by_context.items():
        filters = _delete_filter_expressions(context, backend_to_slugs)
        logger.info(
            "Deleting integration catalogue units project=%s context=%s units=%d filters=%d",
            project,
            context,
            sum(len(slugs) for slugs in backend_to_slugs.values()),
            len(filters),
        )
        _delete_units(project, context, filters)
    for context, rows in insert_rows_by_context.items():
        logger.info(
            "Writing integration catalogue rows project=%s context=%s rows=%d",
            project,
            context,
            len(rows),
        )
        if context == BUILTINS_INTEGRATION_TOOLS_CONTEXT:
            _delete_function_ids(
                project,
                (int(row["function_id"]) for row in rows),
            )
        _insert_rows(project, context, rows)

    if changed:
        logger.info(
            "Writing integration catalogue seed hashes project=%s units=%d",
            project,
            len(next_hashes),
        )
        write_seed_hashes(
            project,
            next_hashes,
            meta_context=BUILTINS_INTEGRATION_META_CONTEXT,
            key=_HASH_MAP_KEY,
        )
        logger.info(
            "Seeded integration catalogue project=%s apps=%d tools=%d",
            project,
            len(app_rows),
            len(tool_rows),
        )
    else:
        logger.info(
            "Integration catalogue already up to date project=%s backend=%s units=%d; "
            "skipping row rewrites",
            project,
            backend_id,
            len(by_unit),
        )

    _ensure_catalog_embeddings_once(project)
    return changed


def list_catalog_apps(
    *,
    query: str | None = None,
    limit: int = 10,
    project: str | None = None,
) -> list[dict[str, Any]]:
    project = project or builtins_project()
    if query:
        rows, _score_key = fetch_top_k_by_terms_combined_client_side(
            BUILTINS_INTEGRATION_APPS_CONTEXT,
            [("_embedding_text_emb", query)],
            k=limit,
            allowed_fields=[
                "app_id",
                "backend_id",
                "provider_app_id",
                "canonical_app_slug",
                "display_name",
                "description",
                "category",
                "icon_url",
                "auth_modes",
                "available_scopes",
                "recommended_scopes",
                "api_key_schema",
                "tool_count",
                "source_type",
                "source_label",
                "supported",
                "raw_provider_metadata",
                "embedding_text",
            ],
            project=project,
        )
        return rows
    rows = unify.get_logs(
        project=project,
        context=BUILTINS_INTEGRATION_APPS_CONTEXT,
        limit=limit,
        exclude_fields=list_private_fields(
            BUILTINS_INTEGRATION_APPS_CONTEXT,
            project=project,
        ),
    )
    return [dict(row.entries) for row in rows or []]


def list_catalog_tools(
    *,
    canonical_app_slug: str | None = None,
    tool_id: str | None = None,
    limit: int = 500,
    project: str | None = None,
) -> list[dict[str, Any]]:
    project = project or builtins_project()
    row_filter = 'metadata["source"] == "provider_backed"'
    if canonical_app_slug:
        row_filter = f'({row_filter}) and metadata["integration"]["app_slug"] == {json.dumps(_normalize_app_slug(canonical_app_slug))}'
    if tool_id:
        row_filter = f'({row_filter}) and metadata["integration"]["tool_id"] == {json.dumps(tool_id)}'
    rows = unify.get_logs(
        project=project,
        context=BUILTINS_INTEGRATION_TOOLS_CONTEXT,
        filter=row_filter,
        limit=limit,
        exclude_fields=list_private_fields(
            BUILTINS_INTEGRATION_TOOLS_CONTEXT,
            project=project,
        ),
    )
    return [dict(row.entries) for row in rows or []]


__all__ = [
    "BUILTINS_INTEGRATION_APPS_CONTEXT",
    "BUILTINS_INTEGRATION_META_CONTEXT",
    "BUILTINS_INTEGRATION_TOOLS_CONTEXT",
    "app_catalog_row",
    "list_catalog_apps",
    "list_catalog_tools",
    "seed_builtin_integrations",
    "tool_catalog_row",
]
