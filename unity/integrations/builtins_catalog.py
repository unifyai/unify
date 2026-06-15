"""Public-read Builtins catalogue for integration apps and provider tools."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, Iterable, List, Optional

import unify

from unity.common.builtins import builtins_project, read_seed_hashes, write_seed_hashes
from unity.common.embed_utils import ensure_vector_column, list_private_fields
from unity.common.semantic_search import fetch_top_k_by_terms_combined_client_side
from unity.function_manager.hash_utils import stable_hash_for_rows
from unity.function_manager.types.function import Function

logger = logging.getLogger(__name__)

BUILTINS_INTEGRATION_APPS_CONTEXT = "Integrations/Apps"
BUILTINS_INTEGRATION_TOOLS_CONTEXT = "Integrations/Tools"
BUILTINS_INTEGRATION_META_CONTEXT = "Integrations/Meta"
_HASH_MAP_KEY = "integration_catalog_hash_by_unit"


def _stable_int_id(namespace: str, value: str) -> int:
    digest = hashlib.sha256(f"{namespace}:{value}".encode()).digest()
    return int.from_bytes(digest[:4], "big") & 0x7FFFFFFF


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _normalize_app_slug(value: Any) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _ensure_catalog_storage(project: str) -> None:
    unify.create_project(project, exist_ok=True, is_public_read=True)
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


def app_catalog_row(app: Dict[str, Any]) -> Dict[str, Any]:
    slug = _normalize_app_slug(app.get("canonical_app_slug") or app.get("app_slug"))
    backend_id = str(app.get("backend_id") or "provider")
    display_name = app.get("display_name") or app.get("app_display_name") or slug
    description = app.get("description") or ""
    source_type = app.get("source_type") or (
        "native" if backend_id == "unity_native" else "third_party"
    )
    auth_modes = app.get("auth_modes") or []
    available_scopes = (
        app.get("available_scopes") or app.get("available_scopes_json") or []
    )
    recommended_scopes = app.get("recommended_scopes") or []
    category = app.get("category")
    embedding_text = "\n".join(
        part
        for part in (
            f"Integration App: {display_name}",
            f"Slug: {slug}",
            f"Source: {source_type}",
            f"Category: {category}" if category else "",
            f"Description: {description}" if description else "",
            (
                f"Scopes: {', '.join(str(scope) for scope in available_scopes)}"
                if available_scopes
                else ""
            ),
        )
        if part
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


def tool_catalog_row(tool: Dict[str, Any]) -> Dict[str, Any]:
    from unity.function_manager.function_manager import FunctionManager

    return FunctionManager.__new__(
        FunctionManager,
    )._integration_tool_to_function_row(tool)


def _unit_hash(rows: Iterable[Dict[str, Any]], *, fields: tuple[str, ...]) -> str:
    return stable_hash_for_rows(rows, fields=fields)


def _delete_units(project: str, context: str, filters: list[str]) -> None:
    if not filters:
        return
    logs = unify.get_logs(
        project=project,
        context=context,
        filter=" or ".join(f"({flt})" for flt in filters),
        exclude_fields=list_private_fields(context, project=project),
    )
    if logs:
        unify.delete_logs(
            project=project,
            context=context,
            logs=[log.id for log in logs],
        )


def _insert_rows(project: str, context: str, rows: List[Dict[str, Any]]) -> None:
    if rows:
        unify.create_logs(
            project=project,
            context=context,
            entries=rows,
            recompute_derived=True,
        )


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
    _ensure_catalog_storage(project)
    reconcile_apps = apps is not None
    if apps is None and tools is None:
        ensure_vector_column(
            BUILTINS_INTEGRATION_APPS_CONTEXT,
            embed_column="_embedding_text_emb",
            source_column="embedding_text",
            project=project,
        )
        ensure_vector_column(
            BUILTINS_INTEGRATION_TOOLS_CONTEXT,
            embed_column="_embedding_text_emb",
            source_column="embedding_text",
            project=project,
        )
        return False
    apps = apps or []
    tools = tools or []
    app_rows = [app_catalog_row(app) for app in apps]
    tool_rows = [
        Function.model_validate(row).model_dump(include=set(row.keys()))
        for row in (tool_catalog_row(tool) for tool in tools)
    ]
    scope_backends = {str(backend_id)} if backend_id else set()
    scope_slugs = {
        _normalize_app_slug(slug)
        for slug in (app_slugs or [])
        if _normalize_app_slug(slug)
    }
    by_unit: dict[str, tuple[str, str, list[dict[str, Any]], str]] = {}
    for row in app_rows:
        slug = row["canonical_app_slug"]
        scope_backends.add(str(row["backend_id"]))
        scope_slugs.add(slug)
        key = f'app:{row["backend_id"]}:{slug}'
        by_unit[key] = (
            BUILTINS_INTEGRATION_APPS_CONTEXT,
            f'backend_id == {json.dumps(row["backend_id"])} and canonical_app_slug == {json.dumps(slug)}',
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
            f'metadata["source"] == "provider_backed" and metadata["integration"]["backend_id"] == {json.dumps(backend_id)} and metadata["integration"]["app_slug"] == {json.dumps(app_slug)}',
            rows,
            _unit_hash(
                rows,
                fields=(
                    "function_id",
                    "name",
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

    current_hashes = read_seed_hashes(
        project,
        meta_context=BUILTINS_INTEGRATION_META_CONTEXT,
        key=_HASH_MAP_KEY,
    )
    next_hashes = dict(current_hashes)
    changed = False
    for key, (context, filter_expr, rows, row_hash) in by_unit.items():
        if current_hashes.get(key) == row_hash:
            continue
        _delete_units(project, context, [filter_expr])
        _insert_rows(project, context, rows)
        next_hashes[key] = row_hash
        changed = True

    stale_keys = sorted(set(current_hashes) - set(by_unit))
    app_filters: list[str] = []
    tool_filters: list[str] = []
    for key in stale_keys:
        prefix, stale_backend_id, app_slug = key.split(":", 2)
        if scope_backends and stale_backend_id not in scope_backends:
            continue
        if prune_unlisted_apps:
            should_delete = True
        else:
            should_delete = app_slug in scope_slugs
        if prefix == "app" and reconcile_apps and should_delete:
            app_filters.append(
                f"backend_id == {json.dumps(stale_backend_id)} and canonical_app_slug == {json.dumps(app_slug)}",
            )
            next_hashes.pop(key, None)
        elif prefix == "tools" and should_delete:
            tool_filters.append(
                f'metadata["source"] == "provider_backed" and metadata["integration"]["backend_id"] == {json.dumps(stale_backend_id)} and metadata["integration"]["app_slug"] == {json.dumps(app_slug)}',
            )
            next_hashes.pop(key, None)
    if app_filters:
        _delete_units(project, BUILTINS_INTEGRATION_APPS_CONTEXT, app_filters)
        changed = True
    if tool_filters:
        _delete_units(project, BUILTINS_INTEGRATION_TOOLS_CONTEXT, tool_filters)
        changed = True

    if changed:
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

    ensure_vector_column(
        BUILTINS_INTEGRATION_APPS_CONTEXT,
        embed_column="_embedding_text_emb",
        source_column="embedding_text",
        project=project,
    )
    ensure_vector_column(
        BUILTINS_INTEGRATION_TOOLS_CONTEXT,
        embed_column="_embedding_text_emb",
        source_column="embedding_text",
        project=project,
    )
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
