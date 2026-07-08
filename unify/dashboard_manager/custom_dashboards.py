"""
Collection of custom dashboard tile and layout definitions from deployment directories.

Each entity lives under a dashboards root in one of two namespaces::

    dashboards/
      tiles/
        expiring_certificates/
          meta.json
          rows.jsonl
      layouts/
        compliance_overview/
          meta.json
          rows.jsonl

Layout rows reference tiles by deployment ``tile_id``; sync resolves those to
runtime tile tokens after tiles are reconciled.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional

from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)

META_JSON_FILENAME = "meta.json"
ROWS_JSONL_FILENAME = "rows.jsonl"
TILES_NAMESPACE = "tiles"
LAYOUTS_NAMESPACE = "layouts"


class CustomDashboardEntityMeta(BaseModel):
    """Entity-level metadata from ``meta.json``."""

    description: str = ""
    seed_key: str = Field(default="id", min_length=1)
    destination: str = "personal"
    auto_sync: bool = True
    data_scope: str = "dashboard"


def tile_entry_key(*, tile_id: str) -> str:
    """Return the stable merge key for one custom tile row."""
    return f"tile|{tile_id}"


def layout_entry_key(*, layout_id: str) -> str:
    """Return the stable merge key for one custom layout row."""
    return f"layout|{layout_id}"


def _compute_tile_hash(
    *,
    custom_key: str,
    tile_id: str,
    destination: str,
    row_fields: Dict[str, Any],
) -> str:
    components = [custom_key, tile_id, destination or "personal"]
    for field_name in sorted(row_fields.keys()):
        if field_name in {
            "custom_key",
            "custom_hash",
            "tile_id",
            "dashboard_id",
            "token",
        }:
            continue
        value = row_fields[field_name]
        if isinstance(value, (dict, list)):
            value = json.dumps(value, sort_keys=True, default=str)
        components.append("" if value is None else str(value))
    combined = "\n".join(components)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def _compute_layout_hash(
    *,
    custom_key: str,
    layout_id: str,
    destination: str,
    row_fields: Dict[str, Any],
) -> str:
    components = [custom_key, layout_id, destination or "personal"]
    for field_name in sorted(row_fields.keys()):
        if field_name in {
            "custom_key",
            "custom_hash",
            "tile_id",
            "dashboard_id",
            "token",
        }:
            continue
        value = row_fields[field_name]
        if isinstance(value, (dict, list)):
            value = json.dumps(value, sort_keys=True, default=str)
        components.append("" if value is None else str(value))
    combined = "\n".join(components)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def _normalize_tile_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload)
    if "html_content" not in normalized and "html_template" in normalized:
        normalized["html_content"] = normalized.pop("html_template")
    if "on_data_script" not in normalized and "on_data" in normalized:
        normalized["on_data_script"] = normalized.pop("on_data")
    if "data_bindings" in normalized and normalized["data_bindings"] is None:
        normalized.pop("data_bindings")
    if (
        "data_binding" in normalized
        and "data_bindings" not in normalized
        and normalized["data_binding"] is not None
    ):
        binding = normalized.pop("data_binding")
        normalized["data_bindings"] = (
            [binding] if isinstance(binding, dict) else binding
        )
    return normalized


def _normalize_layout_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload)
    if "positions" not in normalized and "tiles" in normalized:
        normalized["positions"] = normalized.pop("tiles")
    return normalized


def merge_dashboard_specs(
    base: Dict[str, Dict[str, Any]],
    overlay: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Deep-merge dashboard entity specs. Overlay rows win on collision."""
    merged = {name: dict(spec) for name, spec in base.items()}
    for entity_id, spec in overlay.items():
        if entity_id not in merged:
            merged[entity_id] = dict(spec)
            continue
        existing = merged[entity_id]
        if spec.get("description"):
            existing["description"] = spec["description"]
        if spec.get("destination"):
            existing["destination"] = spec["destination"]
        if spec.get("data_scope"):
            existing["data_scope"] = spec["data_scope"]
        seed_key = spec.get("seed_key") or existing.get("seed_key")
        if seed_key:
            existing["seed_key"] = seed_key
        if spec.get("rows"):
            all_rows = existing.get("rows", []) + spec["rows"]
            if seed_key:
                seen: Dict[str, Dict[str, Any]] = {}
                for row in all_rows:
                    seen[str(row.get(seed_key, id(row)))] = row
                existing["rows"] = list(seen.values())
            else:
                existing["rows"] = all_rows
    return merged


def _entity_namespace(entity_dir: Path, root: Path) -> Optional[str]:
    try:
        relative = entity_dir.relative_to(root)
    except ValueError:
        return None
    if not relative.parts:
        return None
    namespace = relative.parts[0]
    if namespace in {TILES_NAMESPACE, LAYOUTS_NAMESPACE}:
        return namespace
    return None


def _entity_id_from_dir(entity_dir: Path, root: Path, namespace: str) -> str:
    relative = entity_dir.relative_to(root / namespace)
    return relative.as_posix()


def _parse_rows_jsonl(
    *,
    jsonl_path: Path,
    entity_id: str,
    namespace: Literal["tiles", "layouts"],
    entity_meta: CustomDashboardEntityMeta,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seed_key = entity_meta.seed_key
    destination = entity_meta.destination or "personal"
    for line_no, raw_line in enumerate(jsonl_path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            logger.warning(
                "Skipping invalid dashboard rows.jsonl line %s:%d: %s",
                jsonl_path,
                line_no,
                exc,
            )
            continue
        if not isinstance(payload, dict):
            logger.warning(
                "Skipping non-object dashboard row %s:%d",
                jsonl_path,
                line_no,
            )
            continue
        if namespace == TILES_NAMESPACE:
            payload = _normalize_tile_payload(payload)
        else:
            payload = _normalize_layout_payload(payload)
        seed_value = str(payload.get(seed_key, "")) or entity_id
        payload[seed_key] = seed_value
        if namespace == TILES_NAMESPACE:
            custom_key = tile_entry_key(tile_id=seed_value)
            row_fields = dict(payload)
            custom_hash = _compute_tile_hash(
                custom_key=custom_key,
                tile_id=seed_value,
                destination=destination,
                row_fields=row_fields,
            )
        else:
            custom_key = layout_entry_key(layout_id=seed_value)
            row_fields = dict(payload)
            custom_hash = _compute_layout_hash(
                custom_key=custom_key,
                layout_id=seed_value,
                destination=destination,
                row_fields=row_fields,
            )
        rows.append(
            {
                **row_fields,
                "custom_key": custom_key,
                "custom_hash": custom_hash,
            },
        )
    return rows


def _load_entity_from_dir(
    entity_dir: Path,
    *,
    entity_id: str,
    namespace: Literal["tiles", "layouts"],
) -> Optional[Dict[str, Any]]:
    meta_path = entity_dir / META_JSON_FILENAME
    rows_path = entity_dir / ROWS_JSONL_FILENAME
    if not meta_path.is_file():
        return None
    try:
        entity_meta = CustomDashboardEntityMeta.model_validate(
            json.loads(meta_path.read_text()),
        )
    except (json.JSONDecodeError, ValidationError) as exc:
        logger.warning(
            "Skipping invalid dashboard meta.json at %s: %s",
            meta_path,
            exc,
        )
        return None
    if not entity_meta.auto_sync:
        logger.debug("Skipping dashboard entity %s: auto_sync=False", entity_id)
        return None
    rows: List[Dict[str, Any]] = []
    if rows_path.is_file():
        rows = _parse_rows_jsonl(
            jsonl_path=rows_path,
            entity_id=entity_id,
            namespace=namespace,
            entity_meta=entity_meta,
        )
    return {
        "entity_id": entity_id,
        "namespace": namespace,
        "description": entity_meta.description,
        "seed_key": entity_meta.seed_key,
        "destination": entity_meta.destination or "personal",
        "data_scope": entity_meta.data_scope,
        "rows": rows,
    }


def collect_custom_dashboards(
    path: Optional[Path] = None,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Load custom dashboard tiles and layouts from a deployment dashboards root."""
    if path is None:
        logger.debug("Custom dashboards path is None, nothing to collect")
        return {TILES_NAMESPACE: {}, LAYOUTS_NAMESPACE: {}}

    root = Path(path)
    if not root.is_dir():
        logger.debug("No dashboards directory found at %s", path)
        return {TILES_NAMESPACE: {}, LAYOUTS_NAMESPACE: {}}

    tiles: Dict[str, Dict[str, Any]] = {}
    layouts: Dict[str, Dict[str, Any]] = {}
    for meta_path in sorted(root.rglob(META_JSON_FILENAME)):
        entity_dir = meta_path.parent
        namespace = _entity_namespace(entity_dir, root)
        if namespace is None:
            continue
        entity_id = _entity_id_from_dir(entity_dir, root, namespace)
        entity_spec = _load_entity_from_dir(
            entity_dir,
            entity_id=entity_id,
            namespace=namespace,
        )
        if entity_spec is None:
            continue
        if namespace == TILES_NAMESPACE:
            tiles[entity_id] = entity_spec
        else:
            layouts[entity_id] = entity_spec

    logger.debug(
        "Collected %d custom dashboard tiles and %d layouts from %s",
        len(tiles),
        len(layouts),
        root,
    )
    return {TILES_NAMESPACE: tiles, LAYOUTS_NAMESPACE: layouts}


def collect_dashboards_from_directories(
    directories: List[Path],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Collect dashboard entities from multiple directories and merge them."""
    merged = {TILES_NAMESPACE: {}, LAYOUTS_NAMESPACE: {}}
    for directory in directories:
        collected = collect_custom_dashboards(path=directory)
        merged[TILES_NAMESPACE] = merge_dashboard_specs(
            merged[TILES_NAMESPACE],
            collected[TILES_NAMESPACE],
        )
        merged[LAYOUTS_NAMESPACE] = merge_dashboard_specs(
            merged[LAYOUTS_NAMESPACE],
            collected[LAYOUTS_NAMESPACE],
        )
    return merged


def compute_custom_dashboards_hash(
    source_entities: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,
) -> str:
    """Compute an aggregate hash of custom dashboard rows."""
    entities = source_entities if source_entities is not None else {}
    row_hashes: List[str] = []
    for namespace in (TILES_NAMESPACE, LAYOUTS_NAMESPACE):
        specs = entities.get(namespace, {})
        for entity_id in sorted(specs.keys()):
            for row in specs[entity_id].get("rows", []):
                custom_hash = row.get("custom_hash")
                custom_key = row.get("custom_key")
                if custom_hash and custom_key:
                    row_hashes.append(f"{custom_key}:{custom_hash}")
    if not row_hashes:
        return ""
    combined = "|".join(row_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def list_dashboard_entity_ids(
    directories: Iterable[Path],
) -> Dict[str, List[str]]:
    """Return sorted tile and layout entity ids discovered under dashboard dirs."""
    tile_ids: set[str] = set()
    layout_ids: set[str] = set()
    for directory in directories:
        collected = collect_custom_dashboards(path=directory)
        tile_ids.update(collected[TILES_NAMESPACE].keys())
        layout_ids.update(collected[LAYOUTS_NAMESPACE].keys())
    return {
        TILES_NAMESPACE: sorted(tile_ids),
        LAYOUTS_NAMESPACE: sorted(layout_ids),
    }
