"""
Collection of custom DataManager table definitions from deployment directories.

Each table lives in a subdirectory of a custom data root::

    custom_data/
      CRM/
        ReferenceCodes/
          meta.json
          rows.jsonl
      MidlandHeart/
        PilotEscalation/
          meta.json
          rows.jsonl

The relative path from the custom data root to ``meta.json`` is the default
table context. ``meta.json`` may override that with an explicit ``context``.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)

META_JSON_FILENAME = "meta.json"
ROWS_JSONL_FILENAME = "rows.jsonl"


class CustomDataTableMeta(BaseModel):
    """Table-level metadata from ``meta.json``."""

    description: str = ""
    fields: Dict[str, Any] = Field(default_factory=dict)
    seed_key: str = Field(min_length=1)
    destination: str = "personal"
    auto_sync: bool = True
    context: str = ""
    unique_keys: Optional[Dict[str, str]] = None
    auto_counting: Optional[Dict[str, Optional[str]]] = None


def data_entry_key(*, context: str, seed_value: str) -> str:
    """Return the stable merge key for one custom data row."""
    return f"{context}|{seed_value}"


def _compute_row_hash(
    *,
    custom_key: str,
    context: str,
    seed_key: str,
    destination: str,
    row_fields: Dict[str, Any],
) -> str:
    components = [custom_key, context, seed_key, destination or "personal"]
    for field_name in sorted(row_fields.keys()):
        if field_name in {"custom_key", "custom_hash", "row_id"}:
            continue
        value = row_fields[field_name]
        components.append("" if value is None else str(value))
    combined = "\n".join(components)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def merge_data_table_specs(
    base: Dict[str, Dict[str, Any]],
    overlay: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Deep-merge data tables. Overlay columns and rows win on collision."""
    merged = {name: dict(spec) for name, spec in base.items()}
    for context, spec in overlay.items():
        if context not in merged:
            merged[context] = dict(spec)
            continue
        existing = merged[context]
        if spec.get("fields"):
            existing.setdefault("fields", {}).update(spec["fields"])
        if spec.get("description"):
            existing["description"] = spec["description"]
        seed_key = spec.get("seed_key") or existing.get("seed_key")
        if seed_key:
            existing["seed_key"] = seed_key
        if spec.get("destination"):
            existing["destination"] = spec["destination"]
        if spec.get("unique_keys") is not None:
            existing["unique_keys"] = spec["unique_keys"]
        if spec.get("auto_counting") is not None:
            existing["auto_counting"] = spec["auto_counting"]
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


def _context_from_meta_path(root: Path, meta_path: Path) -> str:
    return meta_path.parent.relative_to(root).as_posix()


def _parse_rows_jsonl(
    *,
    jsonl_path: Path,
    context: str,
    table_meta: CustomDataTableMeta,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seed_key = table_meta.seed_key
    destination = table_meta.destination or "personal"
    for line_no, raw_line in enumerate(jsonl_path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            logger.warning(
                "Skipping invalid data rows.jsonl line %s:%d: %s",
                jsonl_path,
                line_no,
                exc,
            )
            continue
        if not isinstance(payload, dict):
            logger.warning(
                "Skipping non-object data row %s:%d",
                jsonl_path,
                line_no,
            )
            continue
        seed_value = str(payload.get(seed_key, ""))
        if not seed_value:
            logger.warning(
                "Skipping data row without %s at %s:%d",
                seed_key,
                jsonl_path,
                line_no,
            )
            continue
        custom_key = data_entry_key(context=context, seed_value=seed_value)
        row_fields = dict(payload)
        custom_hash = _compute_row_hash(
            custom_key=custom_key,
            context=context,
            seed_key=seed_key,
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


def _load_table_from_dir(
    table_dir: Path,
    *,
    context: str,
) -> Optional[Dict[str, Any]]:
    meta_path = table_dir / META_JSON_FILENAME
    rows_path = table_dir / ROWS_JSONL_FILENAME
    if not meta_path.is_file():
        return None
    try:
        raw_meta = json.loads(meta_path.read_text())
        if "fields" not in raw_meta and "columns" in raw_meta:
            raw_meta = {**raw_meta, "fields": raw_meta["columns"]}
        table_meta = CustomDataTableMeta.model_validate(raw_meta)
    except (json.JSONDecodeError, ValidationError) as exc:
        logger.warning("Skipping invalid data meta.json at %s: %s", meta_path, exc)
        return None
    resolved_context = table_meta.context.strip() or context
    if not resolved_context:
        logger.warning("Skipping data table with empty context at %s", meta_path)
        return None
    if not table_meta.auto_sync:
        logger.debug("Skipping data table %s: auto_sync=False", resolved_context)
        return None
    rows: List[Dict[str, Any]] = []
    if rows_path.is_file():
        rows = _parse_rows_jsonl(
            jsonl_path=rows_path,
            context=resolved_context,
            table_meta=table_meta,
        )
    return {
        "context": resolved_context,
        "description": table_meta.description,
        "fields": dict(table_meta.fields),
        "seed_key": table_meta.seed_key,
        "destination": table_meta.destination or "personal",
        "unique_keys": table_meta.unique_keys,
        "auto_counting": table_meta.auto_counting,
        "rows": rows,
    }


def collect_custom_data(
    path: Optional[Path] = None,
) -> Dict[str, Dict[str, Any]]:
    """Load all data tables from a deployment custom data root directory."""
    if path is None:
        logger.debug("Custom data path is None, nothing to collect")
        return {}

    root = Path(path)
    if not root.is_dir():
        logger.debug("No custom data directory found at %s", path)
        return {}

    tables: Dict[str, Dict[str, Any]] = {}
    for meta_path in sorted(root.rglob(META_JSON_FILENAME)):
        context = _context_from_meta_path(root, meta_path)
        table_spec = _load_table_from_dir(meta_path.parent, context=context)
        if table_spec is not None:
            tables[table_spec["context"]] = table_spec

    logger.debug("Collected %d custom data tables from %s", len(tables), root)
    return tables


def collect_data_from_directories(
    directories: List[Path],
) -> Dict[str, Dict[str, Any]]:
    """Collect data tables from multiple directories and merge them."""
    merged: Dict[str, Dict[str, Any]] = {}
    for directory in directories:
        merged = merge_data_table_specs(
            merged,
            collect_custom_data(path=directory),
        )
    return merged


def compute_custom_data_hash(
    source_tables: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """Compute an aggregate hash of custom data rows across all tables."""
    tables = source_tables if source_tables is not None else {}
    row_hashes: List[str] = []
    for context in sorted(tables.keys()):
        spec = tables[context]
        for row in spec.get("rows", []):
            custom_hash = row.get("custom_hash")
            custom_key = row.get("custom_key")
            if custom_hash and custom_key:
                row_hashes.append(f"{custom_key}:{custom_hash}")
    if not row_hashes:
        return ""
    combined = "|".join(row_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def list_data_table_contexts(directories: Iterable[Path]) -> List[str]:
    """Return sorted table contexts discovered under custom data directories."""
    names: set[str] = set()
    for directory in directories:
        root = Path(directory)
        if not root.is_dir():
            continue
        for meta_path in root.rglob(META_JSON_FILENAME):
            context = _context_from_meta_path(root, meta_path)
            table_spec = _load_table_from_dir(meta_path.parent, context=context)
            if table_spec is not None:
                names.add(table_spec["context"])
    return sorted(names)
