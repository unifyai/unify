"""
Collection of custom knowledge table definitions from deployment directories.

Each table lives in a subdirectory of a knowledge root::

    knowledge/
      Companies/
        meta.json
        rows.jsonl
      CRM/
        OperatingRules/
          meta.json
          rows.jsonl

The relative path from the knowledge root to ``meta.json`` is the table name.
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


class CustomKnowledgeTableMeta(BaseModel):
    """Table-level metadata from ``meta.json``."""

    description: str = ""
    columns: Dict[str, str] = Field(default_factory=dict)
    seed_key: str = Field(min_length=1)
    destination: str = "personal"
    auto_sync: bool = True


def knowledge_entry_key(*, table_name: str, seed_value: str) -> str:
    """Return the stable merge key for one knowledge row."""
    return f"{table_name}|{seed_value}"


def _compute_row_hash(
    *,
    custom_key: str,
    table_name: str,
    seed_key: str,
    destination: str,
    row_fields: Dict[str, Any],
) -> str:
    components = [custom_key, table_name, seed_key, destination or "personal"]
    for field_name in sorted(row_fields.keys()):
        if field_name in {"custom_key", "custom_hash", "row_id"}:
            continue
        value = row_fields[field_name]
        components.append("" if value is None else str(value))
    combined = "\n".join(components)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def merge_knowledge_table_specs(
    base: Dict[str, Dict[str, Any]],
    overlay: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Deep-merge knowledge tables. Overlay columns and rows win on collision."""
    merged = {name: dict(spec) for name, spec in base.items()}
    for table_name, spec in overlay.items():
        if table_name not in merged:
            merged[table_name] = dict(spec)
            continue
        existing = merged[table_name]
        if spec.get("columns"):
            existing.setdefault("columns", {}).update(spec["columns"])
        if spec.get("description"):
            existing["description"] = spec["description"]
        seed_key = spec.get("seed_key") or existing.get("seed_key")
        if seed_key:
            existing["seed_key"] = seed_key
        if spec.get("destination"):
            existing["destination"] = spec["destination"]
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


def _table_name_from_meta_path(root: Path, meta_path: Path) -> str:
    return meta_path.parent.relative_to(root).as_posix()


def _parse_rows_jsonl(
    *,
    jsonl_path: Path,
    table_name: str,
    table_meta: CustomKnowledgeTableMeta,
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
                "Skipping invalid knowledge rows.jsonl line %s:%d: %s",
                jsonl_path,
                line_no,
                exc,
            )
            continue
        if not isinstance(payload, dict):
            logger.warning(
                "Skipping non-object knowledge row %s:%d",
                jsonl_path,
                line_no,
            )
            continue
        seed_value = str(payload.get(seed_key, ""))
        if not seed_value:
            logger.warning(
                "Skipping knowledge row without %s at %s:%d",
                seed_key,
                jsonl_path,
                line_no,
            )
            continue
        custom_key = knowledge_entry_key(
            table_name=table_name,
            seed_value=seed_value,
        )
        row_fields = dict(payload)
        custom_hash = _compute_row_hash(
            custom_key=custom_key,
            table_name=table_name,
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


def _load_table_from_dir(table_dir: Path, table_name: str) -> Optional[Dict[str, Any]]:
    meta_path = table_dir / META_JSON_FILENAME
    rows_path = table_dir / ROWS_JSONL_FILENAME
    if not meta_path.is_file():
        return None
    try:
        table_meta = CustomKnowledgeTableMeta.model_validate(
            json.loads(meta_path.read_text()),
        )
    except (json.JSONDecodeError, ValidationError) as exc:
        logger.warning("Skipping invalid knowledge meta.json at %s: %s", meta_path, exc)
        return None
    if not table_meta.auto_sync:
        logger.debug("Skipping knowledge table %s: auto_sync=False", table_name)
        return None
    rows: List[Dict[str, Any]] = []
    if rows_path.is_file():
        rows = _parse_rows_jsonl(
            jsonl_path=rows_path,
            table_name=table_name,
            table_meta=table_meta,
        )
    return {
        "description": table_meta.description,
        "columns": dict(table_meta.columns),
        "seed_key": table_meta.seed_key,
        "destination": table_meta.destination or "personal",
        "rows": rows,
    }


def collect_custom_knowledge(
    path: Optional[Path] = None,
) -> Dict[str, Dict[str, Any]]:
    """Load all knowledge tables from a deployment knowledge root directory."""
    if path is None:
        logger.debug("Custom knowledge path is None, nothing to collect")
        return {}

    root = Path(path)
    if not root.is_dir():
        logger.debug("No knowledge directory found at %s", path)
        return {}

    tables: Dict[str, Dict[str, Any]] = {}
    for meta_path in sorted(root.rglob(META_JSON_FILENAME)):
        table_name = _table_name_from_meta_path(root, meta_path)
        table_spec = _load_table_from_dir(meta_path.parent, table_name)
        if table_spec is not None:
            tables[table_name] = table_spec

    logger.debug("Collected %d custom knowledge tables from %s", len(tables), root)
    return tables


def collect_knowledge_from_directories(
    directories: List[Path],
) -> Dict[str, Dict[str, Any]]:
    """Collect knowledge tables from multiple directories and merge them."""
    merged: Dict[str, Dict[str, Any]] = {}
    for directory in directories:
        merged = merge_knowledge_table_specs(
            merged,
            collect_custom_knowledge(path=directory),
        )
    return merged


def compute_custom_knowledge_hash(
    source_tables: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """Compute an aggregate hash of custom knowledge rows across all tables."""
    tables = source_tables if source_tables is not None else {}
    row_hashes: List[str] = []
    for table_name in sorted(tables.keys()):
        spec = tables[table_name]
        for row in spec.get("rows", []):
            custom_hash = row.get("custom_hash")
            custom_key = row.get("custom_key")
            if custom_hash and custom_key:
                row_hashes.append(f"{custom_key}:{custom_hash}")
    if not row_hashes:
        return ""
    combined = "|".join(row_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def list_knowledge_table_names(directories: Iterable[Path]) -> List[str]:
    """Return sorted table names discovered under knowledge directories."""
    names: set[str] = set()
    for directory in directories:
        root = Path(directory)
        if not root.is_dir():
            continue
        for meta_path in root.rglob(META_JSON_FILENAME):
            names.add(_table_name_from_meta_path(root, meta_path))
    return sorted(names)
