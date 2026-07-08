"""
Collection helpers for deployment-defined integration registry rows.

Rows are projected from integration manifests during deployment resolution
and keyed by integration ``slug``.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, Iterable, List

logger = logging.getLogger(__name__)

INTEGRATION_REGISTRY_SYNC_FIELDS = (
    "slug",
    "label",
    "category",
    "version",
    "tier",
    "quality",
    "required_secrets_json",
    "optional_secrets_json",
    "capability_ids_json",
    "function_names_json",
    "guidance_titles_json",
    "tags_json",
    "homepage",
    "description",
)


def integration_registry_entry_key(*, slug: str) -> str:
    """Return the stable merge key for one integration registry row."""
    return slug


def _compute_registry_hash(
    *,
    key: str,
    fields: Dict[str, Any],
) -> str:
    components = [key]
    for field_name in INTEGRATION_REGISTRY_SYNC_FIELDS:
        value = fields.get(field_name)
        components.append("" if value is None else str(value))
    combined = "\n".join(components)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def _entry_to_source_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    slug = str(row.get("slug", "")).strip()
    key = integration_registry_entry_key(slug=slug)
    custom_hash = _compute_registry_hash(key=key, fields=row)
    return {
        **row,
        "custom_key": key,
        "custom_hash": custom_hash,
    }


def collect_integration_registry_from_rows(
    rows: Iterable[Dict[str, Any]] | None,
) -> Dict[str, Dict[str, Any]]:
    """Collect integration registry rows keyed by ``slug``."""
    if rows is None:
        logger.debug("Integration registry rows are None, nothing to collect")
        return {}

    collected: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        slug = str(row.get("slug", "")).strip()
        if not slug:
            logger.debug("Skipping integration registry row without slug")
            continue
        key = integration_registry_entry_key(slug=slug)
        collected[key] = _entry_to_source_dict(dict(row))

    logger.debug("Collected %d integration registry rows", len(collected))
    return collected


def compute_custom_integration_registry_hash(
    source_registry: Dict[str, Dict[str, Any]] | None = None,
) -> str:
    """Compute an aggregate hash of custom integration registry rows."""
    registry = source_registry if source_registry is not None else {}
    if not registry:
        return ""

    sorted_hashes = [registry[key]["custom_hash"] for key in sorted(registry.keys())]
    combined = "|".join(sorted_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def registry_rows_from_source(
    source_registry: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Return manifest-shaped rows without custom sync metadata."""
    rows: List[Dict[str, Any]] = []
    for key in sorted(source_registry.keys()):
        row = {
            k: v
            for k, v in source_registry[key].items()
            if k not in {"custom_key", "custom_hash"}
        }
        rows.append(row)
    return rows
