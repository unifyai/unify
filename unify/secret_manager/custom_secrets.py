"""
Collection of custom secret entries from ``secrets.jsonl`` files.

Source entries live in per-deployment directories as one JSON object per
line. Collection helpers accept explicit paths so sync can target different
source trees across org -> user -> assistant cascade layers.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)

SECRETS_JSONL_FILENAME = "secrets.jsonl"  # pragma: allowlist secret


class CustomSecretSourceEntry(BaseModel):
    """One source-defined secret row from ``secrets.jsonl``."""

    key: str = ""
    name: str = Field(min_length=1)
    value: str = ""
    description: str = ""
    destination: str = "personal"
    auto_sync: bool = True


def secret_entry_key(*, name: str) -> str:
    """Return the stable merge key for a secret row."""
    return name


def _compute_secret_hash(
    *,
    key: str,
    name: str,
    value: str,
    description: str,
    destination: str,
) -> str:
    components = [
        key,
        name,
        value,
        description or "",
        destination or "personal",
    ]
    combined = "\n".join(components)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def _resolve_jsonl_path(path: Path) -> Optional[Path]:
    if path.is_file() and path.suffix == ".jsonl":
        return path
    if path.is_dir():
        candidate = path / SECRETS_JSONL_FILENAME
        if candidate.is_file():
            return candidate
    return None


def _entry_to_source_dict(entry: CustomSecretSourceEntry) -> Dict[str, Any]:
    destination = entry.destination or "personal"
    custom_hash = _compute_secret_hash(
        key=entry.key,
        name=entry.name,
        value=entry.value,
        description=entry.description,
        destination=destination,
    )
    return {
        "custom_key": entry.key,
        "name": entry.name,
        "value": entry.value,
        "description": entry.description,
        "destination": destination,
        "custom_hash": custom_hash,
    }


def _parse_jsonl_file(jsonl_path: Path) -> List[CustomSecretSourceEntry]:
    entries: List[CustomSecretSourceEntry] = []
    for line_no, raw_line in enumerate(jsonl_path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            payload = json.loads(line)
            entry = CustomSecretSourceEntry.model_validate(payload)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning(
                "Skipping invalid secrets.jsonl line %s:%d: %s",
                jsonl_path,
                line_no,
                exc,
            )
            continue
        if not entry.key:
            entry.key = secret_entry_key(name=entry.name)
        if not entry.auto_sync:
            logger.debug("Skipping %s: auto_sync=False", entry.key)
            continue
        if not (entry.value or "").strip():
            logger.debug("Skipping %s: empty deploy-time value", entry.key)
            continue
        entries.append(entry)
    return entries


def collect_custom_secrets(
    path: Optional[Path] = None,
) -> Dict[str, Dict[str, Any]]:
    """Load ``secrets.jsonl`` from a directory or direct file path."""
    if path is None:
        logger.debug("Custom secrets path is None, nothing to collect")
        return {}

    jsonl_path = _resolve_jsonl_path(Path(path))
    if jsonl_path is None:
        logger.debug("No secrets.jsonl found at %s", path)
        return {}

    secrets: Dict[str, Dict[str, Any]] = {}
    for entry in _parse_jsonl_file(jsonl_path):
        secrets[entry.key] = _entry_to_source_dict(entry)

    logger.debug(
        "Collected %d custom secret entries from %s",
        len(secrets),
        jsonl_path,
    )
    return secrets


def collect_secrets_from_secret_models(
    secrets: Iterable[Any],
) -> Dict[str, Dict[str, Any]]:
    """Collect deploy-time secrets from resolved supplemental Secret models."""
    collected: Dict[str, Dict[str, Any]] = {}
    for secret in secrets:
        value = getattr(secret, "value", "") or ""
        if not str(value).strip():
            continue
        name = getattr(secret, "name", "")
        if not name:
            continue
        entry = CustomSecretSourceEntry(
            key=secret_entry_key(name=name),
            name=name,
            value=str(value),
            description=getattr(secret, "description", "") or "",
            destination=getattr(secret, "destination", "personal") or "personal",
        )
        collected[entry.key] = _entry_to_source_dict(entry)
    return collected


def compute_custom_secrets_hash(
    source_secrets: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """Compute an aggregate hash of custom secret entries."""
    secrets = source_secrets if source_secrets is not None else {}
    if not secrets:
        return ""

    sorted_hashes = [secrets[key]["custom_hash"] for key in sorted(secrets.keys())]
    combined = "|".join(sorted_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def collect_secrets_from_directories(
    directories: List[Path],
) -> Dict[str, Dict[str, Any]]:
    """Collect custom secrets from multiple directories and merge.

    Later directories override earlier ones when keys collide.
    """
    merged: Dict[str, Dict[str, Any]] = {}
    for directory in directories:
        merged.update(collect_custom_secrets(path=directory))
    return merged
