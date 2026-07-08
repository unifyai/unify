"""
Collection of custom blacklist entries from ``blacklist.jsonl`` files.

Source entries live in per-deployment directories as one JSON object per
line. Collection helpers accept explicit paths so sync can target different
source trees across org -> user -> assistant cascade layers.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)

BLACKLIST_JSONL_FILENAME = "blacklist.jsonl"


class CustomBlacklistSourceEntry(BaseModel):
    """One source-defined blacklist row from ``blacklist.jsonl``."""

    key: str = Field(min_length=1)
    medium: str = Field(min_length=1)
    contact_detail: str = Field(min_length=1)
    reason: str = ""
    destination: str = "personal"
    auto_sync: bool = True


def blacklist_entry_key(*, medium: str, contact_detail: str) -> str:
    """Return the stable merge key for a blacklist row."""
    return f"{medium}|{contact_detail}"


def _compute_blacklist_hash(
    *,
    key: str,
    medium: str,
    contact_detail: str,
    reason: str,
    destination: str,
) -> str:
    components = [
        key,
        medium,
        contact_detail,
        reason or "",
        destination or "personal",
    ]
    combined = "\n".join(components)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def _resolve_jsonl_path(path: Path) -> Optional[Path]:
    if path.is_file() and path.suffix == ".jsonl":
        return path
    if path.is_dir():
        candidate = path / BLACKLIST_JSONL_FILENAME
        if candidate.is_file():
            return candidate
    return None


def _parse_jsonl_file(jsonl_path: Path) -> List[CustomBlacklistSourceEntry]:
    entries: List[CustomBlacklistSourceEntry] = []
    for line_no, raw_line in enumerate(jsonl_path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            payload = json.loads(line)
            entry = CustomBlacklistSourceEntry.model_validate(payload)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning(
                "Skipping invalid blacklist.jsonl line %s:%d: %s",
                jsonl_path,
                line_no,
                exc,
            )
            continue
        if not entry.auto_sync:
            logger.debug("Skipping %s: auto_sync=False", entry.key)
            continue
        entries.append(entry)
    return entries


def collect_custom_blacklist(
    path: Optional[Path] = None,
) -> Dict[str, Dict[str, Any]]:
    """Load ``blacklist.jsonl`` from a directory or direct file path."""
    if path is None:
        logger.debug("Custom blacklist path is None, nothing to collect")
        return {}

    jsonl_path = _resolve_jsonl_path(Path(path))
    if jsonl_path is None:
        logger.debug("No blacklist.jsonl found at %s", path)
        return {}

    blacklist: Dict[str, Dict[str, Any]] = {}
    for entry in _parse_jsonl_file(jsonl_path):
        destination = entry.destination or "personal"
        custom_hash = _compute_blacklist_hash(
            key=entry.key,
            medium=entry.medium,
            contact_detail=entry.contact_detail,
            reason=entry.reason,
            destination=destination,
        )
        blacklist[entry.key] = {
            "custom_key": entry.key,
            "medium": entry.medium,
            "contact_detail": entry.contact_detail,
            "reason": entry.reason,
            "destination": destination,
            "custom_hash": custom_hash,
        }

    logger.debug(
        "Collected %d custom blacklist entries from %s",
        len(blacklist),
        jsonl_path,
    )
    return blacklist


def compute_custom_blacklist_hash(
    source_blacklist: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """Compute an aggregate hash of custom blacklist entries."""
    blacklist = source_blacklist if source_blacklist is not None else {}
    if not blacklist:
        return ""

    sorted_hashes = [blacklist[key]["custom_hash"] for key in sorted(blacklist.keys())]
    combined = "|".join(sorted_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def collect_blacklist_from_directories(
    directories: List[Path],
) -> Dict[str, Dict[str, Any]]:
    """Collect custom blacklist from multiple directories and merge.

    Later directories override earlier ones when keys collide.
    """
    merged: Dict[str, Dict[str, Any]] = {}
    for directory in directories:
        merged.update(collect_custom_blacklist(path=directory))
    return merged
