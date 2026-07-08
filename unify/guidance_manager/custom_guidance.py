"""
Collection and synchronization of custom guidance from ``guidance.jsonl`` files.

Source entries live in per-deployment or per-integration directories as a
``guidance.jsonl`` file with one JSON object per line. Collection helpers
accept explicit paths so sync can target different source trees for different
clients (org -> user -> assistant cascade).
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)

GUIDANCE_JSONL_FILENAME = "guidance.jsonl"


class CustomGuidanceSourceEntry(BaseModel):
    """One source-defined guidance row from ``guidance.jsonl``."""

    key: str = Field(min_length=1)
    title: str = Field(min_length=1, max_length=200)
    content: str = Field(min_length=1)
    function_names: List[str] = Field(default_factory=list)
    destination: str = "personal"
    auto_sync: bool = True


def _compute_guidance_hash(
    *,
    key: str,
    title: str,
    content: str,
    function_names: List[str],
    destination: str,
) -> str:
    components = [
        key,
        title,
        content,
        "|".join(sorted(function_names or [])),
        destination or "personal",
    ]
    combined = "\n".join(components)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def _resolve_jsonl_path(path: Path) -> Optional[Path]:
    if path.is_file() and path.suffix == ".jsonl":
        return path
    if path.is_dir():
        candidate = path / GUIDANCE_JSONL_FILENAME
        if candidate.is_file():
            return candidate
    return None


def _parse_jsonl_file(jsonl_path: Path) -> List[CustomGuidanceSourceEntry]:
    entries: List[CustomGuidanceSourceEntry] = []
    for line_no, raw_line in enumerate(jsonl_path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            payload = json.loads(line)
            entry = CustomGuidanceSourceEntry.model_validate(payload)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning(
                "Skipping invalid guidance.jsonl line %s:%d: %s",
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


def collect_custom_guidance(
    path: Optional[Path] = None,
) -> Dict[str, Dict[str, Any]]:
    """Load ``guidance.jsonl`` from a directory or direct file path.

    Args:
        path: Directory containing ``guidance.jsonl`` or a ``.jsonl`` file.
            If *None* or missing, returns an empty dict.

    Returns:
        Dict mapping stable ``key`` to metadata with title, content,
        function_names, destination, and custom_hash.
    """
    if path is None:
        logger.debug("Custom guidance path is None, nothing to collect")
        return {}

    jsonl_path = _resolve_jsonl_path(Path(path))
    if jsonl_path is None:
        logger.debug("No guidance.jsonl found at %s", path)
        return {}

    guidance: Dict[str, Dict[str, Any]] = {}
    for entry in _parse_jsonl_file(jsonl_path):
        destination = entry.destination or "personal"
        custom_hash = _compute_guidance_hash(
            key=entry.key,
            title=entry.title,
            content=entry.content,
            function_names=list(entry.function_names),
            destination=destination,
        )
        guidance[entry.key] = {
            "custom_key": entry.key,
            "title": entry.title,
            "content": entry.content,
            "function_names": list(entry.function_names),
            "destination": destination,
            "custom_hash": custom_hash,
            "images": [],
            "function_ids": [],
            "is_builtin": False,
        }

    logger.debug(
        "Collected %d custom guidance entries from %s",
        len(guidance),
        jsonl_path,
    )
    return guidance


def compute_custom_guidance_hash(
    source_guidance: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """Compute an aggregate hash of custom guidance entries."""
    guidance = source_guidance if source_guidance is not None else {}
    if not guidance:
        return ""

    sorted_hashes = [guidance[key]["custom_hash"] for key in sorted(guidance.keys())]
    combined = "|".join(sorted_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def collect_guidance_from_directories(
    directories: List[Path],
) -> Dict[str, Dict[str, Any]]:
    """Collect custom guidance from multiple directories and merge.

    Later directories override earlier ones when keys collide.
    """
    merged: Dict[str, Dict[str, Any]] = {}
    for directory in directories:
        merged.update(collect_custom_guidance(path=directory))
    return merged


def guidance_titles_from_source(
    source_guidance: Dict[str, Dict[str, Any]],
) -> List[str]:
    """Return sorted guidance titles from a collected source dict."""
    return sorted(
        entry.get("title", "")
        for entry in source_guidance.values()
        if entry.get("title")
    )
