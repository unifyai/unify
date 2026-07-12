"""
Collection and synchronization of custom knowledge claims.

Source entries are claim dicts keyed by a stable ``custom_key``, typically
loaded from deployment directories or passed directly to ``sync_custom``.
Each claim carries title/content/kind/topics (and optional provenance).
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)

KNOWLEDGE_JSONL_FILENAME = "knowledge.jsonl"


class CustomKnowledgeSourceEntry(BaseModel):
    """One source-defined knowledge claim from ``knowledge.jsonl``."""

    key: str = Field(min_length=1)
    title: str = Field(min_length=1, max_length=200)
    content: str = Field(min_length=1)
    kind: str = "fact"
    topics: List[str] = Field(default_factory=list)
    destination: str = "personal"
    auto_sync: bool = True
    source_refs: List[Dict[str, Any]] = Field(default_factory=list)


def _compute_claim_hash(
    *,
    key: str,
    title: str,
    content: str,
    kind: str,
    topics: List[str],
    destination: str,
) -> str:
    components = [
        key,
        title,
        content,
        kind or "fact",
        "|".join(sorted(topics or [])),
        destination or "personal",
    ]
    combined = "\n".join(components)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def _resolve_jsonl_path(path: Path) -> Optional[Path]:
    if path.is_file() and path.suffix == ".jsonl":
        return path
    if path.is_dir():
        candidate = path / KNOWLEDGE_JSONL_FILENAME
        if candidate.is_file():
            return candidate
    return None


def _parse_jsonl_file(jsonl_path: Path) -> List[CustomKnowledgeSourceEntry]:
    entries: List[CustomKnowledgeSourceEntry] = []
    for line_no, raw_line in enumerate(jsonl_path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            payload = json.loads(line)
            entry = CustomKnowledgeSourceEntry.model_validate(payload)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning(
                "Skipping invalid knowledge.jsonl line %s:%d: %s",
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


def collect_custom_knowledge(
    path: Optional[Path] = None,
) -> Dict[str, Dict[str, Any]]:
    """Load ``knowledge.jsonl`` from a directory or direct file path.

    Returns a dict mapping stable ``key`` to claim metadata suitable for
    ``KnowledgeManager.sync_custom``.
    """
    if path is None:
        logger.debug("Custom knowledge path is None, nothing to collect")
        return {}

    jsonl_path = _resolve_jsonl_path(Path(path))
    if jsonl_path is None:
        logger.debug("No knowledge.jsonl found at %s", path)
        return {}

    claims: Dict[str, Dict[str, Any]] = {}
    for entry in _parse_jsonl_file(jsonl_path):
        destination = entry.destination or "personal"
        custom_hash = _compute_claim_hash(
            key=entry.key,
            title=entry.title,
            content=entry.content,
            kind=entry.kind,
            topics=list(entry.topics),
            destination=destination,
        )
        claims[entry.key] = {
            "custom_key": entry.key,
            "title": entry.title,
            "content": entry.content,
            "kind": entry.kind,
            "topics": list(entry.topics),
            "source_refs": list(entry.source_refs),
            "destination": destination,
            "custom_hash": custom_hash,
            "is_builtin": False,
            "status": "active",
        }

    logger.debug(
        "Collected %d custom knowledge claims from %s",
        len(claims),
        jsonl_path,
    )
    return claims


def collect_knowledge_from_directories(
    directories: List[Path],
) -> Dict[str, Dict[str, Any]]:
    """Collect custom knowledge from multiple directories and merge.

    Later directories override earlier ones when keys collide.
    """
    merged: Dict[str, Dict[str, Any]] = {}
    for directory in directories:
        merged.update(collect_custom_knowledge(path=directory))
    return merged


def compute_custom_knowledge_hash(
    source_claims: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """Compute an aggregate hash of custom knowledge claims."""
    claims = source_claims if source_claims is not None else {}
    if not claims:
        return ""

    sorted_hashes = [claims[key]["custom_hash"] for key in sorted(claims.keys())]
    combined = "|".join(sorted_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def knowledge_titles_from_source(
    source_claims: Dict[str, Dict[str, Any]],
) -> List[str]:
    """Return sorted knowledge titles from a collected source dict."""
    return sorted(
        entry.get("title", "") for entry in source_claims.values() if entry.get("title")
    )
