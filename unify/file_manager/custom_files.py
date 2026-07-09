"""
Collection of deployment-defined required file mappings.

Each ``files_dir`` contains a ``files_map.json`` object map::

    {
      "templates/": "/Templates",
      "policies/handbook.pdf": "/Policies/handbook.pdf"
    }

Keys are paths relative to that ``files_dir`` (file or directory). Values are
destination paths under the assistant Local root. Directory keys expand
recursively. Later ``files_dir`` layers override earlier ones on the same
destination path.

The map is a required overlay: listed paths must be present upstream with the
local bytes. It does not claim to own the full filesystem.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

FILES_MAP_FILENAME = "files_map.json"


def normalize_dest_path(dest: str) -> str:
    """Normalize a destination path to a leading-slash Local-relative form."""
    cleaned = str(dest or "").strip().replace("\\", "/")
    while "//" in cleaned:
        cleaned = cleaned.replace("//", "/")
    if not cleaned or cleaned == "/":
        raise ValueError("destination path must not be empty")
    parts = [part for part in cleaned.split("/") if part not in {"", "."}]
    if any(part == ".." for part in parts):
        raise ValueError(f"destination path must not contain '..': {dest!r}")
    return "/" + "/".join(parts)


def _normalize_source_key(key: str) -> str:
    cleaned = str(key or "").strip().replace("\\", "/")
    while cleaned.startswith("./"):
        cleaned = cleaned[2:]
    return cleaned.strip("/")


def _is_under(root: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _file_content_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _expand_mapping(
    *,
    files_dir: Path,
    source_key: str,
    dest_value: str,
) -> List[Dict[str, Any]]:
    rel_key = _normalize_source_key(source_key)
    if not rel_key:
        logger.warning(
            "Skipping empty source key in %s/%s",
            files_dir,
            FILES_MAP_FILENAME,
        )
        return []
    if rel_key.startswith("/") or ".." in Path(rel_key).parts:
        logger.warning(
            "Skipping unsafe source key %r in %s/%s",
            source_key,
            files_dir,
            FILES_MAP_FILENAME,
        )
        return []

    try:
        dest_root = normalize_dest_path(dest_value)
    except ValueError as exc:
        logger.warning(
            "Skipping invalid destination %r for source %r in %s: %s",
            dest_value,
            source_key,
            files_dir,
            exc,
        )
        return []

    source_path = (files_dir / rel_key).resolve()
    if not _is_under(files_dir, source_path):
        logger.warning(
            "Skipping source path outside files_dir (%s): %s",
            files_dir,
            source_path,
        )
        return []
    if not source_path.exists():
        logger.warning(
            "Skipping missing source path %s (mapped to %s)",
            source_path,
            dest_root,
        )
        return []

    mappings: List[Dict[str, Any]] = []
    if source_path.is_file():
        mappings.append(
            {
                "dest_path": dest_root,
                "source_path": str(source_path),
                "content_hash": _file_content_hash(source_path),
            },
        )
        return mappings

    if not source_path.is_dir():
        logger.warning(
            "Skipping non-file/non-dir source path %s",
            source_path,
        )
        return []

    for child in sorted(p for p in source_path.rglob("*") if p.is_file()):
        if not _is_under(source_path, child):
            continue
        rel = child.relative_to(source_path).as_posix()
        dest_path = normalize_dest_path(f"{dest_root.rstrip('/')}/{rel}")
        mappings.append(
            {
                "dest_path": dest_path,
                "source_path": str(child.resolve()),
                "content_hash": _file_content_hash(child),
            },
        )
    if not mappings:
        logger.warning(
            "Directory mapping %s -> %s expanded to zero files",
            source_path,
            dest_root,
        )
    return mappings


def collect_custom_files(
    path: Optional[Path] = None,
) -> Dict[str, Dict[str, Any]]:
    """Load ``files_map.json`` from one deployment files directory."""
    if path is None:
        logger.debug("Custom files path is None, nothing to collect")
        return {}

    root = Path(path)
    if not root.is_dir():
        logger.warning("Custom files path is not a directory: %s", root)
        return {}

    map_path = root / FILES_MAP_FILENAME
    if not map_path.is_file():
        logger.warning("Missing %s in %s", FILES_MAP_FILENAME, root)
        return {}

    try:
        payload = json.loads(map_path.read_text())
    except json.JSONDecodeError as exc:
        logger.warning(
            "Skipping invalid %s at %s: %s",
            FILES_MAP_FILENAME,
            map_path,
            exc,
        )
        return {}

    if not isinstance(payload, dict):
        logger.warning(
            "Skipping non-object %s at %s",
            FILES_MAP_FILENAME,
            map_path,
        )
        return {}

    collected: Dict[str, Dict[str, Any]] = {}
    for source_key, dest_value in payload.items():
        if not isinstance(source_key, str) or not isinstance(dest_value, str):
            logger.warning(
                "Skipping non-string map entry %r -> %r in %s",
                source_key,
                dest_value,
                map_path,
            )
            continue
        for mapping in _expand_mapping(
            files_dir=root,
            source_key=source_key,
            dest_value=dest_value,
        ):
            collected[mapping["dest_path"]] = mapping
    return collected


def merge_file_mappings(
    base: Dict[str, Dict[str, Any]],
    overlay: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Merge required file mappings. Overlay wins on destination collision."""
    merged = {dest: dict(spec) for dest, spec in base.items()}
    for dest, spec in overlay.items():
        merged[dest] = dict(spec)
    return merged


def collect_files_from_directories(
    directories: Iterable[Path],
) -> Dict[str, Dict[str, Any]]:
    """Collect and merge required file mappings from multiple files dirs."""
    merged: Dict[str, Dict[str, Any]] = {}
    for directory in directories:
        merged = merge_file_mappings(merged, collect_custom_files(path=directory))
    return merged


def compute_custom_files_hash(
    *,
    source_files: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """Return a stable aggregate hash for the required file mapping set."""
    if not source_files:
        return ""
    parts: List[str] = []
    for dest_path in sorted(source_files):
        spec = source_files[dest_path]
        parts.append(
            f"{dest_path}\n{spec.get('content_hash', '')}\n{spec.get('source_path', '')}",
        )
    combined = "\n".join(parts)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]
