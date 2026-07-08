"""
Collection of custom task definitions from ``tasks.jsonl`` files.

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

from .types.status import Status

logger = logging.getLogger(__name__)

TASKS_JSONL_FILENAME = "tasks.jsonl"

TASK_SYNC_FIELDS = (
    "name",
    "description",
    "schedule",
    "trigger",
    "deadline",
    "repeat",
    "priority",
    "response_policy",
    "entrypoint_function",
    "offline",
)


class CustomTaskSourceEntry(BaseModel):
    """One source-defined task row from ``tasks.jsonl``."""

    key: str = Field(min_length=1)
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    schedule: Optional[Dict[str, Any]] = None
    trigger: Optional[Dict[str, Any]] = None
    deadline: Optional[str] = None
    repeat: Optional[List[Dict[str, Any]]] = None
    priority: str = "normal"
    response_policy: Optional[str] = None
    entrypoint_function: Optional[str] = None
    offline: bool = False
    destination: str = "personal"
    auto_sync: bool = True


def _stable_json(value: Any) -> str:
    if value is None:
        return ""
    return json.dumps(value, sort_keys=True, default=str)


def derive_initial_task_status(
    *,
    schedule: Optional[Dict[str, Any]],
    trigger: Optional[Dict[str, Any]],
) -> Status:
    """Return the initial status for a newly inserted source-defined task."""
    if trigger is not None:
        return Status.triggerable
    if schedule is not None and schedule.get("start_at") is not None:
        return Status.scheduled
    return Status.scheduled


def _compute_task_hash(
    *,
    key: str,
    destination: str,
    fields: Dict[str, Any],
) -> str:
    components = [key, destination or "personal"]
    for field_name in TASK_SYNC_FIELDS:
        value = fields.get(field_name)
        if field_name == "offline":
            components.append(str(bool(value)))
        elif field_name in {"schedule", "trigger", "repeat"}:
            components.append(_stable_json(value))
        else:
            components.append("" if value is None else str(value))
    combined = "\n".join(components)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def _normalize_task_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload)
    if "task_name" in normalized and "name" not in normalized:
        normalized["name"] = normalized.pop("task_name")
    if "task_description" in normalized and "description" not in normalized:
        normalized["description"] = normalized.pop("task_description")
    if "enabled" in normalized and "auto_sync" not in normalized:
        normalized["auto_sync"] = bool(normalized.pop("enabled"))
    execution_mode = normalized.pop("execution_mode", None)
    if execution_mode is not None and "offline" not in normalized:
        normalized["offline"] = str(execution_mode).strip().lower() == "offline"
    return normalized


def _resolve_jsonl_path(path: Path) -> Optional[Path]:
    if path.is_file() and path.suffix == ".jsonl":
        return path
    if path.is_dir():
        candidate = path / TASKS_JSONL_FILENAME
        if candidate.is_file():
            return candidate
    return None


def _parse_jsonl_file(jsonl_path: Path) -> List[CustomTaskSourceEntry]:
    entries: List[CustomTaskSourceEntry] = []
    for line_no, raw_line in enumerate(jsonl_path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError("task row must be a JSON object")
            entry = CustomTaskSourceEntry.model_validate(
                _normalize_task_payload(payload),
            )
        except (json.JSONDecodeError, ValidationError, ValueError) as exc:
            logger.warning(
                "Skipping invalid tasks.jsonl line %s:%d: %s",
                jsonl_path,
                line_no,
                exc,
            )
            continue
        if entry.schedule is not None and entry.trigger is not None:
            logger.warning(
                "Skipping tasks.jsonl line %s:%d: schedule and trigger are mutually exclusive",
                jsonl_path,
                line_no,
            )
            continue
        if not entry.auto_sync:
            logger.debug("Skipping %s: auto_sync=False", entry.key)
            continue
        entries.append(entry)
    return entries


def collect_custom_tasks(
    path: Optional[Path] = None,
) -> Dict[str, Dict[str, Any]]:
    """Load ``tasks.jsonl`` from a directory or direct file path."""
    if path is None:
        logger.debug("Custom tasks path is None, nothing to collect")
        return {}

    jsonl_path = _resolve_jsonl_path(Path(path))
    if jsonl_path is None:
        logger.debug("No tasks.jsonl found at %s", path)
        return {}

    tasks: Dict[str, Dict[str, Any]] = {}
    for entry in _parse_jsonl_file(jsonl_path):
        destination = entry.destination or "personal"
        fields = {
            "name": entry.name,
            "description": entry.description,
            "schedule": entry.schedule,
            "trigger": entry.trigger,
            "deadline": entry.deadline,
            "repeat": entry.repeat,
            "priority": entry.priority,
            "response_policy": entry.response_policy,
            "entrypoint_function": entry.entrypoint_function,
            "offline": entry.offline,
        }
        custom_hash = _compute_task_hash(
            key=entry.key,
            destination=destination,
            fields=fields,
        )
        tasks[entry.key] = {
            "custom_key": entry.key,
            "custom_hash": custom_hash,
            "destination": destination,
            **fields,
        }

    logger.debug(
        "Collected %d custom task entries from %s",
        len(tasks),
        jsonl_path,
    )
    return tasks


def compute_custom_tasks_hash(
    source_tasks: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """Compute an aggregate hash of custom task entries."""
    tasks = source_tasks if source_tasks is not None else {}
    if not tasks:
        return ""

    sorted_hashes = [tasks[key]["custom_hash"] for key in sorted(tasks.keys())]
    combined = "|".join(sorted_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def collect_tasks_from_directories(
    directories: List[Path],
) -> Dict[str, Dict[str, Any]]:
    """Collect custom tasks from multiple directories and merge."""
    merged: Dict[str, Dict[str, Any]] = {}
    for directory in directories:
        merged.update(collect_custom_tasks(path=directory))
    return merged
