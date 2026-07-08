"""
Collection of custom contact entries from ``contacts.jsonl`` files.

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

from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

CONTACTS_JSONL_FILENAME = "contacts.jsonl"

CONTACT_SYNC_FIELDS = (
    "first_name",
    "surname",
    "email_address",
    "phone_number",
    "whatsapp_number",
    "discord_id",
    "slack_user_id",
    "bio",
    "job_title",
    "should_respond",
    "response_policy",
    "timezone",
)


class CustomContactSourceEntry(BaseModel):
    """One source-defined contact row from ``contacts.jsonl``."""

    key: str = ""
    first_name: Optional[str] = None
    surname: Optional[str] = None
    email_address: Optional[str] = None
    phone_number: Optional[str] = None
    whatsapp_number: Optional[str] = None
    discord_id: Optional[str] = None
    slack_user_id: Optional[str] = None
    bio: Optional[str] = None
    job_title: Optional[str] = None
    should_respond: bool = True
    response_policy: Optional[str] = None
    timezone: Optional[str] = None
    destination: str = "personal"
    auto_sync: bool = True


def contact_entry_key(*, first_name: str | None, surname: str | None) -> str:
    """Return the stable merge key for a contact row."""
    return f"{first_name or ''}|{surname or ''}".lower()


def _compute_contact_hash(
    *,
    key: str,
    destination: str,
    fields: Dict[str, Any],
) -> str:
    components = [key, destination or "personal"]
    for field_name in CONTACT_SYNC_FIELDS:
        value = fields.get(field_name)
        if field_name == "should_respond":
            components.append(str(bool(value)))
        else:
            components.append("" if value is None else str(value))
    combined = "\n".join(components)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def _resolve_jsonl_path(path: Path) -> Optional[Path]:
    if path.is_file() and path.suffix == ".jsonl":
        return path
    if path.is_dir():
        candidate = path / CONTACTS_JSONL_FILENAME
        if candidate.is_file():
            return candidate
    return None


def _entry_fields(entry: CustomContactSourceEntry) -> Dict[str, Any]:
    fields: Dict[str, Any] = {}
    for field_name in CONTACT_SYNC_FIELDS:
        value = getattr(entry, field_name)
        if field_name == "should_respond":
            fields[field_name] = bool(value)
        elif value is not None:
            fields[field_name] = value
    return fields


def _parse_jsonl_file(jsonl_path: Path) -> List[CustomContactSourceEntry]:
    entries: List[CustomContactSourceEntry] = []
    for line_no, raw_line in enumerate(jsonl_path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            payload = json.loads(line)
            entry = CustomContactSourceEntry.model_validate(payload)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning(
                "Skipping invalid contacts.jsonl line %s:%d: %s",
                jsonl_path,
                line_no,
                exc,
            )
            continue
        if not entry.key:
            entry.key = contact_entry_key(
                first_name=entry.first_name,
                surname=entry.surname,
            )
        if not entry.auto_sync:
            logger.debug("Skipping %s: auto_sync=False", entry.key)
            continue
        entries.append(entry)
    return entries


def collect_custom_contacts(
    path: Optional[Path] = None,
) -> Dict[str, Dict[str, Any]]:
    """Load ``contacts.jsonl`` from a directory or direct file path."""
    if path is None:
        logger.debug("Custom contacts path is None, nothing to collect")
        return {}

    jsonl_path = _resolve_jsonl_path(Path(path))
    if jsonl_path is None:
        logger.debug("No contacts.jsonl found at %s", path)
        return {}

    contacts: Dict[str, Dict[str, Any]] = {}
    for entry in _parse_jsonl_file(jsonl_path):
        destination = entry.destination or "personal"
        fields = _entry_fields(entry)
        custom_hash = _compute_contact_hash(
            key=entry.key,
            destination=destination,
            fields=fields,
        )
        contacts[entry.key] = {
            "custom_key": entry.key,
            "destination": destination,
            "custom_hash": custom_hash,
            **fields,
        }

    logger.debug(
        "Collected %d custom contact entries from %s",
        len(contacts),
        jsonl_path,
    )
    return contacts


def compute_custom_contacts_hash(
    source_contacts: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """Compute an aggregate hash of custom contact entries."""
    contacts = source_contacts if source_contacts is not None else {}
    if not contacts:
        return ""

    sorted_hashes = [contacts[key]["custom_hash"] for key in sorted(contacts.keys())]
    combined = "|".join(sorted_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def collect_contacts_from_directories(
    directories: List[Path],
) -> Dict[str, Dict[str, Any]]:
    """Collect custom contacts from multiple directories and merge.

    Later directories override earlier ones when keys collide.
    """
    merged: Dict[str, Dict[str, Any]] = {}
    for directory in directories:
        merged.update(collect_custom_contacts(path=directory))
    return merged
