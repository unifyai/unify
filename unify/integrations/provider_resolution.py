"""Cross-provider integration app resolution for Unity catalogue reads."""

from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

PREFERRED_BACKEND_ORDER = ("composio", "pipedream")

EXPLICIT_SLUG_ALIASES: dict[str, str] = {
    "microsoft_outlook": "outlook",
    "microsoft_outlook_calendar": "outlook",
    "microsoft_excel": "excel",
    "microsoft_onedrive": "onedrive",
    "microsoft_365": "office_365",
    "microsoft_365_people": "office_365",
    "microsoft_365_planner": "office_365",
    "google_calendar": "googlecalendar",
    "google_sheets": "googlesheets",
    "google_drive": "googledrive",
    "google_docs": "googledocs",
    "google_meet": "googlemeet",
    "google_forms": "googleforms",
    "google_contacts": "googlecontacts",
    "google_slides": "googleslides",
    "google_tasks": "googletasks",
    "google_chat": "googlechat",
    "airtable_oauth": "airtable",
    "databricks_oauth": "databricks",
    "gorgias_oauth": "gorgias",
    "highlevel_oauth": "highlevel",
    "sendfox_oauth": "sendfox",
    "snowflake_oauth": "snowflake",
    "apify_oauth": "apify",
}

_STRIP_SUFFIXES = (
    "_oauth",
    "_api",
    "_connect",
    "_integration",
    "_integrations",
    "_app",
)


def _slugify(value: str) -> str:
    normalized = "".join(
        char.lower() if char.isalnum() else "_" for char in value.strip()
    )
    return "_".join(part for part in normalized.split("_") if part)


def _normalize_display_name(value: str | None) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()
    for suffix in (" oauth", " api", " integration"):
        if text.endswith(suffix):
            text = text[: -len(suffix)].strip()
    return re.sub(r"\s+", " ", text)


def logical_app_key(
    *,
    canonical_app_slug: str,
    display_name: str | None = None,
    provider_app_id: str | None = None,
) -> str:
    preferred = _slugify(canonical_app_slug)
    if preferred in EXPLICIT_SLUG_ALIASES:
        return EXPLICIT_SLUG_ALIASES[preferred]
    if preferred.startswith("microsoft_"):
        return preferred.removeprefix("microsoft_")
    for suffix in _STRIP_SUFFIXES:
        if preferred.endswith(suffix):
            return preferred[: -len(suffix)]
    name_key = _normalize_display_name(display_name)
    if name_key:
        name_slug = _slugify(name_key)
        if name_slug in EXPLICIT_SLUG_ALIASES:
            return EXPLICIT_SLUG_ALIASES[name_slug]
        return name_slug
    if provider_app_id:
        provider_slug = _slugify(str(provider_app_id))
        if provider_slug in EXPLICIT_SLUG_ALIASES:
            return EXPLICIT_SLUG_ALIASES[provider_slug]
    return preferred


def _backend_preference_rank(backend_id: str) -> int:
    try:
        return PREFERRED_BACKEND_ORDER.index(backend_id)
    except ValueError:
        return len(PREFERRED_BACKEND_ORDER)


def resolve_public_catalog_apps(
    apps: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in apps:
        slug = _slugify(
            str(
                entry.get("canonical_app_slug")
                or entry.get("app_slug")
                or entry.get("provider_app_id")
                or "",
            ),
        )
        key = logical_app_key(
            canonical_app_slug=slug,
            display_name=str(
                entry.get("display_name") or entry.get("app_display_name") or "",
            )
            or None,
            provider_app_id=str(entry.get("provider_app_id") or "") or None,
        )
        grouped.setdefault(key, []).append(dict(entry))
    resolved: list[dict[str, Any]] = []
    for key in sorted(grouped):
        rows = grouped[key]
        rows.sort(
            key=lambda row: (
                _backend_preference_rank(str(row.get("backend_id") or "provider")),
                str(row.get("display_name") or row.get("canonical_app_slug") or ""),
            ),
        )
        resolved.append(rows[0])
    return resolved
