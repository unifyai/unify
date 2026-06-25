"""Helpers for metadata stored on Function rows for integration primitives."""

from __future__ import annotations

from typing import Any

PROVIDER_BACKED_SOURCE = "provider_backed"
INTEGRATION_METADATA_KEY = "integration"


def function_metadata(row: dict[str, Any] | None) -> dict[str, Any]:
    metadata = (row or {}).get("metadata") or {}
    return metadata if isinstance(metadata, dict) else {}


def function_source(row: dict[str, Any] | None) -> str | None:
    source = function_metadata(row).get("source")
    return str(source) if source else None


def is_provider_backed_function(row: dict[str, Any] | None) -> bool:
    return function_source(row) == PROVIDER_BACKED_SOURCE


def integration_metadata(row: dict[str, Any] | None) -> dict[str, Any]:
    integration = function_metadata(row).get(INTEGRATION_METADATA_KEY) or {}
    return integration if isinstance(integration, dict) else {}


def integration_tool_id(row: dict[str, Any] | None) -> str | None:
    value = integration_metadata(row).get("tool_id")
    return str(value) if value else None


def integration_backend_id(row: dict[str, Any] | None) -> str | None:
    value = integration_metadata(row).get("backend_id")
    return str(value) if value else None


def integration_app_slug(row: dict[str, Any] | None) -> str | None:
    value = integration_metadata(row).get("app_slug")
    return str(value) if value else None


def integration_connection_id(row: dict[str, Any] | None) -> str | None:
    value = integration_metadata(row).get("connection_id")
    return str(value) if value else None


def integration_input_schema(row: dict[str, Any] | None) -> dict[str, Any]:
    value = integration_metadata(row).get("input_schema") or {}
    return value if isinstance(value, dict) else {}


def provider_function_metadata(integration: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": PROVIDER_BACKED_SOURCE,
        INTEGRATION_METADATA_KEY: integration,
    }
