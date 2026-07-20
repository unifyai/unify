"""Pure helpers for provider-trigger actor tool reporting."""

from __future__ import annotations

from unify.task_scheduler.provider_trigger_actor import (
    CATALOG_REPORTING_NOTE,
    CATALOG_VISIBILITY,
    EMPTY_CONNECTIONS_NOTE,
    annotate_provider_trigger_catalog,
    annotate_provider_trigger_connections,
)


def test_annotate_provider_trigger_catalog_marks_connection_gated_visibility() -> None:
    details = annotate_provider_trigger_catalog(
        {"available": True, "triggers": [{"provider_trigger_slug": "X"}]},
    )

    assert details["available"] is True
    assert details["triggers"][0]["provider_trigger_slug"] == "X"
    assert details["visibility"] == CATALOG_VISIBILITY
    assert details["reporting_note"] == CATALOG_REPORTING_NOTE


def test_annotate_provider_trigger_connections_notes_empty_active_filter() -> None:
    empty = annotate_provider_trigger_connections([])
    assert empty["connections"] == []
    assert empty["reporting_note"] == EMPTY_CONNECTIONS_NOTE

    present = annotate_provider_trigger_connections(
        [{"connection_id": "conn-1", "canonical_app_slug": "github"}],
    )
    assert present["connections"][0]["connection_id"] == "conn-1"
    assert "reporting_note" not in present
