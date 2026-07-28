"""Mirrored task row field vocabulary for provider-event revision CAS.

Orchestra's ``orchestra.services.task_row_field`` module is the canonical
owner. Keep these enums byte-identical and validate against the shared
fixture in ``tests/fixtures/task_trigger_contract/task_row_field_contract.v1.json``.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class AuthoredTaskField(StrEnum):
    """Task JSONB keys that require revision CAS on provider-event rows."""

    name = "name"
    description = "description"
    schedule = "schedule"
    trigger = "trigger"
    enabled = "enabled"
    offline = "offline"
    requires_filesystem = "requires_filesystem"
    requires_computer = "requires_computer"
    entrypoint = "entrypoint"
    priority = "priority"
    repeat = "repeat"
    deadline = "deadline"
    response_policy = "response_policy"
    destination = "destination"

    @classmethod
    def values(cls) -> frozenset[str]:
        return frozenset(member.value for member in cls)

    @classmethod
    def intersects(cls, keys: set[str]) -> bool:
        return bool(keys & cls.values())


class RuntimeTaskField(StrEnum):
    """Task JSONB keys that may change without bumping task_revision.

    Run outcome lives on ``Tasks/Executions``, so what the runtime may still
    write to a definition is narrow: the one-shot completion stamp and the
    human-readable summary. ``enabled`` is deliberately authored — arming is an
    operator decision and must take the revision CAS path.
    """

    completed_at = "completed_at"
    info = "info"

    @classmethod
    def values(cls) -> frozenset[str]:
        return frozenset(member.value for member in cls)


def split_provider_event_task_update(
    entries: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split one provider-event patch into authored and runtime buckets."""

    known = AuthoredTaskField.values() | RuntimeTaskField.values()
    unknown = set(entries.keys()) - known
    if unknown:
        raise ValueError(
            "Unclassified provider-event task fields: " f"{', '.join(sorted(unknown))}",
        )

    authored: dict[str, Any] = {}
    runtime: dict[str, Any] = {}
    for key, value in entries.items():
        if key in AuthoredTaskField.values():
            authored[key] = value
        elif key in RuntimeTaskField.values():
            runtime[key] = value
    return authored, runtime
