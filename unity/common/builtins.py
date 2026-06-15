"""Shared addressing and seeding state for the global builtins project.

The builtins project is a public-read Unify project (setting
``UNITY_BUILTINS_PROJECT``, default ``"Builtins"``) owned by the platform
admin account. It stores exactly one platform-wide copy of fixed catalogue
data — builtin function primitives and builtin guidance — that every
deployment reads but never writes.

Each catalogue tracks its own seeding convergence through a singleton meta
row holding a per-unit content-hash map, so repeated seeding runs are cheap
and idempotent.
"""

from __future__ import annotations

from typing import Dict

import unify


def builtins_project() -> str:
    """Return the configured name of the public builtins catalogue project."""
    from unity.settings import SETTINGS

    return SETTINGS.UNITY_BUILTINS_PROJECT


def read_seed_hashes(project: str, *, meta_context: str, key: str) -> Dict[str, str]:
    """Read a catalogue's per-unit content-hash map from its meta row."""
    logs = unify.get_logs(
        project=project,
        context=meta_context,
        filter="meta_id == 1",
        limit=1,
    )
    if logs:
        return logs[0].entries.get(key, {}) or {}
    return {}


def write_seed_hashes(
    project: str,
    hashes: Dict[str, str],
    *,
    meta_context: str,
    key: str,
) -> None:
    """Replace a catalogue's per-unit content-hash map in its meta row."""
    logs = unify.get_logs(
        project=project,
        context=meta_context,
        filter="meta_id == 1",
        limit=1,
    )
    if logs:
        unify.delete_logs(
            project=project,
            context=meta_context,
            logs=[logs[0].id],
        )
    unify.create_logs(
        project=project,
        context=meta_context,
        entries=[{"meta_id": 1, key: hashes}],
    )


__all__ = [
    "builtins_project",
    "read_seed_hashes",
    "write_seed_hashes",
]
