"""Shared addressing and seeding state for the builtins project.

The builtins project (setting ``UNIFY_BUILTINS_PROJECT``, default
``"Builtins"``) holds one copy of fixed catalogue data — builtin function
primitives and builtin guidance — seeded from the committed snapshots.

Each catalogue tracks its own seeding convergence through a singleton meta
row holding a per-unit content-hash map, so repeated seeding runs are cheap
and idempotent.
"""

from __future__ import annotations

from typing import Dict

from unify import db


def builtins_project() -> str:
    """Return the configured name of the public builtins catalogue project."""
    from unify.settings import SETTINGS

    return SETTINGS.UNIFY_BUILTINS_PROJECT


def ensure_builtins_project(project: str) -> None:
    """Create the builtins project if it does not exist."""
    db.create_project(project, exist_ok=True, is_public_read=True)


def read_seed_hashes(project: str, *, meta_context: str, key: str) -> Dict[str, str]:
    """Read a catalogue's per-unit content-hash map from its meta row."""
    logs = db.get_logs(
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
    logs = db.get_logs(
        project=project,
        context=meta_context,
        filter="meta_id == 1",
        limit=1,
    )
    if logs:
        db.delete_logs(
            project=project,
            context=meta_context,
            logs=[logs[0].id],
        )
    db.create_logs(
        project=project,
        context=meta_context,
        entries=[{"meta_id": 1, key: hashes}],
    )


__all__ = [
    "builtins_project",
    "ensure_builtins_project",
    "read_seed_hashes",
    "write_seed_hashes",
]
