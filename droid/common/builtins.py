"""Shared addressing and seeding state for the global builtins project.

The builtins project is a public-read Unify project (setting
``DROID_BUILTINS_PROJECT``, default ``"Builtins"``) owned by the platform
admin account. It stores exactly one platform-wide copy of fixed catalogue
data — builtin function primitives and builtin guidance — that every
deployment reads but never writes.

Each catalogue tracks its own seeding convergence through a singleton meta
row holding a per-unit content-hash map, so repeated seeding runs are cheap
and idempotent.
"""

from __future__ import annotations

from typing import Dict
from urllib.parse import quote

import unify
from unify.utils import http
from unify.utils.helpers import _create_request_header, _validate_api_key


def builtins_project() -> str:
    """Return the configured name of the public builtins catalogue project."""
    from droid.settings import SETTINGS

    return SETTINGS.DROID_BUILTINS_PROJECT


def ensure_builtins_project(project: str) -> None:
    """Create and converge the public-read Builtins project."""
    unify.create_project(project, exist_ok=True, is_public_read=True)
    api_key = _validate_api_key(None)
    headers = _create_request_header(api_key)
    http.patch(
        f"{unify.BASE_URL}/project/{quote(project, safe='')}",
        headers=headers,
        json={"is_public_read": True},
    )


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
    "ensure_builtins_project",
    "read_seed_hashes",
    "write_seed_hashes",
]
