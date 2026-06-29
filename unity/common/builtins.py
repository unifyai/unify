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

import os
from contextlib import contextmanager
from typing import Dict, Iterator
from urllib.parse import quote

import unisdk
from unisdk.utils import http
from unisdk.utils.helpers import _create_request_header, _validate_api_key


def builtins_project() -> str:
    """Return the configured name of the public builtins catalogue project."""
    from unity.settings import SETTINGS

    return SETTINGS.UNITY_BUILTINS_PROJECT


def ensure_builtins_project(project: str) -> None:
    """Create and converge the public-read Builtins project."""
    unisdk.create_project(project, exist_ok=True, is_public_read=True)
    api_key = _validate_api_key(None)
    headers = _create_request_header(api_key)
    http.patch(
        f"{unisdk.BASE_URL}/project/{quote(project, safe='')}",
        headers=headers,
        json={"is_public_read": True},
    )


@contextmanager
def builtins_seed_key_override() -> Iterator[None]:
    """Run a Builtins seed/convergence under the Orchestra admin key.

    The Builtins project is reserved: Orchestra only authorises the admin key to
    create or write it (normal users get a read-only public copy). Production
    seeding already runs the whole process with ``UNIFY_KEY`` set to the admin
    key, so this is a no-op there. Test/CI processes run with a *user*
    ``UNIFY_KEY``; when ``ORCHESTRA_ADMIN_KEY`` is configured, swap it in for the
    duration of the seed so the reserved-project writes are authorised, then
    restore the original key.
    """
    from unity.settings import SETTINGS

    admin_key = SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()
    current = os.environ.get("UNIFY_KEY")
    if not admin_key or admin_key == current:
        yield
        return
    os.environ["UNIFY_KEY"] = admin_key
    try:
        yield
    finally:
        if current is None:
            os.environ.pop("UNIFY_KEY", None)
        else:
            os.environ["UNIFY_KEY"] = current


def read_seed_hashes(project: str, *, meta_context: str, key: str) -> Dict[str, str]:
    """Read a catalogue's per-unit content-hash map from its meta row."""
    logs = unisdk.get_logs(
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
    logs = unisdk.get_logs(
        project=project,
        context=meta_context,
        filter="meta_id == 1",
        limit=1,
    )
    if logs:
        unisdk.delete_logs(
            project=project,
            context=meta_context,
            logs=[logs[0].id],
        )
    unisdk.create_logs(
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
