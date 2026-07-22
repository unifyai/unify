"""In-pod admission gate for control-plane drain / restart.

Pods poll ``GET /infra/assistants/{id}/restart`` with their own ``UNIFY_KEY``.
While ``draining`` is true, new ``act`` calls must refuse so a never-idle
assistant can finish in-flight work and recycle onto a fresh client bundle.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

import requests

log = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 5.0
_lock = threading.Lock()
_cached_at = 0.0
_cached_blocked = False
_cached_detail: dict[str, Any] | None = None


class DrainInProgressError(RuntimeError):
    """Raised when a new act is refused because drain/restart is armed."""


def _comms_base_url() -> str:
    return (
        os.environ.get("UNITY_COMMS_URL")
        or os.environ.get("COMMS_URL")
        or os.environ.get("DROID_COMMS_URL")
        or ""
    ).rstrip("/")


def _assistant_id() -> str:
    return str(os.environ.get("ASSISTANT_ID") or "").strip()


def _unify_key() -> str:
    return str(os.environ.get("UNIFY_KEY") or "").strip()


def refresh_drain_status(*, force: bool = False) -> dict[str, Any] | None:
    """Fetch drain status from comms; caches briefly to avoid per-act RTT."""
    global _cached_at, _cached_blocked, _cached_detail

    now = time.monotonic()
    with _lock:
        if (
            not force
            and _cached_detail is not None
            and (now - _cached_at) < _CACHE_TTL_SECONDS
        ):
            return _cached_detail

    base = _comms_base_url()
    assistant_id = _assistant_id()
    key = _unify_key()
    if not base or not assistant_id or not key:
        return None

    url = f"{base}/infra/assistants/{assistant_id}/restart"
    try:
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {key}"},
            timeout=3.0,
        )
        if response.status_code == 404:
            detail = {"draining": False}
        else:
            response.raise_for_status()
            detail = response.json() if response.content else {"draining": False}
    except Exception:  # noqa: BLE001 — fail open for transient control-plane blips
        log.debug("drain status probe failed", exc_info=True)
        return None

    blocked = bool(detail.get("draining"))
    with _lock:
        _cached_at = time.monotonic()
        _cached_blocked = blocked
        _cached_detail = detail if isinstance(detail, dict) else {"draining": blocked}
        return _cached_detail


def is_admission_blocked(*, force_refresh: bool = False) -> bool:
    """True when the control plane has armed drain for this assistant."""
    detail = refresh_drain_status(force=force_refresh)
    if detail is None:
        with _lock:
            # Keep last known blocked state across transient errors.
            return _cached_blocked
    return bool(detail.get("draining"))


def refuse_if_draining() -> None:
    """Raise ``DrainInProgressError`` when new acts must not start."""
    if is_admission_blocked():
        raise DrainInProgressError(
            "Assistant drain/restart is in progress; new act calls are refused",
        )


__all__ = [
    "DrainInProgressError",
    "is_admission_blocked",
    "refuse_if_draining",
    "refresh_drain_status",
]
