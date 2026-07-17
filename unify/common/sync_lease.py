"""Exclusive leases for authoritative ``sync_custom`` critical sections."""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Dict, Iterator, Optional, Set

logger = logging.getLogger(__name__)

# Wait budget for lease acquisition. ``0`` means try once and raise if busy
# (used by live async reconcile so it yields to a blocking Job writer).
_WAIT_SECONDS: ContextVar[Optional[float]] = ContextVar(
    "sync_lease_wait_seconds",
    default=None,
)
_ACTIVE_KEYS: ContextVar[frozenset[str]] = ContextVar(
    "sync_lease_active_keys",
    default=frozenset(),
)

# Process-local fallback when Orchestra has not yet rolled out /sync_lease.
_LOCAL_LOCKS: Dict[str, threading.Lock] = {}
_LOCAL_LOCKS_GUARD = threading.Lock()

DEFAULT_TTL_SECONDS = 900.0
DEFAULT_WAIT_SECONDS = 600.0
DEFAULT_POLL_SECONDS = 1.0


class SyncLeaseBusy(RuntimeError):
    """Raised when the lease could not be acquired within the wait budget."""

    def __init__(self, lease_key: str, *, held_by: str | None = None) -> None:
        self.lease_key = lease_key
        self.held_by = held_by
        super().__init__(
            f"Could not acquire sync lease for {lease_key!r}"
            + (f" (held by {held_by!r})" if held_by else ""),
        )


@contextmanager
def sync_lease_wait_seconds(seconds: float) -> Iterator[None]:
    """Override the wait budget for nested ``exclusive_sync_lease`` calls."""
    token = _WAIT_SECONDS.set(float(seconds))
    try:
        yield
    finally:
        _WAIT_SECONDS.reset(token)


def _default_wait_seconds() -> float:
    override = _WAIT_SECONDS.get()
    if override is not None:
        return float(override)
    raw = os.environ.get("UNITY_SYNC_LEASE_WAIT_SECONDS")
    if raw is not None and raw.strip() != "":
        return float(raw)
    return DEFAULT_WAIT_SECONDS


def _make_holder() -> str:
    host = socket.gethostname() or "host"
    return f"{host}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def _local_lock(lease_key: str) -> threading.Lock:
    with _LOCAL_LOCKS_GUARD:
        lock = _LOCAL_LOCKS.get(lease_key)
        if lock is None:
            lock = threading.Lock()
            _LOCAL_LOCKS[lease_key] = lock
        return lock


@contextmanager
def exclusive_sync_lease(
    lease_key: str,
    *,
    ttl_seconds: float = DEFAULT_TTL_SECONDS,
    wait_seconds: Optional[float] = None,
    poll_seconds: float = DEFAULT_POLL_SECONDS,
) -> Iterator[str]:
    """Hold an exclusive lease for the duration of a ``sync_custom`` body.

    Prefers Orchestra ``/sync_lease/*`` (advisory-lock + durable row). If the
    endpoint is missing (rolling deploy), falls back to a process-local lock so
    same-pod writers still serialize, and logs a warning.

    Nested acquires for the same key in the same task are no-ops (reentrant).
    """
    active: Set[str] = set(_ACTIVE_KEYS.get())
    if lease_key in active:
        yield "reentrant"
        return

    wait_budget = (
        float(wait_seconds) if wait_seconds is not None else _default_wait_seconds()
    )
    holder = _make_holder()
    deadline = time.monotonic() + max(0.0, wait_budget)
    use_remote = True
    held_by: str | None = None

    while True:
        if use_remote:
            try:
                from unisdk import SyncLeaseHeldError, acquire_sync_lease

                acquire_sync_lease(
                    lease_key,
                    holder,
                    ttl_seconds=ttl_seconds,
                )
                break
            except SyncLeaseHeldError as exc:
                held_by = exc.held_by
            except Exception as exc:
                response = getattr(exc, "response", None)
                status_code = getattr(response, "status_code", None)
                if status_code in {404, 405}:
                    logger.warning(
                        "Orchestra sync_lease endpoint unavailable (%s); "
                        "falling back to process-local lock for %s",
                        status_code,
                        lease_key,
                    )
                    use_remote = False
                    continue
                raise
        if not use_remote:
            lock = _local_lock(lease_key)
            acquired = lock.acquire(blocking=False)
            if acquired:
                break
            held_by = "local-process"

        if time.monotonic() >= deadline:
            raise SyncLeaseBusy(lease_key, held_by=held_by)
        time.sleep(max(0.05, float(poll_seconds)))

    active.add(lease_key)
    token = _ACTIVE_KEYS.set(frozenset(active))
    try:
        yield holder
    finally:
        _ACTIVE_KEYS.reset(token)
        if use_remote:
            try:
                from unisdk import release_sync_lease

                release_sync_lease(lease_key, holder)
            except Exception:
                logger.warning(
                    "Failed to release sync lease %s (holder=%s)",
                    lease_key,
                    holder,
                    exc_info=True,
                )
        else:
            _local_lock(lease_key).release()
