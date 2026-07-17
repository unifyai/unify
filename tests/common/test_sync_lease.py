"""Unit tests for exclusive_sync_lease reentrancy and local fallback."""

from __future__ import annotations

import threading
from unittest.mock import patch

from unify.common.sync_lease import SyncLeaseBusy, exclusive_sync_lease


def test_exclusive_sync_lease_is_reentrant():
    with (
        patch(
            "unisdk.acquire_sync_lease",
            return_value={"acquired": True},
        ),
        patch(
            "unisdk.release_sync_lease",
            return_value={"released": True},
        ),
        patch(
            "unisdk.SyncLeaseHeldError",
            type("SyncLeaseHeldError", (Exception,), {}),
        ),
    ):
        with exclusive_sync_lease("k1", wait_seconds=0):
            with exclusive_sync_lease("k1", wait_seconds=0) as holder:
                assert holder == "reentrant"


def test_exclusive_sync_lease_local_fallback_busy_when_held():
    class _MissingEndpoint(Exception):
        def __init__(self) -> None:
            self.response = type("R", (), {"status_code": 404})()

    with (
        patch(
            "unisdk.acquire_sync_lease",
            side_effect=_MissingEndpoint,
        ),
        patch(
            "unisdk.SyncLeaseHeldError",
            type("SyncLeaseHeldError", (Exception,), {}),
        ),
    ):
        held = threading.Event()
        release = threading.Event()
        outcomes: list[str] = []

        def holder() -> None:
            with exclusive_sync_lease("local-key", wait_seconds=0):
                held.set()
                release.wait(timeout=2)
                outcomes.append("holder-done")

        def waiter() -> None:
            held.wait(timeout=2)
            try:
                with exclusive_sync_lease("local-key", wait_seconds=0):
                    outcomes.append("waiter-acquired")
            except SyncLeaseBusy:
                outcomes.append("waiter-busy")

        t1 = threading.Thread(target=holder)
        t2 = threading.Thread(target=waiter)
        t1.start()
        t2.start()
        t2.join(timeout=2)
        release.set()
        t1.join(timeout=2)
        assert "waiter-busy" in outcomes
        assert "holder-done" in outcomes
        assert "waiter-acquired" not in outcomes
