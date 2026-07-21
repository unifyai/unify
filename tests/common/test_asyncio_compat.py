"""Tests for loop-safe sync→async bridging."""

from __future__ import annotations

import asyncio

from unify.common.asyncio_compat import run_coro_sync


async def _value(n: int) -> int:
    await asyncio.sleep(0)
    return n


def test_run_coro_sync_without_running_loop():
    assert run_coro_sync(lambda: _value(7)) == 7


def test_run_coro_sync_inside_running_loop():
    async def outer() -> int:
        # Mirrors TaskScheduler offline_runner: sync helper under asyncio.run.
        return run_coro_sync(lambda: _value(11))

    assert asyncio.run(outer()) == 11
