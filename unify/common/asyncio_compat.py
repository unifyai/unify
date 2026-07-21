"""Loop-safe helpers for bridging sync and async call sites.

Offline TaskScheduler Jobs (and other Unify entrypoints) already own an
event loop via ``asyncio.run``. Nested ``asyncio.run`` then fails with
``RuntimeError: asyncio.run() cannot be called from a running event
loop``. Use :func:`run_coro_sync` from sync façades that need to drive
async work from either context.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Awaitable, Callable
from typing import TypeVar

R = TypeVar("R")


def run_coro_sync(factory: Callable[[], Awaitable[R]]) -> R:
    """Run an async factory from sync code, including under a running loop.

    When no loop is running, delegates to ``asyncio.run``. When a loop is
    already running, schedules the factory on a private worker thread that
    owns its own loop so nested ``asyncio.run`` is avoided on the caller
    thread.

    Prefer ``async def`` + ``await`` end-to-end when authoring stored task
    entrypoints. Use this helper only when a sync façade is required (CLI
    runners, legacy sync libraries, sync symbolic helpers).
    """

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(factory())).result()


__all__ = ["run_coro_sync"]
