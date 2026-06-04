"""Scheduler contracts for gateway maintenance work."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

ScheduledJob = Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class ScheduledHandle:
    """Handle for a scheduled maintenance job."""

    name: str


@runtime_checkable
class Scheduler(Protocol):
    """Run gateway maintenance work on local or hosted backends."""

    async def schedule_interval(
        self,
        name: str,
        *,
        interval_seconds: float,
        job: ScheduledJob,
    ) -> ScheduledHandle:
        """Schedule ``job`` to run repeatedly."""

    async def cancel(self, handle: ScheduledHandle) -> None:
        """Cancel a scheduled job."""


class LocalScheduler:
    """In-process asyncio scheduler for self-hosted gateway processes."""

    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task] = {}

    async def schedule_interval(
        self,
        name: str,
        *,
        interval_seconds: float,
        job: ScheduledJob,
    ) -> ScheduledHandle:
        if name in self._tasks:
            raise RuntimeError(f"Scheduled job already exists: {name}")

        async def _loop() -> None:
            while True:
                await job()
                await asyncio.sleep(interval_seconds)

        self._tasks[name] = asyncio.create_task(_loop(), name=f"gateway:{name}")
        return ScheduledHandle(name=name)

    async def cancel(self, handle: ScheduledHandle) -> None:
        task = self._tasks.pop(handle.name)
        task.cancel()


class MissingScheduler:
    """Scheduler backend that fails when scheduling is not configured."""

    async def schedule_interval(
        self,
        name: str,
        *,
        interval_seconds: float,
        job: ScheduledJob,
    ) -> ScheduledHandle:
        del name, interval_seconds, job
        raise RuntimeError("No gateway scheduler backend is configured.")

    async def cancel(self, handle: ScheduledHandle) -> None:
        del handle


__all__ = [
    "LocalScheduler",
    "MissingScheduler",
    "ScheduledHandle",
    "ScheduledJob",
    "Scheduler",
]
