"""Local subprocess dispatcher for offline-execution tasks.

Local installs run offline tasks as ``unify.task_scheduler.offline_runner``
subprocesses fired by the in-process execution scheduler (or by an explicit
REST trigger). This is the local analogue of the hosted lane, where each
offline run executes as a dedicated one-shot Kubernetes Job; both consume the
same env contract from ``offline_runner_contract``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import TYPE_CHECKING

from unify.task_scheduler.offline_runner_contract import (
    build_offline_run_key,
    build_offline_runner_env,
)

if TYPE_CHECKING:
    from unify.task_scheduler.machine_state import TaskExecutionSnapshot

LOGGER = logging.getLogger(__name__)


class LocalOfflineDispatcher:
    """Spawn ``unify.task_scheduler.offline_runner`` as a child subprocess."""

    def __init__(self) -> None:
        self._inflight: set[asyncio.Task] = set()

    async def dispatch(
        self,
        snap: "TaskExecutionSnapshot",
        *,
        wake: str = "scheduled",
    ) -> asyncio.subprocess.Process:
        """Spawn the offline runner subprocess and return the Process handle."""

        env = _build_local_offline_runner_env(
            snap,
            wake=wake,
        )
        merged_env = {**os.environ, **env}
        merged_env.setdefault("PYTHONUNBUFFERED", "1")

        LOGGER.info(
            "LocalOfflineDispatcher spawning offline_runner for task_id=%s "
            "(wake=%s, run_key=%s)",
            snap.task_id,
            wake,
            snap.run_key,
        )

        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "unify.task_scheduler.offline_runner",
            env=merged_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        watcher = asyncio.create_task(self._watch(process, snap, wake))
        self._inflight.add(watcher)
        watcher.add_done_callback(self._inflight.discard)
        return process

    async def _watch(
        self,
        process: asyncio.subprocess.Process,
        snap: "TaskExecutionSnapshot",
        wake: str,
    ) -> None:
        """Wait for the subprocess to terminate and log its exit reason."""

        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            LOGGER.info(
                "LocalOfflineDispatcher offline_runner completed "
                "(task_id=%s, wake=%s, run_key=%s)",
                snap.task_id,
                wake,
                snap.run_key,
            )
        else:
            LOGGER.warning(
                "LocalOfflineDispatcher offline_runner exited with code %s "
                "(task_id=%s, wake=%s, run_key=%s)\n"
                "stdout=%s\nstderr=%s",
                process.returncode,
                snap.task_id,
                wake,
                snap.run_key,
                _truncate(stdout),
                _truncate(stderr),
            )

    async def stop(self) -> None:
        """Cancel all in-flight watcher tasks. Does not kill subprocesses."""

        for task in list(self._inflight):
            task.cancel()
        await asyncio.gather(*self._inflight, return_exceptions=True)
        self._inflight.clear()


def _truncate(payload: bytes | None, *, limit: int = 2000) -> str:
    """Decode and truncate subprocess output for log lines."""

    if not payload:
        return ""
    text = payload.decode("utf-8", errors="replace")
    if len(text) <= limit:
        return text
    return text[:limit] + "… (truncated)"


def _build_local_offline_runner_env(
    snap: "TaskExecutionSnapshot",
    *,
    wake: str,
    source_ref: str | None = None,
    source_medium: str | None = None,
    source_contact_id: int | None = None,
    source_contact_display_name: str | None = None,
) -> dict[str, str]:
    """Build the env-var dict offline_runner expects for one local subprocess."""

    resolved_medium = source_medium or (
        str(snap.trigger_medium) if snap.trigger_medium else None
    )
    run_key = _build_local_offline_run_key(
        snap,
        wake=wake,
        source_ref=source_ref,
        source_medium=resolved_medium,
        source_contact_id=source_contact_id,
    )
    return build_offline_runner_env(
        assistant_id=str(snap.assistant_id or ""),
        task_id=int(snap.task_id),
        source_task_log_id=int(snap.source_task_log_id or 0),
        revision=str(snap.revision or ""),
        wake=wake,
        run_key=run_key,
        task_name=str(snap.task_name or ""),
        task_description=str(snap.task_description or ""),
        scheduled_for=snap.scheduled_for,
        source_ref=source_ref,
        source_medium=resolved_medium,
        source_contact_id=source_contact_id,
        source_contact_display_name=source_contact_display_name,
        entrypoint=snap.entrypoint,
        destination=snap.destination,
        requires_filesystem=bool(snap.requires_filesystem),
        requires_computer=bool(snap.requires_computer),
    )


def _build_local_offline_run_key(
    snap: "TaskExecutionSnapshot",
    *,
    wake: str,
    source_ref: str | None = None,
    source_medium: str | None = None,
    source_contact_id: int | str | None = None,
) -> str:
    """Build the run_key for one local offline execution attempt."""

    return build_offline_run_key(
        assistant_id=str(snap.assistant_id or "unknown"),
        task_id=int(snap.task_id),
        revision=str(snap.revision or ""),
        wake=wake,
        scheduled_for=snap.scheduled_for,
        source_contact_id=source_contact_id,
        source_medium=source_medium,
        source_ref=source_ref,
    )
