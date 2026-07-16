"""Local subprocess dispatcher for offline-execution tasks.

Local installs run offline tasks as ``unify.task_scheduler.offline_runner``
subprocesses fired by the in-process activation scheduler (or by an explicit
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
    from unify.task_scheduler.machine_state import TaskActivationSnapshot

LOGGER = logging.getLogger(__name__)


class LocalOfflineDispatcher:
    """Spawn ``unify.task_scheduler.offline_runner`` as a child subprocess.

    Used by :class:`LocalActivationScheduler` for activations whose
    ``execution_mode == "offline"``. The dispatcher is fire-and-forget: it
    starts the subprocess and adopts cleanup via a background watcher so
    long-running offline tasks don't keep the scheduler's coroutine pinned.
    """

    def __init__(self) -> None:
        self._inflight: set[asyncio.Task] = set()

    async def dispatch(
        self,
        snap: "TaskActivationSnapshot",
        *,
        source_type: str = "scheduled",
    ) -> asyncio.subprocess.Process:
        """Spawn the offline runner subprocess and return the Process handle.

        Caller does not need to await the subprocess: a background watcher
        task is registered to log the exit code when it terminates. Returned
        ``Process`` is exposed so tests can interrogate the spawn arguments
        and exit status synchronously.
        """

        env = _build_local_offline_runner_env(
            snap,
            source_type=source_type,
        )
        merged_env = {**os.environ, **env}
        # PYTHONUNBUFFERED so subprocess prints reach our log on demand.
        merged_env.setdefault("PYTHONUNBUFFERED", "1")

        LOGGER.info(
            "LocalOfflineDispatcher spawning offline_runner for task_id=%s "
            "(source_type=%s, activation_key=%s)",
            snap.task_id,
            source_type,
            snap.activation_key,
        )

        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "unify.task_scheduler.offline_runner",
            env=merged_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        watcher = asyncio.create_task(self._watch(process, snap, source_type))
        self._inflight.add(watcher)
        watcher.add_done_callback(self._inflight.discard)
        return process

    async def _watch(
        self,
        process: asyncio.subprocess.Process,
        snap: "TaskActivationSnapshot",
        source_type: str,
    ) -> None:
        """Wait for the subprocess to terminate and log its exit reason."""

        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            LOGGER.info(
                "LocalOfflineDispatcher offline_runner completed "
                "(task_id=%s, source_type=%s, activation_key=%s)",
                snap.task_id,
                source_type,
                snap.activation_key,
            )
        else:
            LOGGER.warning(
                "LocalOfflineDispatcher offline_runner exited with code %s "
                "(task_id=%s, source_type=%s, activation_key=%s)\n"
                "stdout=%s\nstderr=%s",
                process.returncode,
                snap.task_id,
                source_type,
                snap.activation_key,
                _truncate(stdout),
                _truncate(stderr),
            )

    async def stop(self) -> None:
        """Cancel all in-flight watcher tasks. Does not kill subprocesses.

        Local offline tasks may run for many minutes; killing them on CM
        shutdown would orphan their durable run rows in an inconsistent
        state. Letting the subprocess finish on its own preserves the
        same semantics as the hosted path, where K8s jobs survive a
        Communication restart.
        """

        for task in list(self._inflight):
            task.cancel()
        # Best-effort wait so logs don't fire after the test exits.
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
    snap: "TaskActivationSnapshot",
    *,
    source_type: str,
    source_ref: str | None = None,
    source_medium: str | None = None,
    source_contact_id: int | None = None,
    source_contact_display_name: str | None = None,
) -> dict[str, str]:
    """Build the env-var dict offline_runner expects for one local subprocess.

    Thin adapter around the shared
    :func:`unify.task_scheduler.offline_runner_contract.build_offline_runner_env`
    builder so the local subprocess sees field-for-field the same env shape
    the hosted Kubernetes job sees. Assistant-identity vars
    (ASSISTANT_FIRST_NAME, USER_NUMBER, VOICE_ID, etc.) are inherited from
    the parent process's ``os.environ`` — a local install already has them
    set by the ``unity`` CLI launcher — so this builder only emits the
    UNITY_OFFLINE_TASK_* + ASSISTANT_ID set.
    """

    resolved_medium = source_medium or (
        str(snap.trigger_medium) if snap.trigger_medium else None
    )
    run_key = _build_local_offline_run_key(
        snap,
        source_type=source_type,
        source_ref=source_ref,
        source_medium=resolved_medium,
        source_contact_id=source_contact_id,
    )
    return build_offline_runner_env(
        assistant_id=str(snap.assistant_id or ""),
        task_id=int(snap.task_id),
        source_task_log_id=int(snap.source_task_log_id or 0),
        activation_revision=str(snap.activation_revision or ""),
        source_type=source_type,
        run_key=run_key,
        task_name=str(snap.task_name or ""),
        task_description=str(snap.task_description or ""),
        scheduled_for=snap.next_due_at,
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
    snap: "TaskActivationSnapshot",
    *,
    source_type: str,
    source_ref: str | None = None,
    source_medium: str | None = None,
    source_contact_id: int | str | None = None,
) -> str:
    """Build the run_key for one local offline execution attempt.

    Thin adapter around the shared
    :func:`unify.task_scheduler.offline_runner_contract.build_offline_run_key`
    so the local and hosted keys collide on the same activation+attempt —
    important when the same Orchestra deployment ever sees runs from both
    topologies (Orchestra's create-or-adopt path then deduplicates).
    """

    return build_offline_run_key(
        assistant_id=str(snap.assistant_id or "unknown"),
        task_id=int(snap.task_id),
        activation_revision=str(snap.activation_revision or ""),
        source_type=source_type,
        scheduled_for=snap.next_due_at,
        source_contact_id=source_contact_id,
        source_medium=source_medium,
        source_ref=source_ref,
    )
