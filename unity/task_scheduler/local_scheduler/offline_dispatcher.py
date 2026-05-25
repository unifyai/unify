"""Local subprocess dispatcher for offline-execution tasks.

The hosted ``Communication`` service materialises offline scheduled /
triggered tasks as Kubernetes jobs that run ``unity.task_scheduler.offline_runner``
with the activation context injected as env vars. Local installs have no
K8s cluster, so this module spawns the runner as an ordinary Python
subprocess from the conversation manager's own process.

Communication's ``_build_offline_runner_env`` shape is the source of truth
for which env vars ``offline_runner._load_config_from_env`` expects. This
module mirrors the minimal subset needed for execution and inherits the
rest from ``os.environ`` (UNIFY_KEY, ORCHESTRA_URL, ORCHESTRA_ADMIN_KEY,
ASSISTANT_*, USER_*) which a local install already has set.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from unity.task_scheduler.machine_state import TaskActivationSnapshot

LOGGER = logging.getLogger(__name__)


class LocalOfflineDispatcher:
    """Spawn ``unity.task_scheduler.offline_runner`` as a child subprocess.

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

        env = _build_local_offline_runner_env(snap, source_type=source_type)
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
            "unity.task_scheduler.offline_runner",
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
    """Build the env-var dict offline_runner expects for one activation.

    Mirrors ``communication.infra.task_activation._build_offline_runner_env``
    field-for-field for the task-specific variables. Assistant-identity vars
    (ASSISTANT_FIRST_NAME, USER_NUMBER, VOICE_ID, etc.) are inherited from
    the parent process's ``os.environ`` — a local install already has them
    set by the ``unity`` CLI launcher.
    """

    request_text = _request_text(snap)
    function_id = snap.entrypoint
    run_key = _build_local_offline_run_key(snap, source_type=source_type)

    env: dict[str, str] = {
        "UNITY_OFFLINE_TASK_MODE": "actor",
        "EVENTBUS_PUBLISHING_ENABLED": "false",
        "EVENTBUS_PUBSUB_STREAMING": "false",
        "UNITY_OFFLINE_TASK_RUN_KEY": run_key,
        "UNITY_OFFLINE_TASK_ID": str(snap.task_id),
        "UNITY_OFFLINE_TASK_SOURCE_TASK_LOG_ID": str(snap.source_task_log_id or 0),
        "UNITY_OFFLINE_TASK_ACTIVATION_REVISION": str(snap.activation_revision or ""),
        "UNITY_OFFLINE_TASK_FUNCTION_ID": str(int(function_id)) if function_id else "",
        "UNITY_OFFLINE_TASK_REQUEST": request_text,
        "UNITY_OFFLINE_TASK_NAME": str(snap.task_name or ""),
        "UNITY_OFFLINE_TASK_DESCRIPTION": str(snap.task_description or ""),
        "UNITY_OFFLINE_TASK_SOURCE_TYPE": source_type,
        "UNITY_OFFLINE_TASK_SCHEDULED_FOR": _iso_utc_or_empty(snap.next_due_at),
        "UNITY_OFFLINE_TASK_SOURCE_REF": source_ref or "",
        "UNITY_OFFLINE_TASK_SOURCE_MEDIUM": (
            source_medium or str(snap.trigger_medium or "")
        ),
        "UNITY_OFFLINE_TASK_SOURCE_CONTACT_ID": (
            str(source_contact_id) if source_contact_id is not None else ""
        ),
    }
    if source_contact_display_name:
        env["UNITY_OFFLINE_TASK_SOURCE_CONTACT_DISPLAY_NAME"] = (
            source_contact_display_name
        )
    if snap.assistant_id:
        env["ASSISTANT_ID"] = str(snap.assistant_id)
    return env


def _request_text(snap: "TaskActivationSnapshot") -> str:
    """Choose the most descriptive text to feed offline_runner as the request."""

    text = (snap.task_description or "").strip()
    if text:
        return text
    text = (snap.task_name or "").strip()
    if text:
        return text
    return f"Execute task {snap.task_id}"


def _iso_utc_or_empty(value: str | None) -> str:
    """Return ``value`` normalised to UTC ISO-8601, or empty string."""

    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _build_local_offline_run_key(
    snap: "TaskActivationSnapshot",
    *,
    source_type: str,
) -> str:
    """Build a deterministic run_key for one offline execution attempt.

    Mirrors the shape of Communication's ``_build_offline_run_key`` so that
    if a single Orchestra deployment ever sees both local and hosted runs
    of the same activation they collide on the same run row (Orchestra's
    create-or-adopt path then takes over and avoids duplicate execution).
    """

    revision = (snap.activation_revision or "").encode("utf-8")
    revision_digest = hashlib.sha256(revision).hexdigest()[:12]

    tail_parts: list[str] = []
    if snap.next_due_at:
        try:
            ts = datetime.fromisoformat(snap.next_due_at.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            tail_parts.append(ts.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        except (TypeError, ValueError):
            pass
    tail = "-".join(tail_parts) or "once"
    return (
        f"offline:{source_type}:{snap.assistant_id or 'unknown'}:"
        f"{snap.task_id}:{revision_digest}:{tail}"
    )
