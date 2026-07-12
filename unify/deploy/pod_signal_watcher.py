"""Watch AssistantSession signals while a headless offline runner is active.

Communication records ``promoteToCm`` / ``offlineTaskDue`` on the session
status.signals map. This watcher translates those into local filesystem
controls the entrypoint supervisor understands, without starting
ConversationManager.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from unify.deploy.session_boot import (
    OFFLINE_TASK_SIGNAL,
    PROMOTE_CM_PATH,
    PROMOTE_SIGNAL,
    write_offline_env_exportable,
)

LOGGER = logging.getLogger(__name__)
POLL_SECONDS = float(os.environ.get("UNITY_POD_SIGNAL_POLL_SECONDS", "2") or "2")
SEEN_PATH = Path("/tmp/unity-seen-session-signals.json")


def _load_seen() -> dict[str, Any]:
    if not SEEN_PATH.exists():
        return {}
    try:
        return json.loads(SEEN_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _store_seen(seen: dict[str, Any]) -> None:
    SEEN_PATH.write_text(json.dumps(seen), encoding="utf-8")


def _session_signals() -> dict[str, Any]:
    job_name = (os.environ.get("UNITY_CONVERSATION_JOB_NAME") or "").strip()
    if not job_name:
        return {}
    from unify.deploy_runtime import (
        read_assistant_session,
        read_job_assignment_record,
        wait_for_assistant_session_name,
    )

    session_name = wait_for_assistant_session_name(job_name)
    _ = read_job_assignment_record(job_name)
    session = read_assistant_session(session_name)
    status = session.get("status") or {}
    signals = status.get("signals") or {}
    return signals if isinstance(signals, dict) else {}


def _spawn_offline_runner(env_overlay: dict[str, str]) -> None:
    merged = {**os.environ, **env_overlay}
    write_offline_env_exportable(
        {k: v for k, v in env_overlay.items()},
    )
    LOGGER.info(
        "pod_signal_watcher: spawning disconnected offline_runner for task_id=%s",
        env_overlay.get("UNITY_OFFLINE_TASK_ID"),
    )
    subprocess.Popen(
        [sys.executable, "-m", "unify.task_scheduler.offline_runner"],
        env=merged,
        start_new_session=True,
    )


def _handle_signals(signals: dict[str, Any], seen: dict[str, Any]) -> dict[str, Any]:
    promote = signals.get(PROMOTE_SIGNAL)
    if promote and seen.get(PROMOTE_SIGNAL) != promote:
        LOGGER.info("pod_signal_watcher: promoteToCm received")
        PROMOTE_CM_PATH.write_text("1\n", encoding="utf-8")
        seen[PROMOTE_SIGNAL] = promote

    offline = signals.get(OFFLINE_TASK_SIGNAL)
    if offline and seen.get(OFFLINE_TASK_SIGNAL) != offline:
        env = {}
        if isinstance(offline, dict):
            raw_env = offline.get("env") or offline
            if isinstance(raw_env, dict):
                env = {str(k): str(v) for k, v in raw_env.items() if v is not None}
        if env.get("UNITY_OFFLINE_TASK_MODE"):
            _spawn_offline_runner(env)
        seen[OFFLINE_TASK_SIGNAL] = offline
    return seen


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    seen = _load_seen()
    LOGGER.info("pod_signal_watcher: started")
    while True:
        try:
            signals = _session_signals()
            seen = _handle_signals(signals, seen)
            _store_seen(seen)
            if PROMOTE_CM_PATH.exists():
                # Entrypoint takes over CM start; watcher can exit.
                LOGGER.info("pod_signal_watcher: promote file present, exiting")
                return
        except Exception:
            LOGGER.exception("pod_signal_watcher: poll failed")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
