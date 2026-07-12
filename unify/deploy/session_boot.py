"""Pod session bootstrap for interactive and headless-offline modes.

Live ConversationManager pods normally discover their AssistantSession
assignment inside ``CommsManager._poll_for_assignment``. Headless offline
boots must do the same *without* starting CM, then export the offline-task
env contract and mark the container ready.

This module is the shared preflight invoked from ``deploy/entrypoint.sh``
before either the offline runner or ConversationManager starts.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

SESSION_MODE_PATH = Path("/tmp/unity-session-mode")
OFFLINE_ENV_PATH = Path("/tmp/unity-offline.env")
PROMOTE_CM_PATH = Path("/tmp/unity-promote-cm")
CM_ATTACHED_SIGNAL = "cmAttached"
PROMOTE_SIGNAL = "promoteToCm"
OFFLINE_TASK_SIGNAL = "offlineTaskDue"


def _write_mode(mode: str) -> None:
    SESSION_MODE_PATH.write_text(mode.strip() + "\n", encoding="utf-8")


def _write_offline_env(env: dict[str, str]) -> None:
    lines = [f"{key}={value}" for key, value in sorted(env.items())]
    OFFLINE_ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def write_offline_env_exportable(env: dict[str, str]) -> None:
    """Write offline env as `KEY='value'` lines safe for `set -a; source`."""

    lines = [f"{key}={_shell_quote(str(value))}" for key, value in sorted(env.items())]
    OFFLINE_ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _offline_env_from_payload(payload: dict[str, Any]) -> dict[str, str]:
    offline = payload.get("offline_task")
    if not isinstance(offline, dict):
        return {}
    env: dict[str, str] = {}
    for key, value in offline.items():
        if value is None:
            continue
        env[str(key)] = str(value)
    return env


def _identity_env_from_payload(payload: dict[str, Any]) -> dict[str, str]:
    """Map bootstrap startup.json fields onto the offline-runner identity env."""

    mapping = {
        "ASSISTANT_ID": "assistant_id",
        "USER_ID": "user_id",
        "UNIFY_KEY": "api_key",
        "ASSISTANT_FIRST_NAME": "assistant_first_name",
        "ASSISTANT_SURNAME": "assistant_surname",
        "ASSISTANT_AGE": "assistant_age",
        "ASSISTANT_NATIONALITY": "assistant_nationality",
        "ASSISTANT_TIMEZONE": "assistant_timezone",
        "ASSISTANT_ABOUT": "assistant_about",
        "ASSISTANT_JOB_TITLE": "assistant_job_title",
        "ASSISTANT_NUMBER": "assistant_number",
        "ASSISTANT_EMAIL": "assistant_email",
        "ASSISTANT_WHATSAPP_NUMBER": "assistant_whatsapp_number",
        "USER_FIRST_NAME": "user_first_name",
        "USER_SURNAME": "user_surname",
        "USER_NUMBER": "user_number",
        "USER_EMAIL": "user_email",
        "USER_WHATSAPP_NUMBER": "user_whatsapp_number",
        "VOICE_PROVIDER": "voice_provider",
        "VOICE_ID": "voice_id",
        "ASSISTANT_DEFAULT_MODEL": "default_model",
        "ASSISTANT_DEFAULT_REASONING_EFFORT": "default_reasoning_effort",
        "ASSISTANT_SLOW_BRAIN_MODEL": "slow_brain_model",
        "ASSISTANT_SLOW_BRAIN_REASONING_EFFORT": "slow_brain_reasoning_effort",
        "SELF_CONTACT_ID": "self_contact_id",
        "BOSS_CONTACT_ID": "boss_contact_id",
        "ORG_ID": "org_id",
    }
    env: dict[str, str] = {}
    for env_key, payload_key in mapping.items():
        value = payload.get(payload_key)
        if value is None or value == "":
            continue
        env[env_key] = str(value)
    team_ids = payload.get("team_ids") or []
    if isinstance(team_ids, list) and team_ids:
        env["TEAM_IDS"] = ",".join(str(int(t)) for t in team_ids)
    user_desktops = payload.get("user_desktops")
    if user_desktops is not None:
        env["ASSISTANT_USER_DESKTOPS"] = json.dumps(user_desktops)
    desktop_mode = payload.get("desktop_mode")
    if desktop_mode:
        env["ASSISTANT_DESKTOP_MODE"] = str(desktop_mode)
    return env


def _boot_local_or_env_mode() -> str:
    """Resolve mode when no K8s job assignment is expected (local / tests)."""

    if (os.environ.get("UNITY_OFFLINE_TASK_MODE") or "").strip():
        write_offline_env_exportable(
            {
                key: value
                for key, value in os.environ.items()
                if key.startswith("UNITY_OFFLINE_TASK_")
                or key
                in {
                    "ASSISTANT_ID",
                    "UNIFY_KEY",
                    "ORCHESTRA_URL",
                    "TASK_DESTINATION",
                    "TEAM_IDS",
                    "ORG_ID",
                    "USER_ID",
                }
            },
        )
        _write_mode("headless_offline")
        return "headless_offline"
    _write_mode("interactive")
    return "interactive"


def _record_cm_attached(attached: bool) -> None:
    """Best-effort status signal so Communication can distinguish headless vs CM."""

    try:
        from unify.deploy_runtime import (
            read_assistant_session,
            read_job_assignment_record,
            wait_for_assistant_session_name,
        )
    except Exception:
        return

    job_name = (os.environ.get("UNITY_CONVERSATION_JOB_NAME") or "").strip()
    if not job_name:
        return
    try:
        session_name = wait_for_assistant_session_name(job_name)
        assignment = read_job_assignment_record(job_name)
        session = read_assistant_session(session_name)
        # Signal recording goes through Communication HTTP when available;
        # fall back to local annotation file for the entrypoint promote loop.
        marker = Path("/tmp/unity-cm-attached")
        if attached:
            marker.write_text("1\n", encoding="utf-8")
        elif marker.exists():
            marker.unlink()
        _ = (assignment, session)
    except Exception:
        LOGGER.exception("Failed to record cmAttached=%s", attached)


def boot_from_assignment() -> str:
    """Poll AssistantSession assignment and choose interactive vs headless-offline."""

    job_name = (os.environ.get("UNITY_CONVERSATION_JOB_NAME") or "").strip()
    if not job_name:
        return _boot_local_or_env_mode()

    from unify.deploy_runtime import (
        mark_job_container_ready,
        read_assistant_session,
        read_job_assignment_record,
        read_session_bootstrap_secret_record,
        wait_for_assistant_session_name,
    )

    LOGGER.info("session_boot: waiting for AssistantSession on job %s", job_name)
    session_name = wait_for_assistant_session_name(job_name)
    assignment = read_job_assignment_record(job_name)
    session = read_assistant_session(session_name)
    spec = session.get("spec") or {}
    secret_name = str(spec.get("startupSecretRef") or "")
    if not secret_name:
        raise RuntimeError(
            f"AssistantSession {session_name} is missing startupSecretRef",
        )

    # Retry briefly while the controller finishes writing the bootstrap Secret.
    payload: dict[str, Any] = {}
    for _ in range(30):
        record = read_session_bootstrap_secret_record(secret_name)
        payload = dict(record.payload or {})
        if payload:
            break
        time.sleep(1)
    if not payload:
        raise RuntimeError(f"Bootstrap secret {secret_name} has empty startup.json")

    headless = bool(payload.get("headless_offline"))
    offline_env = _offline_env_from_payload(payload)
    if headless and offline_env:
        merged = {**_identity_env_from_payload(payload), **offline_env}
        if assignment.binding_id:
            merged.setdefault(
                "UNITY_OFFLINE_TASK_JOB_NAME",
                job_name,
            )
        write_offline_env_exportable(merged)
        for key, value in merged.items():
            os.environ[key] = value
        mark_job_container_ready(job_name)
        _record_cm_attached(False)
        _write_mode("headless_offline")
        LOGGER.info(
            "session_boot: headless_offline mode for assistant_id=%s task_id=%s",
            merged.get("ASSISTANT_ID"),
            merged.get("UNITY_OFFLINE_TASK_ID"),
        )
        return "headless_offline"

    # Interactive: leave assignment polling to CommsManager / StartupEvent.
    _write_mode("interactive")
    LOGGER.info("session_boot: interactive mode for job %s", job_name)
    return "interactive"


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    mode = boot_from_assignment()
    print(mode)


if __name__ == "__main__":
    main()
