"""Entry point for Unity's headless offline task lane.

This module runs inside the short-lived Unity job created by Communication when
a scheduled or triggered task should execute without waking the full live
assistant runtime. It exists to answer one simple question:

"How do we run one task in the background, with the assistant's identity and
normal actor primitives available, but without booting the whole
ConversationManager?"

The runner is intentionally small and procedural:

1. Read the activation/run payload that Communication injected into env vars.
2. Populate `SESSION_DETAILS` so shared primitives know which assistant is
   acting.
3. Initialize Unity's normal runtime substrate for the assistant context.
4. Enter `TaskScheduler.execute(...)` with a CodeActActor-backed execution
   delegate so scheduler lifecycle and recurring rearm semantics stay central.
5. Persist the terminal run state through the scheduler-owned task run lifecycle.

Communication owns orchestration and job creation. The task row owns whether
execution is agentic or symbolic. This module is the thin bridge that starts
the headless actor and keeps the durable run record up to date.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
import traceback
from typing import Any

import requests

import unity
from unity.actor.code_act_actor import CodeActActor
from unity.actor.environments import (
    ActorEnvironment,
    ComputerEnvironment,
    StateManagerEnvironment,
)
from unity.common.context_registry import ContextRegistry
from unity.common.task_execution_context import current_task_execution_delegate
from unity.function_manager.primitives import ComputerPrimitives
from unity.logger import LOGGER
from unity.session_details import SESSION_DETAILS
from unity.task_scheduler.machine_state import (
    TASK_MACHINE_STATE_PROJECT,
    TaskRunProvenance,
    remember_live_task_run_provenance,
)
from unity.task_scheduler.task_scheduler import TaskScheduler
from unity.task_scheduler.types.activated_by import ActivatedBy

TASK_RUN_UPDATE_PATH = "/admin/task-run/update"
HTTP_TIMEOUT_SECONDS = 30
SUMMARY_LIMIT = 4000
SCHEDULER_MANAGED_SOURCE_TYPES = {"scheduled", "triggered"}


@dataclass(frozen=True)
class OfflineTaskConfig:
    """One fully-materialized offline run request from job environment variables.

    Communication injects these values when it creates the short-lived Unity
    job. Together they identify which assistant is acting, which stored
    function should run, why it was activated, and which durable `Tasks/Runs`
    row should be updated as execution progresses.
    """

    assistant_id: str
    run_key: str
    task_id: int
    function_id: int | None
    request: str
    source_type: str
    source_task_log_id: int
    activation_revision: str
    destination: str | None = None
    task_name: str = ""
    task_description: str = ""
    scheduled_for: str = ""
    source_ref: str = ""
    source_medium: str = ""
    source_contact_id: str = ""


def _require_env(name: str) -> str:
    """Return one required environment variable or raise a clear error."""

    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _optional_int_env(name: str) -> int | None:
    """Return an optional integer environment variable."""

    value = os.environ.get(name, "").strip()
    if not value:
        return None
    return int(value)


def _load_config_from_env() -> OfflineTaskConfig:
    """Construct one validated offline task config from process environment."""

    raw_destination = os.environ.get("TASK_DESTINATION")
    try:
        destination = ContextRegistry.canonical_destination(raw_destination)
    except ValueError as exc:
        raise RuntimeError(f"Invalid TASK_DESTINATION: {raw_destination}") from exc
    return OfflineTaskConfig(
        assistant_id=_require_env("ASSISTANT_ID"),
        run_key=_require_env("UNITY_OFFLINE_TASK_RUN_KEY"),
        task_id=int(_require_env("UNITY_OFFLINE_TASK_ID")),
        function_id=_optional_int_env("UNITY_OFFLINE_TASK_FUNCTION_ID"),
        request=_require_env("UNITY_OFFLINE_TASK_REQUEST"),
        source_type=os.environ.get("UNITY_OFFLINE_TASK_SOURCE_TYPE", "scheduled"),
        source_task_log_id=int(_require_env("UNITY_OFFLINE_TASK_SOURCE_TASK_LOG_ID")),
        activation_revision=_require_env("UNITY_OFFLINE_TASK_ACTIVATION_REVISION"),
        destination=destination,
        task_name=os.environ.get("UNITY_OFFLINE_TASK_NAME", ""),
        task_description=os.environ.get("UNITY_OFFLINE_TASK_DESCRIPTION", ""),
        scheduled_for=os.environ.get("UNITY_OFFLINE_TASK_SCHEDULED_FOR", ""),
        source_ref=os.environ.get("UNITY_OFFLINE_TASK_SOURCE_REF", ""),
        source_medium=os.environ.get("UNITY_OFFLINE_TASK_SOURCE_MEDIUM", ""),
        source_contact_id=os.environ.get("UNITY_OFFLINE_TASK_SOURCE_CONTACT_ID", ""),
    )


def _orchestra_admin_headers() -> dict[str, str]:
    """Return admin auth headers for Orchestra task-run APIs."""

    admin_key = _require_env("ORCHESTRA_ADMIN_KEY")
    return {"Authorization": f"Bearer {admin_key}"}


def _task_run_update_payload(
    assistant_id: str,
    run_key: str,
    updates: dict[str, Any],
) -> dict[str, Any]:
    """Return the admin payload for one partial run update."""

    return {
        "project_name": TASK_MACHINE_STATE_PROJECT,
        "assistant_id": assistant_id,
        "run_key": run_key,
        "updates": updates,
    }


def _update_task_run(assistant_id: str, run_key: str, updates: dict[str, Any]) -> None:
    """Persist one partial run update back to Orchestra."""

    orchestra_url = _require_env("ORCHESTRA_URL")
    response = requests.post(
        f"{orchestra_url}{TASK_RUN_UPDATE_PATH}",
        json=_task_run_update_payload(assistant_id, run_key, updates),
        headers=_orchestra_admin_headers(),
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()


def _now_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 format."""

    return datetime.now(timezone.utc).isoformat()


def _truncate_text(value: str, limit: int = SUMMARY_LIMIT) -> str:
    """Trim long diagnostic strings so run rows stay compact."""

    if len(value) <= limit:
        return value
    return f"{value[: limit - 3]}..."


def _json_safe_value(value: Any) -> Any:
    """Recursively coerce runtime values into JSON-safe structures."""

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(item) for item in value]
    return repr(value)


def _build_result_summary(config: OfflineTaskConfig, execution_result: Any) -> str:
    """Serialize a compact, hidden summary of the offline execution outcome."""

    if isinstance(execution_result, str):
        result_value: Any = execution_result
        stdout = ""
        stderr = ""
    else:
        result_value = getattr(execution_result, "result", None)
        stdout = str(getattr(execution_result, "stdout", "") or "")
        stderr = str(getattr(execution_result, "stderr", "") or "")
    payload = {
        "task_id": config.task_id,
        "function_id": config.function_id,
        "task_name": config.task_name,
        "source_type": config.source_type,
        "destination": config.destination or None,
        "scheduled_for": config.scheduled_for or None,
        "source_medium": config.source_medium or None,
        "source_contact_id": config.source_contact_id or None,
        "result": _json_safe_value(result_value),
        "stdout": _truncate_text(stdout),
        "stderr": _truncate_text(stderr),
    }
    return _truncate_text(json.dumps(payload, default=str, ensure_ascii=True))


def _is_scheduler_managed(config: OfflineTaskConfig) -> bool:
    """Return whether this request represents a durable assistant task run."""

    return config.source_type in SCHEDULER_MANAGED_SOURCE_TYPES


def _activated_by_for_source_type(source_type: str) -> ActivatedBy:
    """Map the offline source type onto the scheduler activation reason."""

    if source_type == "triggered":
        return ActivatedBy.trigger
    return ActivatedBy.schedule


def _trigger_attempt_token(config: OfflineTaskConfig) -> str | None:
    """Return the pending-provenance claim token for one triggered run."""

    if config.source_type == "triggered":
        return config.run_key
    return None


def _build_offline_actor() -> CodeActActor:
    """Construct the normal actor substrate for a headless task run."""

    return CodeActActor(
        environments=[
            StateManagerEnvironment(),
            ComputerEnvironment(ComputerPrimitives()),
            ActorEnvironment(),
        ],
    )


def _build_offline_provenance(config: OfflineTaskConfig) -> TaskRunProvenance:
    """Return scheduler run provenance matching the Communication run identity."""

    return TaskRunProvenance(
        assistant_id=config.assistant_id,
        task_id=config.task_id,
        source_type=config.source_type,
        execution_mode="offline",
        source_task_log_id=config.source_task_log_id,
        activation_revision=config.activation_revision,
        scheduled_for=config.scheduled_for or None,
        source_medium=config.source_medium or None,
        source_ref=config.source_ref or None,
        source_contact_id=config.source_contact_id or None,
        task_name=config.task_name or None,
        task_description=config.task_description or config.request or None,
        attempt_token=_trigger_attempt_token(config),
    )


class _OfflineTaskHandle:
    """Handle wrapper that converts actor execution results into task outcomes."""

    def __init__(self, config: OfflineTaskConfig, inner_handle: Any) -> None:
        self._config = config
        self._inner_handle = inner_handle

    async def result(self) -> str:
        execution_result = await self._inner_handle.result()
        if isinstance(execution_result, str) and execution_result.startswith("Error:"):
            raise RuntimeError(execution_result)
        error = str(getattr(execution_result, "error", "") or "").strip()
        if error:
            raise RuntimeError(error)
        return _build_result_summary(self._config, execution_result)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner_handle, name)


class _OfflineTaskExecutionDelegate:
    """Task execution delegate that runs one scheduler-owned task headlessly."""

    def __init__(self, config: OfflineTaskConfig) -> None:
        self._config = config
        self._actor: CodeActActor | None = None

    async def start_task_run(
        self,
        *,
        task_description: str,
        entrypoint: int | None = None,
        parent_chat_context: list[dict] | None = None,
        clarification_up_q: asyncio.Queue[str] | None = None,
        clarification_down_q: asyncio.Queue[str] | None = None,
        images: list[str] | None = None,
        guidelines: str | None = None,
        **kwargs: Any,
    ) -> _OfflineTaskHandle:
        requested_symbolic = self._config.function_id is not None
        execution_style = "symbolic" if entrypoint is not None else "agentic"
        if requested_symbolic and entrypoint is None:
            raise RuntimeError(
                "Offline task entrypoint mismatch: activation requested "
                f"{self._config.function_id}, task row is agentic.",
            )
        if (
            self._config.function_id is not None
            and int(entrypoint) != self._config.function_id
        ):
            raise RuntimeError(
                "Offline task entrypoint mismatch: "
                f"activation requested {self._config.function_id}, "
                f"task row provides {entrypoint}.",
            )
        if not requested_symbolic and entrypoint is not None:
            raise RuntimeError(
                "Offline task entrypoint mismatch: activation requested "
                "agentic execution, task row provides a symbolic entrypoint.",
            )

        task_guidelines = kwargs.pop("guidelines", None)
        entrypoint_kwargs = dict(kwargs.pop("entrypoint_kwargs", {}) or {})
        entrypoint_repair_attempts = int(
            kwargs.pop(
                "entrypoint_repair_attempts",
                1 if entrypoint is not None else 0,
            )
            or 0,
        )
        entrypoint_repair_context = kwargs.pop("entrypoint_repair_context", None)
        if self._config.scheduled_for:
            entrypoint_kwargs.setdefault(
                "scheduled_run_timestamp",
                self._config.scheduled_for,
            )
            entrypoint_kwargs.setdefault("scheduled_for", self._config.scheduled_for)
        entrypoint_kwargs.setdefault("task_id", self._config.task_id)
        entrypoint_kwargs.setdefault("run_key", self._config.run_key)
        entrypoint_kwargs.setdefault("source_type", self._config.source_type)
        entrypoint_kwargs.setdefault(
            "activation_revision",
            self._config.activation_revision,
        )
        entrypoint_kwargs.setdefault(
            "task_execution_context",
            {
                "task_id": self._config.task_id,
                "run_key": self._config.run_key,
                "source_type": self._config.source_type,
                "scheduled_for": self._config.scheduled_for or None,
                "activation_revision": self._config.activation_revision,
                "execution_style": execution_style,
                "delivery_mode": "offline",
            },
        )

        self._actor = _build_offline_actor()
        handle = await self._actor.act(
            task_description,
            guidelines="\n\n".join(
                filter(
                    None,
                    [
                        task_guidelines,
                        "This is a headless offline task run. Do not ask the user for live clarification.",
                    ],
                ),
            ),
            entrypoint=entrypoint,
            entrypoint_kwargs=entrypoint_kwargs if entrypoint is not None else None,
            clarification_enabled=False,
            persist=False,
            entrypoint_repair_attempts=entrypoint_repair_attempts,
            entrypoint_repair_context=entrypoint_repair_context,
            **kwargs,
        )
        return _OfflineTaskHandle(self._config, handle)

    async def close(self) -> None:
        if self._actor is not None:
            await self._actor.close()
            self._actor = None


async def _execute_scheduler_managed_task(config: OfflineTaskConfig) -> Any:
    """Execute one offline task through the scheduler-owned lifecycle."""

    remember_live_task_run_provenance(_build_offline_provenance(config))
    delegate = _OfflineTaskExecutionDelegate(config)
    token = current_task_execution_delegate.set(delegate)
    try:
        scheduler = TaskScheduler()
        handle = await scheduler.execute(
            task_id=config.task_id,
            trigger_attempt_token=_trigger_attempt_token(config),
            _activated_by=_activated_by_for_source_type(config.source_type),
            isolated=True,
        )
        return await handle.result()
    finally:
        current_task_execution_delegate.reset(token)
        await delegate.close()


async def _execute_offline_task(config: OfflineTaskConfig) -> Any:
    """Execute one offline task with assistant session context."""

    SESSION_DETAILS.populate_from_env()
    unity.ensure_initialised(project_name=TASK_MACHINE_STATE_PROJECT)
    if not _is_scheduler_managed(config):
        raise RuntimeError(
            "Offline task runner only supports scheduler-managed scheduled "
            "and triggered task runs.",
        )
    return await _execute_scheduler_managed_task(config)


def main() -> int:
    """Run one offline task to completion and persist the final run state."""

    config = _load_config_from_env()
    LOGGER.info(
        "Starting offline task runner for task %s (function_id=%s, run_key=%s)",
        config.task_id,
        config.function_id,
        config.run_key,
    )
    try:
        asyncio.run(_execute_offline_task(config))
    except Exception as exc:
        error_text = _truncate_text(traceback.format_exc())
        LOGGER.exception(
            "Offline task runner failed for task %s (run_key=%s)",
            config.task_id,
            config.run_key,
        )
        _update_task_run(
            config.assistant_id,
            config.run_key,
            {
                "state": "failed",
                "completed_at": _now_iso(),
                "error": error_text,
                "result_summary": _truncate_text(
                    json.dumps(
                        {
                            "task_id": config.task_id,
                            "function_id": config.function_id,
                            "error": str(exc),
                        },
                        ensure_ascii=True,
                    ),
                ),
            },
        )
        return 1

    LOGGER.info(
        "Offline task scheduler lifecycle completed for task %s (run_key=%s)",
        config.task_id,
        config.run_key,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
