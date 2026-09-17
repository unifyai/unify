"""Entry point for the headless offline task lane.

This module runs in the short-lived subprocess the local scheduler starts when
a scheduled, triggered, or explicitly triggered task should execute without
waking the full live assistant runtime. It exists to answer one simple question:

"How do we run one task in the background, with the assistant's identity and
normal actor primitives available, but without booting the whole
ConversationManager?"

The runner is intentionally small and procedural:

1. Read the activation/run payload the dispatcher injected into env vars.
2. Populate `SESSION_DETAILS` so shared primitives know which assistant is
   acting.
3. Initialise the runtime (project, context root, EventBus).
4. Enter `TaskScheduler.execute(...)` with a CodeActActor-backed execution
   delegate so scheduler lifecycle and recurring rearm semantics stay central.
5. Persist the terminal run state through the scheduler-owned task run lifecycle.

There is no ConversationManager and no CM↔offline steering path if a live
session later wakes. The task row owns whether execution is agentic or symbolic.

Event-loop note: ``main()`` drives the whole run with ``asyncio.run``. Sync
symbolic entrypoints therefore execute under an already-running loop (Unify
runs them on a worker thread so nested ``asyncio.run`` in helpers is safe, but
stored code should still prefer ``async def`` + ``await`` or
``run_coro_sync`` rather than nesting ``asyncio.run`` itself).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
import signal
import traceback
from typing import Any

import unify
from unify.actor.code_act_actor import CodeActActor
from unify.actor.environments import (
    ActorEnvironment,
    StateManagerEnvironment,
)
from unify.common.context_registry import ContextRegistry
from unify.common.task_execution_context import current_task_execution_delegate
from unify.logger import LOGGER
from unify.session_details import SESSION_DETAILS
from unify.task_scheduler.machine_state import (
    TASK_MACHINE_STATE_PROJECT,
    TaskRunProvenance,
    TaskRunReference,
    remember_live_task_run_provenance,
    update_task_run_record,
)
from unify.task_scheduler.task_scheduler import (
    StaleActivationSuperseded,
    TaskScheduler,
)
from unify.task_scheduler.types.execution import Delivery, Wake
from unify.task_scheduler.types.run_source import RunSource

SUMMARY_LIMIT = 4000
SCHEDULER_MANAGED_WAKES = frozenset(Wake)
_SIGTERM_EXIT_CODE = 143


@dataclass(frozen=True)
class OfflineTaskConfig:
    """One fully-materialized offline run request from process environment variables.

    The dispatcher injects these values when it starts the runner. Together
    they identify which assistant is acting, which stored function should
    run, why it was activated, and which durable `Tasks/Executions` row
    should be updated as execution progresses.
    """

    assistant_id: str
    run_key: str
    task_id: int
    function_id: int | None
    request: str
    wake: Wake
    source_task_log_id: int
    revision: str
    destination: str | None = None
    task_name: str = ""
    scheduled_for: str = ""
    source_ref: str = ""
    source_medium: str = ""
    source_contact_id: str = ""
    requires_filesystem: bool = False
    requires_computer: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "wake",
            Wake.normalize(self.wake),
        )


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


def _bool_env(name: str, *, default: bool = False) -> bool:
    """Return a boolean environment variable (``1``/``true`` → True)."""

    value = os.environ.get(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes"}


def _load_config_from_env() -> OfflineTaskConfig:
    """Construct one validated offline task config from process environment."""

    raw_destination = os.environ.get("TASK_DESTINATION")
    try:
        destination = ContextRegistry.canonical_destination(raw_destination)
    except ValueError as exc:
        raise RuntimeError(f"Invalid TASK_DESTINATION: {raw_destination}") from exc
    # Empty is a legitimate revision, not a missing one: the machine-state
    # contract stores ``str(revision or "")`` and digests that exact value
    # into ``run_key``, and a task with no authored ``task_revision`` (every
    # deployment-reconciled definition) projects its occurrences with "".
    # Requiring non-empty here boot-crashed each of those runs at dispatch.
    revision = os.environ.get("UNIFY_OFFLINE_TASK_REVISION", "").strip()
    request = _require_env("UNIFY_OFFLINE_TASK_REQUEST")
    return OfflineTaskConfig(
        assistant_id=_require_env("ASSISTANT_ID"),
        run_key=_require_env("UNIFY_OFFLINE_RUN_KEY"),
        task_id=int(_require_env("UNIFY_OFFLINE_TASK_ID")),
        function_id=_optional_int_env("UNIFY_OFFLINE_TASK_FUNCTION_ID"),
        request=request,
        wake=Wake.normalize(
            os.environ.get("UNIFY_OFFLINE_TASK_WAKE", Wake.scheduled),
        ),
        source_task_log_id=int(_require_env("UNIFY_OFFLINE_TASK_SOURCE_TASK_LOG_ID")),
        revision=revision,
        destination=destination,
        task_name=os.environ.get("UNIFY_OFFLINE_TASK_NAME", ""),
        scheduled_for=os.environ.get("UNIFY_OFFLINE_TASK_SCHEDULED_FOR", ""),
        source_ref=os.environ.get("UNIFY_OFFLINE_TASK_SOURCE_REF", ""),
        source_medium=os.environ.get("UNIFY_OFFLINE_TASK_SOURCE_MEDIUM", ""),
        source_contact_id=os.environ.get("UNIFY_OFFLINE_TASK_SOURCE_CONTACT_ID", ""),
        requires_filesystem=_bool_env("UNIFY_OFFLINE_TASK_REQUIRES_FILESYSTEM"),
        requires_computer=_bool_env("UNIFY_OFFLINE_TASK_REQUIRES_COMPUTER"),
    )


def _update_task_run(
    assistant_id: str,
    run_key: str,
    updates: dict[str, Any],
    source_task_log_id: int | None = None,
) -> None:
    """Persist one partial run update to the ``Tasks/Executions`` row."""

    update_task_run_record(
        TaskRunReference(
            assistant_id=assistant_id,
            run_key=run_key,
            source_task_log_id=source_task_log_id,
        ),
        updates,
    )


def _mark_source_task_failed(config: OfflineTaskConfig, error_text: str) -> None:
    """Disarm a one-shot definition whose runner crashed before finalizing.

    Recurring and triggerable definitions are left completely untouched: a
    crashed, killed, or timed-out occurrence says nothing about the series.
    The failure itself is recorded on the Tasks/Executions row by the caller,
    which is the only place a run outcome belongs.
    """

    if config.source_task_log_id <= 0:
        return
    try:
        SESSION_DETAILS.populate_from_env()
        unify.ensure_initialised(project_name=TASK_MACHINE_STATE_PROJECT)
        scheduler = TaskScheduler()
        rows = scheduler._store.get_rows_by_log_ids(  # type: ignore[attr-defined]
            log_ids=[config.source_task_log_id],
        )
        if not rows:
            return
        row = rows[0]
        entries = dict(row.entries or {})
        if entries.get("repeat") is not None or entries.get("trigger") is not None:
            return
        scheduler._write_log_entries(  # type: ignore[attr-defined]
            logs=config.source_task_log_id,
            entries={"enabled": False},
        )
    except Exception:
        LOGGER.exception(
            "Failed to terminalize source task row after offline runner failure "
            "(task_id=%s, source_task_log_id=%s, run_key=%s)",
            config.task_id,
            config.source_task_log_id,
            config.run_key,
        )


def _install_sigterm_handler(config: OfflineTaskConfig) -> None:
    """Terminalize an active Tasks source when the process receives SIGTERM."""

    def _handle_sigterm(signum: int, frame: Any) -> None:
        del signum, frame
        error_text = (
            "Received SIGTERM; terminalizing active Tasks source before process exit."
        )
        LOGGER.warning(
            "Offline task runner received SIGTERM for task %s (run_key=%s)",
            config.task_id,
            config.run_key,
        )
        _mark_source_task_failed(config, error_text)
        try:
            _update_task_run(
                config.assistant_id,
                config.run_key,
                source_task_log_id=config.source_task_log_id,
                updates={
                    "state": "failed",
                    "completed_at": _now_iso(),
                    "error": error_text,
                    "result_summary": error_text,
                },
            )
        except Exception:
            LOGGER.exception(
                "Failed to terminalize Tasks/Executions row on SIGTERM "
                "(task_id=%s, run_key=%s)",
                config.task_id,
                config.run_key,
            )
        raise SystemExit(_SIGTERM_EXIT_CODE)

    signal.signal(signal.SIGTERM, _handle_sigterm)


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
    elif isinstance(execution_result, dict):
        result_value = execution_result.get("result", execution_result)
        stdout = str(execution_result.get("stdout", "") or "")
        stderr = str(execution_result.get("stderr", "") or "")
    else:
        result_value = getattr(execution_result, "result", None)
        stdout = str(getattr(execution_result, "stdout", "") or "")
        stderr = str(getattr(execution_result, "stderr", "") or "")
    payload = {
        "task_id": config.task_id,
        "function_id": config.function_id,
        "task_name": config.task_name,
        "wake": config.wake.value,
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

    return config.wake in SCHEDULER_MANAGED_WAKES


def _trigger_attempt_token(config: OfflineTaskConfig) -> str | None:
    """Return the pending-provenance claim token for one triggered run.

    Explicit REST offline runs intentionally omit this token so
    ``TaskScheduler.execute`` keeps manual provenance instead of forcing a
    communication-trigger source type.
    """

    if config.wake is Wake.triggered:
        return config.run_key
    return None


def _build_offline_actor(config: OfflineTaskConfig) -> CodeActActor:
    """Construct the actor substrate for a headless task run."""

    del config
    return CodeActActor(environments=[StateManagerEnvironment(), ActorEnvironment()])


def _build_offline_provenance(config: OfflineTaskConfig) -> TaskRunProvenance:
    """Return scheduler run provenance matching the Communication run identity."""

    return TaskRunProvenance(
        assistant_id=config.assistant_id,
        task_id=config.task_id,
        wake=config.wake,
        delivery=Delivery.offline,
        source_task_log_id=config.source_task_log_id,
        revision=config.revision,
        scheduled_for=config.scheduled_for or None,
        source_medium=config.source_medium or None,
        source_ref=config.source_ref or None,
        source_contact_id=config.source_contact_id or None,
        task_name=config.task_name or None,
        attempt_token=_trigger_attempt_token(config),
        destination=config.destination,
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
        # An activation with no function id is not a request for agentic
        # execution — it is an activation with no opinion. The definition is
        # the authored intent, so its entrypoint governs. Refusing here failed
        # every projected successor of a symbolic recurring task, because those
        # occurrences historically materialized without an entrypoint.

        task_guidelines = kwargs.pop("guidelines", None)
        entrypoint_kwargs = dict(kwargs.pop("entrypoint_kwargs", {}) or {})
        entrypoint_repair_context = kwargs.pop("entrypoint_repair_context", None)
        destination = kwargs.pop("destination", None)
        if destination is None:
            destination = self._config.destination
        if self._config.scheduled_for:
            entrypoint_kwargs.setdefault(
                "scheduled_run_timestamp",
                self._config.scheduled_for,
            )
            entrypoint_kwargs.setdefault("scheduled_for", self._config.scheduled_for)
        entrypoint_kwargs.setdefault("task_id", self._config.task_id)
        entrypoint_kwargs.setdefault("run_key", self._config.run_key)
        entrypoint_kwargs.setdefault("wake", self._config.wake.value)
        entrypoint_kwargs.setdefault(
            "revision",
            self._config.revision,
        )
        entrypoint_kwargs.setdefault(
            "task_execution_context",
            {
                "wake": self._config.wake.value,
                "delivery": Delivery.offline.value,
                "revision": self._config.revision,
                "scheduled_for": self._config.scheduled_for or None,
                "state": "running",
                "run_key": self._config.run_key,
            },
        )

        self._actor = _build_offline_actor(self._config)
        if kwargs:
            unexpected = ", ".join(sorted(kwargs))
            raise TypeError(
                "OfflineTaskExecutionDelegate.start_task_run got unexpected "
                f"keyword arguments: {unexpected}",
            )
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
            entrypoint_repair_context=entrypoint_repair_context,
            destination=destination,
        )
        return _OfflineTaskHandle(self._config, handle)

    async def close(self) -> None:
        if self._actor is not None:
            await self._actor.close()
            self._actor = None


async def _await_post_run_review(handle: Any) -> None:
    """Wait out any post-run storage review the handle is still running.

    A failure inside the review must not turn a run that succeeded into one
    that reports failure: the run's own outcome is already recorded and the
    review is strictly additional. So the wait swallows, and the review's own
    logging is what says it went wrong.
    """

    waiter = getattr(handle, "wait_until_done", None)
    if waiter is None:
        return
    try:
        await waiter()
    except Exception:
        LOGGER.exception("Post-run storage review ended abnormally")


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
            _activated_by=RunSource.normalize(config.wake.value).to_activated_by(),
        )
        result = await handle.result()
        # The run has its answer, but a post-run review may still be
        # distilling the trajectory into reusable functions -- the mechanism
        # that makes a recurring task cheaper and more deterministic every
        # time it runs. This process owns the actor that review needs, so
        # returning here would close its pools underneath it and orphan the
        # work. The job's own deadline is the bound on all of it.
        await _await_post_run_review(handle)
        return result
    finally:
        current_task_execution_delegate.reset(token)
        await delegate.close()


def _bootstrap_offline_runtime() -> None:
    """Initialise the runtime without a ConversationManager."""

    unify.ensure_initialised(project_name=TASK_MACHINE_STATE_PROJECT)


async def _execute_offline_task(config: OfflineTaskConfig) -> Any:
    """Execute one offline task with assistant session context."""

    SESSION_DETAILS.populate_from_env()
    _bootstrap_offline_runtime()
    if not _is_scheduler_managed(config):
        raise RuntimeError(
            "Offline task runner only supports scheduler-managed scheduled, "
            "triggered, and explicit task runs.",
        )
    return await _execute_scheduler_managed_task(config)


def main() -> int:
    """Run one offline task to completion and persist the final run state."""

    config = _load_config_from_env()
    _install_sigterm_handler(config)
    LOGGER.info(
        "Starting offline task runner for task %s (function_id=%s, run_key=%s)",
        config.task_id,
        config.function_id,
        config.run_key,
    )
    try:
        asyncio.run(_execute_offline_task(config))
    except StaleActivationSuperseded as exc:
        # The definition's schedule moved after this activation was
        # projected (re-arm on a concurrent run start, manual re-arm,
        # overdue catch-up heads). Nothing to run — finalize the run row
        # as a benign no-op and leave the definition untouched.
        LOGGER.info(
            "Offline task runner skipping superseded activation for task %s "
            "(run_key=%s): %s",
            config.task_id,
            config.run_key,
            exc,
        )
        _update_task_run(
            config.assistant_id,
            config.run_key,
            source_task_log_id=config.source_task_log_id,
            updates={
                "state": "completed",
                "completed_at": _now_iso(),
                "error": None,
                "result_summary": _truncate_text(
                    f"Skipped: stale activation superseded by re-armed schedule. {exc}",
                ),
            },
        )
        return 0
    except Exception as exc:
        error_text = _truncate_text(traceback.format_exc())
        LOGGER.exception(
            "Offline task runner failed for task %s (run_key=%s)",
            config.task_id,
            config.run_key,
        )
        _mark_source_task_failed(config, error_text)
        _update_task_run(
            config.assistant_id,
            config.run_key,
            source_task_log_id=config.source_task_log_id,
            updates={
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
