"""Read-only helpers for Orchestra-projected assistant task machine state.

The user-authored ``Tasks`` context is the definition source of truth.
Orchestra projects each wake attempt into ``Tasks/Executions``; Unity reads
that context to validate scheduled wakeups and to narrow triggered-task
candidates without polling the full user task table.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Mapping

from unify.common.context_registry import (
    ContextRegistry,
    PERSONAL_DESTINATION,
)
from unify.session_details import SESSION_DETAILS

from unify import db
from unify.task_scheduler.storage import TasksStore
from unify.task_scheduler.types.activated_by import ActivatedBy
from unify.task_scheduler.types.execution import Delivery, ExecutionState, Wake
from unify.task_scheduler.types.run_source import RunSource

TASKS_CONTEXT_NAME = "Tasks"
TASK_EXECUTIONS_CONTEXT_NAME = "Tasks/Executions"
_TASK_EXECUTIONS_CONTEXT_LEAF = "Executions"
TASK_MACHINE_STATE_PROJECT = "Assistants"
# Assistant-scoped task-machine routes (authenticated with the assistant's own
# UNIFY_KEY; Orchestra enforces ownership). Not the /admin/* variants.
_EXECUTION_QUERY_FIELDS = [
    "assistant_id",
    "destination",
    "run_key",
    "task_id",
    "source_task_log_id",
    "wake",
    "delivery",
    "state",
    "task_name",
    "scheduled_for",
    "dispatch_offset_seconds",
    "trigger_medium",
    "trigger_from_contact_ids",
    "trigger_omit_contact_ids",
    "trigger_recurring",
    "entrypoint",
    "recurring",
    "revision",
    # What a run actually did. Omitting these meant no read path in the
    # runtime could see whether an execution had started, when it finished or
    # why it failed -- so nothing here could answer "did the briefing run this
    # morning?" even though the ledger had recorded the answer.
    "started_at",
    "completed_at",
    "result_summary",
    "error",
]
_DEFAULT_TRIGGER_PAGE_SIZE = 200
_OPEN_EXECUTION_STATES = (
    ExecutionState.scheduled,
    ExecutionState.triggerable,
    ExecutionState.running,
)
_PENDING_LIVE_TASK_RUNS: dict[tuple[int, str | None], "TaskRunProvenance"] = {}
_PENDING_TRIGGER_LIVE_TASK_RUNS: dict[str, "TaskRunProvenance"] = {}

logger = logging.getLogger(__name__)


def _canonical_destination_or_none(destination: object) -> str | None:
    """Return canonical destination when valid, otherwise ``None``."""

    try:
        return ContextRegistry.canonical_destination(destination)
    except ValueError:
        return None


def invalidate_task_machine_state_reads() -> None:
    """Invalidate cached task machine-state readers after membership changes."""

    return None


@dataclass(frozen=True)
class TaskExecutionSnapshot:
    """Machine-facing execution facts for one assistant/task wake."""

    run_key: str
    task_id: int
    assistant_id: str | None = None
    destination: str | None = None
    source_task_log_id: int | None = None
    wake: str | None = None
    delivery: str | None = None
    state: str | None = None
    task_name: str | None = None
    task_summary: str | None = None
    scheduled_for: str | None = None
    dispatch_offset_seconds: float | None = None
    trigger_medium: str | None = None
    trigger_from_contact_ids: list[int] = field(default_factory=list)
    trigger_omit_contact_ids: list[int] = field(default_factory=list)
    trigger_recurring: bool = False
    entrypoint: int | None = None
    max_runtime_seconds: int | None = None
    recurring: bool = False
    revision: str | None = None


@dataclass(frozen=True)
class TaskRunProvenance:
    """Live or offline provenance facts used to materialize one execution row."""

    assistant_id: str
    task_id: int
    wake: Wake
    delivery: Delivery = Delivery.live
    source_task_log_id: int | None = None
    revision: str | None = None
    destination: str | None = None
    scheduled_for: str | None = None
    source_medium: str | None = None
    source_ref: str | None = None
    source_contact_id: str | None = None
    source_contact_display_name: str | None = None
    task_name: str | None = None
    attempt_token: str | None = None
    dispatch_offset_seconds: float | None = None
    # Symbolic function id the definition binds this occurrence to. Dispatch
    # reads it from the materialized row, so a projected occurrence stored
    # without it launches as agentic and a symbolic task refuses the run.
    entrypoint: int | None = None


@dataclass(frozen=True)
class TaskRunReference:
    """Stable identifiers needed to patch a materialized execution later."""

    assistant_id: str
    run_key: str
    source_task_log_id: int | None = field(default=None, compare=False)


def build_task_executions_context_name(
    *,
    user_context: str | None = None,
    assistant_context: str | None = None,
) -> str:
    """Return the assistant-scoped Orchestra context for execution reads."""

    return _build_task_machine_context_name(
        leaf_name=_TASK_EXECUTIONS_CONTEXT_LEAF,
        user_context=user_context,
        assistant_context=assistant_context,
    )


def _build_task_machine_context_name(
    *,
    leaf_name: str,
    user_context: str | None = None,
    assistant_context: str | None = None,
) -> str:
    """Return one assistant-scoped task-machine context path."""

    resolved_user_context = _coerce_str(user_context) or SESSION_DETAILS.user_context
    resolved_assistant_context = (
        _coerce_str(assistant_context) or SESSION_DETAILS.assistant_context
    )
    return (
        f"{resolved_user_context}/{resolved_assistant_context}/"
        f"{TASKS_CONTEXT_NAME}/{leaf_name}"
    )


def remember_live_task_run_provenance(provenance: TaskRunProvenance) -> None:
    """Remember one pending live-run provenance until the task actually starts."""

    provenance = replace(
        provenance,
        wake=Wake.normalize(provenance.wake),
        delivery=Delivery.normalize(provenance.delivery),
    )
    normalized_attempt_token = _normalize_pending_trigger_attempt_token(
        provenance.attempt_token,
    )
    if provenance.wake is Wake.triggered and normalized_attempt_token:
        _PENDING_TRIGGER_LIVE_TASK_RUNS[normalized_attempt_token] = provenance
        return
    _PENDING_LIVE_TASK_RUNS[
        _pending_live_provenance_key(provenance.task_id, provenance.destination)
    ] = provenance


def peek_live_task_run_provenance(
    *,
    assistant_id: str | int | None,
    task_id: int,
    wake: Wake | RunSource | str,
    destination: str | None = None,
    trigger_attempt_token: str | None = None,
) -> TaskRunProvenance | None:
    """Return pending provenance for one task without claiming it."""

    wake = _normalize_wake(wake)
    pending: TaskRunProvenance | None
    if wake is Wake.triggered:
        normalized_attempt_token = _normalize_pending_trigger_attempt_token(
            trigger_attempt_token,
        )
        if not normalized_attempt_token:
            return None
        pending = _PENDING_TRIGGER_LIVE_TASK_RUNS.get(normalized_attempt_token)
    else:
        pending = _PENDING_LIVE_TASK_RUNS.get(
            _pending_live_provenance_key(task_id, destination),
        )
        if pending is None and destination is None:
            matches = [
                item
                for item in _PENDING_LIVE_TASK_RUNS.values()
                if item.task_id == task_id and item.wake is wake
            ]
            if len(matches) == 1:
                pending = matches[0]
    if pending is None:
        return None
    normalized_assistant_id = _coerce_str(assistant_id)
    if pending.task_id != task_id:
        return None
    if normalized_assistant_id and pending.assistant_id != normalized_assistant_id:
        return None
    return pending


def consume_live_task_run_provenance(
    *,
    assistant_id: str | int | None,
    task_id: int,
    wake: Wake | RunSource | str,
    source_task_log_id: int | None = None,
    destination: str | None = None,
    trigger_attempt_token: str | None = None,
    offline: bool | None = None,
) -> TaskRunProvenance | None:
    """Claim the pending live-run provenance for one task, or build a fallback.

    ``offline`` should be the target task definition's own ``offline`` field,
    when known to the caller. It is used only for the fallback provenance
    built below (no pending provenance was registered for this task/wake/
    destination) -- a pending provenance already carries its own correct
    ``delivery`` from whoever registered it. Without ``offline``, the
    fallback defaults to live delivery, matching prior behavior.
    """

    wake = _normalize_wake(wake)
    normalized_assistant_id = _coerce_str(assistant_id)
    pending = _claim_pending_live_task_run_provenance(
        assistant_id=normalized_assistant_id,
        task_id=task_id,
        wake=wake,
        destination=destination,
        trigger_attempt_token=trigger_attempt_token,
    )
    if pending is not None:
        return pending
    if not normalized_assistant_id:
        return None
    execution = None
    if wake in {Wake.scheduled, Wake.triggered}:
        execution = get_open_task_execution(
            assistant_id=normalized_assistant_id,
            task_id=task_id,
            destination=destination,
            wake=wake,
        )
    return TaskRunProvenance(
        assistant_id=normalized_assistant_id,
        task_id=task_id,
        wake=wake,
        delivery=Delivery.offline if offline else Delivery.live,
        source_task_log_id=source_task_log_id
        or (execution.source_task_log_id if execution is not None else None),
        revision=(execution.revision if execution is not None else None),
        destination=(execution.destination if execution is not None else None),
        scheduled_for=(
            execution.scheduled_for if wake is Wake.scheduled and execution else None
        ),
        source_medium=(
            execution.trigger_medium if wake is Wake.triggered and execution else None
        ),
        task_name=(execution.task_name if execution is not None else None),
    )


def _claim_pending_live_task_run_provenance(
    *,
    assistant_id: str | None,
    task_id: int,
    wake: Wake,
    destination: str | None,
    trigger_attempt_token: str | None,
) -> TaskRunProvenance | None:
    """Claim one pending provenance entry without misattributing another attempt."""

    pending: TaskRunProvenance | None
    if wake is Wake.triggered:
        normalized_attempt_token = _normalize_pending_trigger_attempt_token(
            trigger_attempt_token,
        )
        if not normalized_attempt_token:
            return None
        pending = _PENDING_TRIGGER_LIVE_TASK_RUNS.pop(normalized_attempt_token, None)
    else:
        pending = _PENDING_LIVE_TASK_RUNS.pop(
            _pending_live_provenance_key(task_id, destination),
            None,
        )
    if pending is None:
        return None
    if pending.task_id != task_id:
        logger.warning(
            "Discarding pending live task provenance for mismatched task id "
            "(expected=%s, actual=%s, wake=%s)",
            task_id,
            pending.task_id,
            wake,
        )
        return None
    if assistant_id and pending.assistant_id != assistant_id:
        logger.warning(
            "Discarding pending live task provenance for mismatched assistant "
            "(expected=%s, actual=%s, task_id=%s, wake=%s)",
            assistant_id,
            pending.assistant_id,
            task_id,
            wake,
        )
        return None
    return pending


def _pending_live_provenance_key(
    task_id: int,
    destination: str | None,
) -> tuple[int, str | None]:
    """Return the in-memory pending live-run key for a task root."""

    return task_id, _coerce_str(destination)


def _normalize_pending_trigger_attempt_token(attempt_token: str | None) -> str | None:
    """Return the normalized pending-provenance key for one trigger attempt token."""

    return _normalize_run_key_component(attempt_token)


def wake_from_activation_reason(reason: ActivatedBy | str | None) -> Wake:
    """Normalize scheduler activation reasons into execution wake vocabulary."""

    source = RunSource.from_activation_reason(reason)
    return Wake.normalize(source.value)


def create_or_adopt_live_task_run(
    provenance: TaskRunProvenance,
    *,
    started_at: str | None = None,
) -> TaskRunReference | None:
    """Create or adopt one live execution row at the moment execution begins."""

    return _create_or_adopt_task_run(
        provenance,
        state=ExecutionState.running,
        started_at=started_at or _now_iso(),
    )


def _executions_context() -> str:
    """Return the executions context, creating it on first use."""

    context = build_task_executions_context_name()
    db.create_context(
        context,
        unique_keys={"run_key": "str"},
        project=TASK_MACHINE_STATE_PROJECT,
    )
    return context


def _find_run_log_id(context: str, run_key: str) -> int | None:
    ids = db.get_logs(
        project=TASK_MACHINE_STATE_PROJECT,
        context=context,
        filter=f"run_key == {run_key!r}",
        limit=1,
        return_ids_only=True,
    )
    return int(ids[0]) if ids else None


def _create_or_adopt_task_run(
    provenance: TaskRunProvenance,
    *,
    state: ExecutionState,
    started_at: str | None = None,
) -> TaskRunReference | None:
    run_key = build_task_run_key(provenance)
    context = _executions_context()
    if _find_run_log_id(context, run_key) is None:
        row = _drop_none_values(
            {
                "run_key": run_key,
                "assistant_id": provenance.assistant_id,
                "task_id": provenance.task_id,
                "source_task_log_id": provenance.source_task_log_id,
                "wake": provenance.wake.value,
                "delivery": provenance.delivery.value,
                # The digest inside run_key hashed str(revision or ""), so the
                # row must carry the same value the dispatcher will rebuild the
                # key from at fire time.
                "revision": str(provenance.revision or ""),
                "destination": provenance.destination,
                "scheduled_for": provenance.scheduled_for,
                "dispatch_offset_seconds": provenance.dispatch_offset_seconds,
                "entrypoint": provenance.entrypoint,
                "source_medium": provenance.source_medium,
                "source_ref": provenance.source_ref,
                "source_contact_id": provenance.source_contact_id,
                "source_contact_display_name": provenance.source_contact_display_name,
                "task_name": provenance.task_name,
                "started_at": started_at,
                "state": state.value,
            },
        )
        db.create_logs(
            project=TASK_MACHINE_STATE_PROJECT,
            context=context,
            entries=[row],
        )
    return TaskRunReference(
        assistant_id=provenance.assistant_id,
        run_key=run_key,
        source_task_log_id=provenance.source_task_log_id,
    )


def update_task_run_record(
    run_reference: TaskRunReference | None,
    updates: Mapping[str, Any],
) -> None:
    """Patch one previously materialized execution row.

    ``updates`` is applied as given: a None in there is the caller saying
    "clear this", so a run that succeeded after a failed attempt on the same
    run_key can clear the earlier ``error``.
    """

    if run_reference is None:
        return
    context = _executions_context()
    log_id = _find_run_log_id(context, run_reference.run_key)
    if log_id is None:
        return
    db.update_logs(
        logs=log_id,
        context=context,
        entries=dict(updates),
        overwrite=True,
        project=TASK_MACHINE_STATE_PROJECT,
    )


def latest_task_run_reference_for_source(
    *,
    assistant_id: str | int | None,
    task_id: int,
    source_task_log_id: int,
) -> TaskRunReference | None:
    """Return the latest execution row tied to one physical source task row."""

    normalized_assistant_id = _coerce_str(assistant_id)
    if not normalized_assistant_id:
        return None
    rows = db.get_logs(
        project=TASK_MACHINE_STATE_PROJECT,
        context=_executions_context(),
        filter=(
            f"assistant_id == {normalized_assistant_id!r} and "
            f"task_id == {int(task_id)} and "
            f"source_task_log_id == {int(source_task_log_id)}"
        ),
        sorting={"row_id": "descending"},
        limit=1,
        from_fields=["run_key"],
    )
    if not rows:
        return None
    run_key = _coerce_str(rows[0].entries.get("run_key"))
    if not run_key:
        return None
    return TaskRunReference(
        assistant_id=normalized_assistant_id,
        run_key=run_key,
        source_task_log_id=int(source_task_log_id),
    )


def build_task_run_key(provenance: TaskRunProvenance) -> str:
    """Build the canonical run-key shape shared across live and offline lanes."""

    revision_digest = hashlib.sha256(
        str(provenance.revision or "").encode("utf-8"),
    ).hexdigest()[:12]
    destination_part = (
        f"{_normalize_run_key_component(provenance.destination)}:"
        if provenance.destination
        else ""
    )
    tail_parts: list[str] = []
    if provenance.scheduled_for:
        normalized_due = _normalize_run_datetime_fragment(provenance.scheduled_for)
        if normalized_due:
            tail_parts.append(normalized_due)
    normalized_contact_id = _normalize_run_key_component(provenance.source_contact_id)
    if normalized_contact_id:
        tail_parts.append(f"contact-{normalized_contact_id[:24]}")
    normalized_medium = _normalize_run_key_component(provenance.source_medium)
    if normalized_medium:
        tail_parts.append(normalized_medium[:24])
    if provenance.source_ref:
        tail_parts.append(
            hashlib.sha256(provenance.source_ref.encode("utf-8")).hexdigest()[:12],
        )
    tail = "-".join(tail_parts) or "once"
    wake = Wake.normalize(provenance.wake)
    delivery = Delivery.normalize(provenance.delivery)
    return (
        f"{delivery.value}:{wake.value}:"
        f"{provenance.assistant_id}:{destination_part}{provenance.task_id}:"
        f"{revision_digest}:{tail}"
    )


def get_open_task_execution(
    *,
    assistant_id: str | int | None,
    task_id: int,
    destination: str | None = None,
    wake: Wake | str | None = None,
    run_key: str | None = None,
) -> TaskExecutionSnapshot | None:
    """Return the current open execution row for one assistant/task pair, if any."""

    normalized_destination = _canonical_destination_or_none(destination)
    if normalized_destination is None and destination not in (
        None,
        "",
        PERSONAL_DESTINATION,
    ):
        return None
    normalized_assistant_id = _coerce_str(assistant_id)
    if run_key:
        filter_clauses = [f"run_key == '{run_key}'"]
    else:
        if not normalized_assistant_id:
            return None
        filter_clauses = [
            f"assistant_id == '{normalized_assistant_id}'",
            f"task_id == {int(task_id)}",
            _open_execution_state_filter(),
        ]
        normalized_wake = _coerce_str(wake.value if isinstance(wake, Wake) else wake)
        if normalized_wake:
            filter_clauses.append(f"wake == '{normalized_wake}'")
        if normalized_destination is not None:
            filter_clauses.append(f"destination == '{normalized_destination}'")
    rows = _execution_store().get_rows(
        filter=" and ".join(filter_clauses),
        limit=1,
        include_fields=_EXECUTION_QUERY_FIELDS,
    )
    if not rows:
        return None
    return _row_to_execution(rows[0])


def find_running_execution_for_task(
    *,
    task_id: int,
    destination: str | None = None,
    states: tuple[ExecutionState, ...] = _OPEN_EXECUTION_STATES,
) -> TaskExecutionSnapshot | None:
    """Return an execution in one of ``states`` for one task, whatever assistant owns it.

    ``get_open_task_execution`` is assistant-scoped and returns ``None`` when
    the session has no assistant context. "Is a run in flight for this task?"
    must not depend on who is asking, so this queries by ``task_id`` alone —
    the definition it guards is a single row shared by every assistant that
    can execute it.

    ``states`` defaults to the full open-state set (``scheduled``,
    ``triggerable``, ``running``) so existing callers are unaffected; pass a
    narrower tuple to match only a subset, e.g. genuinely ``running`` rows.
    """

    filter_clauses = [
        f"task_id == {int(task_id)}",
        _open_execution_state_filter(states),
    ]
    normalized_destination = _canonical_destination_or_none(destination)
    if normalized_destination is not None:
        filter_clauses.append(f"destination == '{normalized_destination}'")
    rows = _execution_store().get_rows(
        filter=" and ".join(filter_clauses),
        limit=1,
        include_fields=_EXECUTION_QUERY_FIELDS,
    )
    if not rows:
        return None
    return _row_to_execution(rows[0])


#: The run facts worth handing a caller asking what happened, in the order a
#: reader wants them. Deliberately not the whole row: trigger routing and
#: dispatch offsets answer "how would this fire", not "what did it do".
_RUN_HISTORY_FIELDS = (
    "run_key",
    "task_id",
    "task_name",
    "state",
    "wake",
    "delivery",
    "scheduled_for",
    "started_at",
    "completed_at",
    "result_summary",
    "error",
)


def list_task_run_history(
    *,
    task_id: int,
    destination: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Return one task's occurrences, newest first, as plain run facts.

    The ledger has always recorded when a task ran, what it produced and why
    it failed, and nothing in the runtime could read it: the execution
    helpers here each answer one dispatch question ("is one in flight?",
    "has any finished?") and return a snapshot shaped for that decision. So
    "did the briefing run this morning?" had no answer available to the
    assistant, however plainly the rows said yes.

    Dicts rather than ``TaskExecutionSnapshot`` because this is a different
    question: the snapshot carries what dispatch needs to route a wake, and
    none of what a reader wants to know about a run that already happened.

    Includes the open occurrence when there is one, because "when does it run
    next" is the same question asked forwards.
    """

    filter_clauses = [f"task_id == {int(task_id)}"]
    normalized_destination = _canonical_destination_or_none(destination)
    if normalized_destination is not None:
        filter_clauses.append(f"destination == '{normalized_destination}'")
    rows = _execution_store().get_rows(
        filter=" and ".join(filter_clauses),
        limit=max(int(limit), 1),
        include_fields=_EXECUTION_QUERY_FIELDS,
    )
    history: list[dict[str, Any]] = []
    for row in rows:
        entries = getattr(row, "entries", row)
        if not isinstance(entries, Mapping):
            continue
        history.append(
            {key: entries.get(key) for key in _RUN_HISTORY_FIELDS if key in entries},
        )
    # Newest first by the moment the occurrence belongs to. An on-demand run
    # has no scheduled_for, so it sorts by when it actually started.
    history.sort(
        key=lambda run: str(run.get("scheduled_for") or run.get("started_at") or ""),
        reverse=True,
    )
    return history[: max(int(limit), 1)]


def find_terminal_execution_for_task(
    *,
    task_id: int,
    destination: str | None = None,
) -> TaskExecutionSnapshot | None:
    """Return a finished execution for one task, if any run has terminalized.

    This is how a one-shot knows it has already run. Storing that on the
    definition would put a run fact back on the shared row; the run ledger
    already records it, so the definition can stay authored intent only.
    """

    terminal_states = " or ".join(
        f"state == '{state.value}'" for state in ExecutionState if state.is_terminal
    )
    filter_clauses = [f"task_id == {int(task_id)}", f"({terminal_states})"]
    normalized_destination = _canonical_destination_or_none(destination)
    if normalized_destination is not None:
        filter_clauses.append(f"destination == '{normalized_destination}'")
    rows = _execution_store().get_rows(
        filter=" and ".join(filter_clauses),
        limit=1,
        include_fields=_EXECUTION_QUERY_FIELDS,
    )
    if not rows:
        return None
    return _row_to_execution(rows[0])


def list_scheduled_executions(
    *,
    assistant_id: str | int | None,
    limit: int = _DEFAULT_TRIGGER_PAGE_SIZE,
) -> list[TaskExecutionSnapshot]:
    """List scheduled open executions with a future due time for one assistant."""

    normalized_assistant_id = _coerce_str(assistant_id)
    if not normalized_assistant_id:
        return []
    filter_clauses = [
        f"assistant_id == '{normalized_assistant_id}'",
        f"wake == '{Wake.scheduled.value}'",
        "scheduled_for != None",
        _open_execution_state_filter(),
    ]
    rows = _execution_store().get_rows(
        filter=" and ".join(filter_clauses),
        limit=limit,
        include_fields=_EXECUTION_QUERY_FIELDS,
    )
    executions: list[TaskExecutionSnapshot] = []
    for row in rows:
        execution = _row_to_execution(row)
        if execution is not None:
            executions.append(execution)
    return executions


def list_trigger_executions(
    *,
    assistant_id: str | int | None,
    medium: str | None = None,
    limit: int = _DEFAULT_TRIGGER_PAGE_SIZE,
) -> list[TaskExecutionSnapshot]:
    """List trigger executions for one assistant, optionally scoped by medium."""

    normalized_assistant_id = _coerce_str(assistant_id)
    if not normalized_assistant_id:
        return []
    filter_clauses = [
        f"assistant_id == '{normalized_assistant_id}'",
        f"wake == '{Wake.triggered.value}'",
        _open_execution_state_filter(),
    ]
    normalized_medium = _coerce_str(medium)
    if normalized_medium:
        filter_clauses.append(f"trigger_medium == '{normalized_medium}'")
    rows = _execution_store().get_rows(
        filter=" and ".join(filter_clauses),
        limit=limit,
        include_fields=_EXECUTION_QUERY_FIELDS,
    )
    executions: list[TaskExecutionSnapshot] = []
    for row in rows:
        execution = _row_to_execution(row)
        if execution is not None:
            executions.append(execution)
    return executions


def validate_task_due_execution(
    *,
    assistant_id: str | int | None,
    task_id: int,
    revision: str,
    source_task_log_id: int,
    scheduled_for: str,
    destination: str | None = None,
) -> tuple[TaskExecutionSnapshot | None, str | None]:
    """Validate that a live due event still matches the current open execution."""

    try:
        normalized_destination = ContextRegistry.canonical_destination(destination)
    except ValueError:
        return None, "invalid_destination"
    execution = get_open_task_execution(
        assistant_id=assistant_id,
        task_id=task_id,
        destination=normalized_destination,
        wake=Wake.scheduled,
    )
    if execution is None:
        return None, "execution_missing"
    if execution.wake != Wake.scheduled.value:
        return None, "wake_changed"
    if execution.delivery != Delivery.live.value:
        return None, "delivery_changed"
    if execution.revision != revision:
        return None, "revision_mismatch"
    if execution.destination != normalized_destination:
        return None, "destination_mismatch"
    if execution.source_task_log_id != source_task_log_id:
        return None, "source_task_log_id_mismatch"
    if _normalize_datetime_string(
        execution.scheduled_for,
    ) != _normalize_datetime_string(scheduled_for):
        return None, "scheduled_for_mismatch"
    return execution, None


def _execution_store() -> TasksStore:
    """Return a lightweight reader for the internal executions context."""

    return TasksStore(
        build_task_executions_context_name(),
        project=TASK_MACHINE_STATE_PROJECT,
    )


def _open_execution_state_filter(
    states: tuple[ExecutionState, ...] = _OPEN_EXECUTION_STATES,
) -> str:
    """Return a filter clause matching the given execution states."""

    quoted = " or ".join(f"state == '{state.value}'" for state in states)
    return f"({quoted})"


def _row_to_execution(row: Any) -> TaskExecutionSnapshot | None:
    """Convert a Unify log row or raw mapping into a typed execution snapshot."""

    entries = getattr(row, "entries", row)
    if not isinstance(entries, Mapping):
        return None
    task_id = _coerce_int(entries.get("task_id"))
    if task_id is None:
        return None
    run_key = _coerce_str(entries.get("run_key"))
    if not run_key:
        return None
    assistant_id = _coerce_str(entries.get("assistant_id"))
    raw_destination = _coerce_str(entries.get("destination"))
    try:
        destination = ContextRegistry.canonical_destination(raw_destination)
    except ValueError:
        return None
    return TaskExecutionSnapshot(
        run_key=run_key,
        task_id=task_id,
        assistant_id=assistant_id,
        destination=destination,
        source_task_log_id=_coerce_int(entries.get("source_task_log_id")),
        wake=_coerce_str(entries.get("wake")),
        delivery=_coerce_str(entries.get("delivery")),
        state=_coerce_str(entries.get("state")),
        task_name=_coerce_str(entries.get("task_name")),
        task_summary=_coerce_str(entries.get("task_summary")),
        scheduled_for=_coerce_str(entries.get("scheduled_for")),
        trigger_medium=_coerce_str(entries.get("trigger_medium")),
        trigger_from_contact_ids=_coerce_int_list(
            entries.get("trigger_from_contact_ids"),
        ),
        trigger_omit_contact_ids=_coerce_int_list(
            entries.get("trigger_omit_contact_ids"),
        ),
        trigger_recurring=bool(entries.get("trigger_recurring", False)),
        entrypoint=_coerce_int(entries.get("entrypoint")),
        max_runtime_seconds=_coerce_int(entries.get("max_runtime_seconds")),
        recurring=bool(entries.get("recurring", False)),
        revision=_coerce_str(entries.get("revision")),
    )


def _normalize_wake(value: Wake | RunSource | str) -> Wake:
    """Normalize wake values from legacy run-source call sites."""

    if isinstance(value, Wake):
        return value
    if isinstance(value, RunSource):
        return Wake.normalize(value.value)
    return Wake.normalize(value)


def _drop_none_values(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a shallow copy without `None` values."""

    return {key: value for key, value in payload.items() if value is not None}


def _normalize_run_key_component(value: str | None) -> str | None:
    """Normalize one free-form run-key component into a compact identifier."""

    text = _coerce_str(value)
    if not text:
        return None
    normalized_chars = [
        char.lower() if char.isalnum() else "-" for char in text.strip()
    ]
    normalized = "".join(normalized_chars).strip("-")
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized or None


def _normalize_run_datetime_fragment(value: str | None) -> str | None:
    """Normalize one datetime string into the canonical run-key timestamp fragment."""

    normalized = _normalize_datetime_string(value)
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00")).strftime(
            "%Y%m%dT%H%M%SZ",
        )
    except ValueError:
        return None


def _now_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 format."""

    return datetime.now(timezone.utc).isoformat()


def _coerce_int(value: Any) -> int | None:
    """Best-effort integer coercion for JSON-backed execution rows."""

    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_int_list(value: Any) -> list[int]:
    """Return a list of integer ids, dropping non-coercible elements."""

    values = _coerce_list(value) or []
    coerced: list[int] = []
    for item in values:
        int_value = _coerce_int(item)
        if int_value is not None:
            coerced.append(int_value)
    return coerced


def _coerce_list(value: Any) -> list[Any] | None:
    """Normalize list-like values while preserving `None`."""

    if value is None:
        return None
    if isinstance(value, list):
        return value
    return [value]


def _coerce_str(value: Any) -> str | None:
    """Normalize scalar values to strings while preserving `None`."""

    if value is None:
        return None
    text = str(value)
    return text if text else None


def _normalize_datetime_string(value: str | None) -> str | None:
    """Return one canonical UTC ISO-8601 string for datetime comparisons."""

    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()
