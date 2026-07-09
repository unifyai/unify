"""Task Scheduler: create, search, update, and execute tasks."""

from __future__ import annotations

import asyncio
import functools
import logging
import os
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    Union,
    overload,
)

import unisdk
import unillm
from pydantic import BaseModel

from ..actor.base import BaseActor
from ..common.async_tool_loop import (
    TOOL_LOOP_LINEAGE,
    SteerableToolHandle,
    start_async_tool_loop,
)
from ..common.context_registry import (
    ContextRegistry,
    PERSONAL_DESTINATION,
    TEAM_CONTEXT_PREFIX,
    TEAM_DESTINATION_PREFIX,
    TableContext,
)
from ..common.embed_utils import ensure_vector_column, list_private_fields
from ..common.filter_utils import normalize_filter_expr
from ..common.log_utils import create_logs as unity_create_logs
from ..common.llm_client import new_llm_client
from ..common.llm_helpers import methods_to_tool_dict
from ..common.metrics_utils import reduce_logs
from ..common.model_to_fields import model_to_fields
from ..common.read_only_ask_guard import ReadOnlyAskGuardHandle
from ..common.search_utils import table_search_top_k
from ..common.sentinels import _UnsetSentinel
from ..common.task_execution_context import current_task_execution_delegate
from ..common.tool_outcome import ToolOutcome, ToolErrorException
from ..common.tool_spec import ToolSpec, read_only
from ..events.manager_event_logging import log_manager_call
from ..manager_registry import ManagerRegistry
from ..session_details import SESSION_DETAILS
from ..settings import SETTINGS
from .active_task import ActiveTask
from .base import BaseTaskScheduler
from .custom_tasks import (
    compute_custom_tasks_hash,
    derive_initial_task_status,
)
from .machine_state import (
    TaskRunProvenance,
    build_task_run_key,
    consume_live_task_run_provenance,
    peek_live_task_run_provenance,
    source_type_from_activation_reason,
)
from .prompt_builders import (
    build_ask_prompt,
    build_task_execution_request,
    build_task_run_guidelines,
    build_update_prompt,
)
from .storage import TasksStore
from .types.activated_by import ActivatedBy
from .types.meta import TaskMeta
from .types.priority import Priority
from .types.repetition import (
    Frequency,
    RepeatPattern,
    Weekday,
    next_repeated_start_at,
    normalize_repeat_patterns,
)
from .types.schedule import Schedule
from .types.status import Status, to_status
from .types.task import Task, TaskBase
from .types.trigger import Trigger

ScheduleLike = Optional[Union[Schedule, Dict[str, Any]]]
TriggerLike = Optional[Union[Trigger, Dict[str, Any]]]
RepeatLike = Optional[List[Union[RepeatPattern, Dict[str, Any]]]]
ToolsDict = Dict[str, Callable[..., Any]]

TASKS_META_TABLE = "Tasks/Meta"
logger = logging.getLogger(__name__)

OFFLINE_CERTIFICATION_REQUIRED_EVIDENCE_FIELDS = {
    "idempotency_contract",
    "side_effect_contract",
    "cost_contract",
    "input_contract",
    "failure_contract",
    "observability_contract",
    "equivalence_contract",
    "managed_primitive_contract",
}
OFFLINE_CERTIFICATION_ALLOWED_RISK_CLASSIFICATIONS = {
    "safe_noop",
    "read_only",
    "idempotent_effectful",
    "unsafe_effectful",
}
OFFLINE_CERTIFICATION_REQUIRED_ATTESTATIONS = {
    "no_hardcoded_live_observations",
    "no_removed_validation_gates",
    "no_reordered_side_effects",
    "no_discarded_recovery_branches",
    "no_static_runtime_assumptions",
    "no_ad_hoc_logic_replaced_managed_primitives",
}


def _missing_certification_value(value: Any) -> bool:
    """Return whether a certification evidence field is materially empty."""

    return value in (None, "", [], {})


_UNSET = _UnsetSentinel()


class TaskScheduler(BaseTaskScheduler):
    """Concrete scheduler backed by the Tasks context."""

    _TERMINAL_STATUSES = {Status.completed, Status.cancelled, Status.failed}

    class Config:
        required_contexts = [
            TableContext(
                name="Tasks",
                description=(
                    "List of all tasks with their name, description, status, "
                    "schedule, deadline, repeat pattern, priority and instance_id "
                    "which tracks multiple executions of the same logical task."
                ),
                fields=model_to_fields(Task),
                unique_keys={"task_id": "int", "instance_id": "int"},
                auto_counting={
                    "task_id": None,
                    "instance_id": "task_id",
                },
                foreign_keys=[
                    {
                        "name": "entrypoint",
                        "references": "Functions/Compositional.function_id",
                        "on_delete": "SET NULL",
                        "on_update": "CASCADE",
                    },
                ],
            ),
            TableContext(
                name=TASKS_META_TABLE,
                description="Metadata for source-defined custom task sync state.",
                fields=model_to_fields(TaskMeta),
                unique_keys={"meta_id": "int"},
            ),
        ]

    def __init__(
        self,
        *,
        actor: Optional[BaseActor] = None,
        rolling_summary_in_prompts: bool = True,
    ) -> None:
        """Create a scheduler for durable tasks in the current context."""

        super().__init__()

        # Get ContactManager via registry so its bound methods can act as tools
        self._contact_manager = ManagerRegistry.get_contact_manager()

        ask_tools = {
            **methods_to_tool_dict(
                ToolSpec(fn=self._filter_tasks, display_label="Filtering tasks"),
                ToolSpec(fn=self._search_tasks, display_label="Searching tasks"),
                ToolSpec(fn=self._reduce, display_label="Summarising tasks"),
                include_class_name=False,
            ),
            **methods_to_tool_dict(
                ToolSpec(
                    fn=self._contact_manager.ask,
                    display_label="Looking up contact details",
                ),
                include_class_name=True,
            ),
        }
        self._ask_tools = dict(ask_tools)
        self.add_tools("ask", ask_tools)

        update_tools = {
            **methods_to_tool_dict(
                ToolSpec(fn=self.ask, display_label="Querying tasks"),
                ToolSpec(fn=self._filter_tasks, display_label="Filtering tasks"),
                ToolSpec(fn=self._search_tasks, display_label="Searching tasks"),
                ToolSpec(
                    fn=self._create_tasks,
                    display_label="Creating multiple tasks",
                ),
                ToolSpec(fn=self._create_task, display_label="Creating a new task"),
                ToolSpec(fn=self._delete_task, display_label="Deleting a task"),
                ToolSpec(fn=self._cancel_tasks, display_label="Cancelling tasks"),
                ToolSpec(fn=self._update_task, display_label="Updating a task"),
                include_class_name=False,
            ),
            **methods_to_tool_dict(
                ToolSpec(
                    fn=self._contact_manager.ask,
                    display_label="Looking up contact details",
                ),
                include_class_name=True,
            ),
        }
        self._update_tools = dict(update_tools)
        self.add_tools("update", update_tools)

        self.__actor = actor
        self._ctx = ContextRegistry.get_context(self, "Tasks")
        self._personal_tasks_context = self._ctx
        self._meta_ctx = ContextRegistry.get_context(self, TASKS_META_TABLE)
        self._root_stores: Dict[str, TasksStore] = {}
        self._active_task_root_context: Optional[str] = None
        self._custom_tasks_synced = False
        self._custom_tasks_synced_contexts: set[str] = set()
        self._destination_context_lock = threading.RLock()
        self._provision_storage()

        self._rolling_summary_in_prompts = rolling_summary_in_prompts
        self._num_tasks_cached: Optional[int] = None

    def _actor_for_task_run(self) -> BaseActor | None:
        """Return the fallback actor only when task execution is not delegated."""

        if current_task_execution_delegate.get() is not None:
            return None
        return self.__actor

    def _build_task_entrypoint_review(
        self,
        *,
        task: Task,
        reason: ActivatedBy,
    ) -> dict[str, Any] | None:
        """Return post-run entrypoint review context for description-driven tasks."""

        if task.entrypoint is not None:
            return None
        if task.repeat is None and task.trigger is None:
            return None

        metadata: dict[str, Any] = {
            "task_id": task.task_id,
            "instance_id": task.instance_id,
            "task_name": task.name,
            "task_description": task.description,
            "activation_reason": reason.value,
            "response_policy": task.response_policy,
            "schedule": (
                task.schedule.model_dump(mode="json")
                if task.schedule is not None
                else None
            ),
            "trigger": (
                task.trigger.model_dump(mode="json")
                if task.trigger is not None
                else None
            ),
            "repeat": (
                [pattern.model_dump(mode="json") for pattern in task.repeat]
                if task.repeat is not None
                else None
            ),
        }

        def _attach_entrypoint(
            *,
            function_id: int,
            rationale: str,
            certification_metadata: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            return self._attach_entrypoint_to_future_instances(
                task_id=task.task_id,
                completed_instance_id=task.instance_id,
                function_id=function_id,
                rationale=rationale,
                certification_metadata=certification_metadata,
            )

        def _promote_entrypoint_offline(
            *,
            function_id: int,
            certification_metadata: dict[str, Any],
            certification_result: dict[str, Any],
        ) -> dict[str, Any]:
            return self._promote_symbolic_candidate_to_offline(
                task_id=task.task_id,
                completed_instance_id=task.instance_id,
                function_id=function_id,
                certification_metadata=certification_metadata,
                certification_result=certification_result,
            )

        return {
            "metadata": metadata,
            "attach_entrypoint": _attach_entrypoint,
            "promote_entrypoint_offline": _promote_entrypoint_offline,
        }

    def _build_task_run_context(
        self,
        *,
        task: Task,
        reason: ActivatedBy,
        source_type: str,
        task_run_provenance: TaskRunProvenance | None,
    ) -> dict[str, Any]:
        """Return deterministic run facts supplied by the scheduler."""

        scheduled_for = None
        activation_revision = None
        source_medium = None
        source_ref = None
        source_contact_id = None
        run_key = None
        if task_run_provenance is not None:
            scheduled_for = task_run_provenance.scheduled_for
            activation_revision = task_run_provenance.activation_revision
            source_medium = task_run_provenance.source_medium
            source_ref = task_run_provenance.source_ref
            source_contact_id = task_run_provenance.source_contact_id
            run_key = build_task_run_key(task_run_provenance)
        if scheduled_for is None and task.schedule_start_at is not None:
            scheduled_for = task.schedule_start_at.isoformat()
        return {
            "task_id": task.task_id,
            "instance_id": task.instance_id,
            "task_name": task.name,
            "source_type": source_type,
            "activation_reason": reason.value,
            "scheduled_for": scheduled_for,
            "scheduled_run_timestamp": scheduled_for,
            "run_key": run_key,
            "activation_revision": activation_revision,
            "source_medium": source_medium,
            "source_ref": source_ref,
            "source_contact_id": source_contact_id,
            "delivery_mode": task.delivery_mode.value,
            "execution_style": task.execution_style.value,
        }

    def _build_entrypoint_kwargs(
        self,
        *,
        task: Task,
        reason: ActivatedBy,
        source_type: str,
        task_run_provenance: TaskRunProvenance | None,
    ) -> dict[str, Any]:
        """Return explicit kwargs available to symbolic task entrypoints."""

        context = self._build_task_run_context(
            task=task,
            reason=reason,
            source_type=source_type,
            task_run_provenance=task_run_provenance,
        )
        return {
            **context,
            "task_execution_context": context,
        }

    def _attach_entrypoint_to_future_instances(
        self,
        *,
        task_id: int,
        completed_instance_id: int,
        function_id: int,
        rationale: str,
        certification_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Record a symbolic executor candidate on future non-terminal instances."""

        if function_id < 0:
            raise ValueError("function_id must be a non-negative integer.")

        task = self._get_task_or_raise(task_id)
        with self._use_task_destination(task.destination):
            future_logs = self._store.get_rows(
                filter=(
                    f"task_id == {task_id} and instance_id > {completed_instance_id} "
                    "and entrypoint is None and status not in ('completed','cancelled','failed','active')"
                ),
                return_ids_only=False,
            )
            if not future_logs:
                return {
                    "outcome": "no_future_instances",
                    "task_id": task_id,
                    "completed_instance_id": completed_instance_id,
                    "function_id": function_id,
                    "rationale": rationale,
                }

            log_ids = [int(log.id) for log in future_logs]
            self._write_log_entries(
                logs=log_ids,
                entries={"entrypoint": int(function_id)},
            )
            return {
                "outcome": "candidate_recorded",
                "task_id": task_id,
                "patched_instance_ids": [
                    log.entries.get("instance_id") for log in future_logs
                ],
                "function_id": int(function_id),
                "rationale": rationale,
                "certification_status": "required_before_offline_promotion",
                "certification_metadata": certification_metadata or {},
            }

    def _offline_promotion_rejection_reasons(
        self,
        *,
        certification_metadata: dict[str, Any],
        certification_result: dict[str, Any],
    ) -> list[str]:
        """Return reasons a symbolic candidate is not certified for offline use."""

        reasons: list[str] = []
        evidence = certification_metadata.get("certification_evidence")
        if not isinstance(evidence, dict) or not evidence:
            reasons.append("missing_certification_evidence")
            evidence = {}

        missing_evidence_fields = sorted(
            field
            for field in OFFLINE_CERTIFICATION_REQUIRED_EVIDENCE_FIELDS
            if _missing_certification_value(evidence.get(field))
        )
        reasons.extend(f"missing_evidence:{field}" for field in missing_evidence_fields)

        risk_classification = evidence.get("risk_classification")
        if _missing_certification_value(risk_classification):
            reasons.append("missing_evidence:risk_classification")
        elif (
            risk_classification
            not in OFFLINE_CERTIFICATION_ALLOWED_RISK_CLASSIFICATIONS
        ):
            reasons.append(f"invalid_risk_classification:{risk_classification}")
        elif risk_classification == "unsafe_effectful":
            reasons.append("unsafe_side_effect_contract")

        managed_primitive_contract = evidence.get("managed_primitive_contract")
        if isinstance(managed_primitive_contract, dict):
            preserved = managed_primitive_contract.get("preserved")
            if preserved is not True:
                reasons.append("primitive_surface_changed")
            ad_hoc_replacements = managed_primitive_contract.get(
                "ad_hoc_replacements",
            )
            if ad_hoc_replacements not in (None, [], {}):
                reasons.append("ad_hoc_logic_replaced_managed_primitive")
        elif not _missing_certification_value(managed_primitive_contract):
            reasons.append("invalid_evidence:managed_primitive_contract")

        cost_contract = evidence.get("cost_contract")
        if isinstance(cost_contract, dict) and cost_contract.get("bounded") is not True:
            reasons.append("cost_contract_too_expensive")

        attestations = evidence.get("attestations")
        if not isinstance(attestations, dict):
            reasons.append("missing_evidence:attestations")
            attestations = {}
        failed_attestations = sorted(
            field
            for field in OFFLINE_CERTIFICATION_REQUIRED_ATTESTATIONS
            if attestations.get(field) is not True
        )
        reasons.extend(f"failed_attestation:{field}" for field in failed_attestations)

        if certification_result.get("evidence_based") is not True:
            reasons.append("certification_evidence_not_attested")
        if certification_result.get("executed_entrypoint") is True:
            reasons.append("certification_must_not_execute_entrypoint")
        return reasons

    def _promote_symbolic_candidate_to_offline(
        self,
        *,
        task_id: int,
        completed_instance_id: int,
        function_id: int,
        certification_metadata: dict[str, Any],
        certification_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Promote future symbolic candidate instances to offline delivery."""

        if function_id < 0:
            raise ValueError("function_id must be a non-negative integer.")

        rejection_reasons = self._offline_promotion_rejection_reasons(
            certification_metadata=certification_metadata,
            certification_result=certification_result,
        )
        if rejection_reasons:
            return {
                "outcome": "certification_rejected",
                "task_id": task_id,
                "completed_instance_id": completed_instance_id,
                "function_id": int(function_id),
                "rejection_reasons": rejection_reasons,
            }

        task = self._get_task_or_raise(task_id)
        with self._use_task_destination(task.destination):
            future_logs = self._store.get_rows(
                filter=(
                    f"task_id == {task_id} and instance_id > {completed_instance_id} "
                    f"and entrypoint == {int(function_id)} "
                    "and status not in ('completed','cancelled','failed','active')"
                ),
                return_ids_only=False,
            )
            future_logs = [
                log for log in future_logs if not bool(log.entries.get("offline"))
            ]
            if not future_logs:
                return {
                    "outcome": "no_matching_candidate_instances",
                    "task_id": task_id,
                    "completed_instance_id": completed_instance_id,
                    "function_id": int(function_id),
                    "certification_status": "passed",
                    "certification_result": certification_result,
                }

            log_ids = [int(log.id) for log in future_logs]
            self._write_log_entries(
                logs=log_ids,
                entries={"offline": True},
            )
            return {
                "outcome": "offline_promoted",
                "task_id": task_id,
                "patched_instance_ids": [
                    log.entries.get("instance_id") for log in future_logs
                ],
                "function_id": int(function_id),
                "certification_status": "passed",
                "certification_metadata": certification_metadata,
                "certification_result": certification_result,
            }

    def warm_embeddings(self) -> None:
        """Ensure vector columns used by semantic search exist."""

        for col in ("name", "description"):
            try:
                ensure_vector_column(
                    self._ctx,
                    embed_column=f"_{col}_emb",
                    source_column=col,
                )
            except Exception:
                pass

    def _provision_storage(self) -> None:
        """Install the storage adapter for the current Tasks context."""

        self._store = TasksStore(self._ctx)
        self._root_stores[self._ctx] = self._store

    def _task_context_from_root(self, root_context: str) -> str:
        """Return the concrete Tasks context under one registry root."""

        return f"{root_context.strip('/')}/Tasks"

    def _destination_from_task_context(self, context_name: str) -> str | None:
        """Return the public destination represented by a concrete Tasks context."""

        if context_name.startswith(TEAM_CONTEXT_PREFIX):
            raw_team_id = context_name[len(TEAM_CONTEXT_PREFIX) :].split("/", 1)[0]
            return f"{TEAM_DESTINATION_PREFIX}{raw_team_id}"
        return None

    def _store_for_task_context(self, context_name: str) -> TasksStore:
        """Return a per-root store for a concrete Tasks context."""

        if context_name in self._root_stores:
            return self._root_stores[context_name]
        store = TasksStore(context_name)
        self._root_stores[context_name] = store
        return store

    def _task_context_for_destination(self, destination: str | None) -> str:
        """Resolve a write destination into a concrete Tasks context."""

        destination = destination or os.environ.get("TASK_DESTINATION") or None
        if destination in (None, PERSONAL_DESTINATION):
            return self._personal_tasks_context
        root_context = ContextRegistry.write_root(
            self,
            "Tasks",
            destination=destination,
        )
        return self._task_context_from_root(root_context)

    def _read_task_contexts(self) -> list[str]:
        """Return ordered concrete Tasks contexts visible to this assistant."""

        if self._active_task_root_context is not None:
            return [self._active_task_root_context]
        root_contexts = ContextRegistry.read_roots(self, "Tasks")
        contexts = [self._task_context_from_root(root) for root in root_contexts]
        return list(dict.fromkeys(contexts))

    @contextmanager
    def _use_task_destination(self, destination: str | None):
        """Temporarily scope scheduler storage to one task destination."""

        context_name = self._task_context_for_destination(destination)
        previous_context = self._ctx
        previous_store = self._store
        previous_active_root = self._active_task_root_context
        self._ctx = context_name
        self._store = self._store_for_task_context(context_name)
        self._active_task_root_context = context_name
        try:
            yield context_name
        finally:
            self._ctx = previous_context
            self._store = previous_store
            self._active_task_root_context = previous_active_root

    @functools.wraps(BaseTaskScheduler.clear, updated=())
    def clear(self) -> None:
        """Delete the current Tasks context and recreate local state."""

        unisdk.delete_context(self._ctx)
        self._num_tasks_cached = None
        self._active_task_root_context = None

        ContextRegistry.forget(self, "Tasks")
        ContextRegistry.forget(self, TASKS_META_TABLE)
        self._ctx = ContextRegistry.get_context(self, "Tasks")
        self._personal_tasks_context = self._ctx
        self._meta_ctx = ContextRegistry.get_context(self, TASKS_META_TABLE)
        self._root_stores.clear()
        self._custom_tasks_synced = False
        self._custom_tasks_synced_contexts.clear()
        self._provision_storage()

    def _task_id_to_log_id_map(self, task_ids: List[int]) -> Dict[int, int]:
        """Resolve a mapping of task_id to log_id in one call."""

        try:
            log_objs = self._get_logs_by_task_ids(
                task_ids=task_ids,
                return_ids_only=False,
            )
        except Exception:
            log_objs = []

        id_map: Dict[int, int] = {}
        for lg in log_objs:
            task_id = lg.entries.get("task_id")
            if task_id is not None:
                id_map[int(task_id)] = int(lg.id)
        return id_map

    def _get_log_by_task_instance(
        self,
        *,
        task_id: int,
        instance_id: int,
    ) -> unisdk.Log:
        """Return the physical task row for one logical task instance."""

        task = self._get_task_or_raise(task_id)
        with self._use_task_destination(task.destination):
            log_objs = self._store.get_rows(
                filter=f"task_id == {task_id} and instance_id == {instance_id}",
                limit=2,
                return_ids_only=False,
            )
        if not log_objs:
            raise ValueError(
                f"No task row found for task_id={task_id}, instance_id={instance_id}.",
            )
        if len(log_objs) != 1:
            raise ValueError(
                f"Ambiguous task rows for task_id={task_id}, instance_id={instance_id}.",
            )
        return log_objs[0]

    def _get_task_for_source_log_id(
        self,
        *,
        source_task_log_id: int,
        expected_task_id: int,
    ) -> Task:
        """Return the task instance addressed by an activation source log id."""

        for context_name in self._read_task_contexts():
            store = self._store_for_task_context(context_name)
            log_objs = store.get_rows_by_log_ids(log_ids=[source_task_log_id])
            if not log_objs:
                continue
            if len(log_objs) != 1:
                raise ValueError(
                    f"Activation source task log {source_task_log_id} is ambiguous.",
                )
            entries = dict(log_objs[0].entries or {})
            row_task_id = entries.get("task_id")
            if row_task_id != expected_task_id:
                raise ValueError(
                    "Activation source task log does not match requested task: "
                    f"expected task_id={expected_task_id}, got task_id={row_task_id}.",
                )
            instance_id = entries.get("instance_id")
            if instance_id is None:
                raise ValueError(
                    f"Activation source task log {source_task_log_id} has no instance_id.",
                )
            entries.setdefault(
                "destination",
                self._destination_from_task_context(context_name),
            )
            entries.setdefault("assistant_id", SESSION_DETAILS.assistant_context)
            sanitized = self._sanitize_activation(entries)
            return Task(**sanitized)
        raise ValueError(
            f"Activation source task log {source_task_log_id} was not found.",
        )

    @staticmethod
    def _normalize_activation_datetime(value: Any) -> str | None:
        """Normalize scheduler timestamps into comparable ISO strings."""

        if value is None:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).isoformat()
        except ValueError:
            return str(value)

    def _validate_task_matches_provenance(
        self,
        *,
        task: Task,
        provenance: TaskRunProvenance | None,
    ) -> None:
        """Reject stale scheduled provenance before mutating a task instance."""

        if provenance is None or provenance.source_type != "scheduled":
            return
        if provenance.scheduled_for is None:
            return
        task_scheduled_for = self._normalize_activation_datetime(task.schedule_start_at)
        provenance_scheduled_for = self._normalize_activation_datetime(
            provenance.scheduled_for,
        )
        if task_scheduled_for != provenance_scheduled_for:
            raise ValueError(
                "Scheduled activation does not match selected task instance: "
                f"task_id={task.task_id}, instance_id={task.instance_id}, "
                f"task_start_at={task_scheduled_for}, "
                f"activation_scheduled_for={provenance_scheduled_for}.",
            )

    def _same_instance_already_active(
        self,
        *,
        task_id: int,
        provenance: TaskRunProvenance | None,
    ) -> bool:
        """Return True when provenance targets an instance that is already active.

        Concurrent instances of the same ``task_id`` are allowed. The only
        blocked case is restarting the exact source instance that is already
        ``active`` (same ``source_task_log_id``).
        """

        if provenance is None or provenance.source_task_log_id is None:
            return False

        source_task_log_id = int(provenance.source_task_log_id)
        for context_name in self._read_task_contexts():
            store = self._store_for_task_context(context_name)
            rows = store.get_rows(
                filter=f"task_id == {task_id} and status == 'active'",
                return_ids_only=False,
            )
            for row in rows:
                if int(row.id) == source_task_log_id:
                    return True
        return False

    @functools.wraps(BaseTaskScheduler.ask, updated=())
    @log_manager_call(
        "TaskScheduler",
        "ask",
        payload_key="question",
        display_label="Checking tasks",
    )
    async def ask(
        self,
        text: str,
        *,
        response_format: Optional[Type[BaseModel]] = None,
        _return_reasoning_steps: bool = False,
        _log_tool_steps: bool = True,
        _parent_chat_context: list[dict] | None = None,
        _clarification_up_q: asyncio.Queue[str] | None = None,
        _clarification_down_q: asyncio.Queue[str] | None = None,
        rolling_summary_in_prompts: Optional[bool] = None,
        tool_policy: Union[
            Literal["default"],
            Callable[[int, Dict[str, Any]], tuple[str, Dict[str, Any]]],
            None,
        ] = "default",
    ) -> SteerableToolHandle:
        """Answer read-only questions about existing tasks."""

        client = new_llm_client()
        tools = dict(self.get_tools("ask"))

        _clar_queues = None
        if _clarification_up_q is not None and _clarification_down_q is not None:
            from ..common.llm_helpers import make_request_clarification_tool

            _clar_queues = (_clarification_up_q, _clarification_down_q)
            tools["request_clarification"] = make_request_clarification_tool(None, None)

        include_activity = (
            self._rolling_summary_in_prompts
            if rolling_summary_in_prompts is None
            else rolling_summary_in_prompts
        )
        client.set_system_message(
            build_ask_prompt(
                tools,
                num_tasks=self._num_tasks(),
                columns=self._list_columns(),
                include_activity=include_activity,
            ).to_list(),
        )

        effective_tool_policy = (
            self._default_ask_tool_policy if tool_policy == "default" else tool_policy
        )

        handle = self._start_loop(
            client,
            text,
            tools,
            loop_id=f"{self.__class__.__name__}.{self.ask.__name__}",
            parent_chat_context=_parent_chat_context,
            log_steps=_log_tool_steps,
            tool_policy=effective_tool_policy,
            handle_cls=(
                ReadOnlyAskGuardHandle if SETTINGS.UNITY_READONLY_ASK_GUARD else None
            ),
            response_format=response_format,
            clarification_queues=_clar_queues,
        )

        if _return_reasoning_steps:
            handle = self._wrap_result_with_messages(handle, client)
        return handle

    @functools.wraps(BaseTaskScheduler.update, updated=())
    @log_manager_call(
        "TaskScheduler",
        "update",
        payload_key="request",
        display_label="Updating tasks",
    )
    async def update(
        self,
        text: str,
        *,
        response_format: Optional[Type[BaseModel]] = None,
        _return_reasoning_steps: bool = False,
        _log_tool_steps: bool = True,
        _parent_chat_context: list[dict] | None = None,
        _clarification_up_q: asyncio.Queue[str] | None = None,
        _clarification_down_q: asyncio.Queue[str] | None = None,
        rolling_summary_in_prompts: Optional[bool] = None,
        tool_policy: Union[
            Literal["default"],
            Callable[[int, Dict[str, Any]], tuple[str, Dict[str, Any]]],
            None,
        ] = "default",
    ) -> SteerableToolHandle:
        """Apply a mutation request expressed in plain English."""

        client = new_llm_client()
        tools = dict(self.get_tools("update"))

        _clar_queues = None
        if _clarification_up_q is not None and _clarification_down_q is not None:
            from ..common.llm_helpers import make_request_clarification_tool

            _clar_queues = (_clarification_up_q, _clarification_down_q)
            tools["request_clarification"] = make_request_clarification_tool(None, None)

        include_activity = (
            self._rolling_summary_in_prompts
            if rolling_summary_in_prompts is None
            else rolling_summary_in_prompts
        )
        client.set_system_message(
            build_update_prompt(
                tools,
                num_tasks=self._num_tasks(),
                columns=self._list_columns(),
                include_activity=include_activity,
            ).to_list(),
        )

        effective_tool_policy = (
            self._default_update_tool_policy
            if tool_policy == "default"
            else tool_policy
        )

        handle = self._start_loop(
            client,
            text,
            tools,
            loop_id=f"{self.__class__.__name__}.{self.update.__name__}",
            parent_chat_context=_parent_chat_context,
            log_steps=_log_tool_steps,
            tool_policy=effective_tool_policy,
            response_format=response_format,
            clarification_queues=_clar_queues,
        )

        if _return_reasoning_steps:
            handle = self._wrap_result_with_messages(handle, client)
        return handle

    @functools.wraps(BaseTaskScheduler.execute, updated=())
    @log_manager_call(
        "TaskScheduler",
        "execute",
        payload_key="request",
        display_label="Working on task",
    )
    async def execute(
        self,
        task_id: int,
        *,
        trigger_attempt_token: str | None = None,
        response_format: Optional[Type[BaseModel]] = None,
        _activated_by: ActivatedBy | None = None,
        _parent_chat_context: list[dict] | None = None,
        _clarification_up_q: asyncio.Queue[str] | None = None,
        _clarification_down_q: asyncio.Queue[str] | None = None,
    ) -> SteerableToolHandle:
        """Start one runnable task instance and return its live handle."""

        all_task_instances = self._filter_tasks(
            filter=f"task_id == {task_id}",
        )
        if not all_task_instances:
            raise ValueError(f"No task found with id={task_id}")

        candidate_tasks = [
            t
            for t in all_task_instances
            if t.status
            not in (
                Status.completed,
                Status.cancelled,
                Status.failed,
                Status.active,
            )
        ]
        if not candidate_tasks:
            raise ValueError(f"No runnable task found with id={task_id}")
        task = sorted(candidate_tasks, key=lambda t: t.instance_id)[0]

        # Concurrent instances of the same task_id are allowed. Only block when
        # activation provenance targets an instance that is already active.
        source_type = (
            "triggered"
            if trigger_attempt_token
            else source_type_from_activation_reason(
                (_activated_by or ActivatedBy.explicit).value,
            )
        )
        pending_provenance = peek_live_task_run_provenance(
            assistant_id=SESSION_DETAILS.assistant.agent_id,
            task_id=task_id,
            source_type=source_type,
            trigger_attempt_token=trigger_attempt_token,
        )
        if self._same_instance_already_active(
            task_id=task_id,
            provenance=pending_provenance,
        ):
            raise RuntimeError(
                f"Task {task_id} instance source_task_log_id="
                f"{pending_provenance.source_task_log_id} is already active.",
            )

        if _activated_by is not None:
            reason = _activated_by
        else:
            if task.trigger is not None:
                reason = ActivatedBy.trigger
            elif task.schedule_start_at is not None:
                reason = ActivatedBy.schedule
            else:
                reason = ActivatedBy.explicit

        fallback_actor = self._actor_for_task_run()
        if fallback_actor is None and current_task_execution_delegate.get() is None:
            raise RuntimeError(
                "TaskScheduler.execute requires a run-scoped actor delegate or an explicit actor. "
                "Description-driven tasks should be executed from Actor.act via primitives.tasks.execute(...).",
            )

        task_run_source_type = (
            "triggered"
            if trigger_attempt_token
            else source_type_from_activation_reason(reason.value)
        )
        task_run_provenance = consume_live_task_run_provenance(
            assistant_id=SESSION_DETAILS.assistant.agent_id,
            task_id=task_id,
            source_type=task_run_source_type,
            destination=task.destination,
            trigger_attempt_token=trigger_attempt_token,
        )
        if task_run_provenance and task_run_provenance.source_task_log_id is not None:
            task = self._get_task_for_source_log_id(
                source_task_log_id=task_run_provenance.source_task_log_id,
                expected_task_id=task_id,
            )
            if task.status in (
                Status.completed,
                Status.cancelled,
                Status.failed,
                Status.active,
            ):
                raise ValueError(
                    "Activation source task instance is not runnable: "
                    f"task_id={task.task_id}, instance_id={task.instance_id}, "
                    f"status={task.status!r}.",
                )
            if _activated_by is None:
                if task.trigger is not None:
                    reason = ActivatedBy.trigger
                elif task.schedule_start_at is not None:
                    reason = ActivatedBy.schedule
                else:
                    reason = ActivatedBy.explicit
                task_run_source_type = (
                    "triggered"
                    if trigger_attempt_token
                    else source_type_from_activation_reason(reason.value)
                )
        self._validate_task_matches_provenance(
            task=task,
            provenance=task_run_provenance,
        )

        entrypoint_kwargs = None
        if task.entrypoint is not None:
            entrypoint_kwargs = self._build_entrypoint_kwargs(
                task=task,
                reason=reason,
                source_type=task_run_source_type,
                task_run_provenance=task_run_provenance,
            )

        if task.status == Status.triggerable or (
            task.repeat is not None and task.schedule_start_at is not None
        ):
            self._clone_task_instance(task)

        with self._use_task_destination(task.destination):
            self._update_task_status_instance(
                task_id=task_id,
                instance_id=task.instance_id,
                new_status=Status.active,
                activated_by=reason,
            )

        handle = await ActiveTask.create(
            fallback_actor,
            task_description=build_task_execution_request(task),
            _parent_chat_context=_parent_chat_context,
            _clarification_up_q=_clarification_up_q,
            _clarification_down_q=_clarification_down_q,
            task_id=task_id,
            instance_id=task.instance_id,
            scheduler=self,
            entrypoint=task.entrypoint,
            entrypoint_kwargs=entrypoint_kwargs,
            entrypoint_repair_attempts=1 if task.entrypoint is not None else 0,
            entrypoint_repair_context=(
                {
                    "task_run_context": entrypoint_kwargs.get(
                        "task_execution_context",
                        {},
                    ),
                    "task_request": build_task_execution_request(task),
                }
                if entrypoint_kwargs is not None
                else None
            ),
            task_run_provenance=task_run_provenance,
            task_entrypoint_review=self._build_task_entrypoint_review(
                task=task,
                reason=reason,
            ),
            task_guidelines=build_task_run_guidelines(task, reason),
        )

        return handle

    def create_task(
        self,
        *,
        name: str,
        description: str,
        destination: str | None = None,
    ) -> ToolOutcome:
        """Create a task with just the required descriptive fields."""

        return self._create_task(
            name=name,
            description=description,
            destination=destination,
        )

    def _update_task_status_instance(
        self,
        *,
        task_id: int,
        instance_id: int,
        new_status: str | Status,
        activated_by: Optional[ActivatedBy] = None,
    ) -> Dict[str, str]:
        """Update the lifecycle status for one task instance."""

        task = self._get_task_or_raise(task_id)
        with self._use_task_destination(task.destination):
            log_objs = self._store.get_rows(
                filter=f"task_id == {task_id} and instance_id == {instance_id}",
                return_ids_only=False,
            )
            if not log_objs:
                raise ValueError(f"No task instance ({task_id}.{instance_id}) found.")
            assert len(log_objs) == 1, "Composite primary key must be unique."

            new_status_enum = (
                new_status
                if isinstance(new_status, Status)
                else Status(str(new_status))
            )
            entries: Dict[str, Any] = {"status": new_status_enum}
            if new_status_enum == Status.active and activated_by is not None:
                entries["activated_by"] = str(activated_by)
            return self._write_log_entries(
                logs=log_objs[0].id,
                entries=entries,
            )

    def _clone_task_instance(self, task: Task) -> None:
        """Create the next instance for a triggerable or repeating task."""

        clone_payload = task.model_dump(
            exclude={"instance_id", "activated_by"},
            mode="json",
        )
        if task.repeat is not None and task.schedule_start_at is not None:
            next_start_at = next_repeated_start_at(
                previous_start=task.schedule_start_at,
                patterns=task.repeat,
                current_occurrence_index=task.instance_id,
            )
            if next_start_at is None:
                return
            clone_payload["status"] = Status.scheduled
            clone_payload["schedule"] = {"start_at": next_start_at.isoformat()}
        with self._use_task_destination(task.destination):
            self._store.log(entries=clone_payload, new=True)
        if self._num_tasks_cached is not None:
            self._num_tasks_cached += 1

    def _validate_scheduled_invariants(
        self,
        *,
        status: Status | str,
        schedule: ScheduleLike,
        trigger: TriggerLike = None,
        err_prefix: str = "Invalid task state:",
    ) -> None:
        """Validate the remaining scheduler invariants for task state."""

        if isinstance(status, Status):
            status_enum = status
        else:
            try:
                status_enum = Status(str(status))
            except Exception as exc:
                raise ValueError(f"{err_prefix} invalid status {status!r}.") from exc

        start_at = None
        if isinstance(schedule, Schedule):
            start_at = schedule.start_at
        elif isinstance(schedule, dict):
            start_at = schedule.get("start_at")

        if (
            status_enum == Status.scheduled
            and schedule is not None
            and start_at is None
        ):
            raise ValueError(
                f"{err_prefix} a task with status 'scheduled' must have a start_at timestamp.",
            )
        if status_enum == Status.triggerable and trigger is None:
            raise ValueError(
                f"{err_prefix} a task with status 'triggerable' must have a trigger.",
            )

    def _ensure_not_active_task(self, task_ids: Union[int, List[int]]) -> None:
        """Guard against mutating a task that currently has an active row."""

        ids = [task_ids] if isinstance(task_ids, int) else list(task_ids)
        ids = [int(task_id) for task_id in ids]
        if not ids:
            return
        active_rows = self._filter_tasks(
            filter=f"task_id in {ids} and status == 'active'",
            limit=1,
        )
        if active_rows:
            raise RuntimeError(
                f"Operation not permitted on the active task (task_id={active_rows[0].task_id})",
            )

    @overload
    def _get_logs_by_task_ids(
        self,
        *,
        task_ids: Union[int, List[int]],
        return_ids_only: Literal[True] = True,
    ) -> List[int]: ...

    @overload
    def _get_logs_by_task_ids(
        self,
        *,
        task_ids: Union[int, List[int]],
        return_ids_only: Literal[False],
    ) -> List[unisdk.Log]: ...

    def _get_logs_by_task_ids(
        self,
        *,
        task_ids: Union[int, List[int]],
        return_ids_only: bool = True,
    ):
        """Fetch log objects or ids for one or many logical task ids."""

        task_id_list = task_ids if isinstance(task_ids, list) else [task_ids]
        matches: list[unisdk.Log] = []
        for context_name in self._read_task_contexts():
            store = self._store_for_task_context(context_name)
            rows = store.get_logs_by_task_ids(
                task_ids=task_id_list,
                return_ids_only=False,
            )
            destination = self._destination_from_task_context(context_name)
            for row in rows:
                row.entries.setdefault("destination", destination)
                row.entries.setdefault(
                    "assistant_id",
                    SESSION_DETAILS.assistant_context,
                )
                matches.append(row)

        if isinstance(task_ids, int):
            root_destinations = {
                row.entries.get("destination") or PERSONAL_DESTINATION
                for row in matches
            }
            if len(root_destinations) > 1:
                raise ValueError(
                    f"Task id {task_ids} exists in multiple task roots; provide destination.",
                )

        if return_ids_only:
            return [int(row.id) for row in matches]
        return matches

    def _create_task(
        self,
        *,
        name: str,
        description: str,
        status: Optional[Status] = None,
        schedule: ScheduleLike = None,
        trigger: TriggerLike = None,
        deadline: Optional[Union[str, datetime]] = None,
        repeat: RepeatLike = None,
        priority: Priority = Priority.normal,
        response_policy: Optional[str] = None,
        entrypoint: Optional[int] = None,
        offline: bool = False,
        destination: str | None = None,
        _root_applied: bool = False,
    ) -> ToolOutcome:
        """Create a single task with the given name and description.

        Supports optional scheduling (start time, deadline, recurrence),
        event-based triggers, execution mode (agentic vs symbolic via
        ``entrypoint``), and background offline execution.  Returns a
        ``ToolOutcome`` containing the newly assigned ``task_id``.
        """

        if not _root_applied:
            effective_destination = (
                destination or os.environ.get("TASK_DESTINATION") or None
            )
            with self._use_task_destination(effective_destination):
                return self._create_task(
                    name=name,
                    description=description,
                    status=status,
                    schedule=schedule,
                    trigger=trigger,
                    deadline=deadline,
                    repeat=repeat,
                    priority=priority,
                    response_policy=response_policy,
                    entrypoint=entrypoint,
                    offline=offline,
                    destination=effective_destination,
                    _root_applied=True,
                )

        if not name or not description:
            raise ValueError("Both 'name' and 'description' are required")

        duplicate_rows = self._find_name_desc_collisions(
            name=name,
            description=description,
            limit=2,
        )
        if duplicate_rows:
            for row in duplicate_rows:
                if row.get("name") == name:
                    raise ValueError(
                        f"A task with {'name'!r} = {name!r} already exists",
                    )
                if row.get("description") == description:
                    raise ValueError(
                        f"A task with {'description'!r} = {description!r} already exists",
                    )

        explicit_status: Status | None = None
        if status is not None:
            if isinstance(status, Status):
                explicit_status = status
            else:
                try:
                    explicit_status = Status(str(status))
                except Exception as exc:
                    raise ValueError(f"Invalid status {status!r}.") from exc
            if explicit_status == Status.active:
                raise ValueError(
                    "Tasks cannot be created directly in the 'active' state.",
                )

        if schedule is not None and isinstance(schedule, dict):
            schedule = Schedule(**schedule)
        if trigger is not None and isinstance(trigger, dict):
            trigger = Trigger(**trigger)
        if repeat is not None:
            repeat = [
                RepeatPattern(**item) if isinstance(item, dict) else item
                for item in repeat
            ]
            repeat = normalize_repeat_patterns(repeat)

        if schedule is not None and trigger is not None:
            raise ValueError("`schedule` and `trigger` are mutually exclusive.")

        if trigger is not None:
            resolved_status = Status.triggerable
        elif schedule is not None and schedule.start_at is not None:
            resolved_status = Status.scheduled
        elif explicit_status is not None:
            resolved_status = explicit_status
        else:
            resolved_status = Status.scheduled

        self._validate_scheduled_invariants(
            status=resolved_status,
            schedule=schedule,
            trigger=trigger,
            err_prefix="While creating a task:",
        )

        task_details = TaskBase(
            assistant_id=SESSION_DETAILS.assistant_context,
            destination=(
                destination if destination not in (None, PERSONAL_DESTINATION) else None
            ),
            name=name,
            description=description,
            status=resolved_status,
            schedule=schedule,
            trigger=trigger,
            deadline=deadline,
            repeat=repeat,
            priority=priority,
            response_policy=response_policy,
            entrypoint=entrypoint,
            offline=offline,
        ).to_post_json()

        log = self._store.log(entries=task_details, new=True)
        if self._num_tasks_cached is not None:
            self._num_tasks_cached += 1

        return {
            "outcome": "task created successfully",
            "details": {"task_id": int(log.entries["task_id"])},
        }

    def _create_tasks(
        self,
        *,
        tasks: List[Dict[str, Any]],
        destination: str | None = None,
        _root_applied: bool = False,
    ) -> ToolOutcome:
        """Create multiple tasks in the given order and return their IDs.

        Accepts a list of task definitions; each entry follows the same schema
        as ``_create_task``.  Tasks are written in list order and their
        assigned ``task_id`` values are returned.  Destination routing is
        applied uniformly to all created tasks.
        """

        if not _root_applied:
            effective_destination = (
                destination or os.environ.get("TASK_DESTINATION") or None
            )
            with self._use_task_destination(effective_destination):
                return self._create_tasks(
                    tasks=tasks,
                    destination=effective_destination,
                    _root_applied=True,
                )

        if not tasks:
            return {"outcome": "tasks created", "details": {"task_ids": []}}

        seen_names: set[str] = set()
        seen_descs: set[str] = set()
        created_ids: List[int] = []
        for index, spec in enumerate(tasks):
            name = spec.get("name")
            description = spec.get("description")
            if not name or not description:
                raise ValueError(
                    f"Each task spec must include non-empty 'name' and 'description' (index={index}).",
                )
            if name in seen_names:
                raise ValueError(
                    f"Duplicate task name in batch: {name!r} (index={index})",
                )
            if description in seen_descs:
                raise ValueError(
                    "Duplicate task description in batch – descriptions must be unique: "
                    f"{description!r} (index={index})",
                )
            seen_names.add(str(name))
            seen_descs.add(str(description))

            payload: Dict[str, Any] = {}
            for key in (
                "name",
                "description",
                "status",
                "schedule",
                "trigger",
                "deadline",
                "repeat",
                "priority",
                "response_policy",
                "entrypoint",
                "offline",
            ):
                if key in spec:
                    payload[key] = spec[key]

            out = self._create_task(
                **payload,
                destination=destination,
                _root_applied=True,
            )
            created_ids.append(int(out["details"]["task_id"]))

        return {
            "outcome": "tasks created",
            "details": {"task_ids": created_ids},
        }

    def _delete_task(
        self,
        *,
        task_id: int,
        destination: str | None = None,
        _root_applied: bool = False,
    ) -> ToolOutcome:
        """Permanently delete all rows for the given task id.

        Removes every instance row (all recurrence clones included) stored
        under the provided ``task_id``.  Raises if the task is currently
        active.  This action is irreversible.
        """

        if not _root_applied:
            resolved_destination = (
                destination or os.environ.get("TASK_DESTINATION") or None
            )
            if resolved_destination is None:
                resolved_destination = (
                    self._get_task_or_raise(task_id).destination or PERSONAL_DESTINATION
                )
            with self._use_task_destination(resolved_destination):
                return self._delete_task(
                    task_id=task_id,
                    destination=resolved_destination,
                    _root_applied=True,
                )

        self._ensure_not_active_task(task_id)
        log_ids = self._store.get_rows(
            filter=f"task_id == {task_id}",
            return_ids_only=True,
        )
        self._store.delete(logs=log_ids)
        removed_count = len(log_ids)
        if self._num_tasks_cached is not None and removed_count:
            self._num_tasks_cached = max(
                0,
                int(self._num_tasks_cached) - int(removed_count),
            )
        return {
            "outcome": "task deleted",
            "details": {"task_id": task_id},
        }

    def _cancel_tasks(self, task_ids: List[int]) -> ToolOutcome:
        """Cancel one or more tasks by id, marking them as cancelled.

        Raises if any requested task is currently active (running).  Raises
        if any task id is already completed.  All other pending instances of
        a recurring task are cancelled together.
        """

        requested_task_ids = list(dict.fromkeys(int(task_id) for task_id in task_ids))
        self._ensure_not_active_task(requested_task_ids)

        missing: list[int] = []
        for task_id in requested_task_ids:
            task = self._get_task_or_raise(task_id)
            with self._use_task_destination(task.destination):
                logs = self._store.get_rows(
                    filter=f"task_id == {task_id}",
                    return_ids_only=False,
                )
                if not logs:
                    missing.append(task_id)
                    continue
                if any(
                    str(log.entries.get("status")) == Status.completed.value
                    for log in logs
                ):
                    raise ValueError(
                        f"Cannot cancel completed task (id={task_id}).",
                    )
                self._write_log_entries(
                    logs=[int(log.id) for log in logs],
                    entries={"status": Status.cancelled},
                )

        if missing:
            raise ValueError(f"No matching task_ids resolved: {missing}")
        return {
            "outcome": "tasks cancelled",
            "details": {"task_ids": requested_task_ids},
        }

    def _update_task_status(
        self,
        *,
        task_ids: Union[int, List[int]],
        new_status: Status,
    ) -> Dict[str, str]:
        """Change the lifecycle status of one or many tasks."""

        ids = [task_ids] if isinstance(task_ids, int) else list(task_ids)
        ids = [int(task_id) for task_id in ids]
        if not ids:
            return {"detail": "No updates"}

        if new_status == Status.active:
            raise ValueError(
                "Direct status changes to 'active' are not allowed; use the dedicated activation method.",
            )
        self._ensure_not_active_task(ids)

        last_result: Dict[str, str] = {"detail": "No updates"}
        for task_id in ids:
            task = self._get_task_or_raise(task_id)
            self._validate_scheduled_invariants(
                status=new_status,
                schedule=task.schedule,
                trigger=task.trigger,
                err_prefix=f"While changing status of task {task.task_id}:",
            )
            with self._use_task_destination(task.destination):
                log_ids = self._store.get_rows(
                    filter=f"task_id == {task_id}",
                    return_ids_only=True,
                )
                last_result = self._write_log_entries(
                    logs=log_ids,
                    entries={"status": new_status},
                )
        return last_result

    def _update_task(
        self,
        *,
        task_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
        status: Optional[Union[Status, str]] = None,
        start_at: Optional[Union[str, datetime]] = None,
        deadline: Optional[Union[str, datetime]] = None,
        repeat: Optional[List[Union[RepeatPattern, Dict[str, Any]]]] = None,
        priority: Optional[Union[Priority, str]] = None,
        trigger: Any = _UNSET,
        entrypoint: Any = _UNSET,
        offline: Any = _UNSET,
        destination: str | None = None,
        _root_applied: bool = False,
    ) -> Dict[str, Any]:
        """Update mutable fields on an existing task.

        Accepts any subset of a task's mutable attributes (name, description,
        schedule, deadline, repeat, priority, trigger, entrypoint, offline
        flag).  Only the fields that are explicitly provided are changed;
        omitted fields keep their current values.
        """

        if not _root_applied:
            resolved_destination = (
                destination or os.environ.get("TASK_DESTINATION") or None
            )
            if resolved_destination is None:
                resolved_destination = (
                    self._get_task_or_raise(task_id).destination or PERSONAL_DESTINATION
                )
            with self._use_task_destination(resolved_destination):
                return self._update_task(
                    task_id=task_id,
                    name=name,
                    description=description,
                    status=status,
                    start_at=start_at,
                    deadline=deadline,
                    repeat=repeat,
                    priority=priority,
                    trigger=trigger,
                    entrypoint=entrypoint,
                    offline=offline,
                    destination=resolved_destination,
                    _root_applied=True,
                )

        self._ensure_not_active_task(task_id)

        trigger_provided = trigger is not _UNSET
        offline_provided = offline is not _UNSET
        task = self._get_task_or_raise(task_id)

        if (
            name is None
            and description is None
            and status is None
            and start_at is None
            and deadline is None
            and repeat is None
            and priority is None
            and not trigger_provided
            and entrypoint is _UNSET
            and not offline_provided
        ):
            raise ValueError("At least one field must be provided for an update.")

        if trigger_provided and trigger is not None and task.schedule is not None:
            raise ValueError(
                "Cannot add a trigger while a schedule exists. Remove schedule first.",
            )

        if isinstance(start_at, datetime):
            start_at = start_at.isoformat()
        if isinstance(deadline, datetime):
            deadline = deadline.isoformat()

        schedule_payload: Optional[Dict[str, Any]] = None
        if start_at is not None:
            if task.trigger is not None and not (trigger_provided and trigger is None):
                raise ValueError(
                    "Cannot add or update start_at while the task is trigger-based.",
                )
            schedule_payload = {"start_at": start_at}

        desired_status: Optional[Status] = None
        if status is not None:
            if isinstance(status, Status):
                status_enum = status
            else:
                try:
                    status_enum = Status(str(status))
                except Exception as exc:
                    raise ValueError(f"Invalid status {status!r}.") from exc
            if status_enum == Status.active:
                raise ValueError(
                    "Direct status changes to 'active' are not allowed; use the execution method.",
                )
            desired_status = status_enum
        elif trigger_provided and trigger is not None:
            desired_status = Status.triggerable
        elif (
            schedule_payload is not None
            and schedule_payload.get("start_at") is not None
        ):
            desired_status = Status.scheduled
        elif trigger_provided and trigger is None and task.status == Status.triggerable:
            desired_status = Status.scheduled

        prospective_trigger: TriggerLike
        if not trigger_provided:
            prospective_trigger = task.trigger
        elif trigger is None:
            prospective_trigger = None
        elif isinstance(trigger, dict):
            prospective_trigger = Trigger(**trigger)
        else:
            prospective_trigger = trigger

        prospective_schedule: ScheduleLike = (
            schedule_payload if schedule_payload is not None else task.schedule
        )
        if prospective_schedule is not None and prospective_trigger is not None:
            raise ValueError("A task cannot have both a schedule and a trigger.")

        if (
            desired_status is not None
            or schedule_payload is not None
            or trigger_provided
        ):
            self._validate_scheduled_invariants(
                status=desired_status if desired_status is not None else task.status,
                schedule=prospective_schedule,
                trigger=prospective_trigger,
                err_prefix=f"While updating task {task_id}:",
            )

        entries: Dict[str, Any] = {}
        if name is not None:
            entries["name"] = name
        if description is not None:
            entries["description"] = description
        if deadline is not None:
            entries["deadline"] = deadline
        if repeat is not None:
            normalized_repeat = normalize_repeat_patterns(
                [
                    RepeatPattern(**item) if isinstance(item, dict) else item
                    for item in repeat
                ],
            )
            entries["repeat"] = [
                (
                    item.model_dump(mode="json")
                    if isinstance(item, RepeatPattern)
                    else item
                )
                for item in normalized_repeat or []
            ]
        if priority is not None:
            if isinstance(priority, Priority):
                entries["priority"] = priority
            else:
                try:
                    entries["priority"] = Priority(str(priority))
                except Exception as exc:
                    raise ValueError(f"Invalid priority {priority!r}.") from exc
        if trigger_provided:
            if prospective_trigger is None:
                entries["trigger"] = None
            elif isinstance(prospective_trigger, Trigger):
                entries["trigger"] = prospective_trigger.model_dump(mode="json")
            else:
                entries["trigger"] = prospective_trigger
        if schedule_payload is not None:
            entries["schedule"] = schedule_payload
        if desired_status is not None:
            entries["status"] = desired_status
        if entrypoint is not _UNSET:
            if entrypoint is None:
                entries["entrypoint"] = None
            else:
                try:
                    entries["entrypoint"] = int(entrypoint)
                except Exception as exc:
                    raise ValueError("entrypoint must be an integer or None") from exc
        if offline_provided:
            if isinstance(offline, str):
                normalized_offline = offline.strip().lower()
                if normalized_offline in {"true", "1"}:
                    offline = True
                elif normalized_offline in {"false", "0"}:
                    offline = False
                else:
                    raise ValueError("offline must be a boolean value")
            else:
                offline = bool(offline)
            entries["offline"] = offline

        log_ids = self._store.get_rows(
            filter=f"task_id == {task_id}",
            return_ids_only=True,
        )
        return self._write_log_entries(logs=log_ids, entries=entries)

    def _update_task_instance(
        self,
        *,
        task_id: int,
        instance_id: int,
        **kwargs: Any,
    ) -> Dict[str, str]:
        """Update supported fields on one concrete task instance."""

        task = self._get_task_or_raise(task_id)
        with self._use_task_destination(task.destination):
            log_objs = self._store.get_rows(
                filter=f"task_id == {task_id} and instance_id == {instance_id}",
                limit=1,
                return_ids_only=False,
            )
            if not log_objs:
                raise ValueError(
                    f"No task instance found for task_id={task_id}, instance_id={instance_id}",
                )

            log_to_update = log_objs[0]
            current_row = dict(log_to_update.entries or {})
            if to_status(current_row.get("status")) == Status.active:
                return {
                    "outcome": "skipped",
                    "reason": "Cannot update active task instance directly",
                }

            entries_to_write: Dict[str, Any] = {}
            current_schedule = current_row.get("schedule")
            current_trigger = current_row.get("trigger")

            if "status" in kwargs:
                raw_status = kwargs["status"]
                if isinstance(raw_status, Status):
                    new_status = raw_status
                else:
                    try:
                        new_status = Status(str(raw_status))
                    except Exception as exc:
                        raise ValueError(f"Invalid status {raw_status!r}.") from exc
                if new_status == Status.active:
                    raise ValueError(
                        "Direct status changes to 'active' are not allowed.",
                    )
                self._validate_scheduled_invariants(
                    status=new_status,
                    schedule=current_schedule,
                    trigger=current_trigger,
                    err_prefix=f"While updating instance {task_id}.{instance_id}:",
                )
                entries_to_write["status"] = new_status

            if "info" in kwargs:
                entries_to_write["info"] = kwargs["info"]

            if not entries_to_write:
                return {
                    "outcome": "no changes",
                    "details": {"task_id": task_id, "instance_id": instance_id},
                }

            return self._write_log_entries(
                logs=log_to_update.id,
                entries=entries_to_write,
            )

    @staticmethod
    def _default_ask_tool_policy(
        step_index: int,
        current_tools: Dict[str, Any],
    ) -> tuple[str, Dict[str, Any]]:
        """Require search_tasks on the first step when configured."""

        if (
            SETTINGS.FIRST_ASK_TOOL_IS_SEARCH
            and step_index < 1
            and "search_tasks" in current_tools
        ):
            return ("required", {"search_tasks": current_tools["search_tasks"]})
        return ("auto", current_tools)

    @staticmethod
    def _default_update_tool_policy(
        step_index: int,
        current_tools: Dict[str, Any],
    ) -> tuple[str, Dict[str, Any]]:
        """Require ask on the first step when configured."""

        if (
            SETTINGS.FIRST_MUTATION_TOOL_IS_ASK
            and step_index < 1
            and "ask" in current_tools
        ):
            return ("required", {"ask": current_tools["ask"]})
        return ("auto", current_tools)

    def _write_log_entries(
        self,
        *,
        logs: Union[int, unisdk.Log, List[Union[int, unisdk.Log]]],
        entries: Union[Dict[str, Any], List[Dict[str, Any]]],
    ) -> Dict[str, str]:
        """Centralize task-row writes through the current store."""

        return self._store.update(logs=logs, entries=entries)

    def _start_loop(
        self,
        client: unillm.AsyncUnify,
        text: str,
        tools: ToolsDict,
        *,
        loop_id: str,
        parent_chat_context: Optional[List[Dict[str, Any]]] = None,
        log_steps: bool = True,
        tool_policy: Optional[
            Union[
                Literal["default"],
                Callable[[int, Dict[str, Any]], tuple[str, Dict[str, Any]]],
            ]
        ] = None,
        handle_cls: Optional[type[SteerableToolHandle]] = None,
        response_format: Optional[Type[BaseModel]] = None,
        clarification_queues: Optional[Tuple[asyncio.Queue, asyncio.Queue]] = None,
    ) -> SteerableToolHandle:
        """Centralized wrapper around start_async_tool_loop."""

        return start_async_tool_loop(
            client,
            text,
            tools,
            loop_id=loop_id,
            parent_lineage=TOOL_LOOP_LINEAGE.get([]),
            parent_chat_context=parent_chat_context,
            log_steps=log_steps,
            tool_policy=tool_policy,
            handle_cls=handle_cls,
            response_format=response_format,
            clarification_queues=clarification_queues,
        )

    def _wrap_result_with_messages(
        self,
        handle: SteerableToolHandle,
        client: unillm.AsyncUnify,
    ) -> SteerableToolHandle:
        """Wrap handle.result so it also returns client messages."""

        original_result = handle.result

        async def wrapped_result():
            answer = await original_result()
            return answer, client.messages

        handle.result = wrapped_result  # type: ignore[assignment]
        return handle

    def _get_task_or_raise(self, task_id: int) -> Task:
        """Fetch exactly one task id or raise when it is missing or ambiguous."""

        tasks = self._filter_tasks(filter=f"task_id == {task_id}", limit=1000)
        if not tasks:
            raise ValueError(f"No task found with id={task_id}")
        destinations = {task.destination or PERSONAL_DESTINATION for task in tasks}
        if len(destinations) > 1:
            raise ValueError(
                f"Task id {task_id} exists in multiple task roots; provide destination.",
            )
        return tasks[0]

    def _search_tasks(
        self,
        *,
        references: Optional[Dict[str, str]] = None,
        k: int = 10,
    ) -> List[Task]:
        """Run semantic search across all tasks and return the closest matches.

        Uses vector similarity to find tasks whose name or description is
        semantically close to the provided references.  Optionally limits
        results to at most ``k`` rows.  Returns an empty list when no tasks
        are stored or when the query produces no meaningful matches.
        """

        allowed_fields: List[str] = [
            "task_id",
            "instance_id",
            "name",
            "description",
            "status",
            "priority",
            "schedule",
            "deadline",
        ]
        filled = table_search_top_k(
            self._ctx,
            references,
            k=k,
            allowed_fields=allowed_fields,
            row_filter=None,
            unique_id_field="task_id",
        )
        return [Task(**lg) for lg in filled]

    def _filter_tasks(
        self,
        *,
        filter: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> List[Task]:
        """Filter tasks using a boolean expression over task fields.

        Returns all task rows that match the given filter expression.
        The expression uses field names from the task schema (e.g.
        ``status == 'scheduled'``, ``task_id == 42``).  Returns an empty
        list when no rows match.
        """

        normalized_filter = normalize_filter_expr(filter)
        include_fields = list(Task.model_fields.keys())

        rows: list[dict[str, Any]] = []
        for context_name in self._read_task_contexts():
            store = self._store_for_task_context(context_name)
            destination = self._destination_from_task_context(context_name)
            root_logs = store.get_rows(
                filter=normalized_filter,
                offset=0,
                limit=max(limit + offset, 1000 if limit >= 1000 else limit),
                return_ids_only=False,
                include_fields=include_fields,
            )
            for log in root_logs:
                row = dict(log.entries or {})
                row.setdefault("assistant_id", SESSION_DETAILS.assistant_context)
                row["destination"] = destination
                rows.append(row)
        rows = rows[offset : offset + limit]

        def _rehydrate_repeat(item: dict) -> dict:
            if not isinstance(item, dict):
                return item
            out = dict(item)
            freq = out.get("frequency")
            if isinstance(freq, str):
                token = freq.split(".")[-1] if "." in freq else freq
                try:
                    out["frequency"] = Frequency[token]
                except Exception:
                    try:
                        out["frequency"] = Frequency(token)
                    except Exception:
                        pass

            weekdays = out.get("weekdays")
            if isinstance(weekdays, list):
                new_weekdays = []
                for weekday in weekdays:
                    if isinstance(weekday, str):
                        token = weekday.split(".")[-1] if "." in weekday else weekday
                        try:
                            new_weekdays.append(Weekday[token])
                        except Exception:
                            try:
                                new_weekdays.append(Weekday(token))
                            except Exception:
                                new_weekdays.append(weekday)
                    else:
                        new_weekdays.append(weekday)
                out["weekdays"] = new_weekdays

            for optional_key in ("count", "until", "time_of_day"):
                if optional_key not in out:
                    out[optional_key] = None
            return out

        hydrated: list[Task] = []
        for row in rows:
            repeat = row.get("repeat")
            if isinstance(repeat, list):
                row["repeat"] = [_rehydrate_repeat(item) for item in repeat]
            sanitized = self._sanitize_activation(row)
            hydrated.append(Task(**sanitized))
        return hydrated

    def _get_columns(self) -> Dict[str, str]:
        """Return the tasks-table schema for the current context."""

        return self._store.fields

    def _list_columns(
        self,
        *,
        include_types: bool = True,
    ) -> Dict[str, str] | list[str]:
        """Return available task columns, optionally with types."""

        cols = self._get_columns()
        return cols if include_types else list(cols)

    def _num_tasks(self) -> int:
        """Return the total number of rows in the current Tasks context."""

        if self._num_tasks_cached is None:
            try:
                self._num_tasks_cached = int(
                    self._store.get_metric_count(key="task_id"),
                )
            except Exception:
                self._num_tasks_cached = 0
        return int(self._num_tasks_cached)

    @read_only
    def _reduce(
        self,
        *,
        metric: str,
        keys: str | list[str],
        filter: Optional[str | dict[str, str]] = None,
        group_by: Optional[str | list[str]] = None,
    ) -> Any:
        """Compute aggregate metrics over the current task list.

        Supports count, sum, mean, min, max, and other standard reductions
        grouped by one or more task fields (e.g. ``status``, ``priority``).
        Returns a dictionary of group keys to metric values.
        """

        return reduce_logs(
            context=self._ctx,
            metric=metric,
            keys=keys,
            filter=filter,
            group_by=group_by,
        )

    def _find_name_desc_collisions(
        self,
        *,
        name: str,
        description: str,
        limit: int = 2,
    ) -> List[Dict[str, Any]]:
        """Return existing rows that collide on name or description."""

        try:
            logs = self._store.get_rows(
                filter=f"name == {name!r} or description == {description!r}",
                limit=limit,
                return_ids_only=False,
            )
        except Exception:
            return []
        return [dict(log.entries or {}) for log in logs]

    @overload
    def _sanitize_activation(self, task: Dict[str, Any]) -> Dict[str, Any]: ...

    @overload
    def _sanitize_activation(self, task: Task) -> Task: ...

    def _sanitize_activation(
        self,
        task: Union[Dict[str, Any], Task],
    ) -> Union[Dict[str, Any], Task]:
        """Drop activated_by unless the row is currently active."""

        if isinstance(task, Task):
            if task.status != Status.active:
                task.activated_by = None
            return task
        try:
            if to_status(task.get("status")) != Status.active:  # type: ignore[arg-type]
                task.pop("activated_by", None)
        except Exception:
            if str(task.get("status")) != str(Status.active):
                task.pop("activated_by", None)
        return task

    def _meta_context_for_destination(self, destination: str | None) -> str:
        """Resolve a public destination into one concrete Tasks/Meta context."""
        root_context = ContextRegistry.write_root(
            self,
            TASKS_META_TABLE,
            destination=destination,
        )
        return f"{root_context.strip('/')}/{TASKS_META_TABLE}"

    @contextmanager
    def _temporary_tasks_meta_context(self, context: str):
        """Temporarily bind task meta reads/writes to a resolved context."""
        with self._destination_context_lock:
            original = self._meta_ctx
            self._meta_ctx = context
            try:
                yield
            finally:
                self._meta_ctx = original

    def _sync_destination_contexts(
        self,
        destination: str | None,
    ) -> tuple[str, str, bool]:
        """Return destination-scoped tasks context, meta context, and personal flag."""
        data_context = self._task_context_for_destination(destination)
        meta_context = self._meta_context_for_destination(destination)
        return data_context, meta_context, destination in (None, "personal")

    def _get_stored_custom_tasks_hash(self) -> str:
        try:
            logs = unisdk.get_logs(
                context=self._meta_ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                return logs[0].entries.get("custom_tasks_hash", "") or ""
        except Exception as exc:
            logger.warning("Failed to read custom tasks hash: %s", exc)
        return ""

    def _store_custom_tasks_hash(self, hash_value: str) -> None:
        try:
            logs = unisdk.get_logs(
                context=self._meta_ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                unisdk.update_logs(
                    context=self._meta_ctx,
                    logs=[logs[0].id],
                    entries={"custom_tasks_hash": hash_value},
                    overwrite=True,
                )
            else:
                unity_create_logs(
                    context=self._meta_ctx,
                    entries=[{"meta_id": 1, "custom_tasks_hash": hash_value}],
                    stamp_authoring=True,
                )
        except Exception as exc:
            logger.warning("Failed to store custom tasks hash: %s", exc)

    def _get_custom_tasks_from_db(self) -> Dict[str, Dict[str, Any]]:
        logs = unisdk.get_logs(
            context=self._ctx,
            filter="custom_hash != None and instance_id == 0",
            limit=1000,
            exclude_fields=list_private_fields(self._ctx),
        )
        return {
            lg.entries.get("custom_key"): lg.entries
            for lg in logs
            if lg.entries.get("custom_key")
        }

    def _delete_custom_task_by_key(self, custom_key: str) -> bool:
        logs = unisdk.get_logs(
            context=self._ctx,
            filter=(
                f"custom_key == '{custom_key}' and custom_hash != None "
                "and instance_id == 0"
            ),
            limit=1,
        )
        if not logs:
            return False
        task_id = int(logs[0].entries["task_id"])
        try:
            self._delete_task(task_id=task_id, _root_applied=True)
        except RuntimeError:
            logger.warning(
                "Skipping delete for active custom task key=%s task_id=%s",
                custom_key,
                task_id,
            )
            return False
        return True

    def _insert_custom_task(
        self,
        data: Dict[str, Any],
        *,
        function_name_to_id: Dict[str, int],
    ) -> int:
        payload = dict(data)
        custom_key = payload.pop("custom_key")
        custom_hash = payload.pop("custom_hash")
        destination = payload.pop("destination", None)
        entrypoint_function = payload.pop("entrypoint_function", None)
        schedule = payload.pop("schedule", None)
        trigger = payload.pop("trigger", None)
        deadline = payload.pop("deadline", None)
        repeat = payload.pop("repeat", None)
        priority = payload.pop("priority", Priority.normal)
        response_policy = payload.pop("response_policy", None)
        offline = bool(payload.pop("offline", False))
        name = payload.pop("name")
        description = payload.pop("description")

        entrypoint = None
        if entrypoint_function:
            entrypoint = function_name_to_id.get(entrypoint_function)
            if entrypoint is None:
                logger.warning(
                    "Could not resolve entrypoint_function=%s for task key=%s",
                    entrypoint_function,
                    custom_key,
                )

        destination_arg = None if destination in (None, "personal") else destination
        with self._use_task_destination(destination_arg):
            result = self._create_task(
                name=name,
                description=description,
                schedule=schedule,
                trigger=trigger,
                deadline=deadline,
                repeat=repeat,
                priority=priority,
                response_policy=response_policy,
                entrypoint=entrypoint,
                offline=offline,
                destination=destination_arg,
                _root_applied=True,
            )
            task_id = int(result["details"]["task_id"])
            log_ids = self._store.get_rows(
                filter=f"task_id == {task_id}",
                return_ids_only=True,
            )
            self._write_log_entries(
                logs=log_ids,
                entries={
                    "custom_key": custom_key,
                    "custom_hash": custom_hash,
                },
            )
            return task_id

    def _update_custom_task(
        self,
        *,
        task_id: int,
        data: Dict[str, Any],
        function_name_to_id: Dict[str, int],
        current_status: Status,
    ) -> None:
        payload = dict(data)
        custom_key = payload.pop("custom_key")
        custom_hash = payload.pop("custom_hash")
        payload.pop("destination", None)
        entrypoint_function = payload.pop("entrypoint_function", None)
        schedule = payload.pop("schedule", None)
        trigger = payload.pop("trigger", None)
        deadline = payload.pop("deadline", None)
        repeat = payload.pop("repeat", None)
        priority = payload.pop("priority", None)
        response_policy = payload.pop("response_policy", None)
        offline = payload.pop("offline", None)
        name = payload.pop("name", None)
        description = payload.pop("description", None)

        entrypoint: Any = _UNSET
        if entrypoint_function is not None:
            if entrypoint_function == "":
                entrypoint = None
            else:
                resolved = function_name_to_id.get(entrypoint_function)
                if resolved is None:
                    logger.warning(
                        "Could not resolve entrypoint_function=%s for task key=%s",
                        entrypoint_function,
                        custom_key,
                    )
                else:
                    entrypoint = resolved

        desired_status = None
        if current_status in (Status.scheduled, Status.triggerable):
            desired_status = derive_initial_task_status(
                schedule=schedule,
                trigger=trigger,
            )

        self._ensure_not_active_task(task_id)

        if schedule is not None and trigger is not None:
            raise ValueError("A task cannot have both a schedule and a trigger.")

        if desired_status is not None:
            self._validate_scheduled_invariants(
                status=desired_status,
                schedule=schedule,
                trigger=trigger,
                err_prefix=f"While updating custom task {task_id}:",
            )

        entries: Dict[str, Any] = {
            "custom_key": custom_key,
            "custom_hash": custom_hash,
            "name": name,
            "description": description,
            "schedule": schedule,
            "trigger": trigger,
            "deadline": deadline,
            "response_policy": response_policy,
            "offline": bool(offline) if offline is not None else None,
        }
        if repeat is not None:
            normalized_repeat = normalize_repeat_patterns(
                [
                    RepeatPattern(**item) if isinstance(item, dict) else item
                    for item in repeat
                ],
            )
            entries["repeat"] = [
                (
                    item.model_dump(mode="json")
                    if isinstance(item, RepeatPattern)
                    else item
                )
                for item in normalized_repeat or []
            ]
        if priority is not None:
            entries["priority"] = (
                priority if isinstance(priority, Priority) else Priority(str(priority))
            )
        if entrypoint is not _UNSET:
            entries["entrypoint"] = entrypoint
        if desired_status is not None:
            entries["status"] = desired_status

        entries = {key: value for key, value in entries.items() if value is not _UNSET}

        log_ids = self._store.get_rows(
            filter=f"task_id == {task_id}",
            return_ids_only=True,
        )
        self._write_log_entries(logs=log_ids, entries=entries)

    def sync_custom_tasks(
        self,
        *,
        source_tasks: Optional[Dict[str, Dict[str, Any]]] = None,
        function_name_to_id: Optional[Dict[str, int]] = None,
        destination: str | None = None,
    ) -> bool:
        """Ensure custom task rows match source ``tasks.jsonl`` definitions."""
        try:
            tasks_context, meta_context, is_personal = self._sync_destination_contexts(
                destination,
            )
        except ToolErrorException as exc:
            return exc.payload  # type: ignore[return-value]

        previous_context = self._ctx
        previous_store = self._store
        previous_active_root = self._active_task_root_context
        self._ctx = tasks_context
        self._store = self._store_for_task_context(tasks_context)
        self._active_task_root_context = tasks_context

        with self._temporary_tasks_meta_context(meta_context):
            if source_tasks is None:
                source_tasks = {}
            expected_hash = compute_custom_tasks_hash(source_tasks=source_tasks)
            current_hash = self._get_stored_custom_tasks_hash()
            already_synced = (
                self._custom_tasks_synced
                if is_personal
                else tasks_context in self._custom_tasks_synced_contexts
            )

            if already_synced and current_hash == expected_hash:
                self._ctx = previous_context
                self._store = previous_store
                self._active_task_root_context = previous_active_root
                return False

            if current_hash == expected_hash:
                logger.debug("Custom tasks hash matches, skipping sync")
                if is_personal:
                    self._custom_tasks_synced = True
                else:
                    self._custom_tasks_synced_contexts.add(tasks_context)
                self._ctx = previous_context
                self._store = previous_store
                self._active_task_root_context = previous_active_root
                return False

            logger.info(
                "Custom tasks hash mismatch (current=%s, expected=%s), syncing...",
                current_hash,
                expected_hash,
            )

            function_name_to_id = function_name_to_id or {}
            db_tasks = self._get_custom_tasks_from_db()
            processed_keys: set[str] = set()

            for custom_key, source_data in source_tasks.items():
                processed_keys.add(custom_key)
                task_data = dict(source_data)
                destination_value = task_data.get("destination") or "personal"
                destination_arg = (
                    None
                    if destination_value in (None, "personal")
                    else destination_value
                )

                if custom_key in db_tasks:
                    db_entry = db_tasks[custom_key]
                    if db_entry.get("custom_hash") == task_data.get("custom_hash"):
                        logger.debug("Custom task unchanged: %s", custom_key)
                        continue
                    task_id = int(db_entry["task_id"])
                    current_status = to_status(db_entry.get("status"))
                    if current_status == Status.active:
                        logger.warning(
                            "Skipping update for active custom task key=%s task_id=%s",
                            custom_key,
                            task_id,
                        )
                        continue
                    logger.info("Updating custom task: %s", custom_key)
                    try:
                        self._update_custom_task(
                            task_id=task_id,
                            data=task_data,
                            function_name_to_id=function_name_to_id,
                            current_status=current_status,
                        )
                    except RuntimeError:
                        logger.warning(
                            "Skipping update for active custom task key=%s task_id=%s",
                            custom_key,
                            task_id,
                        )
                else:
                    existing = unisdk.get_logs(
                        context=self._ctx,
                        filter=f"custom_key == '{custom_key}' and instance_id == 0",
                        limit=1,
                    )
                    if existing:
                        logger.info(
                            "Overwriting user-added task with custom definition: %s",
                            custom_key,
                        )
                        task_id = int(existing[0].entries["task_id"])
                        try:
                            self._delete_task(task_id=task_id, _root_applied=True)
                        except RuntimeError:
                            logger.warning(
                                "Skipping adopt for active task key=%s task_id=%s",
                                custom_key,
                                task_id,
                            )
                            continue

                    logger.info("Inserting custom task: %s", custom_key)
                    self._insert_custom_task(
                        task_data,
                        function_name_to_id=function_name_to_id,
                    )

            for custom_key in db_tasks:
                if custom_key not in processed_keys:
                    logger.info("Deleting removed custom task: %s", custom_key)
                    self._delete_custom_task_by_key(custom_key)

            self._store_custom_tasks_hash(expected_hash)
            if is_personal:
                self._custom_tasks_synced = True
            else:
                self._custom_tasks_synced_contexts.add(tasks_context)

        self._ctx = previous_context
        self._store = previous_store
        self._active_task_root_context = previous_active_root
        return True

    def sync_custom(
        self,
        *,
        source_tasks: Optional[Dict[str, Dict[str, Any]]] = None,
        function_name_to_id: Optional[Dict[str, int]] = None,
    ) -> bool:
        """Sync custom tasks from pre-collected sources across destinations."""
        if source_tasks is None:
            source_tasks = {}

        by_destination: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for custom_key, source_data in source_tasks.items():
            destination = source_data.get("destination") or "personal"
            by_destination.setdefault(destination, {})[custom_key] = source_data

        changed = False
        for destination, group in by_destination.items():
            destination_arg = None if destination == "personal" else destination
            changed |= self.sync_custom_tasks(
                source_tasks=group,
                function_name_to_id=function_name_to_id,
                destination=destination_arg,
            )
        return changed
