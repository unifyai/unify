"""
ActiveTask provides a handle for a single running task backed by an actor.

It wraps a SteerableToolHandle returned by a BaseActor and, when a scheduler
is provided, mirrors lifecycle status to the task row on completion or stop.
It supports read-only ask, steering via interject (cancel intent only), stopping,
and result retrieval.
"""

import functools
import asyncio
import textwrap
from datetime import datetime, timezone
from typing import Optional, Dict, TYPE_CHECKING, List, Any

from .base import BaseActiveTask
from ..actor.base import BaseActor
from unify.common.async_tool_loop import SteerableToolHandle
from unify.common.task_execution_context import (
    PostRunReviewContext,
    current_post_run_review_context,
    current_task_execution_delegate,
)
from unify.common._async_tool.messages import forward_handle_call
from unify.events.task_run_lineage import (
    push_task_run_lineage,
    reset_task_run_lineage,
)
from .machine_state import (
    TaskRunProvenance,
    TaskRunReference,
    build_task_run_key,
    create_or_adopt_live_task_run,
    update_task_run_record,
)
from .types.status import Status
from ..common.llm_client import new_llm_client
import logging
from ..common.handle_wrappers import HandleWrapperMixin

logger = logging.getLogger(__name__)
_TASK_RUN_SUMMARY_LIMIT = 4000


def _resolve_active_task_run_key(
    *,
    task_run_reference: TaskRunReference | None,
    task_run_provenance: TaskRunProvenance | None,
) -> str | None:
    """Return the durable Tasks/Executions join key for EventBus lineage."""

    if task_run_reference is not None and task_run_reference.run_key:
        return str(task_run_reference.run_key)
    if task_run_provenance is not None:
        return str(build_task_run_key(task_run_provenance))
    return None


def _now_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 format."""

    return datetime.now(timezone.utc).isoformat()


def _truncate_task_run_text(value: str, limit: int = _TASK_RUN_SUMMARY_LIMIT) -> str:
    """Keep persisted run diagnostics compact for observability rows."""

    if len(value) <= limit:
        return value
    return f"{value[: limit - 3]}..."


async def classify_steering_intent(
    message: str,
    parent_chat_context: Optional[List[Dict[str, Any]]] = None,  # type: ignore[name-defined]
) -> tuple[str, str]:
    """Classify steering into: cancel | continue | none."""
    try:
        client = new_llm_client(origin="ActiveTask.classify_steering_intent")
        system = (
            "You are a router that classifies an in-flight steering message.\n"
            "Labels: cancel | continue | none.\n"
            "Definitions:\n"
            "- cancel: abandon/kill/drop the task (terminal).\n"
            "- continue: keep going (no change).\n"
            "- none: message is not a steering instruction.\n"
            'Output ONLY JSON with rationale first: {"rationale": <short string>, "action": <label>, "reason": <short substring or null>}'
        )
        client.set_system_message(system)

        def _format_ctx(ctx: Optional[List[Dict[str, Any]]], limit_chars: int = 2000) -> str:  # type: ignore[name-defined]
            try:
                if not ctx:
                    return "(no prior context)"
                lines: List[str] = []
                total = 0
                for msg in reversed(ctx[-20:]):
                    role = str(msg.get("role", "")).strip() or "user"
                    content = str(msg.get("content", "")).strip()
                    line = f"{role}: {content}"
                    if total + len(line) > limit_chars:
                        break
                    lines.append(line)
                    total += len(line)
                return "\n".join(reversed(lines)) if lines else "(no prior context)"
            except Exception:
                return "(no prior context)"

        ctx_block = _format_ctx(parent_chat_context)
        user = (
            "Recent conversation (most recent last):\n"
            f"{ctx_block}\n\n"
            "Steering message:\n"
            f"{(message or '').strip()}"
        )
        raw = await client.generate(user)
        import json as _json  # local import

        try:
            data = _json.loads(raw)
            action = str(data.get("action", "none")).strip().lower()
            reason = data.get("reason")
            if action not in {"cancel", "continue", "none"}:
                action = "none"
            if reason is not None:
                reason = str(reason)
            else:
                reason = message
            return action, str(reason)
        except Exception:
            low = (raw or "").lower()
            for tok in ["cancel", "continue"]:
                if tok in low:
                    return tok, message
            return "none", message
    except Exception:
        return "none", message


if TYPE_CHECKING:
    from .task_scheduler import TaskScheduler


class ActiveTask(BaseActiveTask, HandleWrapperMixin):
    def __init__(
        self,
        actor_handle: SteerableToolHandle,
        *,
        task_id: Optional[int] = None,
        instance_id: Optional[int] = None,
        scheduler: Optional["TaskScheduler"] = None,
        task_run_reference: Optional[TaskRunReference] = None,
    ):
        """
        Thin wrapper around an actor-backed active plan handle, keeping the
        corresponding Tasks row in sync when a scheduler is provided.

        Use ``ActiveTask.create(...)`` to start execution through the current
        run delegate or an explicit fallback actor.
        """
        self._actor_handle = actor_handle
        self._scheduler: Optional["TaskScheduler"] = scheduler
        self._task_id: Optional[int] = task_id
        self._instance_id: Optional[int] = instance_id
        self._task_run_reference: Optional[TaskRunReference] = task_run_reference
        self._was_stopped: bool = False
        self._last_intent: Optional[str] = None
        self._last_intent_reason: Optional[str] = None
        self._task_run_lineage_tokens = None
        self._definition_rearmed = False
        self._preserve_definition_status = False

        # Register the underlying actor handle for standardized wrapper discovery
        self._wrap_handle(actor_handle)

    @classmethod
    async def create(
        cls,
        fallback_actor: BaseActor | None,
        *,
        task_description: str,
        _parent_chat_context: Optional[list[dict]] = None,
        _clarification_up_q: Optional["asyncio.Queue[str]"] = None,
        _clarification_down_q: Optional["asyncio.Queue[str]"] = None,
        task_id: Optional[int] = None,
        instance_id: Optional[int] = None,
        scheduler: Optional["TaskScheduler"] = None,
        entrypoint: Optional[int] = None,
        entrypoint_kwargs: Optional[dict[str, Any]] = None,
        task_run_reference: Optional[TaskRunReference] = None,
        task_run_provenance: Optional[TaskRunProvenance] = None,
        task_entrypoint_review: Optional[dict[str, Any]] = None,
        task_guidelines: Optional[str] = None,
        entrypoint_repair_attempts: int = 0,
        entrypoint_repair_context: Optional[dict[str, Any]] = None,
        destination: Optional[str] = None,
        definition_rearmed: bool = False,
        preserve_definition_status: bool = False,
    ) -> "ActiveTask":
        """
        Create an ActiveTask by starting work through a delegate or fallback actor.

        This is the preferred constructor: it ensures the underlying active
        handle is running before returning an instance. When a run-scoped task
        execution delegate is active, ``fallback_actor`` is expected to be ``None``
        because execution is routed through the delegate instead.
        """
        delegate = current_task_execution_delegate.get()
        review_token = None
        lineage_tokens = None
        # Materialize/adopt the durable Tasks/Executions row before EventBus lineage
        # so every nested event can carry the join key ``run_key``.
        materialized_task_run_reference = task_run_reference
        if materialized_task_run_reference is None and task_run_provenance is not None:
            try:
                materialized_task_run_reference = await asyncio.to_thread(
                    create_or_adopt_live_task_run,
                    task_run_provenance,
                )
            except Exception:
                logger.exception(
                    "Failed to materialize live task run before execution started "
                    "(task_id=%s, instance_id=%s)",
                    task_id,
                    instance_id,
                )
        if task_id is not None and instance_id is not None:
            run_key = _resolve_active_task_run_key(
                task_run_reference=materialized_task_run_reference,
                task_run_provenance=task_run_provenance,
            )
            if not run_key and (
                materialized_task_run_reference is not None
                or task_run_provenance is not None
            ):
                raise RuntimeError(
                    "ActiveTask EventBus lineage requires a non-empty run_key when a "
                    f"Tasks/Executions row is adopted or materialized (task_id={task_id}, "
                    f"instance_id={instance_id}).",
                )
            if not run_key:
                logger.error(
                    "ActiveTask started without run_key; Events will not join "
                    "Tasks/Executions (task_id=%s)",
                    task_id,
                )
            lineage_tokens = push_task_run_lineage(
                task_id=int(task_id),
                run_key=run_key,
            )
        if task_entrypoint_review is not None:
            review_token = current_post_run_review_context.set(
                PostRunReviewContext(
                    display_label="Storing reusable workflow",
                    instructions=(
                        "Review the successful task trajectory and decide whether "
                        "a stable reusable workflow should be stored and attached "
                        "to future scheduled or triggered task instances."
                    ),
                    extensions={"task_entrypoint_review": task_entrypoint_review},
                ),
            )
        try:
            try:
                if delegate is not None:
                    actor_steerable_handle = await delegate.start_task_run(
                        task_description=task_description,
                        entrypoint=entrypoint,
                        parent_chat_context=_parent_chat_context,
                        clarification_up_q=_clarification_up_q,
                        clarification_down_q=_clarification_down_q,
                        guidelines=task_guidelines,
                        entrypoint_kwargs=entrypoint_kwargs,
                        entrypoint_repair_attempts=entrypoint_repair_attempts,
                        entrypoint_repair_context=entrypoint_repair_context,
                        destination=destination,
                    )
                else:
                    if fallback_actor is None:
                        raise RuntimeError(
                            "Task execution requires an actor when no run-scoped delegate is active.",
                        )
                    actor_steerable_handle = await fallback_actor.act(
                        task_description,
                        guidelines=task_guidelines,
                        _parent_chat_context=_parent_chat_context,
                        _clarification_up_q=_clarification_up_q,
                        _clarification_down_q=_clarification_down_q,
                        entrypoint=entrypoint,
                        entrypoint_kwargs=entrypoint_kwargs,
                        entrypoint_repair_attempts=entrypoint_repair_attempts,
                        entrypoint_repair_context=entrypoint_repair_context,
                        destination=destination,
                        persist=False,
                    )
            except Exception as exc:
                if materialized_task_run_reference is not None:
                    await asyncio.to_thread(
                        update_task_run_record,
                        materialized_task_run_reference,
                        {
                            "state": "failed",
                            "completed_at": _now_iso(),
                            "error": _truncate_task_run_text(str(exc)),
                            "result_summary": _truncate_task_run_text(
                                f"Task failed before execution fully started: {type(exc).__name__}({exc})",
                            ),
                        },
                    )
                reset_task_run_lineage(lineage_tokens)
                lineage_tokens = None
                raise
        finally:
            if review_token is not None:
                current_post_run_review_context.reset(review_token)
        instance = cls(
            actor_steerable_handle,  # type: ignore[arg-type]
            task_id=task_id,
            instance_id=instance_id,
            scheduler=scheduler,
            task_run_reference=materialized_task_run_reference,
        )
        instance._task_run_lineage_tokens = lineage_tokens
        instance._definition_rearmed = bool(definition_rearmed)
        instance._preserve_definition_status = bool(preserve_definition_status)
        return instance

    @functools.wraps(BaseActiveTask.ask, updated=())
    async def ask(
        self,
        question: str,
        *,
        _return_reasoning_steps: bool = False,
    ) -> SteerableToolHandle:
        """Answer a read-only question about the live activity and return a handle."""
        return await self._actor_handle.ask(question)

    @functools.wraps(BaseActiveTask.interject, updated=())
    async def interject(
        self,
        message: str,
        *,
        _parent_chat_context_cont: list[dict] | None = None,
    ) -> None:
        intent: Optional[str] = None
        reason: Optional[str] = None

        try:
            intent, reason = await classify_steering_intent(
                message,
                parent_chat_context=_parent_chat_context_cont,
            )
        except Exception:
            intent, reason = "none", message

        self._last_intent = intent
        self._last_intent_reason = reason or message

        if intent == "cancel":
            stop_reason = reason or message
            if hasattr(self._actor_handle, "stop"):
                asyncio.create_task(
                    forward_handle_call(
                        self._actor_handle,
                        "stop",
                        {"reason": stop_reason, "cancel": True},
                        fallback_positional_keys=("reason",),
                    ),
                )
            self._was_stopped = True
            if not self._preserve_definition_status:
                self._mirror_status(Status.cancelled)
            asyncio.create_task(
                self._persist_task_run_terminal_state(
                    state="cancelled",
                    result_summary=f"Task cancelled: {stop_reason}",
                ),
            )
            reset_task_run_lineage(self._task_run_lineage_tokens)
            self._task_run_lineage_tokens = None
            return

        await self._actor_handle.interject(message)  # type: ignore[arg-type]

    @functools.wraps(BaseActiveTask.stop, updated=())
    async def stop(
        self,
        *,
        cancel: bool = False,
        reason: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Stop the running activity.

        Both ``cancel=True`` and ``cancel=False`` mark the task as cancelled,
        since without queue semantics there is no prior schedule position to
        reinstate the task to.
        """
        await forward_handle_call(
            self._actor_handle,
            "stop",
            {"reason": reason, "cancel": cancel},
            fallback_positional_keys=("reason",),
        )
        self._was_stopped = True
        if not self._preserve_definition_status:
            self._mirror_status(Status.cancelled)
        asyncio.create_task(
            self._persist_task_run_terminal_state(
                state="cancelled",
                result_summary=reason or "Task stopped.",
            ),
        )
        asyncio.create_task(self._save_final_summary("cancelled"))
        reset_task_run_lineage(self._task_run_lineage_tokens)
        self._task_run_lineage_tokens = None

    @functools.wraps(BaseActiveTask.pause, updated=())
    async def pause(self) -> Optional[str]:
        return await self._actor_handle.pause()

    @functools.wraps(BaseActiveTask.resume, updated=())
    async def resume(self) -> Optional[str]:
        return await self._actor_handle.resume()

    @functools.wraps(BaseActiveTask.done, updated=())
    def done(self) -> bool:
        return self._actor_handle.done()

    async def _generate_summary_from_log(self, action_log: List[str]) -> str:
        """Generate a concise summary of the execution from the actor's action log."""
        client = new_llm_client(origin="ActiveTask.generate_summary")
        prompt = textwrap.dedent(
            f"""
            You are an assistant summarizing a complex task's execution log.
            Your summary will be stored in a database `info` column
            to provide a quick overview of "what actually happened".

            - Focus on the final outcome (e.g., completed, stopped, error).
            - Mention any user interjections or clarifications.
            - Mention any major verification failures and recoveries.
            - Be concise (1-3 sentences).

            EXECUTION LOG:
            ---
            {chr(10).join(action_log)}
            ---

            Concisely summarize what happened:
        """,
        )
        try:
            summary = await client.generate(prompt)
            return summary.strip()
        except Exception as e:
            logger.error("Error during summary generation: %s", e)
            return "Summary generation failed. Final Status: <UNKNOWN>"

    async def _save_final_summary(self, final_status: str):
        """Generate the final summary and update the task row in the database."""
        if self._scheduler and self._task_id is not None:
            summary = "No execution log was available to generate a summary."
            try:
                if (
                    hasattr(self._actor_handle, "action_log")
                    and self._actor_handle.action_log
                ):
                    summary = await self._generate_summary_from_log(
                        self._actor_handle.action_log,
                    )
                else:
                    summary = f"Task finished with status '{final_status}'. No detailed log found."

                summary = summary.replace("<UNKNOWN>", final_status)

                self._scheduler._update_task_definition_info(  # type: ignore[attr-defined]
                    task_id=self._task_id,
                    info=summary,
                )

            except Exception as e:
                logger.error(
                    "Error saving final task summary: %s",
                    e,
                )

    async def _persist_task_run_terminal_state(
        self,
        *,
        state: str,
        result_summary: str | None = None,
        error: str | None = None,
    ) -> None:
        """Patch the durable run row when the live execution reaches a terminal state."""

        if self._task_run_reference is None:
            return
        await asyncio.to_thread(
            update_task_run_record,
            self._task_run_reference,
            {
                "state": state,
                "completed_at": _now_iso(),
                "result_summary": (
                    _truncate_task_run_text(result_summary) if result_summary else None
                ),
                "error": _truncate_task_run_text(error) if error else None,
            },
        )

    @functools.wraps(BaseActiveTask.result, updated=())
    async def result(self) -> str:
        final_status: Optional[str] = None
        ret: Optional[str] = None
        error: Optional[Exception] = None

        try:
            ret = await self._actor_handle.result()
            if not self._was_stopped:
                final_status = "completed"

        except Exception as e:
            error = e
            if not self._was_stopped:
                final_status = "failed"
                ret = f"Task failed with error: {type(e).__name__}({e})"
                logger.error(
                    "--- Task %s.%s failed: %s ---",
                    self._task_id,
                    self._instance_id,
                    e,
                )

        finally:
            try:
                if (
                    final_status
                    and not self._was_stopped
                    and self._scheduler
                    and self._task_id is not None
                    and not self._preserve_definition_status
                ):
                    definition_status = final_status
                    if final_status == "completed" and self._definition_rearmed:
                        # Rearm-on-start already advanced the definition to the
                        # next open slot; restore scheduled/triggerable status.
                        task = self._scheduler._get_task_or_raise(self._task_id)
                        definition_status = (
                            "triggerable"
                            if task.trigger is not None and task.repeat is None
                            else "scheduled"
                        )
                    self._scheduler._update_task_definition_status(  # type: ignore[attr-defined]
                        task_id=self._task_id,
                        new_status=definition_status,
                    )
                    await self._persist_task_run_terminal_state(
                        state=final_status,
                        result_summary=ret,
                        error=str(error) if error is not None else None,
                    )

                    if not getattr(self, "_summary_scheduled", False):
                        try:
                            logger.info(
                                "--- Scheduling save_final_summary for %s.%s with status: %s ---",
                                self._task_id,
                                self._instance_id,
                                final_status,
                            )
                            asyncio.create_task(self._save_final_summary(final_status))
                            self._summary_scheduled = True  # type: ignore[attr-defined]
                        except Exception as summary_e:
                            logger.error("Error creating summary task: %s", summary_e)
            finally:
                reset_task_run_lineage(self._task_run_lineage_tokens)
                self._task_run_lineage_tokens = None

        if error and final_status == "failed":
            raise error
        elif self._was_stopped and not ret:
            return "Task stopped."
        elif ret is not None:
            return ret
        else:
            return "Task finished."

    # ------------------------------------------------------------------ #
    # Bottom-up event APIs (delegate to underlying actor handle)         #
    # ------------------------------------------------------------------ #
    @functools.wraps(SteerableToolHandle.next_clarification, updated=())
    async def next_clarification(self) -> dict:
        try:
            return await self._actor_handle.next_clarification()  # type: ignore[attr-defined]
        except Exception:
            return {}

    @functools.wraps(SteerableToolHandle.next_notification, updated=())
    async def next_notification(self) -> dict:
        try:
            return await self._actor_handle.next_notification()  # type: ignore[attr-defined]
        except Exception:
            return {}

    @functools.wraps(SteerableToolHandle.answer_clarification, updated=())
    async def answer_clarification(self, call_id: str, answer: str) -> None:
        try:
            await self._actor_handle.answer_clarification(call_id, answer)  # type: ignore[attr-defined]
        except Exception:
            return None

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #

    def _mirror_status(self, new_status: Status) -> None:
        """Update the task-row status if we were instantiated by a scheduler."""
        if self._preserve_definition_status:
            return
        if self._scheduler and self._task_id is not None:
            self._scheduler._update_task_definition_status(  # type: ignore[attr-defined]
                task_id=self._task_id,
                new_status=new_status,
            )
