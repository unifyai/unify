"""Task model: queue membership, scheduling/triggering, priority, and metadata."""

from enum import Enum
from pydantic import Field, model_validator
from typing import Optional, List

from unity.common.authorship import AuthoredRow

from .priority import Priority
from .status import Status
from .schedule import Schedule
from .trigger import Trigger
from .repetition import RepeatPattern
from .activated_by import ActivatedBy
from datetime import datetime


class DeliveryMode(str, Enum):
    live = "live"
    offline = "offline"


class ExecutionStyle(str, Enum):
    agentic = "agentic"
    symbolic = "symbolic"


class TaskBase(AuthoredRow):
    assistant_id: Optional[str] = Field(
        default=None,
        description="Assistant that owns execution state for this task.",
    )
    destination: Optional[str] = Field(
        default=None,
        description="Shared-space destination for routed task writes, if any.",
    )
    # Top-level queue identifier for tasks that are members of a runnable queue.
    # When a task is queued/scheduled, this must be populated. The schedule
    # object never carries a queue_id field; use this top-level column solely.
    queue_id: Optional[int] = Field(
        default=None,
        description=("Identifier of the runnable queue this task belongs to."),
    )
    name: str = Field(description="Short title of the task")
    description: str = Field(
        description="Detailed explanation of what the task involves",
    )
    status: Status = Field(
        description="Current state of the task (e.g., queued, active, completed)",
    )
    schedule: Optional[Schedule] = Field(
        default=None,
        description="Information about task scheduling, including adjacent tasks in the queue and ideal start time",
        json_schema_extra={"unify_type": "dict"},
    )
    trigger: Optional[Trigger] = Field(
        default=None,
        description="Event definition that starts the task (mutually exclusive with *schedule*)",
    )
    deadline: Optional[datetime] = Field(
        default=None,
        description="Due date/time for the task in ISO-8601 format",
    )
    repeat: Optional[List[RepeatPattern]] = Field(
        default=None,
        description=(
            "Pattern defining how the task recurs over time. Use minutely/hourly "
            "frequencies for sub-daily intervals; use daily/weekly/monthly/yearly "
            "for calendar recurrences. Recurring live tasks may begin with "
            "entrypoint=null and execute from the natural-language description "
            "until a post-run review stores a stable function."
        ),
    )
    priority: Priority = Field(
        description="Importance level of the task (low, normal, high, urgent)",
    )
    response_policy: Optional[str] = Field(
        default=None,
        description=(
            "Freeform policy for contact handling during this task (authority, "
            "information visibility, who may interject/steer). When it conflicts with "
            "a contact's own response_policy, the task-level policy takes precedence."
        ),
    )
    entrypoint: Optional[int] = Field(
        default=None,
        description=(
            "Optional function_id from the Functions table that should act as this task's symbolic "
            "executor. When null, the task is agentic: an Actor interprets the task name, description, "
            "and metadata at run time. Entrypoint availability is independent from live/offline delivery."
        ),
    )
    offline: bool = Field(
        default=False,
        description=(
            "Whether this task should execute in the hidden headless lane instead of waking "
            "the live assistant runtime. Offline controls delivery only; entrypoint controls "
            "whether execution is symbolic or agentic."
        ),
    )
    activated_by: Optional[ActivatedBy] = Field(
        default=None,
        description=(
            "Reason the task instance transitioned to the active state.\n"
            "This is set automatically at activation time and is never directly editable."
        ),
    )
    info: Optional[str] = Field(
        default=None,
        description="A summary of what happened during the execution of the task, generated upon completion.",
    )

    @model_validator(mode="after")
    def _mutually_exclusive_schedule_trigger(self):
        """Enforce the invariants that must hold for every local task payload."""

        if self.schedule is not None and self.trigger is not None:
            raise ValueError("A task cannot have both *schedule* and *trigger*.")

        return self

    @property
    def delivery_mode(self) -> DeliveryMode:
        """Return the normalized delivery lane for this task."""

        return DeliveryMode.offline if self.offline else DeliveryMode.live

    @property
    def execution_style(self) -> ExecutionStyle:
        """Return whether execution is actor-interpreted or function-backed."""

        return (
            ExecutionStyle.symbolic
            if self.entrypoint is not None
            else ExecutionStyle.agentic
        )

    def to_post_json(self) -> dict:
        exclude: set[str] = set()
        # Allow backend auto-increment for queue_id by omitting it when unset
        if self.queue_id is None:
            exclude.add("queue_id")
        return self.model_dump(mode="json", exclude=exclude)

    @property
    def schedule_next(self) -> Optional[int]:
        return self.schedule.next_task if self.schedule is not None else None

    @property
    def schedule_prev(self) -> Optional[int]:
        return self.schedule.prev_task if self.schedule is not None else None

    @property
    def schedule_start_at(self) -> Optional[datetime]:
        return self.schedule.start_at if self.schedule is not None else None


class Task(TaskBase):
    task_id: int = Field(description="Unique identifier for the task")
    instance_id: int = Field(
        description=(
            "Auto-incrementing counter that distinguishes multiple *instances* "
            "of the same logical task.  The very first row receives `0`; "
            "each subsequent clone is incremented by the backend."
        ),
    )
