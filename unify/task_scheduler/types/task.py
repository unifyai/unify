"""Task model: scheduling, triggering, priority, and metadata."""

from enum import Enum
from pydantic import Field, model_validator
from typing import Optional, List

from unify.common.authorship import AuthoredRow

from .priority import Priority
from .status import Status
from .schedule import Schedule
from .trigger import TaskTrigger
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
        description="Shared-team destination for routed task writes, if any.",
    )
    name: str = Field(description="Short title of the task")
    description: str = Field(
        description="Detailed explanation of what the task involves",
    )
    status: Status = Field(
        description=(
            "Current state of the task. "
            "Valid values: scheduled, triggerable, active, completed, cancelled, failed."
        ),
    )
    schedule: Optional[Schedule] = Field(
        default=None,
        description="Optional scheduling information, including ideal start time.",
        json_schema_extra={"unify_type": "dict"},
    )
    trigger: Optional[TaskTrigger] = Field(
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
    enabled: bool = Field(
        default=True,
        description=(
            "Whether this task may fire. When false, scheduled start times and trigger "
            "criteria do not activate the task, and manual execute is rejected until the "
            "task is re-enabled."
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
    custom_key: Optional[str] = Field(
        default=None,
        description=(
            "Stable source-defined key for sync identity. "
            "None for runtime-created entries."
        ),
    )
    custom_hash: Optional[str] = Field(
        default=None,
        description=(
            "Hash of source-defined custom task content for sync detection. "
            "None for runtime-created entries."
        ),
    )
    task_revision: Optional[int] = Field(
        default=None,
        description=(
            "Monotonic authored revision stamped by Orchestra for revision-safe "
            "provider-event mutations."
        ),
    )

    @model_validator(mode="after")
    def _mutually_exclusive_schedule_trigger(self):
        """Enforce that schedule and trigger are mutually exclusive."""

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
        return self.model_dump(mode="json")

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
