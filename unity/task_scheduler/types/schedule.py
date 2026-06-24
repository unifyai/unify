"""Start-time scheduling metadata for tasks."""

from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field


class Schedule(BaseModel):
    start_at: Optional[datetime] = Field(
        default=None,
        description="The scheduled start time for the task in ISO-8601 format.",
    )


def sched_start_at(sched: "Schedule | dict | None") -> Optional[str]:
    """Return ``start_at`` from a :class:`Schedule` or dict."""
    if sched is None:
        return None
    if isinstance(sched, dict):
        return sched.get("start_at")
    try:
        return getattr(sched, "start_at", None)
    except Exception:
        return None
