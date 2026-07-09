"""Start-time scheduling metadata for tasks."""

from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field


class Schedule(BaseModel):
    start_at: Optional[datetime] = Field(
        default=None,
        description="The scheduled start time for the task in ISO-8601 format.",
    )
    jitter_applied_seconds: Optional[float] = Field(
        default=None,
        description=(
            "Bookkeeping: random jitter (in seconds) already baked into "
            "``start_at`` for this occurrence. The re-arm subtracts it to "
            "recover the canonical, un-jittered anchor before computing the "
            "next occurrence, so a jittered dispatch time never causes drift. "
            "Absent/None means ``start_at`` is the exact canonical slot."
        ),
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
