"""Task-run lineage for EventBus hierarchy and payload fields.

When a ``TaskScheduler`` / ``ActiveTask`` run is in flight, push a
``Task.run(task_id=…,instance_id=…[,run_key=…])`` segment onto
``TOOL_LOOP_LINEAGE`` so nested ``execute_code`` / ``execute_function``
events inherit a deterministic parent. Structured ids are also held in a
ContextVar so Orchestra payloads can carry ``task_id`` / ``instance_id`` /
``run_key`` without parsing hierarchy strings.
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Any, Generator

from unify.common._async_tool.loop_config import TOOL_LOOP_LINEAGE

__all__ = [
    "TaskRunLineage",
    "CURRENT_TASK_RUN_LINEAGE",
    "TASK_RUN_LINEAGE_SEGMENT_RE",
    "format_task_run_lineage_segment",
    "parse_task_run_lineage_segment",
    "push_task_run_lineage",
    "reset_task_run_lineage",
    "task_run_lineage_scope",
    "enrich_payload_with_task_run_lineage",
]

TASK_RUN_LINEAGE_SEGMENT_RE = re.compile(
    r"^Task\.run\("
    r"task_id=(?P<task_id>\d+),"
    r"instance_id=(?P<instance_id>\d+)"
    r"(?:,run_key=(?P<run_key>[^)]+))?"
    r"\)$",
)


@dataclass(frozen=True)
class TaskRunLineage:
    """Structured ids for the currently executing durable task instance."""

    task_id: int
    instance_id: int
    run_key: str | None = None

    def as_payload_fields(self) -> dict[str, Any]:
        fields: dict[str, Any] = {
            "task_id": int(self.task_id),
            "instance_id": int(self.instance_id),
        }
        if self.run_key:
            fields["run_key"] = str(self.run_key)
        return fields


CURRENT_TASK_RUN_LINEAGE: ContextVar[TaskRunLineage | None] = ContextVar(
    "CURRENT_TASK_RUN_LINEAGE",
    default=None,
)


def format_task_run_lineage_segment(
    *,
    task_id: int,
    instance_id: int,
    run_key: str | None = None,
) -> str:
    """Return the canonical hierarchy segment for one task run."""

    base = f"Task.run(task_id={int(task_id)},instance_id={int(instance_id)}"
    if run_key:
        return f"{base},run_key={run_key})"
    return f"{base})"


def parse_task_run_lineage_segment(segment: str) -> TaskRunLineage | None:
    """Parse a ``Task.run(...)`` hierarchy segment, or return None."""

    match = TASK_RUN_LINEAGE_SEGMENT_RE.match(segment or "")
    if match is None:
        return None
    return TaskRunLineage(
        task_id=int(match.group("task_id")),
        instance_id=int(match.group("instance_id")),
        run_key=match.group("run_key"),
    )


@dataclass
class _TaskRunLineageTokens:
    lineage_token: Token
    tool_loop_token: Token


def push_task_run_lineage(
    *,
    task_id: int,
    instance_id: int,
    run_key: str | None = None,
) -> _TaskRunLineageTokens:
    """Push task-run context onto lineage ContextVars; return reset tokens."""

    lineage = TaskRunLineage(
        task_id=int(task_id),
        instance_id=int(instance_id),
        run_key=str(run_key) if run_key else None,
    )
    segment = format_task_run_lineage_segment(
        task_id=lineage.task_id,
        instance_id=lineage.instance_id,
        run_key=lineage.run_key,
    )
    parent = list(TOOL_LOOP_LINEAGE.get([]) or [])
    if not any(parse_task_run_lineage_segment(str(s)) for s in parent):
        parent = [*parent, segment]
    return _TaskRunLineageTokens(
        lineage_token=CURRENT_TASK_RUN_LINEAGE.set(lineage),
        tool_loop_token=TOOL_LOOP_LINEAGE.set(parent),
    )


def reset_task_run_lineage(tokens: _TaskRunLineageTokens | None) -> None:
    """Reset tokens from :func:`push_task_run_lineage`."""

    if tokens is None:
        return
    TOOL_LOOP_LINEAGE.reset(tokens.tool_loop_token)
    CURRENT_TASK_RUN_LINEAGE.reset(tokens.lineage_token)


@contextmanager
def task_run_lineage_scope(
    *,
    task_id: int,
    instance_id: int,
    run_key: str | None = None,
) -> Generator[_TaskRunLineageTokens, None, None]:
    """Context manager that pushes then resets task-run lineage."""

    tokens = push_task_run_lineage(
        task_id=task_id,
        instance_id=instance_id,
        run_key=run_key,
    )
    try:
        yield tokens
    finally:
        reset_task_run_lineage(tokens)


def enrich_payload_with_task_run_lineage(
    payload_dict: dict[str, Any],
) -> dict[str, Any]:
    """Copy current task-run ids onto a payload dict when present.

    Also ensures ``hierarchy`` includes the ``Task.run(...)`` segment when
    the ContextVar is set but the list was built without it.
    """

    lineage = CURRENT_TASK_RUN_LINEAGE.get()
    if lineage is None:
        return payload_dict

    payload_dict.update(lineage.as_payload_fields())

    hierarchy = payload_dict.get("hierarchy")
    if isinstance(hierarchy, list):
        segment = format_task_run_lineage_segment(
            task_id=lineage.task_id,
            instance_id=lineage.instance_id,
            run_key=lineage.run_key,
        )
        if not any(
            parse_task_run_lineage_segment(str(s)) is not None for s in hierarchy
        ):
            payload_dict["hierarchy"] = [segment, *hierarchy]
            payload_dict["hierarchy_label"] = "->".join(
                str(s) for s in payload_dict["hierarchy"]
            )
    return payload_dict
