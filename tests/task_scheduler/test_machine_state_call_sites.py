"""Every machine-state constructor call site must match the dataclass.

``TaskRunProvenance`` and ``TaskExecutionSnapshot`` are constructed by
keyword from a dozen places across the task scheduler and the
conversation manager, and read attribute-wise from more. When a field is
removed from one of them, a missed call site does not fail at import or
in any unit test that mocks the surrounding path — it raises
``TypeError``/``AttributeError`` at runtime, on the specific wake that
touches it.

That is not hypothetical. Removing ``task_description`` from both
dataclasses left five call sites still passing it, and the first
symptom was a deployed assistant failing to process inbound mail:

    error processing event_id=evt-000026 event=UnifyMessageReceived
    'TaskExecutionSnapshot' object has no attribute 'task_description'

The live trigger lane was dead for as long as that shipped. This test
parses the tree instead of trusting a grep.
"""

from __future__ import annotations

import ast
from pathlib import Path

from unify.task_scheduler.machine_state import (
    TaskExecutionSnapshot,
    TaskRunProvenance,
)

_TRACKED = {
    "TaskRunProvenance": set(TaskRunProvenance.__dataclass_fields__),
    "TaskExecutionSnapshot": set(TaskExecutionSnapshot.__dataclass_fields__),
}


def _unify_sources() -> list[Path]:
    root = Path(__file__).resolve().parents[2] / "unify"
    return sorted(root.rglob("*.py"))


def test_no_call_site_passes_a_field_the_dataclass_lost() -> None:
    """Keyword arguments must exist on the target dataclass."""

    offenders: list[str] = []
    for path in _unify_sources():
        try:
            tree = ast.parse(path.read_text())
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = getattr(func, "id", None) or getattr(func, "attr", None)
            if name not in _TRACKED:
                continue
            for keyword in node.keywords:
                if keyword.arg is None:
                    continue  # **kwargs splat: not statically checkable
                if keyword.arg not in _TRACKED[name]:
                    offenders.append(
                        f"{path.name}:{keyword.value.lineno} "
                        f"{name}(..., {keyword.arg}=...) — no such field",
                    )

    assert not offenders, (
        "call sites pass fields their dataclass does not have; these raise "
        "TypeError at runtime on the wake that reaches them:\n  "
        + "\n  ".join(offenders)
    )


def test_no_attribute_read_of_a_field_the_dataclass_lost() -> None:
    """Attribute reads named after a removed field are equally fatal.

    Scoped to the names these objects are conventionally bound to in the
    task paths, which is where the regression actually lived.
    """

    suspects = {"activation", "candidate", "snap", "snapshot", "execution"}
    removed = {"task_description", "repeat"}
    offenders: list[str] = []

    for path in _unify_sources():
        try:
            tree = ast.parse(path.read_text())
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Attribute):
                continue
            value = node.value
            if not isinstance(value, ast.Name) or value.id not in suspects:
                continue
            if (
                node.attr in removed
                and node.attr not in _TRACKED["TaskExecutionSnapshot"]
            ):
                offenders.append(f"{path.name}:{node.lineno} {value.id}.{node.attr}")

    assert not offenders, (
        "attribute reads of fields the execution snapshot no longer "
        "carries:\n  " + "\n  ".join(offenders)
    )
