"""Validate and resolve canvas actions.

An action is the write path out of a rendered canvas, so it is checked harder
than anything else a canvas declares. Three things are established before one is
stored:

- It names exactly one dispatch target, and that target exists.
- Its input schema bounds every array and string. Those bounds are the blast
  radius of the action once a viewer can supply arguments, and they are enforced
  server-side on every invocation.
- Anything irreversible says so and carries confirmation text, which console
  renders outside the frame alongside the actual arguments.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence

from unify.canvas_manager.types.action import CanvasAction

# Ceiling on actions per canvas. A surface with more than this is a workflow
# tool rather than a view, and should be several canvases.
MAX_ACTIONS = 12

_TARGET_FIELDS = (
    "function_id",
    "function_name",
    "implementation",
    "task_id",
    "request",
)


class ActionError(ValueError):
    """An action was declared incorrectly."""


def coerce_actions(actions: Sequence[object] | None) -> List[CanvasAction]:
    """Accept dicts or models and return validated actions."""
    if not actions:
        return []

    coerced: List[CanvasAction] = []
    for entry in actions:
        if isinstance(entry, CanvasAction):
            coerced.append(entry)
        elif isinstance(entry, dict):
            coerced.append(CanvasAction.model_validate(entry))
        else:
            raise ActionError(
                f"Action must be a CanvasAction or dict, got {type(entry).__name__}.",
            )

    if len(coerced) > MAX_ACTIONS:
        raise ActionError(
            f"A canvas may declare at most {MAX_ACTIONS} actions; got {len(coerced)}.",
        )

    return coerced


def _assert_bounded(schema: Dict[str, Any], *, path: str = "input_schema") -> None:
    """Require an explicit ceiling on every array and string in the schema.

    Without this a viewer could submit an unbounded recipient list or a
    megabyte-long field, and the action would faithfully act on it.
    """
    node_type = schema.get("type")

    if node_type == "array":
        if "maxItems" not in schema:
            raise ActionError(
                f"{path} is an array without `maxItems`; bound it explicitly.",
            )
        items = schema.get("items")
        if isinstance(items, dict):
            _assert_bounded(items, path=f"{path}.items")

    elif node_type == "string":
        if (
            "maxLength" not in schema
            and "enum" not in schema
            and "format" not in schema
        ):
            raise ActionError(
                f"{path} is a string without `maxLength`, `enum` or `format`; bound it explicitly.",
            )

    elif node_type == "object":
        for name, child in (schema.get("properties") or {}).items():
            if isinstance(child, dict):
                _assert_bounded(child, path=f"{path}.{name}")


def validate_actions(actions: Sequence[CanvasAction]) -> List[CanvasAction]:
    """Check every action, raising on the first problem."""
    seen: set[str] = set()

    for action in actions:
        if action.name in seen:
            raise ActionError(
                f"Duplicate action name {action.name!r}; names must be unique.",
            )
        seen.add(action.name)

        targets = [
            field
            for field in _TARGET_FIELDS
            if getattr(action, field, None) is not None
        ]
        if len(targets) != 1:
            raise ActionError(
                f"Action {action.name!r} must name exactly one of {', '.join(_TARGET_FIELDS)}; "
                f"got {targets or 'none'}.",
            )

        target = targets[0]
        if action.kind == "task" and target != "task_id":
            raise ActionError(
                f"Action {action.name!r} has kind 'task' but names {target!r}.",
            )
        if action.kind == "function" and target not in {
            "function_id",
            "function_name",
            "implementation",
        }:
            raise ActionError(
                f"Action {action.name!r} has kind 'function' but names {target!r}.",
            )
        if action.kind == "assistant" and target != "request":
            raise ActionError(
                f"Action {action.name!r} has kind 'assistant' but names {target!r}.",
            )

        if action.input_schema is not None:
            if action.input_schema.get("type") != "object":
                raise ActionError(
                    f"Action {action.name!r} input_schema must be an object schema.",
                )
            _assert_bounded(action.input_schema)

        if action.destructive and not (action.confirm or "").strip():
            raise ActionError(
                f"Action {action.name!r} is destructive and must supply `confirm` text; it is shown "
                f"to the viewer, outside the canvas, before anything runs.",
            )

    return list(actions)


def resolve_function_id(action: CanvasAction, *, function_manager) -> int | None:
    """Resolve a function-kind action to a concrete stored function id.

    An action must point at something that already exists, so a canvas cannot
    ship a button wired to nothing.
    """
    if action.kind != "function":
        return None

    if action.function_id is not None:
        matches = function_manager.filter_functions(
            filter=f"function_id == {action.function_id}",
            limit=1,
        )
        if not matches:
            raise ActionError(
                f"Action {action.name!r} names function_id {action.function_id}, which does not exist.",
            )
        return action.function_id

    if action.function_name:
        matches = function_manager.filter_functions(
            filter=f"name == '{action.function_name}'",
            limit=1,
        )
        if not matches:
            raise ActionError(
                f"Action {action.name!r} names function {action.function_name!r}, which does not exist.",
            )
        return int(matches[0].get("function_id"))

    if action.implementation:
        added = function_manager.add_functions(action.implementation, overwrite=True)
        if not added:
            raise ActionError(
                f"Action {action.name!r} supplied an implementation that could not be stored.",
            )
        first = added[0] if isinstance(added, list) else added
        return int(first.get("function_id") if isinstance(first, dict) else first)

    return None


def serialize_input_schema(action: CanvasAction) -> str | None:
    """Serialise an action's input schema for storage."""
    return (
        json.dumps(action.input_schema, separators=(",", ":"))
        if action.input_schema
        else None
    )
