"""Action wiring helpers for DashboardManager.

Resolves TileAction specs against the Functions catalogue and upserts
rows into the Dashboards/Actions context.
"""

from __future__ import annotations

import ast
from typing import Any, List, Optional, Sequence

from unify.dashboard_manager.types.action import (
    ActionRecordRow,
    ResultMode,
    TileAction,
)


def validate_tile_actions(
    actions: Optional[List[TileAction]],
) -> Optional[List[TileAction]]:
    """Validate and normalize a list of TileAction specs."""
    if not actions:
        return None
    seen: set[str] = set()
    cleaned: List[TileAction] = []
    for action in actions:
        if isinstance(action, dict):
            action = TileAction.model_validate(action)
        elif not isinstance(action, TileAction):
            action = TileAction.model_validate(action)
        name = action.action_name.strip()
        if name in seen:
            raise ValueError(f"Duplicate action_name '{name}' on the same tile")
        seen.add(name)
        cleaned.append(action)
    return cleaned


def _function_name_from_implementation(implementation: str) -> str:
    """Extract the top-level function name from Python source."""
    tree = ast.parse(implementation)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return node.name
    raise ValueError("implementation must define a top-level Python function")


def resolve_function_id(
    action: TileAction,
    *,
    function_manager: Any,
) -> int:
    """Resolve or author the Functions-catalogue id for one TileAction."""
    import json

    if action.implementation is not None and action.implementation.strip():
        name = _function_name_from_implementation(action.implementation)
        function_manager.add_functions(
            implementations=[action.implementation],
            overwrite=True,
        )
        rows = function_manager.filter_functions(
            filter=f"name == {json.dumps(name)}",
            limit=1,
            include_implementations=False,
        )
        if not rows or rows[0].get("function_id") is None:
            raise ValueError(
                f"Failed to resolve function_id after authoring '{name}'",
            )
        return int(rows[0]["function_id"])

    if action.function_id is not None:
        rows = function_manager.filter_functions(
            filter=f"function_id == {int(action.function_id)}",
            limit=1,
            include_implementations=False,
        )
        if not rows:
            raise ValueError(
                f"function_id {action.function_id} not found in Functions catalogue",
            )
        return int(action.function_id)

    name = (action.function_name or "").strip()
    rows = function_manager.filter_functions(
        filter=f"name == {json.dumps(name)}",
        limit=1,
        include_implementations=False,
    )
    if not rows or rows[0].get("function_id") is None:
        raise ValueError(
            f"function_name '{name}' not found in Functions catalogue",
        )
    return int(rows[0]["function_id"])


def build_action_record_rows(
    *,
    tile_token: str,
    actions: Sequence[TileAction],
    function_manager: Any,
    scope: str = "dashboard",
) -> List[ActionRecordRow]:
    """Resolve each action and build ActionRecordRow entries for insert."""
    rows: List[ActionRecordRow] = []
    for action in actions:
        function_id = resolve_function_id(action, function_manager=function_manager)
        result_mode: ResultMode = action.result_mode
        rows.append(
            ActionRecordRow(
                tile_token=tile_token,
                action_name=action.action_name.strip(),
                function_id=function_id,
                request=action.request or "",
                label=action.label.strip(),
                icon=action.icon,
                scope=scope,
                result_mode=result_mode,
            ),
        )
    return rows


def replace_tile_actions(
    *,
    actions_context: str,
    tile_token: str,
    actions: Optional[Sequence[TileAction]],
    data_manager: Any,
    function_manager: Any,
    scope: str = "dashboard",
) -> List[ActionRecordRow]:
    """Replace all action rows for a tile with the given specs.

    Passing ``None`` or an empty list clears all actions for the tile.
    """
    data_manager.delete_rows(
        actions_context,
        filter=f"tile_token == '{tile_token}'",
    )
    if not actions:
        return []
    rows = build_action_record_rows(
        tile_token=tile_token,
        actions=actions,
        function_manager=function_manager,
        scope=scope,
    )
    data_manager.insert_rows(
        actions_context,
        [row.model_dump() for row in rows],
    )
    return rows


def delete_tile_actions(
    *,
    actions_context: str,
    tile_token: str,
    data_manager: Any,
) -> int:
    """Delete all action rows for a tile. Does not delete Functions entries."""
    return int(
        data_manager.delete_rows(
            actions_context,
            filter=f"tile_token == '{tile_token}'",
        )
        or 0,
    )
