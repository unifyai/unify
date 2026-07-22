"""Tests for DashboardManager tile action types and wiring helpers."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from unify.dashboard_manager.ops.action_ops import (
    build_action_record_rows,
    validate_tile_actions,
)
from unify.dashboard_manager.types.action import ActionRecordRow, TileAction


class _FakeFunctionManager:
    def __init__(self) -> None:
        self.added: list[str] = []
        self.rows = [
            {"function_id": 42, "name": "existing_helper"},
            {"function_id": 77, "name": "send_digest"},
        ]

    def add_functions(self, *, implementations, overwrite=False, **kwargs):
        del overwrite, kwargs
        self.added.extend(implementations)
        return {"send_digest": "added"}

    def filter_functions(self, *, filter, limit=100, include_implementations=True):
        del limit, include_implementations
        out = []
        for row in self.rows:
            if f'name == "{row["name"]}"' in filter:
                out.append(row)
            elif f"function_id == {row['function_id']}" in filter:
                out.append(row)
        return out


class TestTileAction:
    def test_requires_exactly_one_source(self):
        with pytest.raises(ValidationError):
            TileAction(action_name="a", label="A")
        with pytest.raises(ValidationError):
            TileAction(
                action_name="a",
                label="A",
                function_id=1,
                function_name="x",
            )

    def test_fire_and_forget_default(self):
        action = TileAction(
            action_name="send_digest",
            label="Send Digest",
            implementation="async def send_digest():\n    return 'ok'\n",
        )
        assert action.result_mode == "fire_and_forget"

    def test_show_result_mode(self):
        action = TileAction(
            action_name="compute",
            label="Compute",
            function_id=42,
            result_mode="show_result",
        )
        assert action.result_mode == "show_result"


class TestActionOps:
    def test_validate_rejects_duplicate_names(self):
        with pytest.raises(ValueError, match="Duplicate action_name"):
            validate_tile_actions(
                [
                    TileAction(
                        action_name="a",
                        label="A",
                        function_id=1,
                    ),
                    TileAction(
                        action_name="a",
                        label="B",
                        function_id=2,
                    ),
                ],
            )

    def test_build_rows_authors_implementation(self):
        fm = _FakeFunctionManager()
        rows = build_action_record_rows(
            tile_token="tile_abc12345",
            actions=[
                TileAction(
                    action_name="send_digest",
                    label="Send Digest",
                    implementation="async def send_digest():\n    return 'ok'\n",
                    result_mode="fire_and_forget",
                ),
            ],
            function_manager=fm,
        )
        assert len(fm.added) == 1
        assert len(rows) == 1
        assert isinstance(rows[0], ActionRecordRow)
        assert rows[0].function_id == 77
        assert rows[0].result_mode == "fire_and_forget"

    def test_build_rows_wires_existing_id(self):
        fm = _FakeFunctionManager()
        rows = build_action_record_rows(
            tile_token="tile_abc12345",
            actions=[
                TileAction(
                    action_name="reuse",
                    label="Reuse",
                    function_id=42,
                    result_mode="show_result",
                ),
            ],
            function_manager=fm,
        )
        assert rows[0].function_id == 42
        assert rows[0].result_mode == "show_result"
        assert fm.added == []
