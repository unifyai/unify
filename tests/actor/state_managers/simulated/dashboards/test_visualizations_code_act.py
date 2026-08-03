"""
CodeActActor eval tests for visualization routing (simulated managers).

A visualization request must reach ``primitives.canvas``, not the superseded
tile surface. Two managers can put something visual in front of the user and
only one should be authored against, so this is the test that catches the tool
surface drifting back toward tiles -- whether through spec wording, ranking, or
a prompt example that overclaims.

Uses ``make_code_act_actor`` with simulated managers and a live LLM.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.actor.state_managers.utils import (
    make_code_act_actor,
)
from unify.manager_registry import ManagerRegistry

pytestmark = pytest.mark.eval


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _safe_call(method: Any, *args: Any, **kwargs: Any) -> Any:
    if asyncio.iscoroutinefunction(method):
        return await method(*args, **kwargs)
    return method(*args, **kwargs)


async def _seed_monthly_revenue(dm: Any) -> None:
    await _safe_call(
        dm.create_table,
        context="Sales/Monthly",
        fields={"month": "str", "revenue": "float", "region": "str"},
    )
    await _safe_call(
        dm.insert_rows,
        context="Sales/Monthly",
        rows=[
            {"month": "Jan", "revenue": 12000.0, "region": "North"},
            {"month": "Feb", "revenue": 15000.0, "region": "North"},
            {"month": "Mar", "revenue": 9000.0, "region": "South"},
            {"month": "Apr", "revenue": 18000.0, "region": "North"},
            {"month": "May", "revenue": 11000.0, "region": "South"},
        ],
    )


async def _seed_repairs(dm: Any) -> None:
    await _safe_call(
        dm.create_table,
        context="repairs",
        fields={
            "repair_id": "int",
            "category": "str",
            "priority": "str",
            "cost": "float",
        },
    )
    await _safe_call(
        dm.insert_rows,
        context="repairs",
        rows=[
            {"repair_id": 1, "category": "Plumbing", "priority": "high", "cost": 500.0},
            {
                "repair_id": 2,
                "category": "Electrical",
                "priority": "low",
                "cost": 150.0,
            },
            {"repair_id": 3, "category": "Plumbing", "priority": "high", "cost": 800.0},
            {"repair_id": 4, "category": "HVAC", "priority": "medium", "cost": 350.0},
            {
                "repair_id": 5,
                "category": "Electrical",
                "priority": "high",
                "cost": 600.0,
            },
        ],
    )


# ---------------------------------------------------------------------------
# Routing tests: actor should use primitives.dashboards for visualizations
# ---------------------------------------------------------------------------

VISUALIZATION_QUESTIONS = [
    "Create a bar chart showing monthly revenue from the Sales/Monthly data.",
    "Plot the repairs data by category as a chart or visualization.",
]


@pytest.mark.asyncio
@pytest.mark.timeout(300)
@pytest.mark.parametrize("question", VISUALIZATION_QUESTIONS)
async def test_code_act_visualization_routes_to_canvas(question: str):
    """A chart or plot request must author a canvas, never a tile."""
    async with make_code_act_actor(impl="simulated") as (actor, _primitives, calls):
        dm = ManagerRegistry.get_data_manager()
        await _seed_monthly_revenue(dm)
        await _seed_repairs(dm)

        handle = await actor.act(
            f"{question} Do not ask clarifying questions. Proceed immediately.",
            clarification_enabled=False,
        )
        result = await handle.result()

        assert result is not None
        canvas_calls = [c for c in calls if "canvas" in c]
        assert canvas_calls, (
            f"Expected primitives.canvas calls for a visualization request, "
            f"but only saw: {calls}"
        )
        assert [c for c in canvas_calls if "create_view" in c], (
            f"Expected primitives.canvas.create_view to be called, "
            f"but canvas calls were: {canvas_calls}"
        )
        # Canvas must be the *first* visual surface reached. A later tile call is
        # legitimate fallback -- without an installed toolchain the build gate
        # rejects every canvas, and an actor that then gives the user nothing
        # would be worse. What must not happen is reaching for a tile first.
        first_visual = next(
            (c for c in calls if "canvas" in c or "dashboards" in c),
            "",
        )
        assert (
            "canvas" in first_visual
        ), f"A visualization request reached for a tile before a canvas: {calls}"
