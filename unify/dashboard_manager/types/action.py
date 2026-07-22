"""Tile action types for DashboardManager.

Declares Python functions that authenticated Console can dispatch from
tile chrome. Authored implementations are stored in the Functions
catalogue; wiring metadata lives in ``Dashboards/Actions``.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator

from unify.common.authorship import AuthoredRow

ResultMode = Literal["fire_and_forget", "show_result"]


class TileAction(BaseModel):
    """Declarative action bound to a tile for authenticated Console buttons.

    Provide exactly one of ``implementation``, ``function_id``, or
    ``function_name`` to author a new function or wire an existing one.
    """

    action_name: str = Field(
        description="Stable key for this action within the tile",
    )
    label: str = Field(
        description="Button label shown in authenticated Console chrome",
    )
    icon: Optional[str] = Field(
        default=None,
        description="Optional icon name for the Console button",
    )
    implementation: Optional[str] = Field(
        default=None,
        description=(
            "Python source for a new function. Stored in the Functions "
            "catalogue and wired to this action."
        ),
    )
    function_id: Optional[int] = Field(
        default=None,
        description="Existing Functions-catalogue id to wire",
    )
    function_name: Optional[str] = Field(
        default=None,
        description="Existing Functions-catalogue name to wire",
    )
    request: str = Field(
        default="",
        description="Optional natural-language brief for the offline runner",
    )
    result_mode: ResultMode = Field(
        default="fire_and_forget",
        description=(
            "fire_and_forget: dispatch and do not wait for a return value. "
            "show_result: Console polls the run and presents result_summary."
        ),
    )

    @model_validator(mode="after")
    def _exactly_one_function_source(self) -> "TileAction":
        sources = [
            self.implementation is not None and bool(self.implementation.strip()),
            self.function_id is not None,
            self.function_name is not None and bool(self.function_name.strip()),
        ]
        if sum(sources) != 1:
            raise ValueError(
                "TileAction requires exactly one of implementation, "
                "function_id, or function_name",
            )
        if not self.action_name.strip():
            raise ValueError("action_name must be non-empty")
        if not self.label.strip():
            raise ValueError("label must be non-empty")
        return self


class ActionRecordRow(AuthoredRow):
    """Fields inserted into the Dashboards/Actions Unify context."""

    tile_token: str = Field(description="Token of the owning tile")
    action_name: str = Field(
        description="Stable action key within the tile",
        json_schema_extra={"ui_editable": True},
    )
    function_id: int = Field(
        description="Functions-catalogue id invoked on dispatch",
        json_schema_extra={"ui_editable": True},
    )
    request: str = Field(
        default="",
        description="Optional natural-language brief for the offline runner",
        json_schema_extra={"ui_editable": True},
    )
    label: str = Field(
        default="",
        description="Console button label",
        json_schema_extra={"ui_editable": True},
    )
    icon: Optional[str] = Field(
        default=None,
        description="Optional Console button icon",
        json_schema_extra={"ui_editable": True},
    )
    scope: str = Field(
        default="dashboard",
        description="Action scope (dashboard by default)",
        json_schema_extra={"ui_editable": True},
    )
    result_mode: ResultMode = Field(
        default="fire_and_forget",
        description="How Console presents the dispatched run",
        json_schema_extra={"ui_editable": True},
    )


class ActionRecord(ActionRecordRow):
    """Full action record including the server-assigned action_id."""

    action_id: Optional[int] = Field(
        default=None,
        description="Auto-incremented action identifier",
    )
