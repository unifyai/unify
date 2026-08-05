"""Pydantic model for the ``Workflows`` context."""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field, model_validator

from unify.common.authorship import AuthoredRow

UNASSIGNED = -1


class WorkflowInstallation(AuthoredRow):
    """One installed workflow bundle.

    The row is the installation, not the bundle: it records which bundle
    was installed, at which version, and which surfaces it wrote to. The
    bundle's *content* lives in the surfaces themselves (guidance rows,
    task rows, ...), each stamped with this row's :attr:`slug` as its
    ``managed_by``.
    """

    SHORTHAND_MAP: ClassVar[dict[str, str]] = {
        "workflow_id": "wid",
        "slug": "s",
        "name": "n",
        "version": "v",
        "status": "st",
        "params": "p",
        "surfaces": "sf",
        "destination": "dst",
    }

    workflow_id: int = Field(
        default=UNASSIGNED,
        description="Unique identifier for the installation.",
        ge=UNASSIGNED,
    )
    slug: str = Field(
        description=(
            "Stable bundle identifier, e.g. 'draft_email_replies'. Doubles "
            "as the custom-sync managed_by stamped onto every row this "
            "workflow plants, so it must not change across versions."
        ),
    )
    name: str = Field(
        description="Human-readable workflow name.",
        json_schema_extra={"ui_editable": True},
    )
    version: str = Field(
        default="",
        description="Bundle version last reconciled into the surfaces.",
    )
    description: str = Field(
        default="",
        description="What the workflow does, shown when listing workflows.",
    )
    status: str = Field(
        default="active",
        description=(
            "'active' when the last reconcile completed, 'partial' when "
            "some entries failed and the next pass will retry them. "
            "'needs_connection' is never stored — connections change "
            "without this row being touched, so reads derive it from the "
            "bundle's requirements and the current secret keyset."
        ),
    )
    params: str = Field(
        default="{}",
        description=(
            "JSON object of install-time settings, read at run time by the "
            "workflow's own tasks and functions. Never baked into the "
            "planted rows: doing so would make their content hashes differ "
            "per installation."
        ),
    )
    surfaces: str = Field(
        default="[]",
        description=(
            "JSON array of surface names this workflow wrote to, e.g. "
            '["guidance", "tasks"]. Recorded so uninstall can find the '
            "planted rows without the bundle still being on disk."
        ),
    )
    destination: str = Field(
        default="personal",
        description="Root that owns this installation, personal or team:<id>.",
    )

    @model_validator(mode="before")
    @classmethod
    def _inject_sentinel(cls, data: dict) -> dict:
        data.setdefault("workflow_id", UNASSIGNED)
        return data

    def to_post_json(self) -> dict:
        """Dump for POST; omit the sentinel id when unassigned."""
        exclude = {"destination"}
        if self.workflow_id == UNASSIGNED:
            exclude.add("workflow_id")
        return self.model_dump(mode="json", exclude=exclude)

    @classmethod
    def shorthand_map(cls) -> dict[str, str]:
        return dict(cls.SHORTHAND_MAP)

    @classmethod
    def shorthand_inverse_map(cls) -> dict[str, str]:
        return {v: k for k, v in cls.SHORTHAND_MAP.items()}
