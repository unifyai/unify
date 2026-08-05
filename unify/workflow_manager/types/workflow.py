"""Pydantic model for the ``Workflows`` context."""

from __future__ import annotations

from enum import Enum
from typing import ClassVar

from pydantic import Field, model_validator

from unify.common.authorship import AuthoredRow

UNASSIGNED = -1


class WorkflowMode(str, Enum):
    """How long a workflow keeps ownership of the rows it planted.

    The mode is policy, not provenance. Every row a workflow writes
    carries its ``managed_by`` under either mode, so "what did this
    workflow plant?" always has an answer. What the mode decides is
    whether the bundle keeps reconciling those rows.
    """

    pinned = "pinned"
    """Reconciled on every pass. Source supremacy applies: local edits to
    a planted row are overwritten from the bundle, and a row whose key
    left the bundle is pruned. Use for content the workflow must be able
    to fix or evolve centrally."""

    seed = "seed"
    """Reconciled exactly once, at install. The rows are then left alone
    forever — later passes skip the bundle entirely, so edits survive and
    a key leaving the bundle prunes nothing. Use for a starting point the
    user is meant to grow past."""


class WorkflowInstallation(AuthoredRow):
    """One installed workflow bundle.

    The row is the installation, not the bundle: it records which bundle
    was installed, at which version, under which mode, and which surfaces
    it wrote to. The bundle's *content* lives in the surfaces themselves
    (guidance rows, task rows, ...), each stamped with this row's
    :attr:`slug` as its ``managed_by``.
    """

    SHORTHAND_MAP: ClassVar[dict[str, str]] = {
        "workflow_id": "wid",
        "slug": "s",
        "name": "n",
        "version": "v",
        "mode": "m",
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
    mode: WorkflowMode = Field(
        default=WorkflowMode.seed,
        description=(
            "'pinned' to keep reconciling the planted rows from the "
            "bundle, 'seed' to plant once and leave them to the user."
        ),
    )
    status: str = Field(
        default="installed",
        description=(
            "'installed' when the last reconcile completed, 'partial' when "
            "some entries failed and the next pass will retry them."
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
