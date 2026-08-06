"""Pydantic model for the ``Workflows/Content`` context.

One row per artifact a workflow would plant, published beside the
catalogue in the public-read Builtins project. The catalogue row's
``sets`` names what a bundle sets up; these rows carry the artifacts
themselves — a procedure's text, a claim's statement, a task's brief, a
function's docstring — so a reading surface can show any of them before
anything is installed, without waking an assistant.

Like the catalogue, everything here is derived from the bundle on disk
and rewritten wholesale by the seed; nothing is authored in this
context, and installed assistants never read it — their planted rows are
the live copies.
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from unify.common.authorship import AuthoredRow


class WorkflowContentEntry(AuthoredRow):
    """One bundled artifact, as published for reading surfaces."""

    SHORTHAND_MAP: ClassVar[dict[str, str]] = {
        "content_key": "ck",
        "slug": "s",
        "surface": "sf",
        "key": "k",
        "name": "n",
        "body": "b",
        "schedule": "sc",
        "meta": "m",
    }

    content_key: str = Field(
        description=(
            "The row's identity: '<slug>/<surface>/<key>', e.g. "
            "'daily_briefing/guidance/daily_briefing/compose'."
        ),
    )
    slug: str = Field(description="The workflow this artifact belongs to.")
    surface: str = Field(
        description=(
            "The bundle surface the artifact plants into: 'guidance', "
            "'knowledge', 'tasks' or 'functions'."
        ),
    )
    key: str = Field(
        description="The artifact's own key within its surface.",
    )
    name: str = Field(description="Human-readable artifact title.")
    body: str = Field(
        default="",
        description=(
            "The artifact's readable substance: a procedure's or claim's "
            "content, a task's brief, a function's docstring."
        ),
    )
    schedule: str = Field(
        default="",
        description="Plain-language cadence, for tasks that have one.",
    )
    meta: str = Field(
        default="{}",
        description=(
            "JSON object of surface-specific extras a reader may render: "
            "a claim's kind and topics, a task's tags, a function's "
            "argspec."
        ),
    )

    @classmethod
    def shorthand_map(cls) -> dict[str, str]:
        return dict(cls.SHORTHAND_MAP)

    @classmethod
    def shorthand_inverse_map(cls) -> dict[str, str]:
        return {v: k for k, v in cls.SHORTHAND_MAP.items()}
