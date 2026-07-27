"""Canvas record types.

A canvas is one whole view: a single TSX module that renders the entire surface.
Unlike dashboard tiles there is no separate layout record — React composes, so
the grid, the placement and the responsive behaviour all live in the authored
code. That removes a table, the twelve-column placement model, and the class of
bug where deleting a tile leaves a dashboard pointing at nothing.
"""

from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from unify.common.authorship import AuthoredRow

# Who may open a canvas.
#   private     — the owning assistant's workspace only
#   team        — everyone in the team the canvas was written to
#   public_link — anyone holding the token
Visibility = Literal["private", "team", "public_link"]

# Lifecycle. ``draft`` exists so a canvas can be rendered for review before it
# is servable; ``quarantined`` is the kill switch that stops the bundle, its
# queries and its actions being served without deleting anything.
Status = Literal["draft", "published", "quarantined"]


class BuildReport(BaseModel):
    """Outcome of compiling one canvas.

    A failure here blocks publication, so this is the actor's feedback channel
    for authoring mistakes: ``diagnostics`` carries the compiler's own messages
    rather than a summary, since they are what makes the error correctable.
    """

    ok: bool
    kit_version: str = ""
    bundle_sha: str = ""
    bytes: int = 0
    duration_ms: int = 0
    # Which gate failed, when one did: "lint", "typecheck", "bundle", "render".
    failed_stage: Optional[str] = None
    diagnostics: List[str] = Field(default_factory=list)


class ReviewReport(BaseModel):
    """Outcome of rendering a canvas headlessly and looking at it.

    ``rendered`` is a hard gate — code that compiles but throws on mount is not
    publishable. ``verdict`` and ``issues`` come from a vision model and are
    advisory: they are surfaced to the actor, which decides whether to revise.
    """

    rendered: bool
    # Screenshot paths, light theme then dark. Fed back into the actor's
    # transcript so it sees what it built rather than guessing.
    screenshots: List[str] = Field(default_factory=list)
    verdict: str = ""
    issues: List[str] = Field(default_factory=list)
    error: Optional[str] = None


class CanvasViewRow(AuthoredRow):
    """Row stored in the ``Canvas/Views`` context.

    ``canvas_id`` is deliberately absent: the backend auto-counts it, and
    including it here would let a caller try to set it.
    """

    model_config = ConfigDict(extra="forbid")

    token: str = Field(description="Twelve-character URL-safe identifier.")
    title: str = Field(json_schema_extra={"ui_editable": True})
    description: Optional[str] = Field(
        default=None,
        json_schema_extra={"ui_editable": True},
    )

    # Authored source, retained so the canvas can be revised and so a user can
    # see what the assistant actually wrote on their behalf.
    tsx_source: str = ""

    # Content address of the compiled module, and where it lives. The bundle is
    # private: console fetches it server-side and verifies this hash before
    # handing the bytes to the frame, which is a stronger integrity guarantee
    # than subresource integrity because we enforce it ourselves.
    bundle_sha: str = ""
    bundle_uri: str = ""
    # Kit the bundle was compiled against, so an old canvas keeps rendering on
    # the runtime it was reviewed against rather than silently upgrading.
    kit_version: str = ""

    # Serialised PrimitiveBinding list with context paths already resolved.
    bindings_json: Optional[str] = None
    # Comma-joined resolved contexts, for auditing what a canvas can read.
    binding_contexts: Optional[str] = None
    # Values materialised at author time, for reads too expensive or too
    # LLM-shaped to run per view.
    props_json: Optional[str] = None

    visibility: str = "private"
    status: str = "draft"

    preview_image_path: Optional[str] = None
    build_json: Optional[str] = None

    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    # Deployment-defined canvases: merge key and content hash, matching the
    # convention the other managers use for source-seeded rows.
    custom_key: Optional[str] = None
    custom_hash: Optional[str] = None


class CanvasViewRecord(CanvasViewRow):
    """A stored canvas, as read back."""

    canvas_id: Optional[int] = None


class CanvasResult(BaseModel):
    """What ``create_view`` / ``update_view`` hand back to the actor.

    Carries the shareable URL plus everything needed to decide what to do next:
    whether the build passed, what the compiler said if not, and what the canvas
    actually looked like when rendered.
    """

    token: str = ""
    url: str = ""
    title: str = ""
    status: str = "draft"
    build: Optional[BuildReport] = None
    review: Optional[ReviewReport] = None
    # Set when the call failed outright; the actor should read this first.
    error: Optional[str] = None
