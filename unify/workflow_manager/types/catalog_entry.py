"""Pydantic model for the ``Workflows/Catalog`` context.

The catalogue lives on disk, hand-curated in git, and is loaded into the
assistant's memory at boot. These rows are its **published mirror**, so a
surface that is not the assistant — Console's shelf — can render what is
installable without waking one.

That mirror matters because a hosted assistant is an on-demand job: it is
usually asleep when someone opens Console. Asking it for its catalogue
would mean starting a job to draw a gallery. Reading rows is what Console
already does for every other manager.

The rows are deliberately per-assistant rather than one platform-wide
copy: they mirror *what this deployment shipped*, so a client whose
deployment carries a sector-specific shelf sees exactly that shelf, with
no extra machinery.
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from unify.common.authorship import AuthoredRow


class WorkflowCatalogEntry(AuthoredRow):
    """One installable workflow, as published for reading surfaces.

    Everything here is derived from the bundle on disk; nothing is
    authored in this context. A reconcile pass rewrites it wholesale, so
    editing a row by hand changes nothing that survives the next boot.
    """

    SHORTHAND_MAP: ClassVar[dict[str, str]] = {
        "slug": "s",
        "name": "n",
        "version": "v",
        "category": "c",
        "icon_id": "i",
        "description": "d",
        "requirements": "rq",
        "capabilities": "cp",
        "params_schema": "ps",
        "surfaces": "sf",
        "sets": "st",
    }

    slug: str = Field(
        description=(
            "Stable bundle identifier and the row's identity, e.g. " "'daily_briefing'."
        ),
    )
    name: str = Field(description="Human-readable workflow name.")
    version: str = Field(default="", description="Catalogue version on disk.")
    category: str = Field(
        default="",
        description="Shelf grouping, e.g. 'comms' / 'growth' / 'ops' / 'build'.",
    )
    icon_id: str = Field(
        default="",
        description="Key into the reading surface's workflow tile icon set.",
    )
    description: str = Field(
        default="",
        description="One line describing what the workflow does.",
    )
    requirements: str = Field(
        default="[]",
        description=(
            "JSON array of declared integrations, each with 'slug' (a "
            "provider app slug) and 'name'. Connection state is NOT stored "
            "here: it changes without the catalogue changing, so a reader "
            "resolves it against the integrations it already knows about."
        ),
    )
    capabilities: str = Field(
        default="[]",
        description=(
            "JSON array of assistant capabilities the workflow needs beyond "
            "connected apps, e.g. 'computer' or 'filesystem'."
        ),
    )
    params_schema: str = Field(
        default="{}",
        description=(
            "JSON object of install-time settings the bundle declares, so a "
            "reader can render the settings form before installing."
        ),
    )
    surfaces: str = Field(
        default="[]",
        description="JSON array of surface names this workflow plants into.",
    )
    sets: str = Field(
        default="{}",
        description=(
            "JSON object of what the workflow sets up, per surface: a list "
            "of entry names, and for tasks a human schedule. This is what "
            "lets a reader show 'what this installs' before it is installed."
        ),
    )

    @classmethod
    def shorthand_map(cls) -> dict[str, str]:
        return dict(cls.SHORTHAND_MAP)

    @classmethod
    def shorthand_inverse_map(cls) -> dict[str, str]:
        return {v: k for k, v in cls.SHORTHAND_MAP.items()}
