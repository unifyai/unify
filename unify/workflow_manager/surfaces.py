"""Which managers a workflow may plant into.

The list is short on purpose. A surface belongs here only once its
custom-sync adapter scopes ``live_rows`` and its collision probe to
``managed_by``; until then the reconcile loop's prune step cannot tell one
source's rows from another's, and the first workflow to sync would delete
the deployment's content. :data:`SCOPED_SURFACES` is the record of which
managers have crossed that line.

Workflows register the **per-destination** sync methods, never the
destination-grouping ``sync_custom`` wrappers. The wrappers derive the
destinations to touch from the entries themselves, so an empty source
resolves to zero destinations and never reaches the engine — uninstall
would prune nothing. The per-destination methods run one engine pass
unconditionally against the destination the installation chose.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

from .bundle import SurfaceRegistry

if TYPE_CHECKING:  # pragma: no cover
    from ..guidance_manager.guidance_manager import GuidanceManager
    from ..knowledge_manager.knowledge_manager import KnowledgeManager
    from ..task_scheduler.task_scheduler import TaskScheduler


@dataclass(frozen=True)
class SurfaceSpec:
    """How one manager's per-destination sync is reached."""

    method: str
    """Attribute name of the per-destination sync on the manager."""

    source_kwarg: str
    """Keyword its collected source arrives on."""


SCOPED_SURFACES: Mapping[str, SurfaceSpec] = {
    "guidance": SurfaceSpec("sync_custom_guidance", "source_guidance"),
    "knowledge": SurfaceSpec("sync_custom_knowledge", "source_claims"),
    "tasks": SurfaceSpec("sync_custom_tasks", "source_tasks"),
}
"""Surface name -> the manager method a workflow install drives.

Adding an entry here is a claim that the manager's adapter is scoped.
Make the adapter change first; this mapping is the consequence, not the
mechanism.
"""

PENDING_SCOPING: tuple[str, ...] = (
    "data",
    "canvas",
    "functions",
    "venvs",
)
"""Surfaces workflows will reach once their adapters are scoped, listed so
the gap is visible rather than merely absent.

Deliberately absent rather than pending: contacts, transcripts and
blacklist are populated at runtime by a workflow's own functions, never
pre-seeded from a bundle; secrets and integrations enter a bundle as
declared requirements, never as content; dashboards are deprecated in
favour of canvas.
"""


def register_default_surfaces(
    registry: SurfaceRegistry,
    *,
    guidance_manager: "GuidanceManager | None" = None,
    knowledge_manager: "KnowledgeManager | None" = None,
    task_scheduler: "TaskScheduler | None" = None,
) -> SurfaceRegistry:
    """Wire every managed-scoped manager that was supplied.

    Managers passed as ``None`` are skipped, so a caller holding only some
    of them gets a registry covering exactly those.
    """

    managers: dict[str, Any] = {
        "guidance": guidance_manager,
        "knowledge": knowledge_manager,
        "tasks": task_scheduler,
    }
    for name, manager in managers.items():
        if manager is None:
            continue
        spec = SCOPED_SURFACES[name]
        registry.register(
            name,
            getattr(manager, spec.method),
            source_kwarg=spec.source_kwarg,
            source_scoped=True,
        )
    return registry
