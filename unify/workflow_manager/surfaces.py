"""Which managers a workflow may plant into.

The list is short on purpose. A surface belongs here only once its
custom-sync adapter scopes ``live_rows`` and its collision probe to
``source_id``; until then the reconcile loop's prune step cannot tell one
source's rows from another's, and the first workflow to sync would delete
the deployment's content. :data:`SOURCE_SCOPED` is the record of which
managers have crossed that line.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

from .bundle import SurfaceRegistry

if TYPE_CHECKING:  # pragma: no cover
    from ..guidance_manager.guidance_manager import GuidanceManager
    from ..knowledge_manager.knowledge_manager import KnowledgeManager
    from ..task_scheduler.task_scheduler import TaskScheduler


SOURCE_SCOPED: Mapping[str, str] = {
    "guidance": "source_guidance",
    "knowledge": "source_claims",
    "tasks": "source_tasks",
}
"""Surface name -> the kwarg its manager's ``sync_custom`` takes.

Adding an entry here is a claim that the manager's adapter is scoped.
Make the adapter change first; this mapping is the consequence, not the
mechanism.
"""

PENDING_SCOPING: tuple[str, ...] = (
    "contacts",
    "secrets",
    "blacklist",
    "data",
    "dashboards",
    "functions",
    "venvs",
    "integration_registry",
)
"""Surfaces workflows cannot reach yet, listed so the gap is visible
rather than merely absent."""


def register_default_surfaces(
    registry: SurfaceRegistry,
    *,
    guidance_manager: "GuidanceManager | None" = None,
    knowledge_manager: "KnowledgeManager | None" = None,
    task_scheduler: "TaskScheduler | None" = None,
) -> SurfaceRegistry:
    """Wire every source-scoped manager that was supplied.

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
        registry.register(
            name,
            manager.sync_custom,
            source_kwarg=SOURCE_SCOPED[name],
            source_scoped=True,
        )
    return registry
