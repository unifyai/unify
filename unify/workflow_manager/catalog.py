"""The on-disk workflow catalogue and its boot-time registration.

Curated workflow bundles live in git as one directory per workflow,
following the integration-package layout:

.. code-block:: text

    <workflows_root>/
      daily_briefing/
        manifest.yaml            # identity, requirements, params schema
        guidance/guidance.jsonl  # procedures
        knowledge/knowledge.jsonl
        tasks/tasks.jsonl        # recurring jobs (planted disarmed)
        functions/*.py           # @custom_function atoms
        venvs/*.toml

The loader turns each directory into a :class:`WorkflowBundle` using the
same collectors the deployment sync uses, so a bundle's content is
collector-shaped by construction and its hashes match what a reconcile
expects. Authoring is git-only: there is no runtime write path into the
catalogue.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from .bundle import WorkflowBundle, WorkflowRequirement

logger = logging.getLogger(__name__)

MANIFEST_FILENAME = "manifest.yaml"


def _parse_requirements(raw: Any, *, slug: str) -> tuple[WorkflowRequirement, ...]:
    if not raw:
        return ()
    requirements: List[WorkflowRequirement] = []
    for entry in raw:
        if isinstance(entry, str):
            requirements.append(WorkflowRequirement(slug=entry))
            continue
        if not isinstance(entry, dict) or not entry.get("slug"):
            raise ValueError(
                f"Workflow {slug!r}: each requirement is a slug string or a "
                f"mapping with at least 'slug'; got {entry!r}.",
            )
        requirements.append(
            WorkflowRequirement(
                slug=str(entry["slug"]),
                name=str(entry.get("name", "")),
                required_secrets=tuple(entry.get("required_secrets") or ()),
            ),
        )
    return tuple(requirements)


def _collect_surfaces(path: Path) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Collect every content directory present, using the deployment
    collectors so hashes and field shapes match the reconcile's."""
    from ..function_manager.custom_functions import collect_custom_functions
    from ..guidance_manager.custom_guidance import collect_custom_guidance
    from ..knowledge_manager.custom_knowledge import collect_custom_knowledge
    from ..task_scheduler.custom_tasks import collect_custom_tasks

    collected = {
        "guidance": collect_custom_guidance(path=path / "guidance"),
        "knowledge": collect_custom_knowledge(path=path / "knowledge"),
        "tasks": collect_custom_tasks(path=path / "tasks"),
        "functions": collect_custom_functions(directory=path / "functions"),
    }
    return {name: source for name, source in collected.items() if source}


def load_bundle(path: Path) -> WorkflowBundle:
    """Load one workflow bundle from its directory.

    The manifest's ``slug`` must match the directory name — the slug is
    stamped as ``managed_by`` on every planted row, so a rename is an
    identity change for every installation and must never happen by
    moving a directory.
    """
    manifest_path = path / MANIFEST_FILENAME
    manifest = yaml.safe_load(manifest_path.read_text()) or {}

    slug = str(manifest.get("slug") or "")
    if not slug:
        raise ValueError(f"{manifest_path}: manifest needs a 'slug'.")
    if slug != path.name:
        raise ValueError(
            f"{manifest_path}: slug {slug!r} does not match directory "
            f"{path.name!r}. The slug is the identity stamped on every "
            "planted row; renaming is an identity migration, not a move.",
        )
    name = str(manifest.get("name") or "")
    if not name:
        raise ValueError(f"{manifest_path}: manifest needs a 'name'.")

    return WorkflowBundle(
        slug=slug,
        name=name,
        version=str(manifest.get("version", "")),
        description=str(manifest.get("description", "")),
        about=str(manifest.get("about", "")),
        category=str(manifest.get("category", "")),
        icon_id=str(manifest.get("icon_id", "")),
        surfaces=_collect_surfaces(path),
        params_schema=dict(manifest.get("params_schema") or {}),
        requirements=_parse_requirements(
            manifest.get("requirements"),
            slug=slug,
        ),
        capabilities=tuple(manifest.get("capabilities") or ()),
    )


def load_catalog(root: Path) -> List[WorkflowBundle]:
    """Load every bundle under *root*, sorted by slug.

    Strict: a malformed bundle raises rather than vanishing from the
    shelf. Boot-time callers isolate per-bundle failures themselves so
    one bad bundle cannot empty the whole catalogue.
    """
    if not root.is_dir():
        return []
    bundles: List[WorkflowBundle] = []
    for entry in sorted(root.iterdir()):
        if entry.is_dir() and (entry / MANIFEST_FILENAME).exists():
            bundles.append(load_bundle(entry))
    return bundles


def bootstrap_workflow_catalog(
    root: Optional[Path] = None,
) -> "Optional[Any]":
    """Build the WorkflowManager, wire its surfaces, and fill the catalogue.

    The production caller: run after ``ContextRegistry.setup`` so manager
    constructions can resolve their contexts. Resolves the catalogue root
    from ``UNITY_WORKFLOWS_DIR`` when not given; with no root configured
    there is no catalogue and nothing is built.

    Per-bundle failures are isolated: a malformed bundle is logged and
    skipped so the rest of the shelf still registers. After registration
    the installed workflows are reconciled to the loaded catalogue — this
    is the upkeep tick, so a version bump shipped in git reaches existing
    installations on the next session start.
    """
    from ..settings import SETTINGS

    if root is None:
        configured = (SETTINGS.UNITY_WORKFLOWS_DIR or "").strip()
        if not configured:
            return None
        root = Path(configured)
    if not root.is_dir():
        logger.warning("Workflow catalogue root %s does not exist", root)
        return None

    from ..manager_registry import ManagerRegistry
    from .surfaces import register_default_surfaces
    from .workflow_manager import WorkflowManager

    manager = WorkflowManager()
    register_default_surfaces(
        manager.surfaces,
        guidance_manager=ManagerRegistry.get_guidance_manager(),
        knowledge_manager=ManagerRegistry.get_knowledge_manager(),
        task_scheduler=ManagerRegistry.get_task_scheduler(),
        function_manager=ManagerRegistry.get_function_manager(),
    )

    for entry in sorted(root.iterdir()):
        if not entry.is_dir() or not (entry / MANIFEST_FILENAME).exists():
            continue
        try:
            manager.register_bundle(load_bundle(entry))
        except Exception:
            logger.exception("Skipping malformed workflow bundle at %s", entry)

    # The shelf itself is NOT published here: the catalogue listing is
    # platform data in the public-read Builtins project, seeded by admin
    # processes (see builtins_catalog.seed_builtin_workflows). An assistant
    # key cannot write Builtins and must not try.

    try:
        report = manager.reconcile_installed()
        if report.get("reconciled") or report.get("orphaned"):
            logger.info("Workflow reconcile at boot: %s", report)
    except Exception:
        logger.exception("Workflow reconcile at boot failed; will retry next boot")

    return manager


def schedule_bootstrap_workflow_catalog() -> None:
    """Run :func:`bootstrap_workflow_catalog` on a single daemon thread.

    Same contract as the integrations registration it runs beside: the
    caller is ``unify.__init__`` after ``ContextRegistry.setup``, the
    startup path stays non-blocking, the worker re-applies the calling
    thread's Unify active context, and spawning failures are logged and
    never propagate. Until the worker completes, workflow reads see the
    catalogue empty and fall through to installed-only behaviour.
    """
    import threading

    from ..settings import SETTINGS

    if not (SETTINGS.UNITY_WORKFLOWS_DIR or "").strip():
        return

    try:
        import unisdk

        captured_ctx = unisdk.get_active_context()
    except Exception:
        captured_ctx = None

    def _worker() -> None:
        if captured_ctx is not None:
            try:
                import unisdk

                unisdk.set_context(captured_ctx["read"], skip_create=True)
            except Exception:
                logger.exception(
                    "Workflow catalogue worker could not re-apply context",
                )
                return
        try:
            bootstrap_workflow_catalog()
        except Exception:
            logger.exception("Workflow catalogue bootstrap failed")

    try:
        threading.Thread(
            target=_worker,
            name="workflow-catalog-bootstrap",
            daemon=True,
        ).start()
    except Exception:
        logger.exception("Could not schedule the workflow catalogue bootstrap")
