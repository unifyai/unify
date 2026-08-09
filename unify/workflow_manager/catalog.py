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

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from .bundle import CanvasSource, WorkflowBundle, WorkflowRequirement

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
                kind=str(entry.get("kind", "app")),
                required_secrets=tuple(entry.get("required_secrets") or ()),
            ),
        )
    return tuple(requirements)


def _collect_surfaces(path: Path) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Collect every content directory present, using the deployment
    collectors so hashes and field shapes match the reconcile's."""
    from ..data_manager.custom_data import collect_custom_data
    from ..function_manager.custom_functions import collect_custom_functions
    from ..guidance_manager.custom_guidance import collect_custom_guidance
    from ..knowledge_manager.custom_knowledge import collect_custom_knowledge
    from ..task_scheduler.custom_tasks import collect_custom_tasks

    collected = {
        "guidance": collect_custom_guidance(path=path / "guidance"),
        "knowledge": collect_custom_knowledge(path=path / "knowledge"),
        "tasks": collect_custom_tasks(path=path / "tasks"),
        "functions": collect_custom_functions(directory=path / "functions"),
        # Tables a bundle declares, schemas only: a workflow ships the shape
        # its own job fills, never the contents. Seeded rows in a bundle
        # would be one deployment's data published to everyone.
        "data": _bundle_tables(collect_custom_data(path=path / "data"), path=path),
    }
    return {name: source for name, source in collected.items() if source}


def _bundle_tables(
    tables: Dict[str, Dict[str, Any]],
    *,
    path: Path,
) -> Dict[str, Dict[str, Any]]:
    """Normalise a bundle's declared tables, and refuse seeded rows.

    Two rules, both made structural rather than left to be remembered.

    **Schemas only.** A bundle is published verbatim to the public-read
    Builtins project and installed identically by everyone, so rows in it
    are one author's data handed to every installer. The table is the
    contract; filling it is the workflow's own job at run time.

    **Data-owned contexts.** ``data/Finance/Invoices`` becomes
    ``Data/Finance/Invoices``. A context outside the Data namespace cannot
    be installed to a team — the write refuses the destination — and cannot
    be read by a canvas, whose policy declares ``Data`` and nothing else.
    Both failures land a long way from the author, so the prefix is applied
    here instead.
    """
    seeded = sorted(context for context, spec in tables.items() if spec.get("rows"))
    if seeded:
        raise ValueError(
            f"{path.name}: data tables {', '.join(seeded)} ship seeded rows. "
            "A bundle declares table schemas only — the workflow's own job "
            "fills them. Delete the rows.jsonl.",
        )

    normalised: Dict[str, Dict[str, Any]] = {}
    for context, spec in tables.items():
        owned = context if context.startswith("Data/") else f"Data/{context}"
        normalised[owned] = {**spec, "context": owned}
    return normalised


CANVAS_SOURCE_FILENAME = "view.tsx"
CANVAS_MANIFEST_FILENAME = "view.json"


def _collect_canvas(path: Path) -> tuple[CanvasSource, ...]:
    """Load the views a bundle ships, as source.

    Deliberately not a collected "surface": these never reach the reconcile
    engine. A directory needs both ``view.tsx`` and ``view.json`` — source
    with no manifest has no title to publish under and no bindings to read,
    and a manifest with no source has nothing to compile, so either alone
    is an authoring mistake worth naming rather than skipping.
    """
    root = path / "canvas"
    if not root.is_dir():
        return ()

    views: List[CanvasSource] = []
    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        tsx_path = entry / CANVAS_SOURCE_FILENAME
        manifest_path = entry / CANVAS_MANIFEST_FILENAME
        if not tsx_path.is_file() and not manifest_path.is_file():
            continue
        if not tsx_path.is_file():
            raise ValueError(
                f"{path.name}: canvas {entry.name!r} has no {CANVAS_SOURCE_FILENAME}.",
            )
        if not manifest_path.is_file():
            raise ValueError(
                f"{path.name}: canvas {entry.name!r} has no "
                f"{CANVAS_MANIFEST_FILENAME}; a view needs a title to publish "
                "under and its bindings declared.",
            )
        manifest = json.loads(manifest_path.read_text()) or {}
        title = str(manifest.get("title") or "").strip()
        if not title:
            raise ValueError(
                f"{path.name}: canvas {entry.name!r} declares no title.",
            )
        # A pre-built bundle in a bundle pins a host runtime — it compiles
        # against one kit and is planted into whatever host the deployment
        # runs, so it breaks at view time with nothing failing at plant
        # time. Refuse it where it is written rather than at someone's
        # install.
        for built in ("bundle.js", "bundle.mjs", "view.js"):
            if (entry / built).exists():
                raise ValueError(
                    f"{path.name}: canvas {entry.name!r} ships a built "
                    f"{built}. Bundles ship source; the install compiles it "
                    "against the kit that is actually installed.",
                )
        views.append(
            CanvasSource(
                name=entry.name,
                tsx=tsx_path.read_text(),
                title=title,
                description=str(manifest.get("description") or ""),
                bindings=tuple(manifest.get("bindings") or ()),
                props=dict(manifest.get("props") or {}),
                actions=tuple(manifest.get("actions") or ()),
                visibility=str(manifest.get("visibility") or "private"),
            ),
        )
    return tuple(views)


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
        install_task=str(manifest.get("install_task", "")),
        capabilities=tuple(manifest.get("capabilities") or ()),
        canvas=_collect_canvas(path),
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


def resolve_catalogue_root() -> Optional[Path]:
    """Where this environment's curated bundles live, or None if nowhere.

    One resolution, used by everything that needs the shelf. It used to be
    written twice: the seeder fell back to the installed ``unify_deploy``
    package when ``UNITY_WORKFLOWS_DIR`` was unset, and the runtime registry
    returned None instead. So a deployment with the package installed and no
    env var published six workflows to the catalogue Console renders, while
    the assistant that has to install them registered none — the shelf was
    visibly full and every install failed with "the catalogue is empty".

    Returns None only when there is genuinely nothing to read, which callers
    must treat as "no catalogue here", never as "the catalogue is empty".
    """
    from ..settings import SETTINGS

    configured = (SETTINGS.UNITY_WORKFLOWS_DIR or "").strip()
    if configured:
        root = Path(configured)
    else:
        try:
            from unify_deploy.assistant_deployments.workflows import workflows_root

            root = workflows_root()
        except ImportError:
            return None

    if not root.is_dir():
        logger.warning("Workflow catalogue root %s does not exist", root)
        return None
    return root


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
    if root is None:
        root = resolve_catalogue_root()
        if root is None:
            return None
    elif not root.is_dir():
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
        data_manager=ManagerRegistry.get_data_manager(),
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

    # The sweep behind the wake dispatch: a request recorded while this
    # assistant was asleep — or whose dispatch never arrived — is carried
    # out here. Dispatch is an optimisation for latency; this is what makes
    # the request contract durable.
    try:
        settled = manager.execute_requests().get("settled") or {}
        if settled:
            logger.info("Workflow requests settled at boot: %s", settled)
    except Exception:
        logger.exception("Workflow request sweep at boot failed; will retry next boot")

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
