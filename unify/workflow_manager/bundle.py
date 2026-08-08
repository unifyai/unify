"""Workflow bundles and the surfaces they may write to.

A bundle is content plus identity: a slug, a version, and one collected
source dict per surface it plants into. The dicts are already in the
shape the owning manager's ``sync_custom`` takes — ``{custom_key:
{field: value, ...}}`` — so installing is a fan-out of ordinary custom
syncs, not a second reconcile mechanism.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping

from unify.common.custom_sync import MANAGED_BY_DEPLOYMENT

WORKFLOW_LIBRARY = "workflow_library"
"""The ``managed_by`` for rows on shared-identity surfaces.

Functions and venvs key on their name — the call-site contract — so a
name means one row no matter how many workflows reference it. All
installed bundles' entries on such a surface reconcile together as one
union source under this id; per-slug ownership would make one name two
rows. Which workflows reference a row is recorded on the row itself, in
its ``workflows`` field."""

SurfaceSyncer = Callable[..., bool]
"""A manager's per-destination custom sync (``sync_custom_guidance``,
``sync_custom_tasks``, ...), called with its source kwarg, ``managed_by``
and ``destination``.

The destination-grouping wrappers (``sync_custom``) are deliberately not
accepted here: they derive destinations from the entries themselves, so an
empty source never reaches the reconcile engine and an uninstall would
prune nothing while reporting success. The per-destination methods run the
engine unconditionally, which is what makes an empty source a genuine
"remove everything this workflow planted here"."""


class UnscopedSurfaceError(RuntimeError):
    """A surface was registered before its adapter honours ``managed_by``.

    Registering an unscoped surface is not a degraded mode, it is data
    loss: that manager's ``live_rows`` still selects every managed row in
    the context, so the first workflow to sync there prunes the
    deployment's rows, and the deployment's next pass prunes the
    workflow's. The registry refuses rather than letting the fan-out
    reach such a manager.
    """

    def __init__(self, surface: str) -> None:
        self.surface = surface
        super().__init__(
            f"Surface {surface!r} is not managed-scoped; registering it "
            "would let workflows and the deployment prune each other's "
            "rows. Thread managed_by through that manager's adapter first.",
        )


@dataclass(frozen=True)
class Surface:
    """One manager's custom-sync entry point, as a workflow may drive it."""

    name: str
    """Bundle key, e.g. ``"guidance"``."""

    syncer: SurfaceSyncer
    """The manager's per-destination custom sync (see :data:`SurfaceSyncer`)."""

    source_kwarg: str
    """Keyword its collected source arrives on, e.g. ``"source_guidance"``."""

    shared: bool = False
    """Whether this surface's identity is a global natural key shared by
    every workflow (functions, venvs: ``custom_key == name``, the call-site
    contract).

    A shared surface is never synced per slug. Two workflows planting the
    same function under their own ``managed_by`` would be two rows with one
    name — an ambiguous call-site — and any per-slug adoption probe would
    steal the row back and forth between them. Instead every install and
    uninstall re-syncs the **union** of all installed bundles' entries
    under the single :data:`WORKFLOW_LIBRARY` source, so a shared atom is
    one row for as long as any installed workflow references it."""

    armer: Callable[..., Any] | None = None
    """Arms or disarms one source's planted rows, where the surface has
    such a notion — tasks, whose definitions are born disarmed by the
    custom sync and only fire once armed. Called with ``managed_by``,
    ``enabled`` and ``destination`` keywords. ``None`` for surfaces whose
    content is inert until read (guidance, knowledge, functions)."""

    def arm(self, *, managed_by: str, enabled: bool, destination: str | None) -> Any:
        if self.armer is None:
            return None
        return self.armer(
            managed_by=managed_by,
            enabled=enabled,
            destination=destination,
        )

    def sync(
        self,
        source: Mapping[str, Dict[str, Any]],
        *,
        managed_by: str,
        destination: str | None,
    ) -> bool:
        """Run one reconcile pass for *managed_by* at *destination*.

        The installation's destination governs where every entry lands; a
        per-entry ``destination`` field in the source is ignored (the
        adapters drop it in ``transform``). An empty *source* prunes all
        of *managed_by*'s rows at that destination and nothing else.
        """
        return self.syncer(
            **{self.source_kwarg: dict(source)},
            managed_by=managed_by,
            destination=destination,
        )


@dataclass(frozen=True)
class WorkflowRequirement:
    """One integration a workflow needs connected before its jobs may run.

    Requirements are declared and checked, never carried: a bundle ships
    no OAuth connection and no secret value. An unmet requirement does
    not refuse the install — content plants and the workflow's tasks stay
    disarmed until the connection lands.

    A requirement names the app and stops there. Whether that app is a
    third-party provider-backed connection from the gallery's catalogue,
    a native integration package declared in unify-deploy, or a BYOD
    OAuth provider is not the bundle's business: it differs per app, it
    can change without the workflow changing, and an app may offer more
    than one route at once. Resolution is
    :class:`unify.workflow_manager.requirements.RequirementResolver`.
    """

    slug: str
    """Provider app slug — the id space shared by Console's integrations
    gallery, ``app_slug`` in the integrations primitives, and native
    package manifests (e.g. ``"gmail"``, ``"hubspot"``). Not the OAuth
    provider alias space in ``runtime_oauth`` (where Gmail's connection is
    ``"google"``), because that space is invisible to the gallery."""

    name: str = ""
    """Display name; falls back to the slug."""

    kind: str = "app"
    """``"app"`` for anything the integrations layer connects, ``"workspace"``
    for the user's Workspace.

    Workspace is deliberately its own kind rather than an app with a special
    slug: it is not in the gallery, it is not a package, and it is connected
    somewhere else entirely — the onboarding and profile flows own it. Treating
    it as an app is what led to a name-matching table deciding that anything
    starting with "GMAIL" or "GOOGLE" wanted a pasted refresh token."""

    required_secrets: tuple[str, ...] = ()
    """Secret names whose presence marks this requirement connected, for
    apps no other authority can answer for — a BYOD OAuth provider with
    no package and no gallery connection row.

    Leave empty for native packages and gallery apps: the package's own
    manifest already declares its secrets, and restating them here means
    two places to update when the package changes. Empty with no other
    authority reads as met, so a bundle cannot hold its jobs hostage to a
    signal nothing can check."""


@dataclass
class WorkflowBundle:
    """A workflow's identity and its per-surface content."""

    slug: str
    name: str
    version: str = ""
    description: str = ""
    """One line for a card or a list row."""

    about: str = ""
    """Long-form markdown for a reader deciding whether to install: what
    the workflow does, when it runs, what arrives, and how its settings
    shape it. The ``description`` is the card; this is the page."""

    category: str = ""
    """Catalogue grouping, e.g. ``"comms"`` / ``"growth"`` / ``"ops"`` /
    ``"build"``."""

    icon_id: str = ""
    """Key into the console's workflow tile icon set."""

    surfaces: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)
    """Surface name -> collected source, keyed by ``custom_key``."""

    params_schema: Dict[str, Any] = field(default_factory=dict)
    """Declared install-time settings. Values are supplied per install and
    read at run time; they never enter the collected sources above."""

    requirements: tuple[WorkflowRequirement, ...] = ()
    """Integrations that must be connected before this workflow's jobs
    are armed. Checked at install and on every reconcile."""

    install_task: str = ""
    """``custom_key`` of a task in this bundle to run **once**, after content
    lands on a first install.

    The provisioning one-shot: a mailbox backfill, a CRM import — the long
    thing a workflow needs done before its recurring job is meaningful. Named
    rather than inferred so a bundle can ship several tasks and be explicit
    about which one is setup. Empty means the workflow needs no provisioning.

    Deliberately an ordinary task rather than new machinery: durability,
    resumability, steering and observability come from TaskScheduler, and the
    workflow/task boundary stays intact — a workflow still has no runtime, it
    just asked for a task to be run."""

    capabilities: tuple[str, ...] = ()
    """Assistant capabilities the workflow needs beyond connected apps,
    e.g. ``"computer"`` or ``"filesystem"``. Declared for the catalogue;
    not gated at install."""

    def __post_init__(self) -> None:
        if not self.slug:
            raise ValueError("A workflow bundle needs a slug.")
        if self.slug in (MANAGED_BY_DEPLOYMENT, WORKFLOW_LIBRARY):
            raise ValueError(
                f"{self.slug!r} is a reserved managed_by value; a bundle "
                "may not claim it.",
            )

    def surface_names(self) -> list[str]:
        return sorted(self.surfaces)


class SurfaceRegistry:
    """The surfaces a workflow install is allowed to fan out to.

    Deliberately explicit. A manager appears here only once its adapter
    scopes ``live_rows`` and its collision probe to ``managed_by``; until
    then :class:`UnscopedSurfaceError` keeps workflows away from it.
    """

    def __init__(self) -> None:
        self._surfaces: Dict[str, Surface] = {}

    def register(
        self,
        name: str,
        syncer: SurfaceSyncer,
        *,
        source_kwarg: str,
        source_scoped: bool,
        shared: bool = False,
        armer: Callable[..., Any] | None = None,
    ) -> None:
        if not source_scoped:
            raise UnscopedSurfaceError(name)
        self._surfaces[name] = Surface(
            name=name,
            syncer=syncer,
            source_kwarg=source_kwarg,
            shared=shared,
            armer=armer,
        )

    def get(self, name: str) -> Surface:
        if name not in self._surfaces:
            raise KeyError(
                f"No surface {name!r} is registered; workflows may write to "
                f"{sorted(self._surfaces) or 'nothing'}.",
            )
        return self._surfaces[name]

    def names(self) -> list[str]:
        return sorted(self._surfaces)

    def __contains__(self, name: object) -> bool:
        return name in self._surfaces
