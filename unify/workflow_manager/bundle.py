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

    lister: Callable[..., Any] | None = None
    """Reports one source's planted rows on the surfaces where a caller
    needs their ids — tasks, which are what "run this workflow now"
    resolves to. Called with ``managed_by`` and ``destination``. ``None``
    for surfaces whose rows are only ever read where they live."""

    def arm(self, *, managed_by: str, enabled: bool, destination: str | None) -> Any:
        if self.armer is None:
            return None
        return self.armer(
            managed_by=managed_by,
            enabled=enabled,
            destination=destination,
        )

    def planted(self, *, managed_by: str, destination: str | None) -> Any:
        if self.lister is None:
            return None
        return self.lister(managed_by=managed_by, destination=destination)

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
class CanvasSource:
    """One view a bundle ships, as the author wrote it.

    ``name`` is the directory it came from and, with the slug, its stable
    identity across reinstalls — the published row carries
    ``<slug>/<name>`` as its ``custom_key`` so a repeat install revises the
    view it published last time instead of stacking up a second one.
    """

    name: str
    tsx: str
    title: str
    description: str = ""
    bindings: tuple[Dict[str, Any], ...] = ()
    props: Dict[str, Any] = field(default_factory=dict)
    actions: tuple[Dict[str, Any], ...] = ()
    visibility: str = "private"

    @property
    def custom_key(self) -> str:
        return self.name

    def content_hash(self) -> str:
        """Fingerprint of everything a republish would change.

        Compiled output is deliberately excluded: the same source against a
        newer kit is a different bundle, and that is the kit's business to
        decide at build time, not a reason to consider the source changed.
        """
        import hashlib
        import json as _json

        payload = _json.dumps(
            {
                "tsx": self.tsx,
                "title": self.title,
                "description": self.description,
                "bindings": list(self.bindings),
                "props": self.props,
                "actions": list(self.actions),
                "visibility": self.visibility,
            },
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:16]


@dataclass(frozen=True)
class RequirementOption:
    """One app that would satisfy a requirement."""

    slug: str
    name: str = ""

    @property
    def display_name(self) -> str:
        return self.name or self.slug


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

    alternatives: tuple[RequirementOption, ...] = ()
    """Other apps that would satisfy this same requirement, any one of
    them instead of :attr:`slug`.

    A workflow needs a *capability* — somewhere to post, a calendar to
    read — and the app providing it is the user's choice, not the
    bundle's. Declaring Slack and stopping there excludes everyone on
    Discord or Teams from a workflow that would serve them identically.

    Order is the recommendation: :attr:`slug` first, then these. The
    requirement is met as soon as any one of them is connected, and a
    reader offers the whole set so the user connects the one they
    already use. Keep the list short and genuinely interchangeable —
    alternatives the planted procedures can all actually drive."""

    required_secrets: tuple[str, ...] = ()
    """Secret names whose presence marks this requirement connected, for
    apps no other authority can answer for — a BYOD OAuth provider with
    no package and no gallery connection row.

    Leave empty for native packages and gallery apps: the package's own
    manifest already declares its secrets, and restating them here means
    two places to update when the package changes. Empty with no other
    authority reads as met, so a bundle cannot hold its jobs hostage to a
    signal nothing can check."""

    @property
    def options(self) -> tuple[RequirementOption, ...]:
        """Every app that satisfies this requirement, recommended first."""
        return (RequirementOption(self.slug, self.name), *self.alternatives)


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

    capabilities: tuple[str, ...] = ()
    """Assistant capabilities the workflow needs beyond connected apps,
    e.g. ``"computer"`` or ``"filesystem"``. Declared for the catalogue;
    not gated at install."""

    canvas: tuple["CanvasSource", ...] = ()
    """Views this bundle ships as **source**, deliberately outside
    :attr:`surfaces`.

    A canvas is not custom-synced and never will be. It is real TypeScript
    that has to be linted, typechecked, bundled, rendered and reviewed
    against the canvas kit installed *now*, and its routing token has a
    lifecycle the reconcile engine has no business owning. Shipping a
    pre-built bundle instead would pin a host runtime: the host serves
    versioned runtimes, so a view compiled against one kit and planted into
    another breaks at *view* time, for the user, with nothing failing at
    plant time to warn anyone.

    So the install hands this to CanvasManager's own authoring pipeline and
    uninstall deletes through its own delete, which already releases the
    token."""

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
        lister: Callable[..., Any] | None = None,
    ) -> None:
        if not source_scoped:
            raise UnscopedSurfaceError(name)
        self._surfaces[name] = Surface(
            name=name,
            syncer=syncer,
            source_kwarg=source_kwarg,
            shared=shared,
            armer=armer,
            lister=lister,
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
