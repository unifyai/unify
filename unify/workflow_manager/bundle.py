"""Workflow bundles and the surfaces they may write to.

A bundle is content plus identity: a slug, a version, and one collected
source dict per surface it plants into. The dicts are already in the
shape the owning manager's ``sync_custom`` takes — ``{custom_key:
{field: value, ...}}`` — so installing is a fan-out of ordinary custom
syncs, not a second reconcile mechanism.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping

from unify.workflow_manager.types.workflow import WorkflowMode

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


@dataclass
class WorkflowBundle:
    """A workflow's identity and its per-surface content."""

    slug: str
    name: str
    version: str = ""
    description: str = ""
    mode: WorkflowMode = WorkflowMode.seed
    surfaces: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)
    """Surface name -> collected source, keyed by ``custom_key``."""

    params_schema: Dict[str, Any] = field(default_factory=dict)
    """Declared install-time settings. Values are supplied per install and
    read at run time; they never enter the collected sources above."""

    def __post_init__(self) -> None:
        if not self.slug:
            raise ValueError("A workflow bundle needs a slug.")
        if self.slug == "deployment":
            raise ValueError(
                "'deployment' is the reserved managed_by for the assistant's "
                "own sources; a bundle may not claim it.",
            )

    def content_hash(self) -> str:
        """Fingerprint of everything this bundle would plant.

        Covers the collected sources and the version, so a bundle whose
        content is unchanged short-circuits its whole fan-out. Params are
        excluded by construction: they are not part of what gets planted,
        and folding them in would give two installations of one bundle
        different hashes for identical rows.
        """

        payload = json.dumps(
            {"version": self.version, "surfaces": self.surfaces},
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode()).hexdigest()

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
    ) -> None:
        if not source_scoped:
            raise UnscopedSurfaceError(name)
        self._surfaces[name] = Surface(
            name=name,
            syncer=syncer,
            source_kwarg=source_kwarg,
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
