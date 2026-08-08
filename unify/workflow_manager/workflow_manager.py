"""Installable workflow bundles.

Installing a workflow is a fan-out of ordinary custom syncs, one per
surface the bundle covers. Per-slug surfaces (guidance, knowledge,
tasks) are stamped with the bundle's slug as their ``managed_by``;
shared-identity surfaces (functions) re-sync the union of every
installed bundle's entries under the single ``WORKFLOW_LIBRARY`` source,
with each row's ``workflows`` field recording which installs reference
it. That is the whole mechanism: there is no second reconcile loop, no
parallel store for workflow content, and no lookup path that knows about
workflows. What the stamps buy is provenance — the ability to ask a
surface "which rows did this bundle plant?" and get an exact answer,
which is what makes update and uninstall possible at all.
"""

from __future__ import annotations

import functools
import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

import unisdk

from ..common.authorship import strip_authoring_assistant_id
from ..common.context_registry import ContextRegistry, TableContext
from ..common.custom_sync import CustomSyncPartialFailure
from ..common.embed_utils import list_private_fields
from ..common.log_utils import create_logs as unity_create_logs
from ..common.model_to_fields import model_to_fields
from ..common.sync_lease import exclusive_sync_lease
from ..common.tool_outcome import ToolErrorException
from .base import BaseWorkflowManager
from .bundle import WORKFLOW_LIBRARY, SurfaceRegistry, WorkflowBundle
from .requirements import RequirementResolver
from .types.meta import WorkflowMeta
from .types.request import ACTIONS, WorkflowRequest
from .types.workflow import UNASSIGNED, WorkflowInstallation

logger = logging.getLogger(__name__)

WORKFLOW_TABLE = "Workflows"
WORKFLOW_META_TABLE = "Workflows/Meta"
WORKFLOW_REQUESTS_TABLE = "Workflows/Requests"

STALE_CLAIM_SECONDS = 900
"""How long a claimed request may sit before another executor may take it.

Matches the canvas invocation window. Long enough that a slow reconcile
(several surfaces against a cold backend) is never stolen mid-pass, short
enough that an executor killed between claim and settle does not strand
the user's install for a session."""

_REQUEST_BATCH = 25
"""Requests drained per pass. A bound, not a queue depth: whatever is left
is picked up by the next wake, and the sweep runs on every boot."""


def _now() -> str:
    """Second-precision UTC stamp, the format lease timestamps parse from."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class WorkflowManager(BaseWorkflowManager):
    """Catalogue of available workflow bundles and their installations."""

    class Config:
        required_contexts = [
            TableContext(
                name=WORKFLOW_TABLE,
                description="Installed workflow bundles and their settings.",
                fields=model_to_fields(WorkflowInstallation),
                unique_keys={"workflow_id": "int"},
                auto_counting={"workflow_id": None},
            ),
            TableContext(
                name=WORKFLOW_META_TABLE,
                description="Metadata for workflow catalogue sync state.",
                fields=model_to_fields(WorkflowMeta),
                unique_keys={"meta_id": "int"},
            ),
            TableContext(
                name=WORKFLOW_REQUESTS_TABLE,
                description="Requested changes to workflow install state.",
                fields=model_to_fields(WorkflowRequest),
                unique_keys={"request_id": "str"},
            ),
        ]

    def __init__(self) -> None:
        super().__init__()
        self._ctx = ContextRegistry.get_context(self, WORKFLOW_TABLE)
        self._meta_ctx = ContextRegistry.get_context(self, WORKFLOW_META_TABLE)
        self._requests_ctx = ContextRegistry.get_context(
            self,
            WORKFLOW_REQUESTS_TABLE,
        )
        self._surfaces = SurfaceRegistry()
        self._catalogue: Dict[str, WorkflowBundle] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ #
    # Wiring                                                             #
    # ------------------------------------------------------------------ #
    @property
    def surfaces(self) -> SurfaceRegistry:
        return self._surfaces

    def register_bundle(self, bundle: WorkflowBundle) -> None:
        """Add a bundle to the catalogue of installable workflows.

        Raises if the bundle covers a surface no manager has registered,
        so a bundle referencing an unscoped or absent surface fails at
        registration rather than half-planting itself at install.
        """

        unknown = [s for s in bundle.surface_names() if s not in self._surfaces]
        if unknown:
            raise KeyError(
                f"Bundle {bundle.slug!r} covers unregistered surfaces "
                f"{unknown}; registered surfaces are {self._surfaces.names()}.",
            )
        with self._lock:
            self._catalogue[bundle.slug] = bundle

    def available_bundles(self) -> List[WorkflowBundle]:
        with self._lock:
            return [self._catalogue[s] for s in sorted(self._catalogue)]

    # ------------------------------------------------------------------ #
    # Destination resolution                                             #
    # ------------------------------------------------------------------ #
    def _workflow_context_for_destination(self, destination: str | None) -> str:
        root = ContextRegistry.write_root(
            self,
            WORKFLOW_TABLE,
            destination=destination,
        )
        return f"{root.strip('/')}/{WORKFLOW_TABLE}"

    def _meta_context_for_destination(self, destination: str | None) -> str:
        root = ContextRegistry.write_root(
            self,
            WORKFLOW_META_TABLE,
            destination=destination,
        )
        return f"{root.strip('/')}/{WORKFLOW_META_TABLE}"

    def _requests_context_for_destination(self, destination: str | None) -> str:
        root = ContextRegistry.write_root(
            self,
            WORKFLOW_REQUESTS_TABLE,
            destination=destination,
        )
        return f"{root.strip('/')}/{WORKFLOW_REQUESTS_TABLE}"

    @staticmethod
    def _normalized_destination(value: Any) -> Optional[str]:
        """``None`` for the personal root, matching every internal caller.

        Rows store ``"personal"`` because a column cannot be absent, while
        the methods take ``None`` for the same thing. Collapsing here keeps
        one meaning of "personal" on the way in.
        """
        text = str(value or "").strip()
        return None if text in ("", "personal") else text

    def _read_contexts(self) -> List[str]:
        roots = ContextRegistry.read_roots(self, WORKFLOW_TABLE)
        contexts = [f"{root.strip('/')}/{WORKFLOW_TABLE}" for root in roots]
        return list(dict.fromkeys(contexts))

    # ------------------------------------------------------------------ #
    # Installation rows                                                  #
    # ------------------------------------------------------------------ #
    def _read_installation(
        self,
        slug: str,
        *,
        context: str,
    ) -> Optional[Dict[str, Any]]:
        logs = unisdk.get_logs(
            context=context,
            filter=f"slug == '{slug}'",
            exclude_fields=list_private_fields(context),
            limit=1,
        )
        if not logs:
            return None
        return dict(logs[0].entries or {})

    def _installations_by_slug(self) -> Dict[str, List[Dict[str, Any]]]:
        """Every installation of every slug, grouped, across all read roots.

        One slug can legitimately be installed more than once — personally and
        for a team — and those are separate installations owning separate rows.
        Collapsing them to one silently picks a winner.
        """
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in self._installations():
            slug = row.get("slug")
            if slug:
                grouped.setdefault(str(slug), []).append(row)
        return grouped

    @staticmethod
    def _preferred_installation(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """The installation a read should describe when a slug has several.

        Personal first, because personal is the write default: a caller who
        passes no destination acts on that one, so a read that described a team
        installation instead would be describing something the next write will
        not touch. That mismatch is exactly what made a team-installed slug read
        as installed and then fail to uninstall.
        """
        return next(
            (
                row
                for row in rows
                if str(row.get("destination", "personal")) == "personal"
            ),
            rows[0],
        )

    @staticmethod
    def _destinations_of(rows: List[Dict[str, Any]]) -> List[str]:
        return sorted({str(row.get("destination", "personal")) for row in rows})

    def _installations(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for context in self._read_contexts():
            logs = unisdk.get_logs(
                context=context,
                exclude_fields=list_private_fields(context),
            )
            rows.extend(dict(lg.entries or {}) for lg in logs)
        return rows

    def _write_installation(
        self,
        record: WorkflowInstallation,
        *,
        context: str,
        existing: Optional[Dict[str, Any]],
    ) -> None:
        payload = strip_authoring_assistant_id(record.to_post_json())
        if existing:
            logs = unisdk.get_logs(
                context=context,
                filter=f"slug == '{record.slug}'",
                limit=1,
            )
            if logs:
                payload.pop("workflow_id", None)
                unisdk.update_logs(
                    context=context,
                    logs=[logs[0].id],
                    entries=payload,
                    overwrite=True,
                )
                return
        unity_create_logs(
            context=context,
            entries=[payload],
            stamp_authoring=True,
        )

    def _delete_installation(self, slug: str, *, context: str) -> bool:
        logs = unisdk.get_logs(
            context=context,
            filter=f"slug == '{slug}'",
            limit=1,
        )
        if not logs:
            return False
        unisdk.delete_logs(context=context, logs=[logs[0].id])
        return True

    # ------------------------------------------------------------------ #
    # Fan-out                                                            #
    # ------------------------------------------------------------------ #
    def _shared_union(
        self,
        surface_name: str,
        bundles: List[WorkflowBundle],
    ) -> Dict[str, Dict[str, Any]]:
        """Merge every bundle's entries for one shared surface.

        A shared atom referenced by several bundles converges to one
        entry carrying all their slugs in its ``workflows`` field —
        provided the content is byte-identical. Two bundles shipping the
        same key with different content is a curation error the install
        must surface, not a survivor to pick: whichever won, the other
        workflow would be running someone else's code under its own name.
        """

        union: Dict[str, Dict[str, Any]] = {}
        owners: Dict[str, List[str]] = {}
        for bundle in sorted(bundles, key=lambda b: b.slug):
            for key, fields in bundle.surfaces.get(surface_name, {}).items():
                if key in union:
                    if union[key].get("custom_hash") != fields.get("custom_hash"):
                        raise ToolErrorException(
                            {
                                "error": "conflicting_content",
                                "surface": surface_name,
                                "key": key,
                                "workflows": sorted(
                                    owners[key] + [bundle.slug],
                                ),
                                "message": (
                                    f"Workflows {sorted(owners[key])} and "
                                    f"{bundle.slug!r} both define "
                                    f"{key!r} on {surface_name!r} with "
                                    "different content; identical atoms "
                                    "share one row, divergent ones must "
                                    "be renamed."
                                ),
                            },
                        )
                    owners[key].append(bundle.slug)
                else:
                    union[key] = dict(fields)
                    owners[key] = [bundle.slug]
        for key, slugs in owners.items():
            union[key]["workflows"] = sorted(slugs)
        return union

    def _installed_bundles(self, context: str) -> List[WorkflowBundle]:
        """Catalogue bundles whose slug has an installation row in *context*.

        An installed slug whose bundle has left the catalogue cannot
        contribute to a union, so its shared atoms are pruned by the next
        install or uninstall that re-syncs the library. Hand-curated
        bundles are not dropped while installed; ``list_workflows`` flags
        such orphans.
        """
        logs = unisdk.get_logs(
            context=context,
            exclude_fields=list_private_fields(context),
        )
        slugs = {
            str((lg.entries or {}).get("slug"))
            for lg in logs
            if (lg.entries or {}).get("slug")
        }
        with self._lock:
            return [self._catalogue[s] for s in sorted(slugs) if s in self._catalogue]

    def _plant(
        self,
        bundle: WorkflowBundle,
        *,
        destination: Optional[str],
        installed: List[WorkflowBundle],
        surface_names: Optional[List[str]] = None,
        empty: bool = False,
    ) -> tuple[Dict[str, Any], Dict[str, BaseException]]:
        """Sync each covered surface at *destination*.

        Per-slug surfaces sync the bundle's own entries under its slug.
        Shared surfaces sync the union of every installed bundle's entries
        under ``WORKFLOW_LIBRARY`` — *installed* is the set of already
        installed catalogue bundles at this destination, and the current
        bundle is added to it (install) or dropped from it (uninstall)
        before the union is taken.

        The installation's destination governs where every entry lands —
        a team install plants team content, a personal install plants
        personal content. Surfaces receive it directly on their
        per-destination sync, so the fan-out never depends on per-entry
        destination fields in the bundle.

        With ``empty=True`` every per-slug surface receives an empty
        source, which prunes exactly the rows carrying this slug at this
        destination; shared surfaces receive the union minus this bundle,
        which prunes exactly the atoms no remaining workflow references.
        Either way nobody else's rows — the deployment's, another
        workflow's, the user's own — can be touched. Uninstall is the same
        code path as install precisely because the scoping is what does
        the work.

        Union sources (and their conflict errors) are computed for every
        covered shared surface before anything syncs, so a conflicting
        install fails whole rather than half-planted.
        """

        planted: Dict[str, Any] = {}
        failures: Dict[str, BaseException] = {}
        names = surface_names if surface_names is not None else bundle.surface_names()

        union_bundles = [b for b in installed if b.slug != bundle.slug]
        if not empty:
            union_bundles.append(bundle)

        sources: Dict[str, tuple[Dict[str, Any], str]] = {}
        for name in names:
            surface = self._surfaces.get(name)
            if surface.shared:
                sources[name] = (
                    self._shared_union(name, union_bundles),
                    WORKFLOW_LIBRARY,
                )
            else:
                source = {} if empty else bundle.surfaces.get(name, {})
                sources[name] = (source, bundle.slug)

        for name in names:
            source, managed_by = sources[name]
            try:
                surface = self._surfaces.get(name)
                changed = surface.sync(
                    source,
                    managed_by=managed_by,
                    destination=destination,
                )
                planted[name] = {"entries": len(source), "changed": bool(changed)}
            except CustomSyncPartialFailure as exc:
                failures[name] = exc
                planted[name] = {
                    "entries": len(source),
                    "changed": True,
                    "failed_keys": sorted(exc.failures),
                }
            except Exception as exc:
                failures[name] = exc
                planted[name] = {"entries": len(source), "error": str(exc)}

        return planted, failures

    # ------------------------------------------------------------------ #
    # Requirements                                                       #
    # ------------------------------------------------------------------ #
    def _unmet_requirements(self, bundle: WorkflowBundle) -> List[Dict[str, Any]]:
        """Declared integrations not currently connected, as report dicts.

        Resolution spans every kind of app the gallery offers — see
        :mod:`unify.workflow_manager.requirements`. Secret *names* and
        presence travel through here; values never do.
        """
        if not bundle.requirements:
            return []
        return RequirementResolver().unmet(bundle.requirements)

    def _requirements_report(self, bundle: WorkflowBundle) -> List[Dict[str, Any]]:
        """Every declared requirement with its current connection state."""
        if not bundle.requirements:
            return []
        return RequirementResolver().report(bundle.requirements)

    def _arm_workflow_tasks(
        self,
        bundle: WorkflowBundle,
        *,
        destination: Optional[str],
        enabled: bool,
    ) -> List[int]:
        """Arm or hold the task definitions this workflow planted.

        Best-effort by design: a failed arm leaves the tasks in the safe
        state (disarmed), and the next install or reconcile retries. It
        must not fail the install that planted the content.
        """
        if "tasks" not in bundle.surfaces or "tasks" not in self._surfaces:
            return []
        try:
            return list(
                self._surfaces.get("tasks").arm(
                    managed_by=bundle.slug,
                    enabled=enabled,
                    destination=destination,
                )
                or [],
            )
        except Exception:
            logger.exception(
                "Failed to %s tasks for workflow %r",
                "arm" if enabled else "hold",
                bundle.slug,
            )
            return []

    @staticmethod
    def _derived_status(
        stored_status: Optional[str],
        unmet: List[Dict[str, Any]],
    ) -> Optional[str]:
        """Fold requirement state into the read-side status.

        ``partial`` (something failed to plant) outranks everything;
        otherwise an unmet requirement reads as ``needs_connection``. The
        stored status never records ``needs_connection`` — connections
        change without the installation row being touched, so it must be
        derived at read time.
        """
        if stored_status == "partial":
            return "partial"
        if stored_status is None:
            return None
        return "needs_connection" if unmet else stored_status

    @staticmethod
    def _validate_params(
        bundle: WorkflowBundle,
        params: Mapping[str, Any],
    ) -> Dict[str, Any]:
        required = {
            name
            for name, spec in bundle.params_schema.items()
            if isinstance(spec, Mapping) and spec.get("required")
        }
        missing = sorted(required - set(params))
        if missing:
            raise ToolErrorException(
                {
                    "error": "missing_params",
                    "slug": bundle.slug,
                    "missing": missing,
                    "message": (
                        f"Workflow {bundle.slug!r} needs {missing} before it "
                        "can be installed."
                    ),
                },
            )
        return dict(params)

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    @functools.wraps(BaseWorkflowManager.list_workflows, updated=())
    def list_workflows(
        self,
        *,
        installed: Optional[bool] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Dict[str, Any]:
        grouped = self._installations_by_slug()
        by_slug = {
            slug: self._preferred_installation(rows) for slug, rows in grouped.items()
        }
        entries: List[Dict[str, Any]] = []

        for bundle in self.available_bundles():
            row = by_slug.get(bundle.slug)
            is_installed = row is not None
            if installed is True and not is_installed:
                continue
            if installed is False and is_installed:
                continue
            entry = {
                "slug": bundle.slug,
                "name": bundle.name,
                "description": bundle.description,
                "version": bundle.version,
                "category": bundle.category,
                "icon_id": bundle.icon_id,
                "capabilities": list(bundle.capabilities),
                "requirements": self._requirements_report(bundle),
                "surfaces": bundle.surface_names(),
                "installed": is_installed,
            }
            if row:
                entry.update(
                    {
                        "installed_version": row.get("version", ""),
                        "status": self._derived_status(
                            row.get("status"),
                            self._unmet_requirements(bundle),
                        ),
                        "params": json.loads(row.get("params") or "{}"),
                        # Which installation this describes, and every root it
                        # is installed at. Without these, "installed: true" for
                        # a team-only install reads as personal and the next
                        # uninstall — which defaults to personal — refuses.
                        "destination": row.get("destination", "personal"),
                        "installed_at": self._destinations_of(grouped[bundle.slug]),
                    },
                )
            entries.append(entry)

        # Installations whose bundle has left the catalogue still exist and
        # still own rows; hiding them would make their content unremovable.
        for slug, row in by_slug.items():
            if any(e["slug"] == slug for e in entries):
                continue
            if installed is False:
                continue
            entries.append(
                {
                    "slug": slug,
                    "name": row.get("name", slug),
                    "description": row.get("description", ""),
                    "installed": True,
                    "orphaned": True,
                    "installed_version": row.get("version", ""),
                    "surfaces": json.loads(row.get("surfaces") or "[]"),
                },
            )

        entries.sort(key=lambda e: e["slug"])
        return {
            "workflows": entries[offset : offset + limit],
            "total": len(entries),
        }

    @functools.wraps(BaseWorkflowManager.install_workflow, updated=())
    def install_workflow(
        self,
        *,
        slug: str,
        params: Optional[Dict[str, Any]] = None,
        destination: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            bundle = self._catalogue.get(slug)
        if bundle is None:
            available = [b.slug for b in self.available_bundles()]
            raise ToolErrorException(
                {
                    "error": "unknown_workflow",
                    "slug": slug,
                    "available": available,
                    "message": (
                        f"No workflow {slug!r} is on the shelf. Available: "
                        f"{available or 'nothing — the catalogue is empty'}."
                    ),
                },
            )

        context = self._workflow_context_for_destination(destination)
        meta_context = self._meta_context_for_destination(destination)

        with exclusive_sync_lease(f"{meta_context}:workflow_install"):
            existing = self._read_installation(slug, context=context)

            # Reinstalling without params keeps the recorded settings, so
            # retry / upgrade / arm-on-connect never silently reset a
            # configured mailbox; passing params is the settings change.
            if params is None and existing is not None:
                params = json.loads(existing.get("params") or "{}")
            resolved_params = self._validate_params(bundle, params or {})

            # Every install reconciles to the current bundle — a repeat
            # install is retry, upgrade and arm-on-connect all at once.
            # Unchanged content short-circuits on the aggregate hash, so
            # the idempotent case costs a meta read per surface.
            planted, failures = self._plant(
                bundle,
                destination=destination,
                installed=self._installed_bundles(context),
            )

            # Requirements gate arming, never planting. Content lands
            # either way; the workflow's tasks — born disarmed by the
            # custom sync — are armed only once every declared integration
            # is connected, and held (still planted, still visible)
            # otherwise. A repeat install after connecting is therefore
            # also the arm-on-connect path.
            unmet = self._unmet_requirements(bundle)
            armed = self._arm_workflow_tasks(
                bundle,
                destination=destination,
                enabled=not unmet,
            )

            # Views are published after the surfaces land, because a view
            # binds to tables the data surface just created. A publish that
            # fails joins the surface failures: the installation stays and
            # reports `partial`, so a repeat install retries exactly it.
            canvases, canvas_failures = self._publish_canvases(
                bundle,
                destination=destination,
            )
            failures.update(canvas_failures)

            record = WorkflowInstallation(
                workflow_id=(
                    existing.get("workflow_id", UNASSIGNED) if existing else UNASSIGNED
                ),
                slug=bundle.slug,
                name=bundle.name,
                version=bundle.version,
                description=bundle.description,
                status="partial" if failures else "active",
                params=json.dumps(resolved_params, sort_keys=True),
                surfaces=json.dumps(bundle.surface_names()),
                destination=destination or "personal",
            )
            self._write_installation(record, context=context, existing=existing)

        result: Dict[str, Any] = {
            "installed": record.model_dump(mode="json"),
            "planted": planted,
        }
        if canvases:
            result["canvases"] = canvases
        if unmet:
            result["tasks_held"] = armed
            result["connect_required"] = {
                "requirements": unmet,
                "message": (
                    f"Workflow {slug!r} is installed but held: connect "
                    f"{[r['slug'] for r in unmet]} to arm its jobs. "
                    "Nothing fires in the meantime."
                ),
            }
        else:
            result["tasks_armed"] = armed
            # Provisioning runs once, on a first install, and only when the
            # workflow is not held: a backfill against an unconnected app would
            # fail for exactly the reason its recurring job is disarmed.
            if existing is None:
                provisioned = self._trigger_install_task(
                    bundle,
                    destination=destination,
                )
                if provisioned is not None:
                    result["provisioning_task_id"] = provisioned
        if failures:
            result["failures"] = {n: str(e) for n, e in failures.items()}
            logger.warning(
                "Workflow %r installed with failures on surfaces: %s",
                slug,
                sorted(failures),
            )
        return result

    @functools.wraps(BaseWorkflowManager.reconcile_installed, updated=())
    def reconcile_installed(
        self,
        *,
        slugs: Optional[List[str]] = None,
        destination: Optional[str] = None,
    ) -> Dict[str, Any]:
        # install_workflow re-run with the recorded settings, per slug.
        # Unchanged bundles short-circuit on their aggregate hashes, so a
        # no-op pass costs one meta read per surface.
        context = self._workflow_context_for_destination(destination)
        rows = self._installations_in(context)
        if slugs is not None:
            wanted = set(slugs)
            rows = [row for row in rows if row.get("slug") in wanted]

        reconciled: Dict[str, Any] = {}
        orphaned: List[str] = []
        for row in sorted(rows, key=lambda r: str(r.get("slug"))):
            slug = str(row.get("slug"))
            with self._lock:
                bundle = self._catalogue.get(slug)
            if bundle is None:
                orphaned.append(slug)
                continue
            reconciled[slug] = self.install_workflow(
                slug=slug,
                params=json.loads(row.get("params") or "{}"),
                destination=destination,
            )

        result: Dict[str, Any] = {"reconciled": reconciled}
        if orphaned:
            result["orphaned"] = orphaned
        return result

    # ------------------------------------------------------------------ #
    # Canvas views (published, never reconciled)                         #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _get_canvas_manager():
        from ..manager_registry import ManagerRegistry

        return ManagerRegistry.get_canvas_manager()

    def _published_canvases(
        self,
        slug: str,
    ) -> Dict[str, Dict[str, Any]]:
        """Views this workflow has published, keyed by their ``custom_key``."""
        try:
            records = self._get_canvas_manager().list_views(
                filter=f"managed_by == '{slug}'",
                limit=200,
            )
        except Exception:
            logger.exception("Could not read published canvases for %r", slug)
            return {}
        return {
            str(record.custom_key): record.model_dump()
            for record in records
            if record.custom_key
        }

    def _publish_canvases(
        self,
        bundle: WorkflowBundle,
        *,
        destination: Optional[str],
    ) -> tuple[Dict[str, Any], Dict[str, BaseException]]:
        """Compile and publish the views a bundle ships.

        Deliberately outside the surface fan-out. A view is TypeScript that
        has to be linted, typechecked, bundled, rendered and reviewed
        against the kit installed *now* — the reason a bundle ships source
        and not a built artifact — and its routing token has a lifecycle
        the reconcile engine has no business owning.

        Idempotent by ``custom_key``: a repeat install revises the view it
        published last time, and one whose source is unchanged is left
        alone rather than recompiled, because the compile is the expensive
        part of an otherwise cheap reconcile.
        """
        if not bundle.canvas:
            return {}, {}

        manager = self._get_canvas_manager()
        published = self._published_canvases(bundle.slug)
        report: Dict[str, Any] = {}
        failures: Dict[str, BaseException] = {}

        for source in bundle.canvas:
            key = f"{bundle.slug}/{source.custom_key}"
            content_hash = source.content_hash()
            existing = published.get(key)
            provenance = {
                "custom_key": key,
                "custom_hash": content_hash,
                "managed_by": bundle.slug,
            }
            try:
                if existing and existing.get("custom_hash") == content_hash:
                    report[source.name] = {
                        "token": existing.get("token"),
                        "status": "unchanged",
                    }
                    continue
                if existing:
                    result = manager.update_view(
                        str(existing["token"]),
                        tsx=source.tsx,
                        title=source.title,
                        description=source.description,
                        bindings=list(source.bindings),
                        props=dict(source.props),
                        actions=list(source.actions),
                        visibility=source.visibility,
                        provenance=provenance,
                    )
                else:
                    result = manager.create_view(
                        source.tsx,
                        title=source.title,
                        description=source.description,
                        bindings=list(source.bindings),
                        props=dict(source.props),
                        actions=list(source.actions),
                        destination=destination,
                        visibility=source.visibility,
                        provenance=provenance,
                    )
            except Exception as exc:
                failures[f"canvas:{source.name}"] = exc
                logger.exception(
                    "Publishing canvas %r for workflow %r failed",
                    source.name,
                    bundle.slug,
                )
                continue

            if result.error:
                # A view that does not compile or does not mount is a
                # failure with a message worth relaying — the author needs
                # the compiler's words, not "install failed".
                failures[f"canvas:{source.name}"] = RuntimeError(result.error)
                continue
            report[source.name] = {
                "token": result.token,
                "url": result.url,
                "status": "updated" if existing else "published",
            }

        return report, failures

    def _withdraw_canvases(self, slug: str) -> List[str]:
        """Delete the views this workflow published.

        Through the manager's own delete, which releases the routing token
        and drops the actions and invocations hanging off it — none of
        which a prune adapter could do without reimplementing it.
        """
        removed: List[str] = []
        manager = self._get_canvas_manager()
        for key, row in self._published_canvases(slug).items():
            token = str(row.get("token") or "")
            if not token:
                continue
            try:
                manager.delete_view(token)
                removed.append(key)
            except Exception:
                logger.exception(
                    "Could not delete canvas %r while uninstalling %r",
                    key,
                    slug,
                )
        return removed

    def _trigger_install_task(
        self,
        bundle: WorkflowBundle,
        *,
        destination: Optional[str],
    ) -> Optional[int]:
        """Run the bundle's provisioning one-shot. Returns its task id, or None.

        Triggered rather than awaited: ``TaskScheduler.execute`` is async and
        ``install_workflow`` is not, and the house rule is that work starts via
        the trigger route, never via an update. Triggering also means the run is
        an ordinary execution with the ordinary handle, history and steering —
        the workflow contributes nothing of its own, which is what keeps it
        runtime-free.
        """
        if not bundle.install_task:
            return None

        root = ContextRegistry.write_root(self, "Tasks", destination=destination)
        rows = unisdk.get_logs(
            context=f"{root.strip('/')}/Tasks",
            filter=(
                f"managed_by == '{bundle.slug}' and "
                f"custom_key == '{bundle.install_task}'"
            ),
            limit=1,
        )
        if not rows:
            logger.warning(
                "Workflow %r declares install_task %r but no such planted task "
                "exists; skipping provisioning",
                bundle.slug,
                bundle.install_task,
            )
            return None

        task_id = (rows[0].entries or {}).get("task_id")
        if task_id is None:
            return None

        from ..task_scheduler.typed_tasks_client import trigger_task

        trigger_task(task_id=int(task_id))
        return int(task_id)

    # ------------------------------------------------------------------ #
    # Requested changes (the mutation contract for reading surfaces)     #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _get_dm():
        from ..data_manager.data_manager import DataManager

        return DataManager()

    def _claim_is_stale(self, row: Mapping[str, Any]) -> bool:
        """Whether a running request's holder can no longer finish it.

        A claim younger than the window is someone else's live work. Past
        it with no terminal status written, the holder died mid-pass — the
        one condition under which taking the request over cannot run it
        twice. A running row with no claim at all predates claims, so its
        age is unknowable and it is treated as stale rather than stuck.
        """
        stamp = str(row.get("claimed_at") or "")
        if not stamp:
            return True
        try:
            claimed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        except ValueError:
            logger.warning(
                "Unparseable request claim stamp %r; treating as live",
                stamp,
            )
            return False
        if claimed.tzinfo is None:
            claimed = claimed.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - claimed).total_seconds() >= (
            STALE_CLAIM_SECONDS
        )

    def _claim_request(
        self,
        context: str,
        row: Mapping[str, Any],
    ) -> Optional[str]:
        """Take exclusive execution of one request, or None when another won.

        An atomic compare-and-set on the state this caller just observed, so
        a wake dispatch racing the boot sweep produces exactly one winner —
        which is what keeps at-least-once delivery from planting a workflow
        twice. Expecting the observed ``claim_key`` on a stale takeover
        fences out the dead holder: if it wrote again in between, the
        expectation no longer matches and nobody double-runs.
        """
        nonce = uuid.uuid4().hex
        expect: Dict[str, Any] = {
            "request_id": str(row.get("request_id")),
            "status": str(row.get("status") or ""),
        }
        if row.get("claim_key"):
            expect["claim_key"] = str(row["claim_key"])

        claimed = self._get_dm().claim(
            context,
            expect=expect,
            updates={
                "status": "running",
                "claim_key": nonce,
                "claimed_at": _now(),
            },
            limit=1,
        )
        return nonce if claimed else None

    def _settle_request(
        self,
        context: str,
        request_id: str,
        *,
        status: str,
        outcome: Optional[Any] = None,
        error: Optional[Any] = None,
    ) -> None:
        updates: Dict[str, Any] = {"status": status, "settled_at": _now()}
        if outcome is not None:
            updates["outcome"] = json.dumps(outcome, default=str)[:20000]
        if error is not None:
            updates["error"] = json.dumps(error, default=str)[:4000]
        self._get_dm().update_rows(
            context,
            updates,
            filter=f"request_id == {json.dumps(request_id)}",
        )

    def _run_request(self, row: Mapping[str, Any]) -> Dict[str, Any]:
        """Perform one requested change and return what it produced."""
        action = str(row.get("action") or "")
        slug = str(row.get("slug") or "")
        destination = self._normalized_destination(row.get("destination"))
        params = json.loads(row.get("params") or "{}")

        if action == "install":
            # An empty params object means "keep what is recorded", which is
            # install_workflow's own contract for params=None.
            return self.install_workflow(
                slug=slug,
                params=params or None,
                destination=destination,
            )
        if action == "update":
            return self.install_workflow(
                slug=slug,
                params=None,
                destination=destination,
            )
        if action == "uninstall":
            # keep_data rides on the request's params rather than a column of
            # its own: it is an argument to one action, not a property every
            # request has.
            return self.uninstall_workflow(
                slug=slug,
                destination=destination,
                keep_data=bool(params.get("keep_data")),
            )
        if action == "save_params":
            return self.set_workflow_params(
                slug=slug,
                params=params,
                destination=destination,
            )
        raise ToolErrorException(
            {
                "error": "unknown_action",
                "action": action,
                "supported": list(ACTIONS),
                "message": (
                    f"Requested action {action!r} is not one of "
                    f"{list(ACTIONS)}; nothing was changed."
                ),
            },
        )

    @functools.wraps(BaseWorkflowManager.execute_requests, updated=())
    def execute_requests(
        self,
        *,
        destination: Optional[str] = None,
        limit: int = _REQUEST_BATCH,
    ) -> Dict[str, Any]:
        # The assistant's half of the mutation contract: a reading surface
        # records intent as a row because planting needs the reconcile
        # engine, which is the assistant's, and a hosted assistant is
        # usually asleep when someone clicks Install. This runs on every
        # boot and on the wake a dispatch triggers, so a missed dispatch
        # costs latency rather than a lost request.
        context = self._requests_context_for_destination(destination)
        dm = self._get_dm()

        pending = dm.filter(context, filter="status == 'pending'", limit=limit)
        running = dm.filter(context, filter="status == 'running'", limit=limit)
        stale = [row for row in running if self._claim_is_stale(row)]

        # Oldest first: a user who clicked install then uninstall must not
        # have them applied out of order.
        queue = sorted(
            [*pending, *stale],
            key=lambda row: str(row.get("ts") or ""),
        )

        settled: Dict[str, str] = {}
        for row in queue:
            request_id = str(row.get("request_id") or "")
            if not request_id:
                continue
            if self._claim_request(context, row) is None:
                continue
            try:
                outcome = self._run_request(row)
            except Exception as exc:
                error = (
                    exc.payload
                    if isinstance(exc, ToolErrorException)
                    else {"error": str(exc)}
                )
                self._settle_request(
                    context,
                    request_id,
                    status="failed",
                    error=error,
                )
                settled[request_id] = "failed"
                logger.warning(
                    "Workflow request %s (%s %s) failed: %s",
                    request_id,
                    row.get("action"),
                    row.get("slug"),
                    exc,
                )
                continue

            # A per-surface failure means the user asked for an install that
            # did not fully land, so the request is honest about it while the
            # outcome still carries what did. `connect_required` is NOT a
            # failure — a held workflow is the designed inert install.
            failures = outcome.get("failures") if isinstance(outcome, Mapping) else None
            self._settle_request(
                context,
                request_id,
                status="failed" if failures else "succeeded",
                outcome=outcome,
                error=failures or None,
            )
            settled[request_id] = "failed" if failures else "succeeded"

        return {"settled": settled}

    @functools.wraps(BaseWorkflowManager.set_workflow_params, updated=())
    def set_workflow_params(
        self,
        *,
        slug: str,
        params: Dict[str, Any],
        destination: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            bundle = self._catalogue.get(slug)

        context = self._workflow_context_for_destination(destination)
        meta_context = self._meta_context_for_destination(destination)

        with exclusive_sync_lease(f"{meta_context}:workflow_install"):
            existing = self._read_installation(slug, context=context)
            if existing is None:
                raise ToolErrorException(
                    {
                        "error": "not_installed",
                        "slug": slug,
                        "destination": destination or "personal",
                        "message": (
                            f"Workflow {slug!r} is not installed at "
                            f"{destination or 'personal'}, so there are no "
                            "settings to change."
                        ),
                    },
                )
            # Validated against the bundle when the catalogue has it, so a
            # settings write cannot drop a required value. An orphaned
            # installation (bundle gone from this deployment) still accepts
            # settings — refusing would strand it.
            resolved = self._validate_params(bundle, params) if bundle else dict(params)
            logs = unisdk.get_logs(
                context=context,
                filter=f"slug == '{slug}'",
                limit=1,
            )
            if logs:
                # overwrite=True because the row already carries params;
                # Orchestra refuses to replace an existing value otherwise.
                unisdk.update_logs(
                    context=context,
                    logs=[logs[0].id],
                    entries={"params": json.dumps(resolved, sort_keys=True)},
                    overwrite=True,
                )

        return {"slug": slug, "params": resolved}

    def _installations_in(self, context: str) -> List[Dict[str, Any]]:
        logs = unisdk.get_logs(
            context=context,
            exclude_fields=list_private_fields(context),
        )
        return [dict(lg.entries or {}) for lg in logs]

    @functools.wraps(BaseWorkflowManager.get_installation_params, updated=())
    def get_installation_params(
        self,
        *,
        slug: str,
        destination: Optional[str] = None,
    ) -> Dict[str, Any]:
        context = self._workflow_context_for_destination(destination)
        existing = self._read_installation(slug, context=context)
        if existing is None:
            raise ToolErrorException(
                {
                    "error": "not_installed",
                    "slug": slug,
                    "destination": destination or "personal",
                    "installed_at": self._destinations_of(
                        self._installations_by_slug().get(slug) or [],
                    ),
                    "message": (
                        f"Workflow {slug!r} is not installed at "
                        f"{destination or 'personal'}, so it has no settings "
                        "to read."
                    ),
                },
            )
        return json.loads(existing.get("params") or "{}")

    @functools.wraps(BaseWorkflowManager.uninstall_workflow, updated=())
    def uninstall_workflow(
        self,
        *,
        slug: str,
        destination: Optional[str] = None,
        keep_data: bool = False,
    ) -> Dict[str, Any]:
        context = self._workflow_context_for_destination(destination)
        meta_context = self._meta_context_for_destination(destination)

        with exclusive_sync_lease(f"{meta_context}:workflow_install"):
            existing = self._read_installation(slug, context=context)
            if existing is None:
                # The slug may be installed at a different destination —
                # say where, so the caller can retry with the right one
                # instead of concluding the workflow does not exist.
                installed_at = self._destinations_of(
                    self._installations_by_slug().get(slug) or [],
                )
                raise ToolErrorException(
                    {
                        "error": "not_installed",
                        "slug": slug,
                        "destination": destination or "personal",
                        "installed_at": installed_at,
                        "message": (
                            f"Workflow {slug!r} is not installed at "
                            f"{destination or 'personal'}."
                            + (
                                f" It is installed at {installed_at}."
                                if installed_at
                                else ""
                            )
                        ),
                    },
                )

            # Prefer the recorded surfaces over the bundle's: the bundle may
            # have been dropped from the catalogue, or may cover a different
            # set now than when it planted.
            recorded = json.loads(existing.get("surfaces") or "[]")
            bundle = self._catalogue.get(slug) or WorkflowBundle(
                slug=slug,
                name=existing.get("name", slug),
            )

            # keep_data preserves the stored tables and prunes everything
            # else. A table is the only surface holding work the workflow
            # *produced* rather than content it was given — invoices it
            # reconciled, prospects it sourced — so removing the setup should
            # not have to mean throwing that away. Procedures, claims, tasks
            # and functions are the bundle's own content and always go: they
            # are re-plantable from git, and leaving them would be leaving a
            # half-installed workflow behind.
            pruned_surfaces = (
                [name for name in recorded if name != "data"] if keep_data else recorded
            )
            removed, failures = self._plant(
                bundle,
                destination=destination,
                installed=self._installed_bundles(context),
                surface_names=pruned_surfaces,
                empty=True,
            )
            # Views go whatever `keep_data` says: a canvas is the bundle's
            # own content, not work the workflow produced, and leaving one
            # behind would leave a live URL rendering against tables that
            # may no longer be filled.
            withdrawn = self._withdraw_canvases(slug)
            kept = sorted(set(recorded) - set(pruned_surfaces))
            # The installation row goes only after every surface actually
            # cleared. On failure it stays — it holds the recorded surfaces,
            # which are the only durable record of what still needs pruning.
            if not failures:
                self._delete_installation(slug, context=context)

        result: Dict[str, Any] = {"slug": slug, "removed": removed}
        if withdrawn:
            result["canvases_removed"] = withdrawn
        if kept:
            result["kept"] = kept
        if failures:
            result["failures"] = {n: str(e) for n, e in failures.items()}
            result["retained"] = True
            logger.warning(
                "Workflow %r left installed; surfaces failed to clear: %s",
                slug,
                sorted(failures),
            )
        return result

    @functools.wraps(BaseWorkflowManager.get_workflow, updated=())
    def get_workflow(self, *, slug: str) -> Dict[str, Any]:
        bundle = self._catalogue.get(slug)
        rows = self._installations_by_slug().get(slug) or []
        row = self._preferred_installation(rows) if rows else None
        if bundle is None and row is None:
            return {"found": False, "slug": slug}

        entry: Dict[str, Any] = {"found": True, "slug": slug}
        if bundle:
            entry.update(
                {
                    "name": bundle.name,
                    "description": bundle.description,
                    "about": bundle.about,
                    "version": bundle.version,
                    "category": bundle.category,
                    "icon_id": bundle.icon_id,
                    "capabilities": list(bundle.capabilities),
                    "requirements": self._requirements_report(bundle),
                    "surfaces": bundle.surface_names(),
                    "params_schema": bundle.params_schema,
                    "entries_per_surface": {
                        name: len(source) for name, source in bundle.surfaces.items()
                    },
                },
            )
        entry["installed"] = row is not None
        if row:
            entry.update(
                {
                    "installed_version": row.get("version", ""),
                    "status": self._derived_status(
                        row.get("status"),
                        self._unmet_requirements(bundle) if bundle else [],
                    ),
                    "params": json.loads(row.get("params") or "{}"),
                    "installed_surfaces": json.loads(row.get("surfaces") or "[]"),
                    "destination": row.get("destination", "personal"),
                    "installed_at": self._destinations_of(rows),
                },
            )
            if bundle is None:
                entry["orphaned"] = True
        return entry
