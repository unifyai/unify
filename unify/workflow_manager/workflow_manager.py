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
import hashlib
import json
import logging
import threading
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
from .types.catalog_entry import WorkflowCatalogEntry
from .types.meta import WorkflowMeta
from .types.workflow import UNASSIGNED, WorkflowInstallation

logger = logging.getLogger(__name__)

WORKFLOW_TABLE = "Workflows"
WORKFLOW_META_TABLE = "Workflows/Meta"
WORKFLOW_CATALOG_TABLE = "Workflows/Catalog"


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
                name=WORKFLOW_CATALOG_TABLE,
                description=(
                    "Installable workflows published for reading surfaces. "
                    "Derived from the bundles on disk; never authored here."
                ),
                fields=model_to_fields(WorkflowCatalogEntry),
                unique_keys={"slug": "str"},
            ),
        ]

    def __init__(self) -> None:
        super().__init__()
        self._ctx = ContextRegistry.get_context(self, WORKFLOW_TABLE)
        self._meta_ctx = ContextRegistry.get_context(self, WORKFLOW_META_TABLE)
        self._catalog_ctx = ContextRegistry.get_context(self, WORKFLOW_CATALOG_TABLE)
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
        by_slug = {row.get("slug"): row for row in self._installations()}
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
            raise ToolErrorException(
                {
                    "error": "unknown_workflow",
                    "slug": slug,
                    "available": [b.slug for b in self.available_bundles()],
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
                },
            )
        return json.loads(existing.get("params") or "{}")

    @functools.wraps(BaseWorkflowManager.uninstall_workflow, updated=())
    def uninstall_workflow(
        self,
        *,
        slug: str,
        destination: Optional[str] = None,
    ) -> Dict[str, Any]:
        context = self._workflow_context_for_destination(destination)
        meta_context = self._meta_context_for_destination(destination)

        with exclusive_sync_lease(f"{meta_context}:workflow_install"):
            existing = self._read_installation(slug, context=context)
            if existing is None:
                # The slug may be installed at a different destination —
                # say where, so the caller can retry with the right one
                # instead of concluding the workflow does not exist.
                installed_at = sorted(
                    {
                        row.get("destination", "personal")
                        for row in self._installations()
                        if row.get("slug") == slug
                    },
                )
                raise ToolErrorException(
                    {
                        "error": "not_installed",
                        "slug": slug,
                        "destination": destination or "personal",
                        "installed_at": installed_at,
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

            removed, failures = self._plant(
                bundle,
                destination=destination,
                installed=self._installed_bundles(context),
                surface_names=recorded,
                empty=True,
            )
            # The installation row goes only after every surface actually
            # cleared. On failure it stays — it holds the recorded surfaces,
            # which are the only durable record of what still needs pruning.
            if not failures:
                self._delete_installation(slug, context=context)

        result: Dict[str, Any] = {"slug": slug, "removed": removed}
        if failures:
            result["failures"] = {n: str(e) for n, e in failures.items()}
            result["retained"] = True
            logger.warning(
                "Workflow %r left installed; surfaces failed to clear: %s",
                slug,
                sorted(failures),
            )
        return result

    # ------------------------------------------------------------------ #
    # Publishing the catalogue for reading surfaces                      #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _human_schedule(entry: Mapping[str, Any]) -> str:
        """A plain-language cadence for one task entry, or "" if it has none.

        Reading surfaces show a workflow's recurring jobs before it is
        installed, and "every weekday at 08:30" is the part a user actually
        weighs. Derived here rather than in the reader so every surface
        says it the same way.
        """
        repeat = entry.get("repeat") or []
        if isinstance(repeat, str):
            try:
                repeat = json.loads(repeat)
            except ValueError:
                return ""
        if not repeat:
            return "Once, at install" if not entry.get("trigger") else "On a trigger"

        first = repeat[0] if isinstance(repeat[0], Mapping) else {}
        frequency = str(first.get("frequency") or "").lower()
        weekdays = [str(d).upper() for d in (first.get("weekdays") or [])]
        at = str(first.get("time_of_day") or "")[:5]

        if frequency == "weekly" and weekdays:
            weekday_set = {"MO", "TU", "WE", "TH", "FR"}
            if set(weekdays) == weekday_set:
                cadence = "Every weekday"
            else:
                order = ["MO", "TU", "WE", "TH", "FR", "SA", "SU"]
                names = {
                    "MO": "Mon",
                    "TU": "Tue",
                    "WE": "Wed",
                    "TH": "Thu",
                    "FR": "Fri",
                    "SA": "Sat",
                    "SU": "Sun",
                }
                ordered = [names[d] for d in order if d in weekdays]
                cadence = "Every " + ", ".join(ordered)
        elif frequency:
            cadence = {
                "minutely": "Every minute",
                "hourly": "Every hour",
                "daily": "Every day",
                "weekly": "Every week",
                "monthly": "Every month",
                "yearly": "Every year",
            }.get(frequency, f"Every {frequency}")
        else:
            return ""

        return f"{cadence} at {at}" if at else cadence

    @classmethod
    def _bundle_sets(cls, bundle: WorkflowBundle) -> Dict[str, Any]:
        """What a bundle sets up, per surface, for a reader to show."""
        sets: Dict[str, Any] = {}
        for surface, source in bundle.surfaces.items():
            items = []
            for key, entry in sorted(source.items()):
                fields = entry if isinstance(entry, Mapping) else {}
                item: Dict[str, Any] = {
                    "name": str(fields.get("name") or fields.get("title") or key),
                }
                if surface == "tasks":
                    schedule = cls._human_schedule(fields)
                    if schedule:
                        item["schedule"] = schedule
                items.append(item)
            sets[surface] = items
        return sets

    def _catalog_entry(self, bundle: WorkflowBundle) -> Dict[str, Any]:
        """One catalogue row's fields, derived wholly from the bundle."""
        entry = WorkflowCatalogEntry(
            slug=bundle.slug,
            name=bundle.name,
            version=bundle.version,
            category=bundle.category,
            icon_id=bundle.icon_id,
            description=bundle.description,
            requirements=json.dumps(
                [
                    {
                        "slug": requirement.slug,
                        "name": requirement.name or requirement.slug,
                    }
                    for requirement in bundle.requirements
                ],
            ),
            capabilities=json.dumps(list(bundle.capabilities)),
            params_schema=json.dumps(bundle.params_schema, sort_keys=True),
            surfaces=json.dumps(bundle.surface_names()),
            sets=json.dumps(self._bundle_sets(bundle), sort_keys=True),
        )
        return strip_authoring_assistant_id(entry.model_dump(mode="json"))

    def _get_stored_catalog_hash(self) -> str:
        try:
            logs = unisdk.get_logs(
                context=self._meta_ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                return str((logs[0].entries or {}).get("custom_workflow_hash", ""))
        except Exception:
            logger.debug("Could not read the workflow catalogue hash", exc_info=True)
        return ""

    def _store_catalog_hash(self, hash_value: str) -> None:
        try:
            logs = unisdk.get_logs(
                context=self._meta_ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                unisdk.update_logs(
                    context=self._meta_ctx,
                    logs=[logs[0].id],
                    entries={"custom_workflow_hash": hash_value},
                    overwrite=True,
                )
            else:
                unity_create_logs(
                    context=self._meta_ctx,
                    entries=[{"meta_id": 1, "custom_workflow_hash": hash_value}],
                    stamp_authoring=True,
                )
        except Exception:
            logger.debug("Could not store the workflow catalogue hash", exc_info=True)

    def publish_catalog(self) -> Dict[str, Any]:
        """Mirror the in-memory catalogue into ``Workflows/Catalog``.

        Reading surfaces then see the shelf without waking the assistant,
        which matters because a hosted assistant is usually asleep when
        someone opens Console.

        Cheap by construction, in three steps:

        1. **Hash short-circuit.** The published shape is fingerprinted and
           compared with the stored aggregate. A boot whose catalogue has
           not changed costs one meta read and nothing else — which is
           every boot between deploys, i.e. almost all of them.
        2. **One batched insert** for rows that are new, rather than a call
           per bundle.
        3. **Per-row updates only for rows that actually changed**, since
           Orchestra applies one entries payload to every id in a call and
           each row's content differs. Deletes batch into one call.

        Nothing is authored in that context, so there is no user edit to
        preserve and no merge to get wrong.

        Best-effort by design: a publish failure must not stop an assistant
        booting or block installs, because the in-memory catalogue is the
        authority either way. Failures are logged and the next boot retries
        (the hash is stored only on success, so a failed pass does not pin
        itself as up to date).
        """
        bundles = self.available_bundles()
        entries = {bundle.slug: self._catalog_entry(bundle) for bundle in bundles}
        expected_hash = hashlib.sha256(
            json.dumps(entries, sort_keys=True).encode(),
        ).hexdigest()

        if entries and self._get_stored_catalog_hash() == expected_hash:
            logger.debug("Workflow catalogue unchanged; skipping publish")
            return {"unchanged": True, "published": [], "removed": []}

        published: List[str] = []
        removed: List[str] = []
        try:
            live_logs = unisdk.get_logs(
                context=self._catalog_ctx,
                exclude_fields=list_private_fields(self._catalog_ctx),
            )
            live = {
                str((lg.entries or {}).get("slug")): lg
                for lg in live_logs
                if (lg.entries or {}).get("slug")
            }

            inserts = [payload for slug, payload in entries.items() if slug not in live]
            if inserts:
                unity_create_logs(
                    context=self._catalog_ctx,
                    entries=inserts,
                    stamp_authoring=True,
                    batched=True,
                )
                published.extend(payload["slug"] for payload in inserts)

            for slug, payload in entries.items():
                existing = live.get(slug)
                if existing is None:
                    continue
                current = {key: (existing.entries or {}).get(key) for key in payload}
                if current == payload:
                    continue
                unisdk.update_logs(
                    context=self._catalog_ctx,
                    logs=[existing.id],
                    entries=payload,
                    overwrite=True,
                )
                published.append(slug)

            stale = [lg.id for slug, lg in live.items() if slug not in entries]
            if stale:
                unisdk.delete_logs(context=self._catalog_ctx, logs=stale)
                removed.extend(slug for slug in live if slug not in entries)
        except Exception:
            logger.exception(
                "Could not publish the workflow catalogue; the shelf may read "
                "stale until the next boot",
            )
            return {"published": sorted(published), "removed": sorted(removed)}

        self._store_catalog_hash(expected_hash)
        return {"published": sorted(published), "removed": sorted(removed)}

    @functools.wraps(BaseWorkflowManager.get_workflow, updated=())
    def get_workflow(self, *, slug: str) -> Dict[str, Any]:
        bundle = self._catalogue.get(slug)
        row = next(
            (r for r in self._installations() if r.get("slug") == slug),
            None,
        )
        if bundle is None and row is None:
            return {"found": False, "slug": slug}

        entry: Dict[str, Any] = {"found": True, "slug": slug}
        if bundle:
            entry.update(
                {
                    "name": bundle.name,
                    "description": bundle.description,
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
                },
            )
            if bundle is None:
                entry["orphaned"] = True
        return entry
