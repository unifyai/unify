"""Installable workflow bundles.

Installing a workflow is a fan-out of ordinary custom syncs, one per
surface the bundle covers, each stamped with the bundle's slug as its
``managed_by``. That is the whole mechanism: there is no second reconcile
loop, no parallel store for workflow content, and no lookup path that
knows about workflows. What the slug buys is provenance — the ability to
ask a surface "which rows did this bundle plant?" and get an exact
answer, which is what makes update and uninstall possible at all.
"""

from __future__ import annotations

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
from .bundle import SurfaceRegistry, WorkflowBundle
from .types.meta import WorkflowMeta
from .types.workflow import UNASSIGNED, WorkflowInstallation, WorkflowMode

logger = logging.getLogger(__name__)

WORKFLOW_TABLE = "Workflows"
WORKFLOW_META_TABLE = "Workflows/Meta"


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
        ]

    def __init__(self) -> None:
        super().__init__()
        self._ctx = ContextRegistry.get_context(self, WORKFLOW_TABLE)
        self._meta_ctx = ContextRegistry.get_context(self, WORKFLOW_META_TABLE)
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
    def _plant(
        self,
        bundle: WorkflowBundle,
        *,
        surface_names: Optional[List[str]] = None,
        empty: bool = False,
    ) -> tuple[Dict[str, Any], Dict[str, BaseException]]:
        """Sync each covered surface under the bundle's slug.

        With ``empty=True`` every surface receives an empty source, which
        prunes exactly the rows carrying this slug and leaves every other
        source's rows — the deployment's, another workflow's, the user's
        own — untouched. That is uninstall, and it is the same code path
        as install precisely because the scoping is what does the work.
        """

        planted: Dict[str, Any] = {}
        failures: Dict[str, BaseException] = {}
        names = surface_names if surface_names is not None else bundle.surface_names()

        for name in names:
            source = {} if empty else bundle.surfaces.get(name, {})
            try:
                surface = self._surfaces.get(name)
                changed = surface.sync(source, managed_by=bundle.slug)
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
                "surfaces": bundle.surface_names(),
                "installed": is_installed,
            }
            if row:
                entry.update(
                    {
                        "installed_version": row.get("version", ""),
                        "mode": row.get("mode"),
                        "status": row.get("status"),
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
                    "mode": row.get("mode"),
                    "surfaces": json.loads(row.get("surfaces") or "[]"),
                },
            )

        entries.sort(key=lambda e: e["slug"])
        return {
            "workflows": entries[offset : offset + limit],
            "total": len(entries),
        }

    def install_workflow(
        self,
        *,
        slug: str,
        mode: str = "seed",
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

        try:
            resolved_mode = WorkflowMode(mode)
        except ValueError:
            raise ToolErrorException(
                {
                    "error": "invalid_mode",
                    "mode": mode,
                    "expected": [m.value for m in WorkflowMode],
                },
            )

        resolved_params = self._validate_params(bundle, params or {})
        context = self._workflow_context_for_destination(destination)
        meta_context = self._meta_context_for_destination(destination)

        with exclusive_sync_lease(f"{meta_context}:workflow_install"):
            existing = self._read_installation(slug, context=context)

            # Seed plants once. A repeat install refreshes the settings and
            # leaves the content alone, because the whole point of seed mode
            # is that what the user did to those rows since is theirs.
            already_seeded = (
                existing is not None
                and existing.get("mode") == WorkflowMode.seed.value
                and resolved_mode is WorkflowMode.seed
                and existing.get("status") == "installed"
            )

            if already_seeded:
                planted: Dict[str, Any] = {}
                failures: Dict[str, BaseException] = {}
            else:
                planted, failures = self._plant(bundle)

            record = WorkflowInstallation(
                workflow_id=(
                    existing.get("workflow_id", UNASSIGNED) if existing else UNASSIGNED
                ),
                slug=bundle.slug,
                name=bundle.name,
                version=bundle.version,
                description=bundle.description,
                mode=resolved_mode,
                status="partial" if failures else "installed",
                params=json.dumps(resolved_params, sort_keys=True),
                surfaces=json.dumps(bundle.surface_names()),
                destination=destination or "personal",
            )
            self._write_installation(record, context=context, existing=existing)

        result: Dict[str, Any] = {
            "installed": record.model_dump(mode="json"),
            "planted": planted,
            "content_unchanged": already_seeded,
        }
        if failures:
            result["failures"] = {n: str(e) for n, e in failures.items()}
            logger.warning(
                "Workflow %r installed with failures on surfaces: %s",
                slug,
                sorted(failures),
            )
        return result

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
                raise ToolErrorException(
                    {
                        "error": "not_installed",
                        "slug": slug,
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
                surface_names=recorded,
                empty=True,
            )
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
                    "mode": row.get("mode"),
                    "status": row.get("status"),
                    "params": json.loads(row.get("params") or "{}"),
                    "installed_surfaces": json.loads(row.get("surfaces") or "[]"),
                    "destination": row.get("destination", "personal"),
                },
            )
            if bundle is None:
                entry["orphaned"] = True
        return entry
