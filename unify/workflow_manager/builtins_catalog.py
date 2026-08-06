"""Public-read Builtins catalogue for the workflow shelf.

The shelf listing is platform data, not tenant data: one hand-curated
collection, identical for every assistant, exactly like the integrations
app catalogue. It lives as rows in the public-read Builtins project so a
reading surface (Console's gallery) renders it without waking an
assistant — a hosted assistant is an on-demand job and is usually asleep
when someone opens Console.

Everything per-assistant stays in each assistant's own contexts: the
installation rows, params, requirement/connection state, and the planted
content itself. Only the listing is global.

Seeding runs in bootstrap/admin processes (the deploy seed script, the
self-host install, the test harness) whose key owns the Builtins project;
assistants never write here. It is hash-guarded, so repeated runs are
cheap and idempotent — the same contract as the primitives, guidance and
integrations catalogues beside it.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import unisdk
from unisdk.utils.http import RequestError

from ..common.authorship import strip_authoring_assistant_id
from ..common.builtins import (
    builtins_project,
    ensure_builtins_project,
    read_seed_hashes,
    write_seed_hashes,
)
from ..common.log_utils import create_logs as unity_create_logs
from ..function_manager.hash_utils import stable_hash_for_rows
from .bundle import WorkflowBundle
from .types.catalog_entry import WorkflowCatalogEntry

logger = logging.getLogger(__name__)

BUILTINS_WORKFLOWS_CONTEXT = "Workflows/Catalog"
BUILTINS_WORKFLOWS_META_CONTEXT = "Workflows/Meta"
_HASH_MAP_KEY = "workflows_catalog_hash_by_unit"
_UNIT = "workflows"


def _ensure_catalog_storage(project: str) -> None:
    ensure_builtins_project(project)
    unisdk.create_context(
        BUILTINS_WORKFLOWS_CONTEXT,
        description="Public catalogue of installable workflows.",
        unique_keys={"slug": "str"},
        project=project,
    )
    unisdk.create_context(
        BUILTINS_WORKFLOWS_META_CONTEXT,
        description="Seeding state for the workflow catalogue.",
        unique_keys={"meta_id": "int"},
        project=project,
    )


def human_schedule(entry: Mapping[str, Any]) -> str:
    """A plain-language cadence for one task entry, or "" if it has none.

    Reading surfaces show a workflow's recurring jobs before it is
    installed, and "every weekday at 08:30" is the part a user actually
    weighs. Derived here rather than in the reader so every surface says
    it the same way.
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
        if set(weekdays) == {"MO", "TU", "WE", "TH", "FR"}:
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


def bundle_sets(bundle: WorkflowBundle) -> Dict[str, Any]:
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
                schedule = human_schedule(fields)
                if schedule:
                    item["schedule"] = schedule
            items.append(item)
        sets[surface] = items
    return sets


def catalog_row(bundle: WorkflowBundle) -> Dict[str, Any]:
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
        sets=json.dumps(bundle_sets(bundle), sort_keys=True),
    )
    return strip_authoring_assistant_id(entry.model_dump(mode="json"))


def _default_bundles() -> Optional[List[WorkflowBundle]]:
    """The curated bundles this environment ships, or None when it has none.

    Resolution order matches the runtime's: an explicit
    ``UNITY_WORKFLOWS_DIR``, then the installed ``unify_deploy`` package.
    ``None`` (as opposed to ``[]``) means "nothing to reconcile" — the seed
    ensures storage exists and stops, so an environment without the curated
    tree cannot empty a catalogue another environment seeded.
    """
    from ..settings import SETTINGS
    from .catalog import load_catalog

    configured = (SETTINGS.UNITY_WORKFLOWS_DIR or "").strip()
    if configured:
        return load_catalog(Path(configured))
    try:
        from unify_deploy.assistant_deployments.workflows import workflows_root

        return load_catalog(workflows_root())
    except Exception:
        return None


def seed_builtin_workflows(
    *,
    bundles: Optional[List[WorkflowBundle]] = None,
    project: str | None = None,
) -> bool:
    """Seed the public workflow catalogue. Returns True when rows changed.

    Omitting *bundles* resolves them from this environment's curated tree;
    an environment with no tree only guarantees storage exists. Passing
    ``bundles=[]`` explicitly empties the catalogue.
    """
    project = project or builtins_project()

    if bundles is None:
        bundles = _default_bundles()

    # Read-only probe first: a pre-seeded, unchanged catalogue must be
    # verifiable by any principal without owner-guarded writes.
    try:
        current_hashes = read_seed_hashes(
            project,
            meta_context=BUILTINS_WORKFLOWS_META_CONTEXT,
            key=_HASH_MAP_KEY,
        )
        storage_ready = True
    except RequestError:
        storage_ready = False
        current_hashes = {}

    if bundles is None:
        if not storage_ready:
            _ensure_catalog_storage(project)
        return False

    rows = {bundle.slug: catalog_row(bundle) for bundle in bundles}
    hash_fields = sorted(WorkflowCatalogEntry.model_fields)
    expected_hash = stable_hash_for_rows(
        list(rows.values()),
        fields=hash_fields,
        sort_field="slug",
    )
    if storage_ready and current_hashes.get(_UNIT) == expected_hash:
        logger.debug("Workflow catalogue unchanged; skipping seed")
        return False

    _ensure_catalog_storage(project)

    live_logs = unisdk.get_logs(
        project=project,
        context=BUILTINS_WORKFLOWS_CONTEXT,
        limit=1000,
    )
    live = {
        str((lg.entries or {}).get("slug")): lg
        for lg in live_logs
        if (lg.entries or {}).get("slug")
    }

    inserts = [payload for slug, payload in rows.items() if slug not in live]
    if inserts:
        unity_create_logs(
            context=BUILTINS_WORKFLOWS_CONTEXT,
            project=project,
            entries=inserts,
            stamp_authoring=True,
            batched=True,
        )

    for slug, payload in rows.items():
        existing = live.get(slug)
        if existing is None:
            continue
        current = {key: (existing.entries or {}).get(key) for key in payload}
        if current == payload:
            continue
        unisdk.update_logs(
            project=project,
            context=BUILTINS_WORKFLOWS_CONTEXT,
            logs=[existing.id],
            entries=payload,
            overwrite=True,
        )

    stale = [lg.id for slug, lg in live.items() if slug not in rows]
    if stale:
        unisdk.delete_logs(
            project=project,
            context=BUILTINS_WORKFLOWS_CONTEXT,
            logs=stale,
        )

    hashes = dict(current_hashes)
    hashes[_UNIT] = expected_hash
    write_seed_hashes(
        project,
        hashes,
        meta_context=BUILTINS_WORKFLOWS_META_CONTEXT,
        key=_HASH_MAP_KEY,
    )
    logger.info(
        "Seeded workflow catalogue project=%s rows=%d removed=%d",
        project,
        len(rows),
        len(stale),
    )
    return True
