"""Public-read Builtins catalogue for the workflow shelf.

The shelf listing is platform data, not tenant data: one hand-curated
collection, identical for every assistant, exactly like the integrations
app catalogue. It lives as rows in the public-read Builtins project so a
reading surface (Console's gallery) renders it without waking an
assistant — a hosted assistant is an on-demand job and is usually asleep
when someone opens Console.

Two contexts are published: ``Workflows/Catalog`` (one listing row per
bundle) and ``Workflows/Content`` (one row per artifact a bundle would
plant, substance included, so a reader can open a procedure or a task
brief before anything is installed). Everything per-assistant stays in
each assistant's own contexts: the installation rows, params,
requirement/connection state, and the planted content itself — the
global rows are the shelf, never the live copies.

Seeding runs in bootstrap/admin processes (the deploy seed script, the
self-host install, the test harness) whose key owns the Builtins project;
assistants never write here. It is hash-guarded, so repeated runs are
cheap and idempotent — the same contract as the primitives, guidance and
integrations catalogues beside it.
"""

from __future__ import annotations

import json
import logging
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
from .types.content_entry import WorkflowContentEntry

logger = logging.getLogger(__name__)

BUILTINS_WORKFLOWS_CONTEXT = "Workflows/Catalog"
BUILTINS_WORKFLOWS_CONTENT_CONTEXT = "Workflows/Content"
BUILTINS_WORKFLOWS_META_CONTEXT = "Workflows/Meta"
_HASH_MAP_KEY = "workflows_catalog_hash_by_unit"
_CATALOG_UNIT = "workflows"
_CONTENT_UNIT = "workflows_content"


def ensure_catalog_storage(project: str) -> None:
    """Create the catalogue contexts, without reading or writing a single row.

    The storage-only half of seeding. A caller that only needs the contexts
    to exist must reach this rather than the seeder, because the seeder
    takes desired state and an empty desired state is a wipe.
    """
    ensure_builtins_project(project)
    unisdk.create_context(
        BUILTINS_WORKFLOWS_CONTEXT,
        description="Public catalogue of installable workflows.",
        unique_keys={"slug": "str"},
        project=project,
    )
    unisdk.create_context(
        BUILTINS_WORKFLOWS_CONTENT_CONTEXT,
        description="The artifacts each catalogued workflow would plant.",
        unique_keys={"content_key": "str"},
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
        about=bundle.about,
        requirements=json.dumps(
            [
                {
                    "slug": requirement.slug,
                    "name": requirement.name or requirement.slug,
                    # `kind` and the declared secrets travel with the
                    # requirement or a reader cannot resolve it. A workspace
                    # is not in the gallery by design, so a reader that knows
                    # only the slug looks it up, finds nothing, and reports
                    # "couldn't check this app" about the one requirement
                    # whose answer never depended on the gallery.
                    "kind": requirement.kind,
                    # Every app that would satisfy it, recommended first.
                    # A reader that only saw the first one would offer a
                    # Slack connect to a Discord user for a workflow that
                    # serves them identically.
                    "alternatives": [
                        {"slug": option.slug, "name": option.display_name}
                        for option in requirement.alternatives
                    ],
                    "required_secrets": list(requirement.required_secrets),
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


_CONTENT_BODY_FIELD = {
    "tasks": "description",
    "functions": "docstring",
}
_CONTENT_META_FIELDS = {
    # Whatever the artifact's own page shows, so a reading surface can render
    # the real view rather than an approximation of it. Everything here is
    # already in the collected entry; the alternative is a second, thinner
    # description of each artifact that drifts from the page it imitates.
    "guidance": ("function_names",),
    "knowledge": ("kind", "topics", "status", "source_refs"),
    "tasks": (
        "repeat",
        "trigger",
        "tags",
        "priority",
        "schedule",
        "deadline",
        "entrypoint_function",
    ),
    "functions": (
        "argspec",
        "depends_on",
        "guidance_ids",
        "precondition",
        "is_primitive",
        "verify",
        "implementation",
    ),
}

_CONTENT_META_CONSTANTS = {
    # Bundles ship functions as `functions/*.py`, and the artifact's page names
    # the language. Not on the collected entry because the deployment sync
    # never needed it.
    "functions": {"language": "python"},
}


def content_rows(bundle: WorkflowBundle) -> List[Dict[str, Any]]:
    """One row per artifact the bundle would plant, readable substance
    included, so a reading surface can open any of them pre-install."""
    rows: List[Dict[str, Any]] = []
    for surface, source in sorted(bundle.surfaces.items()):
        body_field = _CONTENT_BODY_FIELD.get(surface, "content")
        meta_fields = _CONTENT_META_FIELDS.get(surface, ())
        for key, entry in sorted(source.items()):
            fields = entry if isinstance(entry, Mapping) else {}
            meta = {
                name: fields[name]
                for name in meta_fields
                if fields.get(name) not in (None, "", [], {})
            }
            meta.update(_CONTENT_META_CONSTANTS.get(surface, {}))
            row = WorkflowContentEntry(
                content_key=f"{bundle.slug}/{surface}/{key}",
                slug=bundle.slug,
                surface=surface,
                key=key,
                name=str(fields.get("name") or fields.get("title") or key),
                body=str(fields.get(body_field) or ""),
                schedule=human_schedule(fields) if surface == "tasks" else "",
                meta=json.dumps(meta, sort_keys=True, default=str),
            )
            rows.append(strip_authoring_assistant_id(row.model_dump(mode="json")))
    return rows


def _reconcile_rows(
    *,
    project: str,
    context: str,
    rows: Dict[str, Dict[str, Any]],
    key_field: str,
) -> None:
    """Converge one context onto *rows*.

    Changed rows are replaced (delete + insert), the same shape the sibling
    builtins seeders use: the Builtins writer routes deletes and inserts by
    project, and rows here carry no state beyond what the seed derives, so
    replacement loses nothing.
    """
    live_logs = unisdk.get_logs(project=project, context=context, limit=1000)
    live = {
        str((lg.entries or {}).get(key_field)): lg
        for lg in live_logs
        if (lg.entries or {}).get(key_field)
    }

    def unchanged(key: str) -> bool:
        existing = live.get(key)
        if existing is None:
            return False
        payload = rows[key]
        return {name: (existing.entries or {}).get(name) for name in payload} == payload

    doomed = [
        lg.id for key, lg in live.items() if key not in rows or not unchanged(key)
    ]
    if doomed:
        unisdk.delete_logs(project=project, context=context, logs=doomed)

    inserts = [payload for key, payload in rows.items() if not unchanged(key)]
    if inserts:
        unity_create_logs(
            context=context,
            project=project,
            entries=inserts,
            stamp_authoring=True,
            batched=True,
        )


def _default_bundles() -> Optional[List[WorkflowBundle]]:
    """The curated bundles this environment ships, or None when it has none.

    Resolution order matches the runtime's: an explicit
    ``UNITY_WORKFLOWS_DIR``, then the installed ``unify_deploy`` package.
    ``None`` (as opposed to ``[]``) means "nothing to reconcile" — the seed
    ensures storage exists and stops, so an environment without the curated
    tree cannot empty a catalogue another environment seeded.
    """
    from .catalog import load_catalog, resolve_catalogue_root

    # A tree that is not there is "nothing to reconcile", never "the shelf is
    # empty". `load_catalog` answers `[]` for a missing root, and `[]` here
    # means *delete every published workflow*.
    root = resolve_catalogue_root()
    if root is None:
        return None

    try:
        return load_catalog(root)
    except Exception:
        # A malformed bundle must not read as "no shelf". Loud, because the
        # seed's only other signal is "already up to date" — which is what a
        # missing canvas source in the packaged tree reported for hours while
        # the catalogue sat empty.
        logger.exception(
            "Workflow catalogue at %s failed to load; the published shelf is "
            "left as it is and will not be updated until this is fixed",
            root,
        )
        raise


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
            ensure_catalog_storage(project)
        return False

    catalog = {bundle.slug: catalog_row(bundle) for bundle in bundles}
    content = {
        row["content_key"]: row for bundle in bundles for row in content_rows(bundle)
    }
    units = {
        _CATALOG_UNIT: (
            BUILTINS_WORKFLOWS_CONTEXT,
            "slug",
            catalog,
            sorted(WorkflowCatalogEntry.model_fields),
        ),
        _CONTENT_UNIT: (
            BUILTINS_WORKFLOWS_CONTENT_CONTEXT,
            "content_key",
            content,
            sorted(WorkflowContentEntry.model_fields),
        ),
    }

    expected_hashes = {
        unit: stable_hash_for_rows(
            list(rows.values()),
            fields=hash_fields,
            sort_field=key_field,
        )
        for unit, (_, key_field, rows, hash_fields) in units.items()
    }
    stale_units = {
        unit
        for unit, expected in expected_hashes.items()
        if current_hashes.get(unit) != expected
    }
    if storage_ready and not stale_units:
        logger.debug("Workflow catalogue unchanged; skipping seed")
        return False

    ensure_catalog_storage(project)

    for unit in sorted(stale_units):
        context, key_field, rows, _ = units[unit]
        _reconcile_rows(
            project=project,
            context=context,
            rows=rows,
            key_field=key_field,
        )

    write_seed_hashes(
        project,
        {**current_hashes, **expected_hashes},
        meta_context=BUILTINS_WORKFLOWS_META_CONTEXT,
        key=_HASH_MAP_KEY,
    )
    logger.info(
        "Seeded workflow catalogue project=%s workflows=%d artifacts=%d",
        project,
        len(catalog),
        len(content),
    )
    return True
