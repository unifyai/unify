"""Loading the curated workflow catalogue from disk.

File-only symbolic tests: the loader turns bundle directories into
``WorkflowBundle``s through the same collectors the deployment sync
uses, so what these pin is the manifest contract — identity, strictness,
and the requirement/params parsing the console gallery renders.
"""

import json
from pathlib import Path

import pytest

from unify.workflow_manager.bundle import WorkflowBundle
from unify.workflow_manager.catalog import (
    MANIFEST_FILENAME,
    load_bundle,
    load_catalog,
)

_MANIFEST = """\
slug: daily_briefing
name: Daily briefing
version: "1.2.0"
category: comms
icon_id: briefing
description: Your calendar and the unread that matters, before stand-up.
about: |
  Every weekday morning the briefing reads your calendar and inbox.

  It arrives as one chat message before stand-up.
requirements:
  - slug: gmail
    name: Gmail
    required_secrets: [GMAIL_TOKEN]
  - notion
capabilities: [filesystem]
params_schema:
  mailbox:
    required: true
    help: Which mailbox to read.
"""


def _write_bundle(root: Path, slug: str = "daily_briefing") -> Path:
    bundle_dir = root / slug
    bundle_dir.mkdir(parents=True)
    (bundle_dir / MANIFEST_FILENAME).write_text(_MANIFEST)

    guidance_dir = bundle_dir / "guidance"
    guidance_dir.mkdir()
    (guidance_dir / "guidance.jsonl").write_text(
        json.dumps(
            {
                "key": "wf/triage",
                "title": "Triage",
                "content": "Oldest first.",
            },
        )
        + "\n",
    )
    tasks_dir = bundle_dir / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "tasks.jsonl").write_text(
        json.dumps(
            {
                "key": "wf/morning",
                "name": "Morning run",
                "description": "The recurring job.",
                "repeat": [{"frequency": "daily"}],
            },
        )
        + "\n",
    )
    return bundle_dir


def test_load_bundle_reads_identity_and_content(tmp_path: Path):
    bundle = load_bundle(_write_bundle(tmp_path))

    assert bundle.slug == "daily_briefing"
    assert bundle.name == "Daily briefing"
    assert bundle.version == "1.2.0"
    assert bundle.category == "comms"
    assert bundle.icon_id == "briefing"
    assert bundle.about.startswith("Every weekday morning")
    assert "\n\n" in bundle.about
    assert bundle.capabilities == ("filesystem",)
    assert bundle.params_schema["mailbox"]["required"] is True

    # Only the content directories actually present become surfaces.
    assert sorted(bundle.surfaces) == ["guidance", "tasks"]
    assert "wf/triage" in bundle.surfaces["guidance"]
    assert bundle.surfaces["guidance"]["wf/triage"]["custom_hash"]


def test_load_bundle_parses_requirements_in_both_shapes(tmp_path: Path):
    """A bare string is shorthand for naming an app and letting the
    resolver decide what connecting means; the mapping shape additionally
    declares secrets, for apps no other authority can answer for."""
    bundle = load_bundle(_write_bundle(tmp_path))

    gmail, notion = bundle.requirements
    assert gmail.slug == "gmail"
    assert gmail.required_secrets == ("GMAIL_TOKEN",)
    assert notion.slug == "notion"
    assert notion.required_secrets == ()
    # Absent `kind` means an ordinary app the integrations layer connects.
    assert (gmail.kind, notion.kind) == ("app", "app")


def test_slug_must_match_the_directory_name(tmp_path: Path):
    """The slug is stamped as managed_by on every planted row, so a
    directory rename must never silently re-identify a workflow."""
    bundle_dir = _write_bundle(tmp_path)
    renamed = tmp_path / "renamed_briefing"
    bundle_dir.rename(renamed)

    with pytest.raises(ValueError, match="identity migration"):
        load_bundle(renamed)


def test_manifest_without_identity_is_refused(tmp_path: Path):
    bundle_dir = tmp_path / "nameless"
    bundle_dir.mkdir()
    (bundle_dir / MANIFEST_FILENAME).write_text("slug: nameless\n")

    with pytest.raises(ValueError, match="'name'"):
        load_bundle(bundle_dir)


def test_load_catalog_walks_only_bundle_directories(tmp_path: Path):
    _write_bundle(tmp_path)
    (tmp_path / "not_a_bundle").mkdir()
    (tmp_path / "README.md").write_text("shelf notes\n")

    bundles = load_catalog(tmp_path)
    assert [b.slug for b in bundles] == ["daily_briefing"]


def test_load_catalog_of_a_missing_root_is_empty(tmp_path: Path):
    assert load_catalog(tmp_path / "nowhere") == []


def test_load_catalog_is_strict(tmp_path: Path):
    """The direct loader raises on a malformed bundle — a curated bundle
    silently vanishing from the shelf is worse than a loud failure. The
    boot path isolates per-bundle failures on top of this."""
    _write_bundle(tmp_path)
    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / MANIFEST_FILENAME).write_text("slug: broken\n")

    with pytest.raises(ValueError, match="'name'"):
        load_catalog(tmp_path)


def test_actor_tools_are_gated_on_the_catalogue():
    """WorkflowManager_* tools enter the actor's schema only when the
    curated catalogue is configured: deployments without a shelf keep
    their tool set — and their LLM caches — byte-identical."""

    def tool():
        pass

    from unify.actor.prompt_builders import build_code_act_prompt

    with_shelf = build_code_act_prompt(
        environments={},
        tools={"execute_code": tool, "WorkflowManager_install_workflow": tool},
    )
    assert "Installable workflows" in with_shelf

    without_shelf = build_code_act_prompt(
        environments={},
        tools={"execute_code": tool},
    )
    assert "Installable workflows" not in without_shelf


def test_workflow_manager_tools_carry_the_llm_contract():
    """The tool docstrings are the LLM-facing contract, attached from
    base.py via functools.wraps — an implementation method without its
    base docstring ships an undocumented tool."""
    from unify.workflow_manager.workflow_manager import WorkflowManager

    for method in (
        "list_workflows",
        "get_workflow",
        "install_workflow",
        "uninstall_workflow",
        "reconcile_installed",
        "get_installation_params",
    ):
        doc = getattr(WorkflowManager, method).__doc__ or ""
        assert len(doc) > 100, f"{method} lost its base docstring"


# --------------------------------------------------------------------- #
# Human-readable cadence                                                #
# --------------------------------------------------------------------- #
def test_human_schedule_reads_as_plain_language():
    """Reading surfaces show a workflow's cadence before it is installed,
    and that phrasing is the part a user actually weighs. Derived once
    here so every surface says it identically."""
    from unify.workflow_manager.builtins_catalog import human_schedule as schedule

    weekdays = {
        "repeat": [
            {
                "frequency": "weekly",
                "weekdays": ["MO", "TU", "WE", "TH", "FR"],
                "time_of_day": "08:30:00",
            },
        ],
    }
    assert schedule(weekdays) == "Every weekday at 08:30"
    assert (
        schedule({"repeat": [{"frequency": "daily", "time_of_day": "09:00:00"}]})
        == "Every day at 09:00"
    )
    assert (
        schedule(
            {
                "repeat": [
                    {
                        "frequency": "weekly",
                        "weekdays": ["FR"],
                        "time_of_day": "17:00:00",
                    },
                ],
            },
        )
        == "Every Fri at 17:00"
    )
    # A task with no recurrence is the provisioning one-shot; a triggered
    # task fires on an event. Both need saying, neither is a cadence.
    assert schedule({}) == "Once, at install"
    assert schedule({"trigger": {"medium": "email"}}) == "On a trigger"


def test_bundle_sets_names_what_each_surface_receives(tmp_path: Path):
    """The 'what this installs' list a reader shows before installing."""
    from unify.workflow_manager.builtins_catalog import bundle_sets

    bundle = load_bundle(_write_bundle(tmp_path))
    sets = bundle_sets(bundle)

    assert sets["guidance"] == [{"name": "Triage"}]
    assert sets["tasks"] == [{"name": "Morning run", "schedule": "Every day"}]


def test_content_rows_carry_each_artifact_whole(tmp_path: Path):
    """The listing names what a workflow sets up; the content rows beside
    it carry the artifacts' substance, so a reader can open any of them
    before anything is installed."""
    from unify.workflow_manager.builtins_catalog import content_rows

    bundle = load_bundle(_write_bundle(tmp_path))
    rows = {row["content_key"]: row for row in content_rows(bundle)}

    assert set(rows) == {
        "daily_briefing/guidance/wf/triage",
        "daily_briefing/tasks/wf/morning",
    }

    triage = rows["daily_briefing/guidance/wf/triage"]
    assert triage["slug"] == "daily_briefing"
    assert triage["surface"] == "guidance"
    assert triage["key"] == "wf/triage"
    assert triage["name"] == "Triage"
    assert triage["body"] == "Oldest first."
    assert triage["schedule"] == ""

    morning = rows["daily_briefing/tasks/wf/morning"]
    assert morning["name"] == "Morning run"
    assert morning["body"] == "The recurring job."
    assert morning["schedule"] == "Every day"


def test_content_rows_publish_what_the_artifacts_own_page_shows(tmp_path: Path):
    """A preview is meant to *be* the artifact's page, not a thinner retelling
    of it, so the fields that page renders travel with the row. Anything absent
    is simply absent — the reader degrades rather than inventing."""
    import json as _json

    from unify.workflow_manager.builtins_catalog import content_rows

    bundle = load_bundle(_write_bundle(tmp_path))
    rows = {row["content_key"]: row for row in content_rows(bundle)}

    task_meta = _json.loads(rows["daily_briefing/tasks/wf/morning"]["meta"])
    # The cadence the page shows comes from `repeat`; `schedule` on the row is
    # the already-phrased prose, and both are needed for different fields.
    assert task_meta["repeat"] == [{"frequency": "daily"}]

    guidance_meta = _json.loads(rows["daily_briefing/guidance/wf/triage"]["meta"])
    # Nothing declared on this entry, so nothing is published for it.
    assert guidance_meta == {}


# --------------------------------------------------------------------- #
# Canvas + data: what a bundle may ship, and what it may not            #
# --------------------------------------------------------------------- #
def _write_canvas(bundle_dir: Path, name: str, manifest: dict, tsx: str) -> Path:
    view_dir = bundle_dir / "canvas" / name
    view_dir.mkdir(parents=True)
    (view_dir / "view.json").write_text(json.dumps(manifest) + "\n")
    (view_dir / "view.tsx").write_text(tsx)
    return view_dir


def test_a_bundle_ships_canvas_source_outside_the_surfaces(tmp_path: Path):
    """A view is not a synced surface and must not become one.

    It is TypeScript that has to be compiled, rendered and reviewed against
    the kit installed *now*, and its routing token has a lifecycle the
    reconcile engine does not own — so it loads onto its own field, where
    the fan-out cannot reach it.
    """
    bundle_dir = _write_bundle(tmp_path)
    _write_canvas(
        bundle_dir,
        "monthly_kpis",
        {
            "title": "Monthly KPIs",
            "description": "Revenue and burn, by month.",
            "bindings": [{"context": "Finance/Invoices"}],
            "visibility": "private",
        },
        "export default function View() { return null; }\n",
    )

    bundle = load_bundle(bundle_dir)

    assert "canvas" not in bundle.surfaces
    assert [view.name for view in bundle.canvas] == ["monthly_kpis"]
    view = bundle.canvas[0]
    assert view.title == "Monthly KPIs"
    assert view.bindings == ({"context": "Finance/Invoices"},)
    assert view.tsx.startswith("export default")
    # Identity across reinstalls, and a fingerprint that changes with the
    # source rather than with the compiled output.
    assert view.custom_key == "monthly_kpis"
    first = view.content_hash()
    assert first == load_bundle(bundle_dir).canvas[0].content_hash()
    (bundle_dir / "canvas" / "monthly_kpis" / "view.tsx").write_text(
        "export default 1;",
    )
    assert load_bundle(bundle_dir).canvas[0].content_hash() != first


def test_a_prebuilt_canvas_is_refused(tmp_path: Path):
    """The reason decision 9 was reversed. A compiled bundle in a git
    bundle pins a host runtime: it is built against one kit and planted
    into whatever host the deployment runs, so it breaks at *view* time,
    for the user, with nothing failing at plant time."""
    bundle_dir = _write_bundle(tmp_path)
    view_dir = _write_canvas(
        bundle_dir,
        "monthly_kpis",
        {"title": "Monthly KPIs"},
        "export default function View() { return null; }\n",
    )
    (view_dir / "bundle.js").write_text("/* compiled elsewhere */")

    with pytest.raises(ValueError, match="ships a built"):
        load_bundle(bundle_dir)


def test_a_canvas_needs_both_its_source_and_its_manifest(tmp_path: Path):
    bundle_dir = _write_bundle(tmp_path)
    view_dir = bundle_dir / "canvas" / "orphan"
    view_dir.mkdir(parents=True)
    (view_dir / "view.tsx").write_text(
        "export default function View() { return null; }",
    )

    with pytest.raises(ValueError, match="no view.json"):
        load_bundle(bundle_dir)


def test_a_bundle_ships_table_schemas_but_never_rows(tmp_path: Path):
    """A bundle is published verbatim and installed identically by
    everyone, so rows in it are one author's data handed to every
    installer. The table is the contract; filling it is the workflow's own
    job at run time."""
    bundle_dir = _write_bundle(tmp_path)
    table_dir = bundle_dir / "data" / "Finance" / "Invoices"
    table_dir.mkdir(parents=True)
    (table_dir / "meta.json").write_text(
        json.dumps(
            {
                "description": "Invoices this workflow reconciles.",
                "fields": {"reference": "str", "amount": "float"},
                "seed_key": "reference",
            },
        )
        + "\n",
    )

    bundle = load_bundle(bundle_dir)
    # Normalised into the Data namespace: a context outside it cannot be
    # installed to a team and cannot be read by a canvas.
    assert "Data/Finance/Invoices" in bundle.surfaces["data"]
    assert bundle.surfaces["data"]["Data/Finance/Invoices"]["rows"] == []
    assert bundle.surfaces["data"]["Data/Finance/Invoices"]["context"] == (
        "Data/Finance/Invoices"
    )

    (table_dir / "rows.jsonl").write_text(
        json.dumps({"reference": "INV-1", "amount": 10.0}) + "\n",
    )
    with pytest.raises(ValueError, match="ship seeded rows"):
        load_bundle(bundle_dir)


def test_a_requirement_publishes_how_it_is_resolved(tmp_path: Path):
    """A reader gets the slug *and* how to answer it.

    The shelf published only the slug and the display name, so a reader had
    nothing to go on but a gallery lookup — and a workspace is the user's own
    account, deliberately not a catalogue app. Meeting prep's Google
    Workspace requirement therefore rendered as "couldn't check this app",
    about the one requirement whose answer never depended on the gallery.
    """
    from unify.workflow_manager.builtins_catalog import catalog_row
    from unify.workflow_manager.bundle import WorkflowRequirement

    bundle = load_bundle(_write_bundle(tmp_path))
    published = json.loads(
        catalog_row(
            WorkflowBundle(
                slug=bundle.slug,
                name=bundle.name,
                requirements=(
                    WorkflowRequirement(slug="gmail", name="Gmail"),
                    WorkflowRequirement(
                        slug="google_workspace",
                        name="Google Workspace",
                        kind="workspace",
                        required_secrets=("GOOGLE_REFRESH_TOKEN",),
                    ),
                ),
            ),
        )["requirements"],
    )

    assert published == [
        {
            "slug": "gmail",
            "name": "Gmail",
            "kind": "app",
            "alternatives": [],
            "required_secrets": [],
        },
        {
            "slug": "google_workspace",
            "name": "Google Workspace",
            "kind": "workspace",
            "alternatives": [],
            "required_secrets": ["GOOGLE_REFRESH_TOKEN"],
        },
    ]


def test_a_requirement_publishes_every_app_that_would_satisfy_it(tmp_path: Path):
    """A choice the bundle offers has to survive publication.

    The requirement is met by any one of its apps, so a reader that saw only
    the first would offer a Slack connect to someone on Discord for a
    workflow that serves them identically.
    """
    from unify.workflow_manager.builtins_catalog import catalog_row
    from unify.workflow_manager.bundle import RequirementOption, WorkflowRequirement

    bundle = load_bundle(_write_bundle(tmp_path))
    published = json.loads(
        catalog_row(
            WorkflowBundle(
                slug=bundle.slug,
                name=bundle.name,
                requirements=(
                    WorkflowRequirement(
                        slug="slack",
                        name="Slack",
                        alternatives=(
                            RequirementOption("discord", "Discord"),
                            # Name omitted on purpose: the slug stands in, so
                            # a chip is never blank.
                            RequirementOption("microsoft_teams"),
                        ),
                    ),
                ),
            ),
        )["requirements"],
    )

    assert published[0]["alternatives"] == [
        {"slug": "discord", "name": "Discord"},
        {"slug": "microsoft_teams", "name": "microsoft_teams"},
    ]


def test_alternatives_are_read_from_the_manifest(tmp_path: Path):
    """Declared as slugs or as mappings, in recommendation order."""
    bundle_dir = _write_bundle(tmp_path)
    manifest = _MANIFEST.replace(
        """requirements:
  - slug: gmail
    name: Gmail
    required_secrets: [GMAIL_TOKEN]
  - notion
""",
        """requirements:
  - slug: slack
    name: Slack
    alternatives:
      - slug: discord
        name: Discord
      - microsoft_teams
""",
    )
    (bundle_dir / MANIFEST_FILENAME).write_text(manifest)

    requirement = load_bundle(bundle_dir).requirements[0]
    assert [option.slug for option in requirement.options] == [
        "slack",
        "discord",
        "microsoft_teams",
    ]
    assert requirement.options[2].display_name == "microsoft_teams"


def test_a_missing_tree_never_reads_as_an_empty_shelf(tmp_path: Path, monkeypatch):
    """The failure that emptied the staging catalogue.

    ``load_catalog`` answers ``[]`` for a root that does not exist, and ``[]``
    to the seeder means *delete every published workflow*. A mispointed
    ``UNIFY_WORKFLOWS_DIR``, or an image built without the curated tree, would
    therefore wipe the shelf for everyone — and the wipe is indistinguishable
    from a deliberate one.
    """
    from unify.settings import SETTINGS
    from unify.workflow_manager import builtins_catalog

    monkeypatch.setattr(
        SETTINGS,
        "UNIFY_WORKFLOWS_DIR",
        str(tmp_path / "not-here"),
        raising=False,
    )
    assert builtins_catalog._default_bundles() is None


def test_a_malformed_bundle_raises_rather_than_reading_as_no_shelf(
    tmp_path: Path,
    monkeypatch,
):
    """The other half of the same incident.

    The packaged tree carried a canvas manifest with no ``view.tsx`` beside
    it — the loader refuses that, correctly. Swallowing the error turned it
    into "no shelf to reconcile", which the seed reports as "already up to
    date", so an empty catalogue stayed empty with every run claiming success.
    """
    from unify.settings import SETTINGS
    from unify.workflow_manager import builtins_catalog

    bundle_dir = _write_bundle(tmp_path)
    view_dir = bundle_dir / "canvas" / "orphan"
    view_dir.mkdir(parents=True)
    (view_dir / "view.json").write_text(json.dumps({"title": "Orphan"}) + "\n")

    monkeypatch.setattr(SETTINGS, "UNIFY_WORKFLOWS_DIR", str(tmp_path), raising=False)
    with pytest.raises(ValueError, match="no view.tsx"):
        builtins_catalog._default_bundles()


def test_making_room_for_the_catalogue_never_touches_its_rows():
    """The third way an empty shelf gets published: asking the wrong question.

    A caller that only wants the contexts to exist has one verb for it.
    Reaching the seeder instead means handing it desired state, and the
    empty desired state that reads like "nothing to do" is the one input
    that deletes every row.
    """
    import inspect

    from unify.workflow_manager.builtins_catalog import ensure_catalog_storage

    body = inspect.getsource(ensure_catalog_storage)
    assert "create_context" in body
    for row_verb in ("delete_logs", "create_logs", "update_logs", "_reconcile_rows"):
        assert row_verb not in body


def test_the_shelf_has_one_resolution_not_two(tmp_path: Path, monkeypatch):
    """Console showed six workflows; every install said the shelf was empty.

    The seeder fell back to the installed unify_deploy package when
    UNIFY_WORKFLOWS_DIR was unset. The runtime registry did not — it just
    returned None. So a deployment with the package installed and no env var
    published six workflows to the catalogue Console renders, while the
    assistant that has to install them registered none, and every install
    failed with "Available: nothing — the catalogue is empty" against a
    catalogue that plainly had six rows in it.
    """
    import inspect

    from unify.settings import SETTINGS
    from unify.workflow_manager import builtins_catalog
    from unify.workflow_manager.catalog import (
        bootstrap_workflow_catalog,
        resolve_catalogue_root,
    )

    _write_bundle(tmp_path)
    monkeypatch.setattr(SETTINGS, "UNIFY_WORKFLOWS_DIR", str(tmp_path), raising=False)

    # Both reach the same tree, because both ask the same question.
    assert resolve_catalogue_root() == tmp_path
    assert [b.slug for b in builtins_catalog._default_bundles()] == ["daily_briefing"]

    for func in (bootstrap_workflow_catalog, builtins_catalog._default_bundles):
        assert "resolve_catalogue_root" in inspect.getsource(
            func,
        ), f"{func.__name__} must not resolve the catalogue root itself"


def test_the_install_path_sees_the_bundles_boot_loaded(tmp_path: Path, monkeypatch):
    """The shelf is full and every install still said it was empty.

    bootstrap_workflow_catalog constructed its own WorkflowManager, loaded
    every bundle into it, and returned it — while the install path asks
    ``ManagerRegistry.get_workflow_manager()``, which lazily built a
    *different*, bundle-less instance. The registry getter never returns
    None once asked, so the request handler did not report "no shelf here";
    it reported "the catalogue is empty" against a catalogue that had just
    been filled, and the request rows recorded unknown_workflow.

    Loading bundles into an object nobody else holds is the bug, so this
    asserts on the object the installer actually reaches.
    """
    from unify.manager_registry import ManagerRegistry
    from unify.settings import SETTINGS
    from unify.workflow_manager.catalog import bootstrap_workflow_catalog

    _write_bundle(tmp_path)
    monkeypatch.setattr(SETTINGS, "UNIFY_WORKFLOWS_DIR", str(tmp_path), raising=False)

    booted = bootstrap_workflow_catalog()
    assert booted is not None

    # What install_workflow resolves, not what bootstrap happened to return.
    from_registry = ManagerRegistry.get_workflow_manager()
    assert from_registry is booted
    assert [b.slug for b in from_registry.available_bundles()] == ["daily_briefing"]


def test_a_path_from_another_image_never_empties_the_shelf(tmp_path: Path, monkeypatch):
    """The one that actually broke staging for a day.

    The deployment resolves the catalogue inside the comms image, where the
    package sits at /app/unify_deploy, and stamps that absolute path into an
    assistant Job whose own image installs it under site-packages. The
    assistant then logged "Workflow catalogue root
    /app/unify_deploy/assistant_deployments/workflows does not exist",
    registered nothing, and every install reported an empty catalogue —
    with the package two directories away the whole time.

    A configured path is advice from another process. When it is not there,
    the container's own installed copy is the fact.
    """
    from unify.settings import SETTINGS
    from unify.workflow_manager import catalog as catalog_module

    packaged = tmp_path / "site-packages" / "workflows"
    packaged.mkdir(parents=True)
    _write_bundle(packaged)

    monkeypatch.setattr(
        SETTINGS,
        "UNIFY_WORKFLOWS_DIR",
        "/app/unify_deploy/assistant_deployments/workflows",
        raising=False,
    )

    import sys
    import types

    module = types.ModuleType("unify_deploy.assistant_deployments.workflows")
    module.workflows_root = lambda: packaged  # type: ignore[attr-defined]
    parent = types.ModuleType("unify_deploy.assistant_deployments")
    root_mod = types.ModuleType("unify_deploy")
    monkeypatch.setitem(sys.modules, "unify_deploy", root_mod)
    monkeypatch.setitem(sys.modules, "unify_deploy.assistant_deployments", parent)
    monkeypatch.setitem(
        sys.modules,
        "unify_deploy.assistant_deployments.workflows",
        module,
    )

    assert catalog_module.resolve_catalogue_root() == packaged


def test_nothing_resolves_the_catalogue_behind_the_resolver(
    tmp_path: Path,
    monkeypatch,
):
    """Three places decided where the shelf was, and disagreed in turn.

    First the seeder and the runtime registry disagreed when
    UNIFY_WORKFLOWS_DIR was unset. Then the scheduler that decides whether
    the bootstrap runs at all was found reading the env var directly and
    returning early when it was empty — so a deployment that ships the
    catalogue inside the image and sets no env var never loaded the shelf,
    silently, because an early return leaves nothing in the log.

    Each fix moved one caller onto the shared resolver and left the next one
    to be discovered in production. This asserts the property instead: the
    module reads that setting in exactly one function.
    """
    import inspect

    from unify.workflow_manager import catalog as catalog_module

    readers = [
        name
        for name, obj in vars(catalog_module).items()
        if inspect.isfunction(obj) and obj.__module__ == catalog_module.__name__
        # The read itself, not the name: the comments explaining this
        # history mention the setting and must not count as reading it.
        and "SETTINGS.UNIFY_WORKFLOWS_DIR" in inspect.getsource(obj)
    ]
    assert readers == ["resolve_catalogue_root"], (
        f"only resolve_catalogue_root may read the setting; also read by: "
        f"{sorted(set(readers) - {'resolve_catalogue_root'})}"
    )


def test_the_bootstrap_is_scheduled_when_the_image_ships_the_shelf(
    tmp_path,
    monkeypatch,
):
    """No env var, catalogue in the image: the shelf must still load."""
    import sys
    import types

    from unify.settings import SETTINGS
    from unify.workflow_manager import catalog as catalog_module

    packaged = tmp_path / "site-packages" / "workflows"
    packaged.mkdir(parents=True)
    _write_bundle(packaged)

    monkeypatch.setattr(SETTINGS, "UNIFY_WORKFLOWS_DIR", "", raising=False)
    module = types.ModuleType("unify_deploy.assistant_deployments.workflows")
    module.workflows_root = lambda: packaged  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "unify_deploy", types.ModuleType("unify_deploy"))
    monkeypatch.setitem(
        sys.modules,
        "unify_deploy.assistant_deployments",
        types.ModuleType("unify_deploy.assistant_deployments"),
    )
    monkeypatch.setitem(
        sys.modules,
        "unify_deploy.assistant_deployments.workflows",
        module,
    )

    # The gate that decides whether the bootstrap runs must not bail here.
    assert catalog_module.resolve_catalogue_root() == packaged
