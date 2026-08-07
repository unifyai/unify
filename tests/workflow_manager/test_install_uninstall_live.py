"""Install and uninstall against the real managers.

These drive the real ``sync_custom_*`` receivers end to end — plant,
verify the rows landed, uninstall, verify they are gone and nobody
else's rows went with them. The symbolic fan-out tests pin only what
the WorkflowManager *sends*; a recording double is structurally blind
to a receiver that discards an empty source, which is exactly the
defect class this file exists to keep dead.
"""

import json
from pathlib import Path
from typing import Any, Dict

import pytest
import unisdk

from tests.helpers import _handle_project
from unify.common.context_registry import ContextRegistry
from unify.guidance_manager.custom_guidance import (
    GUIDANCE_JSONL_FILENAME,
    collect_custom_guidance,
)
from unify.guidance_manager.guidance_manager import GuidanceManager
from unify.task_scheduler.custom_tasks import (
    TASKS_JSONL_FILENAME,
    collect_custom_tasks,
)
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.workflow_manager.bundle import WorkflowBundle
from unify.workflow_manager.surfaces import register_default_surfaces
from unify.workflow_manager.workflow_manager import WorkflowManager

SLUG = "wf_live_demo"

_WORKFLOW_GUIDANCE = [
    {
        "key": "wf/triage",
        "title": "Workflow triage procedure",
        "content": "Read the inbox oldest-first.",
    },
    {
        "key": "wf/tone",
        "title": "Workflow tone procedure",
        "content": "Reply in the sender's register.",
    },
]

_DEPLOYMENT_GUIDANCE = [
    {
        # Same custom_key as a workflow entry: keys are scoped per source,
        # so these are two distinct rows that must never touch each other.
        "key": "wf/triage",
        "title": "Deployment triage procedure",
        "content": "The deployment's own triage notes.",
    },
]

_WORKFLOW_TASKS = [
    {
        "key": "wf/morning",
        "name": "Workflow morning run",
        "description": "The recurring job this workflow exists for.",
        "repeat": [{"frequency": "daily"}],
    },
]


def _write_jsonl(directory: Path, filename: str, rows: list) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    lines = "\n".join(json.dumps(row) for row in rows)
    (directory / filename).write_text(lines + "\n")
    return directory


@pytest.fixture
def live_managers():
    for cls, contexts in (
        (GuidanceManager, ("Guidance", "Guidance/Meta")),
        (TaskScheduler, ("Tasks", "Tasks/Meta")),
        (WorkflowManager, ("Workflows", "Workflows/Meta")),
    ):
        for name in contexts:
            ContextRegistry.forget(cls, name)

    gm = GuidanceManager()
    ts = TaskScheduler()
    wm = WorkflowManager()
    register_default_surfaces(
        wm.surfaces,
        guidance_manager=gm,
        task_scheduler=ts,
    )
    yield gm, ts, wm

    try:
        gm.clear()
    except Exception:
        pass


@pytest.fixture
def bundle(tmp_path: Path) -> WorkflowBundle:
    guidance_dir = _write_jsonl(
        tmp_path / "guidance",
        GUIDANCE_JSONL_FILENAME,
        _WORKFLOW_GUIDANCE,
    )
    tasks_dir = _write_jsonl(
        tmp_path / "tasks",
        TASKS_JSONL_FILENAME,
        _WORKFLOW_TASKS,
    )
    return WorkflowBundle(
        slug=SLUG,
        name="Live demo workflow",
        version="1.0.0",
        surfaces={
            "guidance": collect_custom_guidance(path=guidance_dir),
            "tasks": collect_custom_tasks(path=tasks_dir),
        },
    )


def _rows(context: str, managed_by: str) -> list[Dict[str, Any]]:
    logs = unisdk.get_logs(
        context=context,
        filter=f"managed_by == '{managed_by}'",
    )
    return [dict(lg.entries or {}) for lg in logs]


@_handle_project
@pytest.mark.asyncio
async def test_install_plants_and_uninstall_prunes_for_real(
    live_managers,
    bundle,
    tmp_path: Path,
):
    gm, ts, wm = live_managers

    # A deployment row sharing a workflow entry's custom_key, planted
    # first: uninstall must prune around it.
    deployment_dir = _write_jsonl(
        tmp_path / "deployment_guidance",
        GUIDANCE_JSONL_FILENAME,
        _DEPLOYMENT_GUIDANCE,
    )
    gm.sync_custom_guidance(
        source_guidance=collect_custom_guidance(path=deployment_dir),
    )

    wm.register_bundle(bundle)
    result = wm.install_workflow(slug=SLUG)

    assert "failures" not in result
    assert result["planted"]["guidance"]["entries"] == 2
    assert result["planted"]["tasks"]["entries"] == 1

    planted_guidance = _rows(gm._ctx, SLUG)
    assert {row["custom_key"] for row in planted_guidance} == {
        "wf/triage",
        "wf/tone",
    }
    planted_tasks = _rows(ts._ctx, SLUG)
    assert [row["custom_key"] for row in planted_tasks] == ["wf/morning"]

    listed = wm.get_workflow(slug=SLUG)
    assert listed["installed"] is True

    removed = wm.uninstall_workflow(slug=SLUG)

    assert "failures" not in removed
    assert removed["removed"]["guidance"]["changed"] is True
    assert removed["removed"]["tasks"]["changed"] is True

    # The workflow's rows are gone from every surface it wrote to...
    assert _rows(gm._ctx, SLUG) == []
    assert _rows(ts._ctx, SLUG) == []
    # ...the deployment's same-key row survived untouched...
    deployment_rows = _rows(gm._ctx, "deployment")
    assert [row["title"] for row in deployment_rows] == [
        "Deployment triage procedure",
    ]
    # ...and the installation record itself is gone.
    assert wm.get_workflow(slug=SLUG)["installed"] is False


@_handle_project
@pytest.mark.asyncio
async def test_uninstall_is_idempotent_and_reinstall_replants(
    live_managers,
    bundle,
):
    gm, ts, wm = live_managers
    wm.register_bundle(bundle)

    wm.install_workflow(slug=SLUG)
    wm.uninstall_workflow(slug=SLUG)

    # A second install after a full uninstall must replant from scratch.
    result = wm.install_workflow(slug=SLUG)
    assert "failures" not in result
    assert {row["custom_key"] for row in _rows(gm._ctx, SLUG)} == {
        "wf/triage",
        "wf/tone",
    }

    wm.uninstall_workflow(slug=SLUG)
    assert _rows(gm._ctx, SLUG) == []
    assert _rows(ts._ctx, SLUG) == []


@_handle_project
@pytest.mark.asyncio
async def test_install_arms_tasks_and_holds_them_while_disconnected(
    live_managers,
    bundle,
    monkeypatch,
):
    """The full requirements cycle against real managers.

    Custom-synced task definitions are born disarmed, so install must arm
    them when requirements are met — otherwise nothing a workflow sets up
    ever fires. With a requirement unmet the same install plants
    everything, reports connect_required, and leaves the definitions
    held; a repeat install after the connection lands is the
    arm-on-connect path.
    """
    from unify.workflow_manager import requirements as req_module
    from unify.workflow_manager.bundle import WorkflowRequirement

    gm, ts, wm = live_managers
    held_bundle = WorkflowBundle(
        slug=bundle.slug,
        name=bundle.name,
        version=bundle.version,
        surfaces=bundle.surfaces,
        requirements=(
            WorkflowRequirement(slug="gmail", required_secrets=("GMAIL_TOKEN",)),
        ),
    )
    wm.register_bundle(held_bundle)

    # Isolate the secret keyset as the only authority: no gallery
    # connection and no native package for this slug, so the bundle's own
    # declared secret decides.
    monkeypatch.setattr(
        req_module.RequirementResolver,
        "connected_apps",
        lambda self: frozenset(),
    )
    monkeypatch.setattr(
        req_module.RequirementResolver,
        "native_manifest",
        lambda self, slug: None,
    )

    # No token yet: plants, holds, and says so.
    monkeypatch.setattr(
        req_module.RequirementResolver,
        "keyset",
        lambda self: frozenset(),
    )
    result = wm.install_workflow(slug=SLUG)

    assert "tasks_armed" not in result
    assert len(result["tasks_held"]) == 1
    assert result["connect_required"]["requirements"][0]["slug"] == "gmail"
    (task_row,) = _rows(ts._ctx, SLUG)
    assert task_row["enabled"] is False
    assert wm.get_workflow(slug=SLUG)["status"] == "needs_connection"

    # Token lands: the repeat install is the arm-on-connect path.
    monkeypatch.setattr(
        req_module.RequirementResolver,
        "keyset",
        lambda self: frozenset({"GMAIL_TOKEN"}),
    )
    result = wm.install_workflow(slug=SLUG)

    assert "connect_required" not in result
    assert len(result["tasks_armed"]) == 1
    (task_row,) = _rows(ts._ctx, SLUG)
    assert task_row["enabled"] is True
    assert wm.get_workflow(slug=SLUG)["status"] == "active"

    wm.uninstall_workflow(slug=SLUG)
    assert _rows(ts._ctx, SLUG) == []


@_handle_project
@pytest.mark.asyncio
async def test_reconcile_applies_a_new_bundle_version_and_keeps_params(
    live_managers,
    bundle,
    tmp_path: Path,
):
    """The upkeep path: a catalogue version bump reaches installed rows
    through reconcile_installed, with the installation's recorded
    settings preserved — and the settings are readable at run time
    through get_installation_params, never from the planted rows."""
    gm, ts, wm = live_managers
    wm.register_bundle(bundle)
    wm.install_workflow(slug=SLUG, params={"mailbox": "work@unify.ai"})

    assert wm.get_installation_params(slug=SLUG) == {"mailbox": "work@unify.ai"}

    # The catalogue moves: one procedure's content changes, one is dropped.
    revised_dir = _write_jsonl(
        tmp_path / "revised_guidance",
        GUIDANCE_JSONL_FILENAME,
        [
            {
                "key": "wf/triage",
                "title": "Workflow triage procedure",
                "content": "Read the inbox newest-first.",
            },
        ],
    )
    revised = WorkflowBundle(
        slug=bundle.slug,
        name=bundle.name,
        version="2.0.0",
        surfaces={
            "guidance": collect_custom_guidance(path=revised_dir),
            "tasks": bundle.surfaces["tasks"],
        },
    )
    wm.register_bundle(revised)

    result = wm.reconcile_installed()

    assert SLUG in result["reconciled"]
    assert "orphaned" not in result
    rows = _rows(gm._ctx, SLUG)
    assert len(rows) == 1, "the dropped procedure must be pruned"
    assert rows[0]["content"] == "Read the inbox newest-first."

    record = wm.get_workflow(slug=SLUG)
    assert record["installed_version"] == "2.0.0"
    assert record["params"] == {"mailbox": "work@unify.ai"}

    wm.uninstall_workflow(slug=SLUG)


@_handle_project
@pytest.mark.asyncio
async def test_bootstrap_fills_the_shelf_and_reconciles_installed(
    live_managers,
    tmp_path: Path,
):
    """The production boot path end to end: a bundle directory on disk
    becomes an installable catalogue entry, and a later boot with a
    changed bundle carries the change into existing installations —
    the upkeep tick is the catalogue load itself."""
    from unify.workflow_manager.catalog import (
        MANIFEST_FILENAME,
        bootstrap_workflow_catalog,
    )

    gm, ts, wm = live_managers

    root = tmp_path / "workflows"
    bundle_dir = root / "shelf_demo"
    guidance_dir = bundle_dir / "guidance"
    guidance_dir.mkdir(parents=True)
    (bundle_dir / MANIFEST_FILENAME).write_text(
        "slug: shelf_demo\nname: Shelf demo\nversion: '1.0.0'\ncategory: ops\n",
    )
    _write_jsonl(
        guidance_dir,
        GUIDANCE_JSONL_FILENAME,
        [{"key": "shelf/how", "title": "How", "content": "First pass."}],
    )

    manager = bootstrap_workflow_catalog(root=root)
    assert manager is not None
    listed = manager.list_workflows()
    assert any(w["slug"] == "shelf_demo" for w in listed["workflows"])

    manager.install_workflow(slug="shelf_demo")
    (rows_before,) = _rows(gm._ctx, "shelf_demo")
    assert rows_before["content"] == "First pass."

    # The catalogue moves in git; the next boot reconciles installs to it.
    _write_jsonl(
        guidance_dir,
        GUIDANCE_JSONL_FILENAME,
        [{"key": "shelf/how", "title": "How", "content": "Second pass."}],
    )
    (bundle_dir / MANIFEST_FILENAME).write_text(
        "slug: shelf_demo\nname: Shelf demo\nversion: '1.1.0'\ncategory: ops\n",
    )

    bootstrap_workflow_catalog(root=root)

    (row,) = _rows(gm._ctx, "shelf_demo")
    assert row["content"] == "Second pass."
    assert manager.get_workflow(slug="shelf_demo")["installed_version"] == "1.1.0"

    manager.uninstall_workflow(slug="shelf_demo")
    assert _rows(gm._ctx, "shelf_demo") == []


@_handle_project
@pytest.mark.asyncio
async def test_catalog_seeds_the_builtins_shelf_and_short_circuits(
    live_managers,
    bundle,
):
    """The shelf a reading surface renders lives in the public-read
    Builtins project — platform data, one copy for everyone, exactly like
    the integrations app catalogue — while everything per-assistant stays
    in the assistant's own contexts. The listing rows carry names; the
    content rows beside them carry the artifacts' substance, so a reader
    can open a procedure or a task brief before anything is installed.
    The seed must also be cheap on every run between deploys: an
    unchanged catalogue reads one meta row and writes nothing.
    """
    import json as _json

    from unify.common.builtins import builtins_project, builtins_seed_key_override
    from unify.workflow_manager.builtins_catalog import (
        BUILTINS_WORKFLOWS_CONTENT_CONTEXT,
        BUILTINS_WORKFLOWS_CONTEXT,
        seed_builtin_workflows,
    )

    project = builtins_project()

    def shelf():
        logs = unisdk.get_logs(
            project=project,
            context=BUILTINS_WORKFLOWS_CONTEXT,
            limit=100,
        )
        return {
            str((lg.entries or {}).get("slug")): dict(lg.entries or {}) for lg in logs
        }

    def artifacts():
        logs = unisdk.get_logs(
            project=project,
            context=BUILTINS_WORKFLOWS_CONTENT_CONTEXT,
            limit=100,
        )
        return {
            str((lg.entries or {}).get("content_key")): dict(lg.entries or {})
            for lg in logs
        }

    with builtins_seed_key_override():
        assert seed_builtin_workflows(bundles=[bundle]) is True

        row = shelf()[SLUG]
        assert row["name"] == "Live demo workflow"
        assert _json.loads(row["surfaces"]) == ["guidance", "tasks"]
        sets = _json.loads(row["sets"])
        assert sets["tasks"] == [
            {"name": "Workflow morning run", "schedule": "Every day"},
        ]

        content = artifacts()
        assert set(content) == {
            f"{SLUG}/guidance/wf/triage",
            f"{SLUG}/guidance/wf/tone",
            f"{SLUG}/tasks/wf/morning",
        }
        triage = content[f"{SLUG}/guidance/wf/triage"]
        assert triage["name"] == "Workflow triage procedure"
        assert triage["body"] == "Read the inbox oldest-first."
        morning = content[f"{SLUG}/tasks/wf/morning"]
        assert morning["body"] == "The recurring job this workflow exists for."
        assert morning["schedule"] == "Every day"

        # Unchanged catalogue: the hash short-circuits, nothing is written.
        assert seed_builtin_workflows(bundles=[bundle]) is False

        # A bundle leaving the curated tree leaves the shelf whole —
        # listing and artifacts both.
        other = WorkflowBundle(slug="other_demo", name="Other demo", surfaces={})
        assert seed_builtin_workflows(bundles=[other]) is True
        assert set(shelf()) == {"other_demo"}
        assert artifacts() == {}

        # The harness re-seeds an empty catalogue for the next session.
        seed_builtin_workflows(bundles=[])
