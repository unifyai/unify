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
    from unify.workflow_manager import workflow_manager as wm_module
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

    # No token yet: plants, holds, and says so.
    monkeypatch.setattr(wm_module, "_read_local_secret_keyset", lambda: set())
    result = wm.install_workflow(slug=SLUG)

    assert "tasks_armed" not in result
    assert len(result["tasks_held"]) == 1
    assert result["connect_required"]["requirements"][0]["slug"] == "gmail"
    (task_row,) = _rows(ts._ctx, SLUG)
    assert task_row["enabled"] is False
    assert wm.get_workflow(slug=SLUG)["status"] == "needs_connection"

    # Token lands: the repeat install is the arm-on-connect path.
    monkeypatch.setattr(
        wm_module,
        "_read_local_secret_keyset",
        lambda: {"GMAIL_TOKEN"},
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
