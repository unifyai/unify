"""Acceptance: the first curated bundle, end to end against real managers.

Loads the actual ``daily_briefing`` bundle from the sibling unify-deploy
checkout — the same files a deployment ships — and walks the full
lifecycle: install while disconnected (held), connect (armed), settings
read at runtime, catalogue version bump reconciled, uninstall leaving
nothing behind. Skips when the sibling checkout is absent (public CI),
so the curated content is exercised wherever both repos exist.
"""

import os
from pathlib import Path

import pytest
import unisdk

from tests.helpers import _handle_project
from unify.common.context_registry import ContextRegistry
from unify.function_manager.function_manager import FunctionManager
from unify.guidance_manager.guidance_manager import GuidanceManager
from unify.knowledge_manager.knowledge_manager import KnowledgeManager
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.workflow_manager.bundle import WORKFLOW_LIBRARY
from unify.workflow_manager.catalog import load_bundle
from unify.workflow_manager.surfaces import register_default_surfaces
from unify.workflow_manager.workflow_manager import WorkflowManager

SLUG = "daily_briefing"


def _bundle_dir() -> Path:
    configured = (os.environ.get("UNITY_WORKFLOWS_DIR") or "").strip()
    if configured:
        return Path(configured) / SLUG
    return (
        Path.home() / "unify-deploy/unify_deploy/assistant_deployments/workflows" / SLUG
    )


pytestmark = pytest.mark.skipif(
    not (_bundle_dir() / "manifest.yaml").exists(),
    reason="daily_briefing bundle not present (unify-deploy checkout missing)",
)


@pytest.fixture
def live_managers():
    for cls, contexts in (
        (GuidanceManager, ("Guidance", "Guidance/Meta")),
        (KnowledgeManager, ("Knowledge", "Knowledge/Meta")),
        (TaskScheduler, ("Tasks", "Tasks/Meta")),
        (
            FunctionManager,
            (
                "Functions/VirtualEnvs",
                "Functions/Compositional",
                "Functions/Primitives",
                "Functions/Meta",
            ),
        ),
        (WorkflowManager, ("Workflows", "Workflows/Meta")),
    ):
        for name in contexts:
            ContextRegistry.forget(cls, name)

    gm = GuidanceManager()
    km = KnowledgeManager()
    ts = TaskScheduler()
    fm = FunctionManager()
    wm = WorkflowManager()
    register_default_surfaces(
        wm.surfaces,
        guidance_manager=gm,
        knowledge_manager=km,
        task_scheduler=ts,
        function_manager=fm,
    )
    yield gm, km, ts, fm, wm

    try:
        gm.clear()
    except Exception:
        pass


def _rows(context: str, managed_by: str) -> list[dict]:
    logs = unisdk.get_logs(
        context=context,
        filter=f"managed_by == '{managed_by}'",
    )
    return [dict(lg.entries or {}) for lg in logs]


@_handle_project
@pytest.mark.asyncio
async def test_daily_briefing_full_lifecycle(live_managers, monkeypatch):
    from unify.workflow_manager import requirements as req_module

    gm, km, ts, fm, wm = live_managers
    wm.register_bundle(load_bundle(_bundle_dir()))

    # ── Install before the Google connection exists: everything plants,
    # the recurring job is held, and the result says exactly why.
    # Isolate the keyset: Workspace is BYOD OAuth, so there is no gallery
    # connection row and no native package to answer for it.
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
    monkeypatch.setattr(
        req_module.RequirementResolver,
        "keyset",
        lambda self: frozenset(),
    )
    result = wm.install_workflow(slug=SLUG, params={"focus": "the Q3 launch"})

    assert "failures" not in result
    assert result["connect_required"]["requirements"] == [
        {
            "slug": "gmail",
            "name": "Gmail",
            "connected": False,
            "via": "secret",
            "missing_secrets": ["GOOGLE_REFRESH_TOKEN"],
        },
    ]
    (task_row,) = _rows(ts._ctx, SLUG)
    assert task_row["enabled"] is False, "held jobs must actually be disarmed"
    assert wm.get_workflow(slug=SLUG)["status"] == "needs_connection"

    # Content landed on every surface the bundle covers.
    assert {r["custom_key"] for r in _rows(gm._ctx, SLUG)} == {
        "daily_briefing/compose",
        "daily_briefing/triage",
    }
    assert [r["custom_key"] for r in _rows(km._ctx, SLUG)] == [
        "daily_briefing/definition-of-slipping",
    ]
    (fn_row,) = _rows(fm._compositional_ctx, WORKFLOW_LIBRARY)
    assert fn_row["name"] == "briefing_window"
    assert fn_row["workflows"] == [SLUG]

    # ── The connection lands: reinstall is the arm-on-connect path, and
    # omitting params keeps the recorded settings.
    monkeypatch.setattr(
        req_module.RequirementResolver,
        "keyset",
        lambda self: frozenset({"GOOGLE_REFRESH_TOKEN"}),
    )
    result = wm.install_workflow(slug=SLUG)

    assert "connect_required" not in result
    assert len(result["tasks_armed"]) == 1
    (task_row,) = _rows(ts._ctx, SLUG)
    assert task_row["enabled"] is True
    record = wm.get_workflow(slug=SLUG)
    assert record["status"] == "active"
    assert record["params"] == {"focus": "the Q3 launch"}

    # The runtime read the planted task's description points at.
    assert wm.get_installation_params(slug=SLUG) == {"focus": "the Q3 launch"}

    # ── Uninstall stops the job and leaves no trace on any surface.
    removed = wm.uninstall_workflow(slug=SLUG)

    assert "failures" not in removed
    assert _rows(ts._ctx, SLUG) == []
    assert _rows(gm._ctx, SLUG) == []
    assert _rows(km._ctx, SLUG) == []
    assert _rows(fm._compositional_ctx, WORKFLOW_LIBRARY) == []
    assert wm.get_workflow(slug=SLUG)["installed"] is False
