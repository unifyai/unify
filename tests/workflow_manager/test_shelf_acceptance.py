"""Acceptance: every curated bundle on the shelf, end to end against real
managers.

Loads the actual bundles from the sibling unify-deploy checkout — the same
files a deployment ships — and walks the full lifecycle for each one:
install while disconnected (held), connect (armed), settings read at
runtime, uninstall leaving nothing behind. Skips when the sibling checkout
is absent (public CI), so the curated content is exercised wherever both
repos exist.

Parametrised over the shelf rather than over a named bundle, so authoring a
workflow is what proves its lifecycle: a new directory is a new case, and a
bundle that plants nothing, declares an unresolvable requirement, or loses
its content on uninstall fails here without anyone remembering to add it.
Expectations are derived from each bundle's own collected surfaces, never
hard-coded — a hard-coded key list would pass while asserting nothing about
the bundle actually on disk.
"""

import os
from pathlib import Path

import pytest
import unisdk

from tests.helpers import _handle_project
from unify.common.context_registry import ContextRegistry
from unify.data_manager.data_manager import DataManager
from unify.function_manager.function_manager import FunctionManager
from unify.guidance_manager.guidance_manager import GuidanceManager
from unify.knowledge_manager.knowledge_manager import KnowledgeManager
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.workflow_manager.bundle import WORKFLOW_LIBRARY
from unify.workflow_manager.catalog import load_bundle, load_catalog
from unify.workflow_manager.surfaces import register_default_surfaces
from unify.workflow_manager.workflow_manager import WorkflowManager


def _shelf_root() -> Path:
    configured = (os.environ.get("UNIFY_WORKFLOWS_DIR") or "").strip()
    if configured:
        return Path(configured)
    # Resolve the sibling checkout from this repo's location, never from
    # Path.home(): the test harness points HOME at a scratch dir, so a
    # home-relative probe skips this test on every machine that uses the
    # runner — silently, forever.
    siblings = Path(__file__).resolve().parents[2].parent
    return siblings / "unify-deploy/unify_deploy/assistant_deployments/workflows"


SHELF_ROOT = _shelf_root()
SHELF_SLUGS = sorted(bundle.slug for bundle in load_catalog(SHELF_ROOT))

pytestmark = pytest.mark.skipif(
    not SHELF_SLUGS,
    reason="no workflow bundles on the shelf (unify-deploy checkout missing)",
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
        (DataManager, ("Data", "Data/Meta")),
        (WorkflowManager, ("Workflows", "Workflows/Meta")),
    ):
        for name in contexts:
            ContextRegistry.forget(cls, name)

    gm = GuidanceManager()
    km = KnowledgeManager()
    ts = TaskScheduler()
    fm = FunctionManager()
    dm = DataManager()
    wm = WorkflowManager()
    register_default_surfaces(
        wm.surfaces,
        guidance_manager=gm,
        knowledge_manager=km,
        task_scheduler=ts,
        function_manager=fm,
        data_manager=dm,
    )
    yield gm, km, ts, fm, dm, wm

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


def _sample_params(bundle) -> dict:
    """A plausible value for every param the bundle declares.

    Every param is filled, not just the required ones, so the round-trip
    through ``get_installation_params`` exercises each declared type rather
    than whichever subset happened to be mandatory.
    """
    values: dict = {}
    for name, spec in (bundle.params_schema or {}).items():
        spec = spec or {}
        kind = str(spec.get("type") or "text")
        if kind == "number":
            values[name] = 3
        elif kind == "boolean":
            values[name] = True
        elif kind == "select":
            options = list(spec.get("options") or [])
            values[name] = options[0] if options else "acceptance"
        else:
            values[name] = f"acceptance value for {name}"
    return values


@_handle_project
@pytest.mark.parametrize("slug", SHELF_SLUGS)
@pytest.mark.asyncio
async def test_bundle_full_lifecycle(slug, live_managers, monkeypatch):
    from unify.workflow_manager import requirements as req_module

    gm, km, ts, fm, dm, wm = live_managers
    bundle = load_bundle(SHELF_ROOT / slug)
    wm.register_bundle(bundle)

    # ── Install before any connection exists: everything plants, the
    # recurring jobs are held, and the result says exactly why.
    # Isolate every authority so the walk does not depend on what this
    # machine happens to have connected.
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

    params = _sample_params(bundle)
    result = wm.install_workflow(slug=slug, params=params)

    # A canvas is compiled at install, against the kit the deployment has.
    # The harness redirects HOME, so the node toolchain is not findable
    # here — the same reason this file resolves the shelf from the repo
    # rather than from ``Path.home()``. Say that out loud rather than
    # letting a toolchain-less environment read as a broken bundle; the
    # compile itself is gated at curation time, in unify-deploy.
    from unify.canvas_manager.ops.build_ops import toolchain_available

    reported = dict(result.get("failures") or {})
    canvas_only = {
        name: reason for name, reason in reported.items() if name.startswith("canvas:")
    }
    if reported and not toolchain_available():
        assert set(canvas_only) == set(reported), (
            "only canvas publishing may fail without a toolchain; "
            f"got {sorted(reported)}"
        )
    else:
        assert not reported

    # Each unmet requirement reports the route that fixes it, and the route
    # follows the kind the bundle declared: a Workspace is connected in the
    # profile's own manager and names the secret those flows store, while an
    # app is connected in the gallery and names nothing. A bundle that got
    # this wrong would land here as a different `via` — which is exactly the
    # authoring mistake worth catching, since the UI offers the wrong fix.
    def _expected(requirement) -> dict:
        entry = {
            "slug": requirement.slug,
            "name": requirement.name or requirement.slug,
            "connected": False,
            "via": "workspace" if requirement.kind == "workspace" else "connection",
        }
        if requirement.kind == "workspace":
            entry["missing_secrets"] = list(requirement.required_secrets)
        # A requirement the bundle lets the user satisfy more than one way
        # reports every option with its own state, because the reader offers
        # all of them: naming only the first would send someone who uses
        # Discord to connect Slack.
        if len(requirement.options) > 1:
            entry["options"] = [
                {
                    "slug": option.slug,
                    "name": option.display_name,
                    "connected": False,
                    "via": "connection",
                }
                for option in requirement.options
            ]
        return entry

    assert result["connect_required"]["requirements"] == [
        _expected(requirement) for requirement in bundle.requirements
    ]
    # `partial` outranks `needs_connection` — a surface that failed is the
    # more urgent fact — so a bundle whose canvas could not compile here
    # reports that instead. Both are correct; which one depends on whether
    # anything actually failed.
    expected_status = "partial" if reported else "needs_connection"
    assert wm.get_workflow(slug=slug)["status"] == expected_status

    task_rows = _rows(ts._ctx, slug)
    assert len(task_rows) == len(bundle.surfaces.get("tasks") or {})
    assert task_rows, "a workflow with no job sets nothing up"
    assert all(
        row["enabled"] is False for row in task_rows
    ), "held jobs must actually be disarmed"

    # Content landed on every surface the bundle covers, keyed exactly as
    # the bundle declares it.
    assert {row["custom_key"] for row in _rows(gm._ctx, slug)} == set(
        bundle.surfaces.get("guidance") or {},
    )
    assert {row["custom_key"] for row in _rows(km._ctx, slug)} == set(
        bundle.surfaces.get("knowledge") or {},
    )
    # Functions share one identity space across bundles, so they reconcile
    # under the library source and carry membership instead.
    function_rows = _rows(fm._compositional_ctx, WORKFLOW_LIBRARY)
    assert {row["name"] for row in function_rows} == set(
        bundle.surfaces.get("functions") or {},
    )
    assert all(row["workflows"] == [slug] for row in function_rows)

    # Tables the bundle declares exist, and hold nothing: a bundle ships the
    # shape its own job fills, never the contents.
    for context in bundle.surfaces.get("data") or {}:
        assert dm._table_exists(context, None), f"{context} was not created"
        assert dm.filter(context, filter="custom_hash != None") == []

    # ── The connections land: reinstall is the arm-on-connect path, and
    # omitting params keeps the recorded settings.
    connected = frozenset(
        str(requirement.slug).strip().lower().replace("-", "_")
        for requirement in bundle.requirements
        if requirement.kind != "workspace"
    )
    monkeypatch.setattr(
        req_module.RequirementResolver,
        "connected_apps",
        lambda self: connected,
    )
    # A Workspace is not connected by a gallery row: its signal is the
    # refresh-token secret the profile and onboarding flows store.
    workspace_secrets = frozenset(
        secret
        for requirement in bundle.requirements
        if requirement.kind == "workspace"
        for secret in requirement.required_secrets
    )
    monkeypatch.setattr(
        req_module.RequirementResolver,
        "keyset",
        lambda self: workspace_secrets,
    )
    result = wm.install_workflow(slug=slug)

    assert "connect_required" not in result
    assert len(result["tasks_armed"]) == len(task_rows)
    assert all(row["enabled"] is True for row in _rows(ts._ctx, slug))
    record = wm.get_workflow(slug=slug)
    assert record["status"] == ("partial" if reported else "active")
    assert record["params"] == params

    # The runtime read every planted task's description points at.
    assert wm.get_installation_params(slug=slug) == params

    # ── Uninstall stops the jobs and leaves no trace on any surface.
    removed = wm.uninstall_workflow(slug=slug)

    assert "failures" not in removed
    assert _rows(ts._ctx, slug) == []
    assert _rows(gm._ctx, slug) == []
    assert _rows(km._ctx, slug) == []
    assert _rows(fm._compositional_ctx, WORKFLOW_LIBRARY) == []
    assert wm.get_workflow(slug=slug)["installed"] is False
