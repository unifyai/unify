"""The mutation contract, against real Orchestra.

Console cannot reconcile — planting needs the custom-sync engine, which is
the assistant's — and a hosted assistant is usually asleep when someone
clicks Install. So a click records a durable request row and the assistant
carries it out on its next wake. What has to hold: exactly one executor
runs a request however many times delivery fires, a dead executor's claim
is recoverable, and every outcome is legible to the surface that asked.

The claim is a server-side compare-and-set, so a recording double could
not observe a lost race at all. These drive the real thing.
"""

import json
from pathlib import Path
from typing import Any, Dict, List

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

SLUG = "wf_request_demo"

_GUIDANCE = [
    {
        "key": "wf/how",
        "title": "How the demo works",
        "content": "Read the inbox oldest-first.",
    },
]
_TASKS = [
    {
        "key": "wf/morning",
        "name": "Demo morning run",
        "description": "The recurring job this workflow exists for.",
        "repeat": [{"frequency": "daily"}],
    },
]


def _write_jsonl(directory: Path, filename: str, rows: list) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / filename).write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    return directory


@pytest.fixture
def live_managers():
    for cls, contexts in (
        (GuidanceManager, ("Guidance", "Guidance/Meta")),
        (TaskScheduler, ("Tasks", "Tasks/Meta")),
        (WorkflowManager, ("Workflows", "Workflows/Meta", "Workflows/Requests")),
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
    bundle_root = Path(__file__).parent
    yield gm, ts, wm, bundle_root

    try:
        gm.clear()
    except Exception:
        pass


@pytest.fixture
def bundle(tmp_path: Path) -> WorkflowBundle:
    return WorkflowBundle(
        slug=SLUG,
        name="Request demo workflow",
        version="1.0.0",
        params_schema={"focus": {"label": "Focus", "required": False}},
        surfaces={
            "guidance": collect_custom_guidance(
                path=_write_jsonl(
                    tmp_path / "guidance",
                    GUIDANCE_JSONL_FILENAME,
                    _GUIDANCE,
                ),
            ),
            "tasks": collect_custom_tasks(
                path=_write_jsonl(tmp_path / "tasks", TASKS_JSONL_FILENAME, _TASKS),
            ),
        },
    )


def _requests_ctx(wm: WorkflowManager) -> str:
    return wm._requests_context_for_destination(None)


def _record_request(
    wm: WorkflowManager,
    *,
    request_id: str,
    action: str,
    params: Dict[str, Any] | None = None,
    status: str = "pending",
    claim_key: str = "",
    claimed_at: str = "",
) -> None:
    """Write a request exactly as a reading surface would."""
    unisdk.create_logs(
        context=_requests_ctx(wm),
        entries=[
            {
                "request_id": request_id,
                "slug": SLUG,
                "action": action,
                "params": json.dumps(params or {}),
                "destination": "personal",
                "status": status,
                "claim_key": claim_key,
                "claimed_at": claimed_at,
            },
        ],
    )


def _request(wm: WorkflowManager, request_id: str) -> Dict[str, Any]:
    logs = unisdk.get_logs(
        context=_requests_ctx(wm),
        filter=f"request_id == '{request_id}'",
        limit=1,
    )
    assert logs, f"request {request_id} vanished"
    return dict(logs[0].entries or {})


def _rows(context: str, managed_by: str) -> List[Dict[str, Any]]:
    logs = unisdk.get_logs(context=context, filter=f"managed_by == '{managed_by}'")
    return [dict(lg.entries or {}) for lg in logs]


@_handle_project
@pytest.mark.asyncio
async def test_recorded_install_plants_and_reports_what_it_did(live_managers, bundle):
    """The whole point: a row written by a surface that cannot reconcile
    becomes a real install, and the outcome is legible without a second
    call."""
    gm, ts, wm, _ = live_managers
    wm.register_bundle(bundle)

    _record_request(
        wm,
        request_id="r-install",
        action="install",
        params={"focus": "Q3"},
    )
    report = wm.execute_requests()

    assert report["settled"] == {"r-install": "succeeded"}

    # Content actually landed on the real surfaces.
    assert [r["custom_key"] for r in _rows(gm._ctx, SLUG)] == ["wf/how"]
    assert len(_rows(ts._ctx, SLUG)) == 1

    row = _request(wm, "r-install")
    assert row["status"] == "succeeded"
    assert row["settled_at"]
    assert row["claim_key"], "a settled request keeps the winner's claim"
    outcome = json.loads(row["outcome"])
    assert outcome["installed"]["slug"] == SLUG
    assert "guidance" in outcome["planted"]
    # Settings recorded by the request are readable at run time.
    assert wm.get_installation_params(slug=SLUG) == {"focus": "Q3"}


@_handle_project
@pytest.mark.asyncio
async def test_a_claimed_request_is_never_run_twice(live_managers, bundle):
    """At-least-once delivery must not plant twice. The claim is a
    compare-and-set, so the second pass over the same row finds nothing to
    take — this is the invariant the whole contract rests on."""
    gm, ts, wm, _ = live_managers
    wm.register_bundle(bundle)

    _record_request(wm, request_id="r-once", action="install")
    first = wm.execute_requests()
    assert first["settled"] == {"r-once": "succeeded"}

    # A second delivery (or a boot sweep racing the dispatch) sees a
    # terminal row and does nothing at all.
    assert wm.execute_requests()["settled"] == {}
    assert _request(wm, "r-once")["status"] == "succeeded"

    # And a live claim someone else holds is left alone, not stolen.
    _record_request(
        wm,
        request_id="r-held",
        action="install",
        status="running",
        claim_key="someone-else",
        claimed_at="2999-01-01T00:00:00Z",
    )
    assert wm.execute_requests()["settled"] == {}
    assert _request(wm, "r-held")["claim_key"] == "someone-else"


@_handle_project
@pytest.mark.asyncio
async def test_an_abandoned_claim_is_taken_over(live_managers, bundle):
    """An executor killed between claim and settle must not strand the
    user's install forever, so a claim past the window is recoverable."""
    _gm, _ts, wm, _ = live_managers
    wm.register_bundle(bundle)

    _record_request(
        wm,
        request_id="r-stale",
        action="install",
        status="running",
        claim_key="dead-executor",
        claimed_at="2020-01-01T00:00:00Z",
    )

    assert wm.execute_requests()["settled"] == {"r-stale": "succeeded"}
    row = _request(wm, "r-stale")
    assert row["status"] == "succeeded"
    assert row["claim_key"] != "dead-executor", "the takeover must fence out the holder"


@_handle_project
@pytest.mark.asyncio
async def test_uninstall_and_save_params_ride_the_same_contract(live_managers, bundle):
    """Every action a reading surface can ask for, end to end."""
    gm, ts, wm, _ = live_managers
    wm.register_bundle(bundle)

    _record_request(wm, request_id="r-1", action="install", params={"focus": "first"})
    wm.execute_requests()

    # Settings change without replanting: content stays, params move.
    _record_request(
        wm,
        request_id="r-2",
        action="save_params",
        params={"focus": "second"},
    )
    assert wm.execute_requests()["settled"] == {"r-2": "succeeded"}, _request(
        wm,
        "r-2",
    )["error"]
    assert wm.get_installation_params(slug=SLUG) == {"focus": "second"}
    assert [r["custom_key"] for r in _rows(gm._ctx, SLUG)] == ["wf/how"]

    # Uninstall prunes every surface and the installation with it.
    _record_request(wm, request_id="r-3", action="uninstall")
    assert wm.execute_requests()["settled"] == {"r-3": "succeeded"}, _request(
        wm,
        "r-3",
    )["error"]
    assert _rows(gm._ctx, SLUG) == []
    assert _rows(ts._ctx, SLUG) == []
    assert wm.get_workflow(slug=SLUG)["installed"] is False


@_handle_project
@pytest.mark.asyncio
async def test_a_failed_request_says_why_in_the_shape_the_caller_reads(
    live_managers,
    bundle,
):
    """A failure has to reach the surface that asked, as data. Before this
    contract existed these payloads were destroyed by a KeyError on the
    way out, so the assertion is on the reason, not just the status."""
    _gm, _ts, wm, _ = live_managers
    wm.register_bundle(bundle)

    # Unknown slug: the bundle is not on this deployment's shelf.
    unisdk.create_logs(
        context=_requests_ctx(wm),
        entries=[
            {
                "request_id": "r-unknown-slug",
                "slug": "not_a_workflow",
                "action": "install",
                "params": "{}",
                "destination": "personal",
                "status": "pending",
            },
        ],
    )
    # Unknown action: a typo must fail one row loudly, never approximate.
    _record_request(wm, request_id="r-bad-action", action="frobnicate")

    settled = wm.execute_requests()["settled"]
    assert settled == {"r-unknown-slug": "failed", "r-bad-action": "failed"}

    slug_error = json.loads(_request(wm, "r-unknown-slug")["error"])
    assert slug_error["error"] == "unknown_workflow"
    assert "not_a_workflow" in slug_error["message"]

    action_error = json.loads(_request(wm, "r-bad-action")["error"])
    assert action_error["error"] == "unknown_action"
    assert "save_params" in action_error["supported"]
