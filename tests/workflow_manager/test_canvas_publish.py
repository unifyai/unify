"""Canvas views are published, never reconciled.

Decision 9 reversed: a bundle ships canvas **source**, and the install
hands it to CanvasManager's own authoring pipeline — lint, typecheck,
bundle, render, review — against the kit installed *now*. A pre-built
artifact in a git bundle would pin a host runtime and break at view time,
for the user, with nothing failing at plant time.

That means canvas is deliberately outside the reconcile engine, so the
properties the engine would have given for free have to be pinned here
instead: identity across reinstalls, an unchanged source costing nothing,
a failed build reported rather than swallowed, and an uninstall that goes
through the manager's own delete so the routing token is released.
"""

from typing import Any, Dict

import pytest

from unify.canvas_manager.simulated import SimulatedCanvasManager
from unify.workflow_manager.bundle import CanvasSource, WorkflowBundle
from unify.workflow_manager.workflow_manager import WorkflowManager

VALID_TSX = (
    'import { Canvas } from "@unity/canvas-kit";\n'
    "export default function View({ canvas }) {\n"
    '  return <Canvas><div className="flex flex-col gap-4" /></Canvas>;\n'
    "}\n"
)

# Colour is the one rule the canvas linter exists to enforce, because
# authored TSX never passes through console's lint or its production build.
OFF_PALETTE_TSX = (
    'import { Canvas } from "@unity/canvas-kit";\n'
    "export default function View({ canvas }) {\n"
    '  return <Canvas><div className="bg-[#ff0000]" /></Canvas>;\n'
    "}\n"
)


def _bundle(slug: str, *views: CanvasSource) -> WorkflowBundle:
    return WorkflowBundle(slug=slug, name=slug, canvas=tuple(views))


def _view(name: str = "ledger", tsx: str = VALID_TSX, title: str = "Ledger"):
    return CanvasSource(
        name=name,
        tsx=tsx,
        title=title,
        description="What is outstanding.",
        bindings=(
            {
                "alias": "rows",
                "manager": "data",
                "table": "Data/Finance/Invoices",
                "args": {"operation": "filter", "limit": 50},
            },
        ),
    )


@pytest.fixture
def manager(monkeypatch) -> tuple[WorkflowManager, SimulatedCanvasManager]:
    canvas = SimulatedCanvasManager()
    workflows = WorkflowManager.__new__(WorkflowManager)
    monkeypatch.setattr(
        WorkflowManager,
        "_get_canvas_manager",
        staticmethod(lambda: canvas),
    )
    return workflows, canvas


def _rows(canvas: SimulatedCanvasManager) -> list[Dict[str, Any]]:
    return [record.model_dump() for record in canvas.list_views(limit=50)]


def test_installing_publishes_the_view_with_its_provenance(manager):
    workflows, canvas = manager

    report, failures = workflows._publish_canvases(
        _bundle("invoice_chaser", _view()),
        destination=None,
    )

    assert not failures
    assert report["ledger"]["status"] == "published"
    (row,) = _rows(canvas)
    assert row["title"] == "Ledger"
    # Written with the row, not stamped after it: a crash between the two
    # would leave a view nobody owns and a second copy on the next install.
    assert row["managed_by"] == "invoice_chaser"
    assert row["custom_key"] == "invoice_chaser/ledger"
    assert row["custom_hash"]


def test_reinstalling_an_unchanged_view_recompiles_nothing(manager):
    workflows, canvas = manager
    bundle = _bundle("invoice_chaser", _view())

    workflows._publish_canvases(bundle, destination=None)
    (first,) = _rows(canvas)

    report, failures = workflows._publish_canvases(bundle, destination=None)

    assert not failures
    assert report["ledger"]["status"] == "unchanged"
    # One view, same token: a reinstall must not stack up a second copy,
    # and the compile is the expensive part of an otherwise cheap pass.
    (second,) = _rows(canvas)
    assert second["token"] == first["token"]
    assert second["updated_at"] == first["updated_at"]


def test_a_revised_view_is_updated_in_place(manager):
    workflows, canvas = manager

    workflows._publish_canvases(_bundle("invoice_chaser", _view()), destination=None)
    (first,) = _rows(canvas)

    revised = _view(
        tsx=VALID_TSX.replace("gap-4", "gap-6"),
        title="Overdue ledger",
    )
    report, failures = workflows._publish_canvases(
        _bundle("invoice_chaser", revised),
        destination=None,
    )

    assert not failures
    assert report["ledger"]["status"] == "updated"
    (second,) = _rows(canvas)
    # The token is the URL. Republishing under a new one would break every
    # place the canvas has been linked from.
    assert second["token"] == first["token"]
    assert second["title"] == "Overdue ledger"
    assert second["custom_hash"] != first["custom_hash"]


def test_a_view_that_does_not_compile_is_a_reported_failure(manager):
    """Not a silent skip: the author needs the linter's words, and the
    installation records `partial` so a repeat install retries exactly
    this."""
    workflows, canvas = manager

    report, failures = workflows._publish_canvases(
        _bundle("invoice_chaser", _view(tsx=OFF_PALETTE_TSX)),
        destination=None,
    )

    assert "ledger" not in report
    assert "canvas:ledger" in failures
    assert _rows(canvas) == []


def test_uninstalling_deletes_through_the_manager(manager):
    """Through `delete_view`, which releases the routing token and drops
    the actions and invocations hanging off it — none of which a prune
    adapter could do without reimplementing it."""
    workflows, canvas = manager
    workflows._publish_canvases(_bundle("invoice_chaser", _view()), destination=None)

    removed = workflows._withdraw_canvases("invoice_chaser")

    assert removed == ["invoice_chaser/ledger"]
    assert _rows(canvas) == []


def test_one_workflow_never_withdraws_another_s_views(manager):
    workflows, canvas = manager
    workflows._publish_canvases(_bundle("invoice_chaser", _view()), destination=None)
    workflows._publish_canvases(
        _bundle("contract_renewals", _view(name="renewals", title="Renewals")),
        destination=None,
    )
    assert len(_rows(canvas)) == 2

    workflows._withdraw_canvases("invoice_chaser")

    surviving = _rows(canvas)
    assert [row["custom_key"] for row in surviving] == ["contract_renewals/renewals"]


def test_a_bundle_with_no_canvas_does_nothing_at_all(manager):
    workflows, canvas = manager

    report, failures = workflows._publish_canvases(
        _bundle("daily_briefing"),
        destination=None,
    )

    assert report == {}
    assert failures == {}
    assert _rows(canvas) == []
