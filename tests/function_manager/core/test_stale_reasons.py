from __future__ import annotations

from types import SimpleNamespace

import unisdk

import unify.function_manager.function_manager as function_manager_module
from tests.helpers import _handle_project
from unify.function_manager.function_manager import FunctionManager
from unify.guidance_manager.guidance_manager import GuidanceManager


def _manager(**kwargs) -> FunctionManager:
    kwargs.setdefault("include_primitives", False)
    return FunctionManager(**kwargs)


@_handle_project
def test_delete_function_marks_guidance_before_fk_cascade():
    fm = _manager()
    gm = GuidanceManager()
    fm.add_functions(implementations="def helper():\n    return 1\n")
    function_id = fm.list_functions()["helper"]["function_id"]
    outcome = gm.add_guidance(
        title="Helper workflow",
        content="Call helper.",
        function_ids=[function_id],
    )

    fm.delete_function(function_id=function_id)

    guidance = gm.get_guidance(guidance_id=outcome["details"]["guidance_id"])
    assert guidance.function_ids == []
    assert [
        (reason.dep_kind, reason.id, reason.name) for reason in guidance.stale_reasons
    ] == [("function", function_id, "helper")]


@_handle_project
def test_delete_without_dependents_keeps_and_marks_dependant():
    fm = _manager()
    fm.add_functions(
        implementations=[
            "def helper():\n    return 1\n",
            "def workflow():\n    return helper()\n",
        ],
    )
    helper_id = fm.list_functions()["helper"]["function_id"]

    fm.delete_function(function_id=helper_id, delete_dependents=False)

    workflow = fm._get_function_data_by_name(name="workflow")
    assert workflow is not None
    assert workflow["depends_on"] == ["helper"]
    assert workflow["stale_reasons"][0]["dep_kind"] == "depends_on"
    assert workflow["stale_reasons"][0]["name"] == "helper"

    fm.add_functions(implementations="def helper():\n    return 2\n")
    result = fm.reconcile_dependencies(
        function_ids=[workflow["function_id"]],
    )
    refreshed = fm._get_function_data_by_name(name="workflow")
    assert result["details"]["stale_count"] == 0
    assert refreshed is not None and refreshed["stale_reasons"] == []


def test_provider_cleanup_marks_compositional_dependencies(monkeypatch):
    primitive = SimpleNamespace(
        id=11,
        entries={
            "name": "primitives.integrations.hubspot.search_contacts",
            "metadata": {
                "source": "provider_backed",
                "integration": {
                    "backend_id": "composio",
                    "app_slug": "hubspot",
                },
            },
        },
    )
    dependant = SimpleNamespace(
        id=22,
        entries={
            "function_id": 7,
            "name": "search_customer",
            "depends_on": [primitive.entries["name"]],
            "stale_reasons": [],
        },
    )
    updates: list[dict] = []

    def get_logs(*, context, **kwargs):
        return [primitive] if context == "primitives" else [dependant]

    monkeypatch.setattr(function_manager_module.unisdk, "get_logs", get_logs)
    monkeypatch.setattr(
        function_manager_module.unisdk,
        "delete_logs",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        function_manager_module.unisdk,
        "update_logs",
        lambda **kwargs: updates.append(kwargs),
    )
    monkeypatch.setattr(
        function_manager_module,
        "list_private_fields",
        lambda _context: [],
    )
    fm = FunctionManager.__new__(FunctionManager)
    fm._primitives_ctx = "primitives"
    fm._compositional_ctx = "compositional"

    deleted = fm._delete_provider_integration_rows_for_apps(
        [("composio", "hubspot")],
    )

    assert deleted == 1
    reason = updates[0]["entries"]["stale_reasons"][0]
    assert reason["dep_kind"] == "depends_on"
    assert reason["name"] == primitive.entries["name"]


@_handle_project
def test_guidance_reconcile_clears_resolved_function_reason():
    fm = _manager()
    gm = GuidanceManager()
    fm.add_functions(implementations="def helper():\n    return 1\n")
    function_id = fm.list_functions()["helper"]["function_id"]
    outcome = gm.add_guidance(
        title="Helper workflow",
        content="Call helper.",
        function_ids=[function_id],
    )
    guidance_id = outcome["details"]["guidance_id"]
    log = unisdk.get_logs(
        context=gm._ctx,
        filter=f"guidance_id == {guidance_id}",
        limit=1,
    )[0]
    unisdk.update_logs(
        context=gm._ctx,
        logs=[log.id],
        entries={
            "stale_reasons": [
                {
                    "dep_kind": "function",
                    "id": function_id,
                    "name": "helper",
                    "message": "missing helper",
                },
            ],
        },
        overwrite=True,
    )

    result = gm.reconcile_dependencies(guidance_ids=[guidance_id])

    assert result["details"]["stale_count"] == 0
    assert gm.get_guidance(guidance_id=guidance_id).stale_reasons == []
