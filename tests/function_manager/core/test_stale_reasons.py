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


def test_dependency_stale_reasons_never_flags_primitive_names():
    """``_dependency_stale_reasons`` must never report a ``primitives.*``
    dependency as missing, since a FunctionManager instance's own primitive
    catalog reflects its ``include_primitives``/``primitive_scope`` and sync
    state, not whether the primitive actually resolves at runtime through
    the actor's injected environments -- it is never authoritative for that.

    Pure unit test on the ``@staticmethod`` itself: no Orchestra/network
    required, unlike the integration test below which needs a live backend
    to exercise the full ``add_functions``/``reconcile_dependencies`` path.
    A genuinely-missing *compositional* name (something this FunctionManager
    does own outright) must still be flagged.
    """
    depends_on = [
        "primitives.comms.send_unify_message",
        "primitives.computer.user_desktop.files.pull",
        "primitives.computer.user_desktop.list_linked",
        "deleted_helper",
    ]
    available_names = {"some_other_stored_function"}

    reasons = FunctionManager._dependency_stale_reasons(
        depends_on,
        available_names=available_names,
    )

    assert [reason.name for reason in reasons] == ["deleted_helper"]


@_handle_project
def test_reconcile_does_not_flag_real_primitive_dependency_as_stale():
    """A function depending on a real primitive must not be flagged stale
    just because the reconciling FunctionManager instance has no primitives
    in its own scope (``include_primitives=False`` here). The dependency
    resolves at runtime through the actor's injected environments, not
    through this instance's primitive catalog -- an instance that cannot see
    a primitive is not authorized to call it missing.
    """
    fm = _manager()  # include_primitives=False: no primitives in scope.
    source = (
        "async def delegate_task(request: str):\n"
        '    """Delegate a task to a sub-agent."""\n'
        "    handle = await primitives.actor.act(request=request)\n"
        "    return await handle.result()\n"
    )
    fm.add_functions(implementations=source)
    function_id = fm.list_functions()["delegate_task"]["function_id"]

    result = fm.reconcile_dependencies(function_ids=[function_id])

    assert function_id not in result["details"]["stale_function_ids"]
    assert result["details"]["stale_count"] == 0
    refreshed = fm._get_function_data_by_name(name="delegate_task")
    assert refreshed is not None
    assert refreshed["stale_reasons"] == []


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
