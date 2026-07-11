"""Regression coverage for CodeAct-owned durable task execution."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tests.helpers import _handle_project
from tests.destination_routing_helpers import (
    manager_routing_context as manager_routing_context,  # noqa: F401
)
from unify.actor.code_act_actor import (
    CodeActActor,
    _CodeActTaskExecutionDelegate,
    _build_storage_tools,
)
from unify.actor.simulated import SimulatedActor
from unify.common.task_execution_context import current_task_execution_delegate
from unify.conversation_manager.domains.task_activation import (
    _ConversationTaskExecutionDelegate,
)
from unify.function_manager.function_manager import FunctionManager
from unify.function_manager.primitives import PrimitiveScope, Primitives
from unify.manager_registry import ManagerRegistry
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.task_scheduler.types.status import Status


def _certification_evidence():
    return {
        "risk_classification": "read_only",
        "input_contract": {"required_inputs": ["task_id"]},
        "equivalence_contract": {
            "result_shape": "summary",
            "live_step_mapping": ["primitives.web.ask -> primitives.web.ask"],
        },
        "managed_primitive_contract": {
            "preserved": True,
            "managed_surfaces": ["primitives.web.ask"],
            "ad_hoc_replacements": [],
        },
        "side_effect_contract": {
            "side_effects": [],
            "ordering": "read before summarize",
        },
        "idempotency_contract": {
            "classification": "read_only",
            "duplicate_run_behavior": "safe",
        },
        "cost_contract": {
            "bounded": True,
            "cost_model": "one managed primitive call",
        },
        "failure_contract": {
            "failure_semantics": "return blocker summary",
        },
        "observability_contract": {
            "result_summary": "summary or blocker",
        },
        "attestations": {
            "no_hardcoded_live_observations": True,
            "no_removed_validation_gates": True,
            "no_reordered_side_effects": True,
            "no_discarded_recovery_branches": True,
            "no_static_runtime_assumptions": True,
            "no_ad_hoc_logic_replaced_managed_primitives": True,
        },
    }


def _storage_actor_stub():
    def _tool_stub(*args, **kwargs):
        return None

    function_manager = SimpleNamespace(
        search_functions=_tool_stub,
        filter_functions=_tool_stub,
        list_functions=_tool_stub,
        add_functions=_tool_stub,
        delete_function=_tool_stub,
        add_venv=_tool_stub,
        list_venvs=_tool_stub,
        get_venv=_tool_stub,
        update_venv=_tool_stub,
        delete_venv=_tool_stub,
        set_function_venv=_tool_stub,
        get_function_venv=_tool_stub,
    )
    guidance_manager = SimpleNamespace(
        search=_tool_stub,
        filter=_tool_stub,
        get_guidance=_tool_stub,
        add_guidance=_tool_stub,
        update_guidance=_tool_stub,
        delete_guidance=_tool_stub,
    )

    class _Actor:
        def __init__(self):
            self.function_manager = function_manager
            self.guidance_manager = guidance_manager

        async def act(self, *args, **kwargs):
            raise AssertionError("certification evidence submission must not execute")

    return _Actor()


@pytest.mark.asyncio
async def test_codeact_task_delegate_runs_description_tasks_in_child_actor_slot():
    calls = []
    actor = SimulatedActor(steps=0)

    original_act = actor.act

    async def _spy_act(*args, **kwargs):
        calls.append(kwargs)
        return await original_act(*args, **kwargs)

    actor.act = _spy_act  # type: ignore[method-assign]
    delegate = _CodeActTaskExecutionDelegate(actor)  # type: ignore[arg-type]

    handle = await delegate.start_task_run(
        task_description="Run the description-driven task.",
        entrypoint=None,
        parent_chat_context=None,
        clarification_up_q=None,
        clarification_down_q=None,
    )
    await handle.result()

    assert calls[0]["_reuse_actor_slot"] is False
    assert calls[0]["persist"] is False


@pytest.mark.asyncio
async def test_codeact_task_delegate_reuses_actor_slot_for_entrypoint_tasks():
    calls = []
    actor = SimulatedActor(steps=0)

    original_act = actor.act

    async def _spy_act(*args, **kwargs):
        calls.append(kwargs)
        return await original_act(*args, **kwargs)

    actor.act = _spy_act  # type: ignore[method-assign]
    delegate = _CodeActTaskExecutionDelegate(actor)  # type: ignore[arg-type]

    handle = await delegate.start_task_run(
        task_description="Run the function-backed task.",
        entrypoint=123,
        parent_chat_context=None,
        clarification_up_q=None,
        clarification_down_q=None,
        destination="team:41001",
    )
    await handle.result()

    assert calls[0]["_reuse_actor_slot"] is True
    assert calls[0]["entrypoint"] == 123
    assert calls[0]["destination"] == "team:41001"


@pytest.mark.asyncio
async def test_task_execution_delegates_forward_named_protocol_kwargs():
    class _FakeHandle:
        async def result(self):
            return "ok"

    class _FakeActor:
        def __init__(self):
            self.calls = []

        async def act(self, *args, **kwargs):
            self.calls.append({"args": args, "kwargs": kwargs})
            return _FakeHandle()

    for delegate_cls in (
        _CodeActTaskExecutionDelegate,
        _ConversationTaskExecutionDelegate,
    ):
        actor = _FakeActor()
        delegate = delegate_cls(actor)  # type: ignore[arg-type]

        handle = await delegate.start_task_run(
            task_description="Run with the shared task delegate protocol.",
            entrypoint=None,
            parent_chat_context=None,
            clarification_up_q=None,
            clarification_down_q=None,
            images=[],
            guidelines="Follow task-specific execution guidelines.",
            entrypoint_kwargs={"scheduled_run_timestamp": "2026-04-10T09:00:00Z"},
            entrypoint_repair_attempts=1,
            entrypoint_repair_context={"reason": "certification"},
        )

        assert await handle.result() == "ok"
        call = actor.calls[0]
        assert call["args"][0] == "Run with the shared task delegate protocol."
        assert (
            call["kwargs"]["guidelines"] == "Follow task-specific execution guidelines."
        )
        assert call["kwargs"]["entrypoint_kwargs"] == {
            "scheduled_run_timestamp": "2026-04-10T09:00:00Z",
        }
        assert call["kwargs"]["entrypoint_repair_attempts"] == 1
        assert call["kwargs"]["entrypoint_repair_context"] == {
            "reason": "certification",
        }

        with pytest.raises(TypeError, match="unexpected keyword arguments"):
            await delegate.start_task_run(
                task_description="Reject undeclared protocol kwargs.",
                entrypoint=None,
                parent_chat_context=None,
                clarification_up_q=None,
                clarification_down_q=None,
                future_option=True,
            )


@pytest.mark.asyncio
@_handle_project
async def test_codeact_task_primitive_delegates_execution_without_fallback_actor():
    """CodeAct should execute a scheduled task through the real task primitive."""

    ManagerRegistry.clear()
    scheduler = TaskScheduler()
    primitives = Primitives(
        primitive_scope=PrimitiveScope(scoped_managers=frozenset({"tasks"})),
    )
    calls = []

    class _Handle:
        async def result(self):
            return "delegated task completed"

    class _Actor:
        async def act(self, *args, **kwargs):
            calls.append({"args": args, "kwargs": kwargs})
            return _Handle()

    actor = _Actor()

    try:
        task_id = scheduler._create_task(
            name="delegated task",
            description="Run through the current CodeAct actor.",
            entrypoint=123,
        )["details"]["task_id"]

        delegate = _CodeActTaskExecutionDelegate(actor)  # type: ignore[arg-type]
        token = current_task_execution_delegate.set(delegate)
        try:
            handle = await primitives.tasks.execute(task_id=task_id)
            assert current_task_execution_delegate.get() is delegate
            result = await handle.result()
        finally:
            current_task_execution_delegate.reset(token)

        assert result == "delegated task completed"
        assert calls[0]["kwargs"]["entrypoint"] == 123
        assert calls[0]["kwargs"]["entrypoint_kwargs"]["task_id"] == task_id
        assert current_task_execution_delegate.get() is None
        assert scheduler.__dict__.get("_TaskScheduler__actor") is None
        assert "_actor" not in scheduler.__dict__
        task = scheduler._get_task_or_raise(task_id)
        assert task.status == Status.completed
    finally:
        ManagerRegistry.clear()


@pytest.mark.asyncio
@_handle_project
async def test_symbolic_entrypoint_failure_can_repair_and_retry(monkeypatch):
    ManagerRegistry.clear()
    function_manager = FunctionManager()
    actor = CodeActActor(function_manager=function_manager)
    repair_calls = []

    try:
        function_manager.add_functions(
            implementations=[
                """
def repairable_task():
    raise RuntimeError("broken implementation")
""".strip(),
            ],
        )
        function_id = function_manager.list_functions()["repairable_task"][
            "function_id"
        ]

        async def _repair(**kwargs):
            repair_calls.append(kwargs)
            function_manager.add_functions(
                implementations=[
                    """
def repairable_task():
    return "repaired task completed"
""".strip(),
                ],
                overwrite=True,
            )
            return "updated existing function"

        monkeypatch.setattr(actor, "_repair_symbolic_entrypoint", _repair)

        handle = await actor.act(
            "Run the repairable symbolic task.",
            entrypoint=function_id,
            entrypoint_kwargs={"scheduled_run_timestamp": "2026-04-10T09:00:00Z"},
            entrypoint_repair_attempts=1,
            clarification_enabled=False,
            persist=False,
        )

        assert await handle.result() == "repaired task completed"
        assert len(repair_calls) == 1
        assert repair_calls[0]["entrypoint_id"] == function_id
    finally:
        await actor.close()
        ManagerRegistry.clear()


@pytest.mark.asyncio
@_handle_project
async def test_symbolic_entrypoint_resolves_by_task_destination(
    manager_routing_context,
):
    """Entrypoint function_id lookup must not prefer a colliding personal catalog row."""

    _, team_id = manager_routing_context
    team_destination = f"team:{team_id}"
    ManagerRegistry.clear()
    function_manager = FunctionManager(include_primitives=False)
    actor = CodeActActor(function_manager=function_manager)

    try:
        function_manager.add_functions(
            implementations="def collision_entrypoint():\n    return 'personal'",
        )
        personal_id = function_manager.list_functions()["collision_entrypoint"][
            "function_id"
        ]
        function_manager.add_functions(
            implementations="def collision_entrypoint():\n    return 'shared'",
            destination=team_destination,
        )
        team_rows = function_manager.filter_functions(
            filter="name == 'collision_entrypoint'",
            destination=team_destination,
        )
        assert team_rows[0]["function_id"] == personal_id

        federated_handle = await actor.act(
            "Run colliding entrypoint without destination.",
            entrypoint=personal_id,
            clarification_enabled=False,
            persist=False,
        )
        assert await federated_handle.result() == "personal"

        team_handle = await actor.act(
            "Run colliding entrypoint scoped to team.",
            entrypoint=personal_id,
            destination=team_destination,
            clarification_enabled=False,
            persist=False,
        )
        assert await team_handle.result() == "shared"
    finally:
        await actor.close()
        ManagerRegistry.clear()


@pytest.mark.asyncio
@_handle_project
async def test_repair_symbolic_entrypoint_snapshot_uses_callable_contract(monkeypatch):
    """Repair must request metadata with _return_callable=True (FM contract)."""

    ManagerRegistry.clear()
    function_manager = FunctionManager()
    actor = CodeActActor(function_manager=function_manager)

    try:
        function_manager.add_functions(
            implementations=[
                """
def snapshot_task():
    return "ok"
""".strip(),
            ],
        )
        function_id = function_manager.list_functions()["snapshot_task"]["function_id"]

        class _FakeHandle:
            async def result(self):
                return "repaired"

        monkeypatch.setattr(
            "unify.actor.code_act_actor.start_async_tool_loop",
            lambda **kwargs: _FakeHandle(),
        )
        monkeypatch.setattr(
            "unify.actor.code_act_actor.new_llm_client",
            lambda model: MagicMock(
                set_system_message=MagicMock(),
            ),
        )

        summary = await actor._repair_symbolic_entrypoint(
            entrypoint_id=function_id,
            request="Run snapshot task.",
            entrypoint_kwargs={},
            failure=RuntimeError("boom"),
            repair_context=None,
        )
        assert summary == "repaired"
    finally:
        await actor.close()
        ManagerRegistry.clear()


@pytest.mark.asyncio
async def test_offline_certification_evidence_tool_does_not_execute_entrypoint():
    promotions = []

    def _promote_entrypoint_offline(**kwargs):
        promotions.append(kwargs)
        return {
            "outcome": "offline_promoted",
            "patched_instance_ids": [1],
            "function_id": kwargs["function_id"],
        }

    tools, _ = _build_storage_tools(
        actor=_storage_actor_stub(),
        ask_tools={},
        task_entrypoint_review={
            "metadata": {
                "task_id": 12,
                "instance_id": 0,
                "task_name": "Daily summary",
            },
            "attach_entrypoint": lambda **kwargs: kwargs,
            "promote_entrypoint_offline": _promote_entrypoint_offline,
        },
    )

    tool = tools["submit_offline_certification_evidence"]
    docstring = tool.__doc__ or ""
    assert "does not execute the entrypoint" in docstring
    assert "Replacing live primitives with ad hoc logic is not equivalent" in docstring
    assert "primitives.web.ask" in docstring
    assert "managed_primitive_contract" in docstring

    result = await tool(
        function_id=321,
        certification_evidence=_certification_evidence(),
        promotion_rationale="The stored function preserves the live workflow.",
    )

    assert "offline_promoted" in result
    assert len(promotions) == 1
    assert promotions[0]["function_id"] == 321
    assert (
        promotions[0]["certification_metadata"]["certification_evidence"][
            "managed_primitive_contract"
        ]["preserved"]
        is True
    )
    assert promotions[0]["certification_result"] == {
        "evidence_based": True,
        "executed_entrypoint": False,
        "attempt": 1,
        "max_revision_attempts": 2,
    }


@pytest.mark.asyncio
async def test_offline_certification_rejection_feedback_is_bounded():
    promotions = []

    def _promote_entrypoint_offline(**kwargs):
        promotions.append(kwargs)
        return {
            "outcome": "certification_rejected",
            "function_id": kwargs["function_id"],
            "rejection_reasons": [
                "ad_hoc_logic_replaced_managed_primitive",
            ],
        }

    tools, _ = _build_storage_tools(
        actor=_storage_actor_stub(),
        ask_tools={},
        task_entrypoint_review={
            "metadata": {
                "task_id": 12,
                "instance_id": 0,
                "task_name": "Daily summary",
            },
            "attach_entrypoint": lambda **kwargs: kwargs,
            "promote_entrypoint_offline": _promote_entrypoint_offline,
        },
    )
    tool = tools["submit_offline_certification_evidence"]

    first = await tool(
        function_id=321,
        certification_evidence=_certification_evidence(),
    )
    second = await tool(
        function_id=322,
        certification_evidence=_certification_evidence(),
    )
    exhausted = await tool(
        function_id=323,
        certification_evidence=_certification_evidence(),
    )

    assert "remaining_revision_attempts': 1" in first
    assert "revision_attempts_exhausted" in second
    assert "certification_revision_attempts_exhausted" in exhausted
    assert len(promotions) == 2
