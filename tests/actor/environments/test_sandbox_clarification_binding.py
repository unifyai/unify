"""Deterministic contracts for sandbox clarification queue rebinding.

Locks the execute_code / execute_function bridge that points nested manager
clarifications at the outer tool call's per-call channel (mailbox A).
"""

from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace

import pytest

from unify.actor.code_act_actor import CodeActActor
from unify.actor.environments.base import (
    _ClarificationQueueInjector,
    bind_sandbox_clarification_queues,
    restore_sandbox_clarification_queues,
)
from unify.actor.execution import PythonExecutionSession, _CURRENT_SANDBOX


class _AskTarget:
    async def ask(
        self,
        text: str,
        *,
        _clarification_up_q: asyncio.Queue[str] | None = None,
        _clarification_down_q: asyncio.Queue[str] | None = None,
    ) -> str:
        assert _clarification_up_q is not None
        assert _clarification_down_q is not None
        await _clarification_up_q.put(text)
        return await _clarification_down_q.get()


def test_bind_and_restore_sandbox_clarification_queues():
    orphan_up: asyncio.Queue[str] = asyncio.Queue()
    orphan_down: asyncio.Queue[str] = asyncio.Queue()
    call_up: asyncio.Queue[str] = asyncio.Queue()
    call_down: asyncio.Queue[str] = asyncio.Queue()
    injector = _ClarificationQueueInjector(
        target=SimpleNamespace(),
        clarification_up_q=orphan_up,
        clarification_down_q=orphan_down,
    )
    global_state = {"primitives": injector}

    token = bind_sandbox_clarification_queues(global_state, call_up, call_down)
    assert injector._clar_up_q is call_up
    assert injector._clar_down_q is call_down
    assert global_state["__clarification_up_q__"] is call_up
    assert global_state["__clarification_down_q__"] is call_down

    restore_sandbox_clarification_queues(global_state, token)
    assert injector._clar_up_q is orphan_up
    assert injector._clar_down_q is orphan_down
    assert "__clarification_up_q__" not in global_state
    assert "__clarification_down_q__" not in global_state


@pytest.mark.asyncio
async def test_bound_injector_routes_manager_clar_to_per_call_queues():
    """After bind, nested manager ask() uses the tool-call queues, not orphans."""
    orphan_up: asyncio.Queue[str] = asyncio.Queue()
    orphan_down: asyncio.Queue[str] = asyncio.Queue()
    call_up: asyncio.Queue[str] = asyncio.Queue()
    call_down: asyncio.Queue[str] = asyncio.Queue()

    hub = SimpleNamespace(contacts=_AskTarget())
    injector = _ClarificationQueueInjector(
        target=hub,
        clarification_up_q=orphan_up,
        clarification_down_q=orphan_down,
    )
    sandbox = PythonExecutionSession(environments={}, computer_primitives=None)
    sandbox.global_state["primitives"] = injector
    sb_token = _CURRENT_SANDBOX.set(sandbox)
    try:

        async def _answer() -> None:
            question = await asyncio.wait_for(call_up.get(), timeout=5)
            assert question == "Which owner?"
            assert orphan_up.empty()
            await call_down.put("acme/repo")

        answerer = asyncio.create_task(_answer())
        with CodeActActor._sandbox_clarification_binding(
            clarification_up_q=call_up,
            clarification_down_q=call_down,
        ):
            # Nested getattr creates a child injector that must see rebound queues.
            result = await sandbox.global_state["primitives"].contacts.ask(
                "Which owner?",
            )
        await answerer
    finally:
        _CURRENT_SANDBOX.reset(sb_token)

    assert result == "acme/repo"
    assert injector._clar_up_q is orphan_up
    assert injector._clar_down_q is orphan_down


def test_execute_code_and_execute_function_accept_clarification_kwargs():
    """Signature contract: ToolsData will allocate per-call clar channels."""
    src = inspect.getsource(CodeActActor._build_tools)
    assert "_clarification_up_q: asyncio.Queue[str] | None = None" in src
    assert src.count("_clarification_up_q: asyncio.Queue[str] | None = None") >= 2
    assert "with self._sandbox_clarification_binding(" in src
