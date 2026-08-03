"""Steering at the shell RPC bridge.

Every ``unity-primitive`` call round-trips through the same RPC handler the
venv paths use, where an in-flight session memoises it and pause holds the
reply. Shell has no functions the patch author can rewrite, so a correction
that invalidates remaining work becomes a stop request and terminates the
process group directly, including while the script is between dispatches.
"""

from __future__ import annotations

import asyncio
from typing import List

import pytest

from tests.helpers import _handle_project
from unify.common.context_registry import ContextRegistry
from unify.function_manager.function_manager import FunctionManager
from unify.function_manager.steering import (
    InterruptionRequest,
    SteeringSession,
    use_session,
)
from unify.function_manager.steering_patcher import LLMPatchAuthor

SCRIPT_SENDS_TWICE = """#!/bin/bash
unity-primitive comms send --to eu-alpha
unity-primitive comms send --to us-beta
"""

SCRIPT_WITHOUT_DISPATCHES = """#!/bin/bash
echo started
sleep 30
echo finished
"""


async def _stop_author(*, interjections, session):
    return InterruptionRequest(reason=interjections[0], stop=True)


class _Comms:
    def __init__(self) -> None:
        self.sent: List[str] = []

    async def send(self, to: str) -> str:
        self.sent.append(to)
        return f"sent:{to}"


class _Prims:
    def __init__(self, comms: _Comms) -> None:
        self.comms = comms


@pytest.fixture
def function_manager_factory():
    """Factory fixture that creates FunctionManager instances."""
    managers = []

    def _create():
        ContextRegistry.forget(FunctionManager, "Functions/VirtualEnvs")
        ContextRegistry.forget(FunctionManager, "Functions/Compositional")
        ContextRegistry.forget(FunctionManager, "Functions/Primitives")
        ContextRegistry.forget(FunctionManager, "Functions/Meta")
        fm = FunctionManager()
        managers.append(fm)
        return fm

    yield _create

    for fm in managers:
        try:
            fm.clear()
        except Exception:
            pass


@_handle_project
@pytest.mark.asyncio
async def test_shell_dispatches_flow_through_the_session(function_manager_factory):
    fm = function_manager_factory()
    comms = _Comms()
    session = SteeringSession()
    session.bind_source(SCRIPT_SENDS_TWICE)

    with use_session(session):
        result = await fm.execute_shell_script(
            implementation=SCRIPT_SENDS_TWICE,
            language="bash",
            primitives=_Prims(comms),
        )

    assert result["error"] is None, result["error"]
    assert result["result"] == 0
    assert comms.sent == ["eu-alpha", "us-beta"]
    # Both dispatches were seen — and recorded — by the session on their way
    # through the bridge.
    assert session.cache.misses == 2
    assert session.cache.completed_calls() == ["comms.send", "comms.send"]
    assert "sent:eu-alpha" in result["stdout"]


@_handle_project
@pytest.mark.asyncio
async def test_shell_runs_unchanged_without_a_session(function_manager_factory):
    fm = function_manager_factory()
    comms = _Comms()

    result = await fm.execute_shell_script(
        implementation=SCRIPT_SENDS_TWICE,
        language="bash",
        primitives=_Prims(comms),
    )

    assert result["error"] is None, result["error"]
    assert comms.sent == ["eu-alpha", "us-beta"]


@pytest.mark.asyncio
async def test_patch_author_can_stop_source_with_no_python_functions():
    class _Client:
        async def generate(self, **kwargs):
            return '{"reason":"cancel it","stop":true,"patches":[]}'

    session = SteeringSession()
    session.bind_source(SCRIPT_WITHOUT_DISPATCHES)
    author = LLMPatchAuthor(client_factory=_Client)

    request = await author(interjections=["cancel it"], session=session)

    assert request is not None
    assert request.stop is True
    assert request.patches == []


@_handle_project
@pytest.mark.asyncio
async def test_stop_terminates_shell_between_dispatches(function_manager_factory):
    fm = function_manager_factory()
    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait("cancel the script")
    session = SteeringSession(interject_q=queue, patch_author=_stop_author)

    with use_session(session):
        result = await fm.execute_shell_script(
            implementation=SCRIPT_WITHOUT_DISPATCHES,
            language="bash",
        )

    assert result["error"] is None
    assert result["result"] == {
        "status": "stopped",
        "reason": "cancel the script",
    }
    assert "finished" not in result["stdout"]
