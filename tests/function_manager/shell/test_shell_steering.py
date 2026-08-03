"""Steering at the shell RPC bridge.

Shell scripts have no AST to patch and the patch author declines to author
corrections for non-Python source, so shell tops out at dispatch-boundary
steering: every ``unity-primitive`` call round-trips through the same RPC
handler the venv paths use, where an in-flight session memoises it and pause
holds the reply. These tests pin that the bridge actually flows through the
session — and that a pending correction can never fire on shell source.
"""

from __future__ import annotations

from typing import List

import pytest

from tests.helpers import _handle_project
from unify.common.context_registry import ContextRegistry
from unify.function_manager.function_manager import FunctionManager
from unify.function_manager.steering import SteeringSession, use_session

SCRIPT_SENDS_TWICE = """#!/bin/bash
unity-primitive comms send --to eu-alpha
unity-primitive comms send --to us-beta
"""


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
