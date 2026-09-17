"""
End-to-end tests for RPC through a real ``Primitives`` object.

Unlike test_venv_rpc.py, which hands the RPC server a mock, these tests
resolve every call through the real ``Primitives`` runtime: its scope check,
its per-alias cache and the attribute dispatch in ``_dispatch_rpc_path``.
The actor runner behind ``primitives.actor`` is a stand-in placed in the
instance cache, so no sub-actor (and no LLM) is spawned.

This validates that:
1. The RPC protocol works against the real primitives runtime
2. Sync and async functions in the venv reach the same primitive
3. Scope and attribute errors raised by the runtime propagate to the venv
4. Results are correctly serialised back to the subprocess
"""

import shutil

import pytest

from unify.function_manager.function_manager import FunctionManager
from unify.function_manager.primitives import Primitives
from unify.common.context_registry import ContextRegistry
from tests.helpers import _handle_project

# Sample pyproject.toml with minimal dependencies
MINIMAL_VENV_CONTENT = """
[project]
name = "test-venv-e2e"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = []
""".strip()


class _RecordingRunner:
    """Stand-in for the actor runner: answers ``act`` from a fixed table."""

    def __init__(self, answers: dict[str, object]):
        self.answers = answers
        self.requests: list[str] = []

    async def act(self, request: str, **kwargs):
        self.requests.append(request)
        return self.answers[request]


def _primitives_with(runner: _RecordingRunner) -> Primitives:
    primitives = Primitives()
    primitives._managers["actor"] = runner
    return primitives


# ────────────────────────────────────────────────────────────────────────────
# Test Functions that Use the Real Primitives Runtime
# ────────────────────────────────────────────────────────────────────────────

SYNC_ACT_FUNCTION = """
def summarise_sync() -> dict:
    \"\"\"Reach the actor primitive from a synchronous function.\"\"\"
    result = primitives.actor.act(request="summarise")
    return result
""".strip()

ASYNC_ACT_FUNCTION = """
async def summarise_async() -> dict:
    \"\"\"Reach the actor primitive from an async function.\"\"\"
    result = await primitives.actor.act(request="summarise")
    return result
""".strip()

MULTI_CALL_FUNCTION = """
def gather_details() -> dict:
    \"\"\"Make separate RPC calls with different arguments.\"\"\"
    summary = primitives.actor.act(request="summarise")
    count = primitives.actor.act(request="count")
    return {"summary": summary, "count": count}
""".strip()


# ────────────────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def function_manager_factory():
    """Factory fixture that creates FunctionManager instances."""
    managers = []

    def _create():
        ContextRegistry.forget(FunctionManager, "Functions/VirtualEnvs")
        ContextRegistry.forget(FunctionManager, "Functions/Compositional")
        fm = FunctionManager()
        managers.append(fm)
        return fm

    yield _create

    for fm in managers:
        try:
            fm.clear()
        except Exception:
            pass


@pytest.fixture
def cleanup_venvs(function_manager_factory):
    """Cleanup venvs after test."""
    venv_dirs = []

    def _track(fm, venv_id):
        venv_dirs.append(fm._get_venv_dir(venv_id))

    yield _track

    for venv_dir in venv_dirs:
        if venv_dir.exists():
            shutil.rmtree(venv_dir, ignore_errors=True)


# ────────────────────────────────────────────────────────────────────────────
# End-to-End Tests through the Real Primitives Runtime
# ────────────────────────────────────────────────────────────────────────────


@_handle_project
@pytest.mark.asyncio
async def test_e2e_rpc_sync_function_returns_runtime_data(
    function_manager_factory,
    cleanup_venvs,
):
    """A sync venv function receives the primitive's structured result."""
    fm = function_manager_factory()
    runner = _RecordingRunner(
        {"summarise": {"summary": "Two contacts, one with email", "items": 2}},
    )

    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    cleanup_venvs(fm, venv_id)

    result = await fm.execute_in_venv(
        venv_id=venv_id,
        implementation=SYNC_ACT_FUNCTION,
        call_kwargs={},
        is_async=False,
        primitives=_primitives_with(runner),
    )

    assert result["error"] is None, f"Unexpected error: {result['error']}"
    assert result["result"] == {"summary": "Two contacts, one with email", "items": 2}
    assert runner.requests == ["summarise"]


@_handle_project
@pytest.mark.asyncio
async def test_e2e_rpc_async_function_awaits_runtime_result(
    function_manager_factory,
    cleanup_venvs,
):
    """An async venv function awaits the same primitive through RPC."""
    fm = function_manager_factory()
    runner = _RecordingRunner({"summarise": {"summary": "ok"}})

    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    cleanup_venvs(fm, venv_id)

    result = await fm.execute_in_venv(
        venv_id=venv_id,
        implementation=ASYNC_ACT_FUNCTION,
        call_kwargs={},
        is_async=True,
        primitives=_primitives_with(runner),
    )

    assert result["error"] is None, f"Unexpected error: {result['error']}"
    assert result["result"] == {"summary": "ok"}
    assert runner.requests == ["summarise"]


@_handle_project
@pytest.mark.asyncio
async def test_e2e_rpc_multiple_calls_in_single_function(
    function_manager_factory,
    cleanup_venvs,
):
    """RPC should work for multiple calls within a single function."""
    fm = function_manager_factory()
    runner = _RecordingRunner({"summarise": "short", "count": 3})

    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    cleanup_venvs(fm, venv_id)

    result = await fm.execute_in_venv(
        venv_id=venv_id,
        implementation=MULTI_CALL_FUNCTION,
        call_kwargs={},
        is_async=False,
        primitives=_primitives_with(runner),
    )

    assert result["error"] is None, f"Unexpected error: {result['error']}"
    assert result["result"] == {"summary": "short", "count": 3}
    assert runner.requests == ["summarise", "count"]


# ────────────────────────────────────────────────────────────────────────────
# Error Propagation Tests through the Real Runtime
# ────────────────────────────────────────────────────────────────────────────


@_handle_project
@pytest.mark.asyncio
async def test_e2e_rpc_invalid_method_error(
    function_manager_factory,
    cleanup_venvs,
):
    """RPC to a method the runner lacks should propagate the error."""
    fm = function_manager_factory()

    bad_function = """
def call_bad_method() -> str:
    result = primitives.actor.this_method_does_not_exist()
    return result
""".strip()

    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    cleanup_venvs(fm, venv_id)

    result = await fm.execute_in_venv(
        venv_id=venv_id,
        implementation=bad_function,
        call_kwargs={},
        is_async=False,
        primitives=_primitives_with(_RecordingRunner({})),
    )

    assert result["error"] is not None
    assert "this_method_does_not_exist" in result["error"]


@_handle_project
@pytest.mark.asyncio
async def test_e2e_rpc_out_of_scope_manager_error(
    function_manager_factory,
    cleanup_venvs,
):
    """The runtime's scope check rejects an alias outside ``primitives.actor``."""
    fm = function_manager_factory()

    out_of_scope_function = """
def call_out_of_scope() -> str:
    return primitives.spreadsheets.describe(file_path="report.xlsx")
""".strip()

    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    cleanup_venvs(fm, venv_id)

    result = await fm.execute_in_venv(
        venv_id=venv_id,
        implementation=out_of_scope_function,
        call_kwargs={},
        is_async=False,
        primitives=_primitives_with(_RecordingRunner({})),
    )

    assert result["error"] is not None
    assert "spreadsheets" in result["error"]


@_handle_project
@pytest.mark.asyncio
async def test_e2e_rpc_runner_exception_is_reported(
    function_manager_factory,
    cleanup_venvs,
):
    """An exception inside the primitive surfaces as the venv function's error."""
    fm = function_manager_factory()

    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    cleanup_venvs(fm, venv_id)

    # The runner has no answer for "summarise", so the lookup raises KeyError.
    result = await fm.execute_in_venv(
        venv_id=venv_id,
        implementation=SYNC_ACT_FUNCTION,
        call_kwargs={},
        is_async=False,
        primitives=_primitives_with(_RecordingRunner({})),
    )

    assert result["error"] is not None
    assert "summarise" in result["error"]


# ────────────────────────────────────────────────────────────────────────────
# Data Consistency Tests
# ────────────────────────────────────────────────────────────────────────────


@_handle_project
@pytest.mark.asyncio
async def test_e2e_rpc_sees_state_changed_after_primitives_init(
    function_manager_factory,
    cleanup_venvs,
):
    """RPC dispatches against live runtime state, not a snapshot taken at init."""
    fm = function_manager_factory()
    runner = _RecordingRunner({"summarise": "before"})

    # Create Primitives FIRST
    primitives = _primitives_with(runner)

    # THEN change what the runtime answers
    runner.answers["summarise"] = "after"

    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    cleanup_venvs(fm, venv_id)

    result = await fm.execute_in_venv(
        venv_id=venv_id,
        implementation=SYNC_ACT_FUNCTION,
        call_kwargs={},
        is_async=False,
        primitives=primitives,
    )

    assert result["error"] is None, f"Unexpected error: {result['error']}"
    assert result["result"] == "after"


@_handle_project
@pytest.mark.asyncio
async def test_e2e_rpc_multiple_sequential_calls(
    function_manager_factory,
    cleanup_venvs,
):
    """Multiple sequential RPC calls should all work correctly."""
    fm = function_manager_factory()
    runner = _RecordingRunner({"count": 1})

    multi_call_function = """
def multi_call() -> list:
    results = []
    for i in range(3):
        results.append(primitives.actor.act(request="count"))
    return results
""".strip()

    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    cleanup_venvs(fm, venv_id)

    result = await fm.execute_in_venv(
        venv_id=venv_id,
        implementation=multi_call_function,
        call_kwargs={},
        is_async=False,
        primitives=_primitives_with(runner),
    )

    assert result["error"] is None, f"Unexpected error: {result['error']}"
    assert result["result"] == [1, 1, 1]
    assert runner.requests == ["count", "count", "count"]
