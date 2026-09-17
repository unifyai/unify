"""
Tests for RPC access to primitives from custom virtual environments.

Tests that functions running in custom venvs can call back to the main process
to access primitives (state managers).
"""

import asyncio
import pytest
import shutil
from unittest.mock import AsyncMock, MagicMock

from unify.function_manager.function_manager import FunctionManager
from unify.common.context_registry import ContextRegistry
from tests.helpers import _handle_project

# Sample pyproject.toml with minimal dependencies
MINIMAL_VENV_CONTENT = """
[project]
name = "test-venv"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = []
""".strip()


# ────────────────────────────────────────────────────────────────────────────
# Test Functions that Use Primitives
# ────────────────────────────────────────────────────────────────────────────

PRIMITIVES_ASK_FUNCTION = """
async def ask_contacts(question: str) -> str:
    \"\"\"Ask the contacts manager a question via RPC.\"\"\"
    result = await primitives.contacts.ask(question=question)
    return result
""".strip()

MULTI_PRIMITIVE_FUNCTION = """
async def multi_primitive_call() -> dict:
    \"\"\"Call multiple primitives.\"\"\"
    contacts_result = await primitives.contacts.ask(question="Who is Alice?")
    secrets_result = await primitives.secrets.ask(question="What is 2+2?")
    return {
        "contacts": contacts_result,
        "secrets": secrets_result,
    }
""".strip()

SIMPLE_PRIMITIVES_FUNCTION = """
async def get_contact_count() -> int:
    \"\"\"Get a count from contacts.\"\"\"
    result = await primitives.contacts.list_all()
    return len(result) if isinstance(result, list) else 0
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


@pytest.fixture
def mock_primitives():
    """Create a mock primitives object for testing RPC."""
    primitives = MagicMock()
    primitives.contacts = MagicMock()
    primitives.contacts.ask = AsyncMock(return_value="Alice is a test contact")
    primitives.contacts.list_all = AsyncMock(
        return_value=[{"name": "Alice"}, {"name": "Bob"}],
    )
    primitives.secrets = MagicMock()
    primitives.secrets.ask = AsyncMock(return_value="4")
    return primitives


# ────────────────────────────────────────────────────────────────────────────
# Basic Primitives RPC Tests
# ────────────────────────────────────────────────────────────────────────────


@_handle_project
@pytest.mark.asyncio
async def test_execute_with_primitives_rpc(
    function_manager_factory,
    mock_primitives,
):
    """Function in venv should be able to call primitives via RPC."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

    try:
        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=PRIMITIVES_ASK_FUNCTION,
            call_kwargs={"question": "Who is Alice?"},
            is_async=True,
            primitives=mock_primitives,
        )

        assert result["error"] is None, f"Unexpected error: {result['error']}"
        mock_primitives.contacts.ask.assert_called_once_with(question="Who is Alice?")
        assert result["result"] == "Alice is a test contact"
    finally:
        venv_dir = fm._get_venv_dir(venv_id)
        if venv_dir.exists():
            shutil.rmtree(venv_dir, ignore_errors=True)


@_handle_project
@pytest.mark.asyncio
async def test_execute_with_multiple_primitive_calls(
    function_manager_factory,
    mock_primitives,
):
    """Function should be able to make multiple RPC calls."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

    try:
        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=MULTI_PRIMITIVE_FUNCTION,
            call_kwargs={},
            is_async=True,
            primitives=mock_primitives,
        )

        assert result["error"] is None, f"Unexpected error: {result['error']}"
        mock_primitives.contacts.ask.assert_called_once()
        mock_primitives.secrets.ask.assert_called_once()
        assert result["result"]["contacts"] == "Alice is a test contact"
        assert result["result"]["secrets"] == "4"
    finally:
        venv_dir = fm._get_venv_dir(venv_id)
        if venv_dir.exists():
            shutil.rmtree(venv_dir, ignore_errors=True)


@_handle_project
@pytest.mark.asyncio
async def test_missing_primitives_errors_gracefully(function_manager_factory):
    """Functions calling primitives without them provided should get an error."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

    try:
        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=PRIMITIVES_ASK_FUNCTION,
            call_kwargs={"question": "test"},
            is_async=True,
            primitives=None,
        )
        assert result["error"] is not None
        assert (
            "primitives" in result["error"].lower() or "rpc" in result["error"].lower()
        )
    finally:
        venv_dir = fm._get_venv_dir(venv_id)
        if venv_dir.exists():
            shutil.rmtree(venv_dir, ignore_errors=True)


# ────────────────────────────────────────────────────────────────────────────
# Error Propagation Tests
# ────────────────────────────────────────────────────────────────────────────


@_handle_project
@pytest.mark.asyncio
async def test_rpc_error_propagation(function_manager_factory):
    """Errors from RPC calls should propagate back to the function."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

    # Test different exception types
    exception_types = [
        (ValueError, "Simulated RPC error"),
        (KeyError, "key_not_found"),
        (RuntimeError, "runtime issue"),
        (TypeError, "wrong type"),
    ]

    try:
        for exc_type, exc_msg in exception_types:
            mock_primitives = MagicMock()
            mock_primitives.contacts = MagicMock()
            mock_primitives.contacts.ask = AsyncMock(side_effect=exc_type(exc_msg))

            result = await fm.execute_in_venv(
                venv_id=venv_id,
                implementation=PRIMITIVES_ASK_FUNCTION,
                call_kwargs={"question": "test"},
                is_async=True,
                primitives=mock_primitives,
            )

            assert (
                result["error"] is not None
            ), f"Expected error for {exc_type.__name__}"
            assert (
                exc_msg in result["error"]
            ), f"Expected '{exc_msg}' in error for {exc_type.__name__}"
    finally:
        venv_dir = fm._get_venv_dir(venv_id)
        if venv_dir.exists():
            shutil.rmtree(venv_dir, ignore_errors=True)


@_handle_project
@pytest.mark.asyncio
async def test_function_errors_propagate(function_manager_factory):
    """Function errors (before RPC, syntax, import) should propagate correctly."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

    # Error before RPC
    func_raises = """
async def raise_immediately() -> str:
    raise ValueError("Immediate function error")
""".strip()

    # Syntax error
    func_syntax = """
def broken_syntax(
    \"\"\"Missing close paren.\"\"\"
    return "never reached"
""".strip()

    # Import error
    func_import = """
async def import_nonexistent() -> str:
    import nonexistent_module_xyz123
    return "never reached"
""".strip()

    try:
        # Test error before RPC
        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=func_raises,
            call_kwargs={},
            is_async=True,
        )
        assert result["error"] is not None
        assert "Immediate function error" in result["error"]

        # Test syntax error
        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=func_syntax,
            call_kwargs={},
            is_async=False,
        )
        assert result["error"] is not None
        assert "SyntaxError" in result["error"] or "syntax" in result["error"].lower()

        # Test import error
        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=func_import,
            call_kwargs={},
            is_async=True,
        )
        assert result["error"] is not None
        assert (
            "ModuleNotFoundError" in result["error"] or "ImportError" in result["error"]
        )
    finally:
        venv_dir = fm._get_venv_dir(venv_id)
        if venv_dir.exists():
            shutil.rmtree(venv_dir, ignore_errors=True)


@_handle_project
@pytest.mark.asyncio
async def test_partial_failure_in_chain(function_manager_factory):
    """When one of multiple RPC calls fails, error should propagate."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

    partial_failure_func = """
async def partial_failure() -> dict:
    first = await primitives.contacts.ask(question="first")
    second = await primitives.secrets.ask(question="second")
    return {"first": first, "second": second}
""".strip()

    mock_primitives = MagicMock()
    mock_primitives.contacts = MagicMock()
    mock_primitives.contacts.ask = AsyncMock(return_value="success")
    mock_primitives.secrets = MagicMock()
    mock_primitives.secrets.ask = AsyncMock(
        side_effect=RuntimeError("Second call failed"),
    )

    try:
        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=partial_failure_func,
            call_kwargs={},
            is_async=True,
            primitives=mock_primitives,
        )

        assert result["error"] is not None
        assert "Second call failed" in result["error"]
        mock_primitives.contacts.ask.assert_called_once()
    finally:
        venv_dir = fm._get_venv_dir(venv_id)
        if venv_dir.exists():
            shutil.rmtree(venv_dir, ignore_errors=True)


@_handle_project
@pytest.mark.asyncio
async def test_error_includes_traceback(function_manager_factory):
    """Error messages should include stack trace information."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

    nested_error_func = """
async def nested_error() -> str:
    def inner():
        def innermost():
            raise ValueError("Deep error")
        return innermost()
    return inner()
""".strip()

    try:
        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=nested_error_func,
            call_kwargs={},
            is_async=True,
        )

        assert result["error"] is not None
        assert "Deep error" in result["error"]
        assert "innermost" in result["error"] or "Traceback" in result["error"]
    finally:
        venv_dir = fm._get_venv_dir(venv_id)
        if venv_dir.exists():
            shutil.rmtree(venv_dir, ignore_errors=True)


@_handle_project
@pytest.mark.asyncio
async def test_stdout_captured_with_error(function_manager_factory):
    """stdout/stderr should still be captured when an error occurs."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

    print_then_fail = """
async def print_then_fail() -> str:
    print("This is stdout before failure")
    raise ValueError("Failure after print")
""".strip()

    try:
        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=print_then_fail,
            call_kwargs={},
            is_async=True,
        )

        assert result["error"] is not None
        assert "Failure after print" in result["error"]
        assert "stdout before failure" in result["stdout"]
    finally:
        venv_dir = fm._get_venv_dir(venv_id)
        if venv_dir.exists():
            shutil.rmtree(venv_dir, ignore_errors=True)


@_handle_project
@pytest.mark.asyncio
async def test_subprocess_crash_handled(function_manager_factory):
    """If the subprocess crashes, error should be returned not raised."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

    crash_func = """
import sys
def crash_subprocess() -> str:
    sys.exit(1)
""".strip()

    try:
        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=crash_func,
            call_kwargs={},
            is_async=False,
        )

        assert result["error"] is not None
    finally:
        venv_dir = fm._get_venv_dir(venv_id)
        if venv_dir.exists():
            shutil.rmtree(venv_dir, ignore_errors=True)


# ────────────────────────────────────────────────────────────────────────────
# Data Handling Tests
# ────────────────────────────────────────────────────────────────────────────


@_handle_project
@pytest.mark.asyncio
async def test_rpc_handles_various_data_types(function_manager_factory):
    """RPC should handle various data types: large data, unicode, None, list, nested dict."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

    large_func = """
async def get_large_data():
    result = await primitives.contacts.ask(question="get large")
    return f"Got {len(result)} chars"
""".strip()

    unicode_func = """
async def process_unicode(text: str):
    result = await primitives.contacts.ask(question=text)
    return f"Received: {result}"
""".strip()

    none_func = """
async def get_none():
    return await primitives.contacts.ask(question="get none")
""".strip()

    list_func = """
async def get_list():
    return await primitives.transcripts.ask(question="list")
""".strip()

    nested_func = """
async def get_nested():
    return await primitives.secrets.ask(question="nested")
""".strip()

    try:
        # Test large data
        mock_p = MagicMock()
        mock_p.contacts = MagicMock()
        mock_p.contacts.ask = AsyncMock(return_value="x" * 100_000)

        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=large_func,
            call_kwargs={},
            is_async=True,
            primitives=mock_p,
        )
        assert result["error"] is None
        assert "Got 100000 chars" in result["result"]

        # Test unicode
        unicode_text = "Hello 世界! 🌍 äöü ∑∫∆"
        mock_p.contacts.ask = AsyncMock(return_value=f"Echo: {unicode_text}")

        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=unicode_func,
            call_kwargs={"text": unicode_text},
            is_async=True,
            primitives=mock_p,
        )
        assert result["error"] is None
        assert "世界" in result["result"]

        # Test None
        mock_p.contacts.ask = AsyncMock(return_value=None)

        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=none_func,
            call_kwargs={},
            is_async=True,
            primitives=mock_p,
        )
        assert result["error"] is None
        assert result["result"] is None

        # Test list
        mock_p.transcripts = MagicMock()
        mock_p.transcripts.ask = AsyncMock(return_value=[{"id": 1}, {"id": 2}])

        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=list_func,
            call_kwargs={},
            is_async=True,
            primitives=mock_p,
        )
        assert result["error"] is None
        assert isinstance(result["result"], list)
        assert len(result["result"]) == 2

        # Test nested dict
        nested_data = {"l1": {"l2": {"l3": {"value": "deep"}}}}
        mock_p.secrets = MagicMock()
        mock_p.secrets.ask = AsyncMock(return_value=nested_data)

        result = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=nested_func,
            call_kwargs={},
            is_async=True,
            primitives=mock_p,
        )
        assert result["error"] is None
        assert "deep" in str(result["result"])
    finally:
        venv_dir = fm._get_venv_dir(venv_id)
        if venv_dir.exists():
            shutil.rmtree(venv_dir, ignore_errors=True)


# ────────────────────────────────────────────────────────────────────────────
# Concurrent Execution Tests
# ────────────────────────────────────────────────────────────────────────────


@_handle_project
@pytest.mark.asyncio
async def test_concurrent_venv_executions(function_manager_factory):
    """Multiple functions can run concurrently in the same and different venvs."""
    fm = function_manager_factory()
    venv_id_1 = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    venv_id_2 = fm.add_venv(
        venv=MINIMAL_VENV_CONTENT.replace("test-venv", "test-venv-2"),
    )

    concurrent_func = """
async def increment(counter_id: str):
    result = await primitives.transcripts.ask(question=f"increment {counter_id}")
    return result
""".strip()

    call_order = []

    async def mock_ask(question: str):
        counter_id = question.split()[-1]
        call_order.append(counter_id)
        await asyncio.sleep(0.05)
        return f"incremented {counter_id}"

    mock_primitives = MagicMock()
    mock_primitives.transcripts = MagicMock()
    mock_primitives.transcripts.ask = mock_ask

    try:
        # Test concurrent in same venv
        tasks = [
            fm.execute_in_venv(
                venv_id=venv_id_1,
                implementation=concurrent_func,
                call_kwargs={"counter_id": str(i)},
                is_async=True,
                primitives=mock_primitives,
            )
            for i in range(3)
        ]
        results = await asyncio.gather(*tasks)
        for result in results:
            assert result["error"] is None
        assert len(call_order) == 3

        # Test concurrent in different venvs
        call_order.clear()
        task1 = fm.execute_in_venv(
            venv_id=venv_id_1,
            implementation=concurrent_func,
            call_kwargs={"counter_id": "A"},
            is_async=True,
            primitives=mock_primitives,
        )
        task2 = fm.execute_in_venv(
            venv_id=venv_id_2,
            implementation=concurrent_func,
            call_kwargs={"counter_id": "B"},
            is_async=True,
            primitives=mock_primitives,
        )
        r1, r2 = await asyncio.gather(task1, task2)
        assert r1["error"] is None
        assert r2["error"] is None
    finally:
        for vid in [venv_id_1, venv_id_2]:
            venv_dir = fm._get_venv_dir(vid)
            if venv_dir.exists():
                shutil.rmtree(venv_dir, ignore_errors=True)
