"""
Tests for shell script execution with access to Unity primitives via RPC bridge.

Coverage
========
✓ Basic shell script execution
✓ Shell scripts calling primitives via unity-primitive CLI
✓ Multiple primitive calls in a single script
✓ Various data types (JSON, lists, dicts)
✓ Error handling and propagation
✓ Timeout handling
✓ Different shell languages (sh, bash, zsh)
✓ Environment variable passing
✓ Working directory support
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.helpers import _handle_project
from unify.function_manager.function_manager import FunctionManager
from unify.common.context_registry import ContextRegistry

# ────────────────────────────────────────────────────────────────────────────
# Sample Shell Scripts
# ────────────────────────────────────────────────────────────────────────────

SIMPLE_ECHO_SCRIPT = """#!/bin/sh
echo "Hello from shell!"
"""

SCRIPT_WITH_ARGS = """#!/bin/sh
echo "Args: $1 $2 $3"
"""

SCRIPT_WITH_EXIT_CODE = """#!/bin/sh
exit 42
"""

SCRIPT_CALLS_PRIMITIVE = """#!/bin/sh
# Call the contacts primitive
result=$(unity-primitive contacts ask --text "Who is Alice?")
echo "Primitive result: $result"
"""

SCRIPT_CALLS_MULTIPLE_PRIMITIVES = """#!/bin/sh
# Call multiple primitives
contacts=$(unity-primitive contacts ask --text "Who is Alice?")
web=$(unity-primitive web ask --text "What is 2+2?")
echo "Contacts: $contacts"
echo "Web: $web"
"""

SCRIPT_WITH_JSON_ARG = """#!/bin/sh
# Call primitive with JSON argument
result=$(unity-primitive files search_files --references '{"query": "budget reports"}' --k 5)
echo "Search result: $result"
"""

SCRIPT_PARSES_JSON_RESULT = """#!/bin/sh
# Call primitive and parse JSON result
result=$(unity-primitive contacts ask --text "list all")
# Check if result is valid JSON by using a simple grep
if echo "$result" | grep -q '^{\\|^\\['; then
    echo "Got JSON result"
else
    echo "Got plain result: $result"
fi
"""

SCRIPT_WITH_ERROR_HANDLING = """#!/bin/sh
# Handle errors from primitives
result=$(unity-primitive contacts ask --text "error please" 2>&1)
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo "Primitive failed with code $exit_code"
    echo "Error: $result"
    exit 1
fi
echo "Success: $result"
"""

SCRIPT_USES_ENV_VAR = """#!/bin/sh
echo "MY_VAR=$MY_VAR"
echo "ANOTHER_VAR=$ANOTHER_VAR"
"""

SCRIPT_CHECKS_CWD = """#!/bin/sh
pwd
"""

SCRIPT_TIMEOUT_TEST = """#!/bin/sh
# Sleep for a long time to test timeout
sleep 100
echo "Never reached"
"""

# Bash features that are not POSIX sh, chosen to work on bash 3.2 as well as
# modern bash: indexed arrays, `[[ ]]`, and pattern substitution. This is what
# proves the runner dispatched to bash rather than sh on every platform.
BASH_SPECIFIC_SCRIPT = """#!/bin/bash
items=("value1" "value2")
echo "Bash map: ${items[0]}, ${items[1]}"
name="hello-world"
echo "Substituted: ${name/world/bash}"
if [[ "$name" == hello-* ]]; then
  echo "Matched: yes"
fi
"""

# Associative arrays arrived in bash 4.0. macOS still ships 3.2 at /bin/bash
# (the last GPLv2 release), and the runner resolves that path directly, so this
# script only runs where the interpreter actually supports it.
BASH_ASSOC_ARRAY_SCRIPT = """#!/bin/bash
declare -A map
map["key1"]="value1"
map["key2"]="value2"
echo "Bash map: ${map["key1"]}, ${map["key2"]}"
"""

SCRIPT_LIST_MANAGERS = """#!/bin/sh
# List available managers
unity-primitive --list-managers
"""

SCRIPT_LIST_METHODS = """#!/bin/sh
# List methods for files manager
unity-primitive files --list-methods
"""


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
    primitives.web = MagicMock()
    primitives.web.ask = AsyncMock(return_value="4")
    primitives.files = MagicMock()
    primitives.files.search_files = AsyncMock(
        return_value=[
            {"file_path": "/reports/budget_2024.csv", "score": 0.95},
            {"file_path": "/reports/budget_2023.csv", "score": 0.87},
        ],
    )
    primitives.files.filter_files = AsyncMock(
        return_value=[
            {"file_id": 1, "file_path": "/data/file1.txt"},
            {"file_id": 2, "file_path": "/data/file2.txt"},
        ],
    )
    primitives.tasks = MagicMock()
    primitives.tasks.ask = AsyncMock(return_value=[{"id": 1, "name": "Task 1"}])
    return primitives


@pytest.fixture
def mock_primitives_with_error():
    """Create a mock primitives that raises errors."""
    primitives = MagicMock()
    primitives.contacts = MagicMock()
    primitives.contacts.ask = AsyncMock(
        side_effect=ValueError("Contact not found: error please"),
    )
    return primitives


# ────────────────────────────────────────────────────────────────────────────
# Basic Shell Execution Tests
# ────────────────────────────────────────────────────────────────────────────


@_handle_project
@pytest.mark.asyncio
async def test_basic_shell_execution(function_manager_factory):
    """Test basic shell script execution without primitives."""
    fm = function_manager_factory()

    result = await fm.execute_shell_script(
        implementation=SIMPLE_ECHO_SCRIPT,
        language="sh",
    )

    assert result["error"] is None
    assert result["result"] == 0  # Exit code
    assert "Hello from shell!" in result["stdout"]


@_handle_project
@pytest.mark.asyncio
async def test_shell_script_with_args(function_manager_factory):
    """Test passing arguments to shell script."""
    fm = function_manager_factory()

    result = await fm.execute_shell_script(
        implementation=SCRIPT_WITH_ARGS,
        language="sh",
        call_args=["arg1", "arg2", "arg3"],
    )

    assert result["error"] is None
    assert "Args: arg1 arg2 arg3" in result["stdout"]


@_handle_project
@pytest.mark.asyncio
async def test_shell_script_exit_code(function_manager_factory):
    """Test that exit codes are captured correctly."""
    fm = function_manager_factory()

    result = await fm.execute_shell_script(
        implementation=SCRIPT_WITH_EXIT_CODE,
        language="sh",
    )

    assert result["result"] == 42
    assert result["error"] is not None  # Non-zero exit is an error


@_handle_project
@pytest.mark.asyncio
async def test_shell_script_with_env_vars(function_manager_factory):
    """Test passing environment variables to shell script."""
    fm = function_manager_factory()

    result = await fm.execute_shell_script(
        implementation=SCRIPT_USES_ENV_VAR,
        language="sh",
        env={"MY_VAR": "hello", "ANOTHER_VAR": "world"},
    )

    assert result["error"] is None
    assert "MY_VAR=hello" in result["stdout"]
    assert "ANOTHER_VAR=world" in result["stdout"]


@_handle_project
@pytest.mark.asyncio
async def test_shell_script_with_cwd(function_manager_factory):
    """Test running shell script in a specific working directory."""
    fm = function_manager_factory()

    with tempfile.TemporaryDirectory() as tmpdir:
        result = await fm.execute_shell_script(
            implementation=SCRIPT_CHECKS_CWD,
            language="sh",
            cwd=tmpdir,
        )

        assert result["error"] is None
        assert tmpdir in result["stdout"]


# ────────────────────────────────────────────────────────────────────────────
# Primitive Bridge Tests
# ────────────────────────────────────────────────────────────────────────────


@_handle_project
@pytest.mark.asyncio
async def test_shell_calls_primitive(function_manager_factory, mock_primitives):
    """Test shell script calling a primitive via unity-primitive CLI."""
    fm = function_manager_factory()

    result = await fm.execute_shell_script(
        implementation=SCRIPT_CALLS_PRIMITIVE,
        language="sh",
        primitives=mock_primitives,
    )

    assert result["error"] is None, f"Unexpected error: {result['stderr']}"
    assert "Alice is a test contact" in result["stdout"]
    mock_primitives.contacts.ask.assert_called_once()


@_handle_project
@pytest.mark.asyncio
async def test_shell_calls_multiple_primitives(
    function_manager_factory,
    mock_primitives,
):
    """Test shell script calling multiple primitives."""
    fm = function_manager_factory()

    result = await fm.execute_shell_script(
        implementation=SCRIPT_CALLS_MULTIPLE_PRIMITIVES,
        language="sh",
        primitives=mock_primitives,
    )

    assert result["error"] is None, f"Unexpected error: {result['stderr']}"
    assert "Alice is a test contact" in result["stdout"]
    assert "4" in result["stdout"]
    mock_primitives.contacts.ask.assert_called_once()
    mock_primitives.web.ask.assert_called_once()


@_handle_project
@pytest.mark.asyncio
async def test_shell_calls_primitive_with_json_arg(
    function_manager_factory,
    mock_primitives,
):
    """Test shell script calling primitive with JSON argument."""
    fm = function_manager_factory()

    result = await fm.execute_shell_script(
        implementation=SCRIPT_WITH_JSON_ARG,
        language="sh",
        primitives=mock_primitives,
    )

    assert result["error"] is None, f"Unexpected error: {result['stderr']}"
    # Check that search_files was called with the JSON references
    mock_primitives.files.search_files.assert_called_once()
    call_kwargs = mock_primitives.files.search_files.call_args.kwargs
    assert call_kwargs["references"] == {"query": "budget reports"}
    assert call_kwargs["k"] == 5


@_handle_project
@pytest.mark.asyncio
async def test_primitive_error_propagation(
    function_manager_factory,
    mock_primitives_with_error,
):
    """Test that primitive errors propagate to the shell script."""
    fm = function_manager_factory()

    result = await fm.execute_shell_script(
        implementation=SCRIPT_WITH_ERROR_HANDLING,
        language="sh",
        primitives=mock_primitives_with_error,
    )

    # The script should detect the error and exit with 1
    assert result["result"] == 1
    assert (
        "Contact not found" in result["stdout"]
        or "Contact not found" in result["stderr"]
    )


# ────────────────────────────────────────────────────────────────────────────
# Introspection Tests
# ────────────────────────────────────────────────────────────────────────────


@_handle_project
@pytest.mark.asyncio
async def test_list_managers_introspection(function_manager_factory, mock_primitives):
    """Test the --list-managers introspection command."""
    fm = function_manager_factory()

    result = await fm.execute_shell_script(
        implementation=SCRIPT_LIST_MANAGERS,
        language="sh",
        primitives=mock_primitives,
    )

    assert result["error"] is None, f"Unexpected error: {result['stderr']}"
    # Should list available managers
    assert "contacts" in result["stdout"].lower() or "files" in result["stdout"].lower()


@_handle_project
@pytest.mark.asyncio
async def test_list_methods_introspection(function_manager_factory, mock_primitives):
    """Test the --list-methods introspection command."""
    fm = function_manager_factory()

    result = await fm.execute_shell_script(
        implementation=SCRIPT_LIST_METHODS,
        language="sh",
        primitives=mock_primitives,
    )

    assert result["error"] is None, f"Unexpected error: {result['stderr']}"
    # Should list methods for files manager
    output = result["stdout"].lower()
    assert "search" in output or "filter" in output or "tables" in output


# ────────────────────────────────────────────────────────────────────────────
# Timeout Tests
# ────────────────────────────────────────────────────────────────────────────


@_handle_project
@pytest.mark.asyncio
async def test_shell_script_timeout(function_manager_factory):
    """Test that shell script timeout is enforced."""
    fm = function_manager_factory()

    result = await fm.execute_shell_script(
        implementation=SCRIPT_TIMEOUT_TEST,
        language="sh",
        timeout=0.5,  # Very short timeout
    )

    assert result["error"] is not None
    assert "timed out" in result["error"].lower()
    assert result["result"] == -1


# ────────────────────────────────────────────────────────────────────────────
# Shell Language Tests
# ────────────────────────────────────────────────────────────────────────────


def _bash_major_version() -> int:
    """Major version of the bash the runner will actually invoke.

    The runner resolves ``/bin/bash`` directly rather than searching PATH, so
    a newer bash installed elsewhere does not change what runs here.
    """
    import subprocess

    try:
        out = subprocess.run(
            ["/bin/bash", "-c", "echo $BASH_VERSINFO"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return int((out.stdout or "0").strip() or 0)
    except (OSError, ValueError, subprocess.SubprocessError):
        return 0


@_handle_project
@pytest.mark.asyncio
async def test_bash_specific_features(function_manager_factory):
    """The runner dispatches to bash, not sh.

    Uses constructs bash has had since 3.2, so this runs everywhere rather
    than only where bash is modern.
    """
    fm = function_manager_factory()

    # Skip if bash is not available
    if not os.path.exists("/bin/bash"):
        pytest.skip("bash not available")

    result = await fm.execute_shell_script(
        implementation=BASH_SPECIFIC_SCRIPT,
        language="bash",
    )

    assert result["error"] is None, f"Unexpected error: {result['stderr']}"
    assert "Bash map: value1, value2" in result["stdout"]
    assert "Substituted: hello-bash" in result["stdout"]
    assert "Matched: yes" in result["stdout"]


@_handle_project
@pytest.mark.asyncio
async def test_bash_associative_arrays(function_manager_factory):
    """Associative arrays, where the interpreter is new enough for them.

    This assertion used to live in the test above and failed on macOS for a
    reason that looked like a product bug: bash 3.2 has no ``declare -A``, so
    both subscripts evaluate arithmetically to 0 and the array behaves as an
    indexed one, leaving both keys reading ``value2``.
    """
    fm = function_manager_factory()

    if not os.path.exists("/bin/bash"):
        pytest.skip("bash not available")
    major = _bash_major_version()
    if major < 4:
        pytest.skip(f"/bin/bash is {major}.x; associative arrays need 4.0+")

    result = await fm.execute_shell_script(
        implementation=BASH_ASSOC_ARRAY_SCRIPT,
        language="bash",
    )

    assert result["error"] is None, f"Unexpected error: {result['stderr']}"
    assert "Bash map: value1, value2" in result["stdout"]


@_handle_project
@pytest.mark.asyncio
async def test_zsh_execution(function_manager_factory):
    """Test zsh script execution."""
    fm = function_manager_factory()

    # Skip if zsh is not available
    if not os.path.exists("/bin/zsh"):
        pytest.skip("zsh not available")

    zsh_script = """#!/bin/zsh
echo "Running in zsh: $ZSH_VERSION"
"""

    result = await fm.execute_shell_script(
        implementation=zsh_script,
        language="zsh",
    )

    assert result["error"] is None, f"Unexpected error: {result['stderr']}"
    assert "Running in zsh" in result["stdout"]


# ────────────────────────────────────────────────────────────────────────────
# Data Type Tests
# ────────────────────────────────────────────────────────────────────────────


@_handle_project
@pytest.mark.asyncio
async def test_primitive_returns_list(function_manager_factory, mock_primitives):
    """Test primitive returning a list is handled correctly."""
    fm = function_manager_factory()

    script = """#!/bin/sh
result=$(unity-primitive tasks ask --text "list all")
echo "Tasks: $result"
"""

    result = await fm.execute_shell_script(
        implementation=script,
        language="sh",
        primitives=mock_primitives,
    )

    assert result["error"] is None, f"Unexpected error: {result['stderr']}"
    # The list should be JSON-serialized in stdout
    assert "Task 1" in result["stdout"]


@_handle_project
@pytest.mark.asyncio
async def test_primitive_returns_dict(function_manager_factory):
    """Test primitive returning a dict is handled correctly."""
    fm = function_manager_factory()

    mock_p = MagicMock()
    mock_p.contacts = MagicMock()
    mock_p.contacts.ask = AsyncMock(
        return_value={"name": "Alice", "email": "alice@example.com"},
    )

    script = """#!/bin/sh
result=$(unity-primitive contacts ask --text "get Alice")
echo "Contact: $result"
"""

    result = await fm.execute_shell_script(
        implementation=script,
        language="sh",
        primitives=mock_p,
    )

    assert result["error"] is None, f"Unexpected error: {result['stderr']}"
    assert "alice@example.com" in result["stdout"]


@_handle_project
@pytest.mark.asyncio
async def test_unicode_handling(function_manager_factory):
    """Test unicode data handling through the bridge."""
    fm = function_manager_factory()

    mock_p = MagicMock()
    mock_p.contacts = MagicMock()
    mock_p.contacts.ask = AsyncMock(return_value="Hello 世界! 🌍 äöü")

    script = """#!/bin/sh
result=$(unity-primitive contacts ask --text "unicode test")
echo "Result: $result"
"""

    result = await fm.execute_shell_script(
        implementation=script,
        language="sh",
        primitives=mock_p,
    )

    assert result["error"] is None, f"Unexpected error: {result['stderr']}"
    assert "世界" in result["stdout"] or "Hello" in result["stdout"]


# ────────────────────────────────────────────────────────────────────────────
# Concurrent Execution Tests
# ────────────────────────────────────────────────────────────────────────────


@_handle_project
@pytest.mark.asyncio
async def test_concurrent_shell_executions(function_manager_factory, mock_primitives):
    """Test multiple shell scripts can run concurrently."""
    fm = function_manager_factory()

    scripts = [
        """#!/bin/sh
echo "Script 1"
result=$(unity-primitive contacts ask --text "q1")
echo "Result: $result"
""",
        """#!/bin/sh
echo "Script 2"
result=$(unity-primitive contacts ask --text "q2")
echo "Result: $result"
""",
        """#!/bin/sh
echo "Script 3"
result=$(unity-primitive contacts ask --text "q3")
echo "Result: $result"
""",
    ]

    tasks = [
        fm.execute_shell_script(
            implementation=script,
            language="sh",
            primitives=mock_primitives,
        )
        for script in scripts
    ]

    results = await asyncio.gather(*tasks)

    for i, result in enumerate(results, 1):
        assert result["error"] is None, f"Script {i} failed: {result['stderr']}"
        assert f"Script {i}" in result["stdout"]

    # All three calls should have been made
    assert mock_primitives.contacts.ask.call_count == 3
