"""
Tests for remote Windows execution in FunctionManager.

File movement runs entirely over FileSync bisync (no /api/files). These tests
cover:
- Routing logic (when to execute remotely)
- Local staging of the wrapper script + venv pyproject in the sync root
- Venv installation on the VM (uv sync over /exec)
- Bisync push/pull ordering and the hard SyncManager requirement
- Result capture (read locally after the post-exec bisync) and error handling
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from unity.function_manager.function_manager import FunctionManager
from unity.common.context_registry import ContextRegistry
from tests.helpers import _handle_project

# ────────────────────────────────────────────────────────────────────────────
# Sample Functions
# ────────────────────────────────────────────────────────────────────────────

SIMPLE_WINDOWS_FUNC = """
def process_data(input_path: str) -> dict:
    \"\"\"Process data from input path.\"\"\"
    return {"processed": True, "path": input_path}
""".strip()

MINIMAL_VENV_CONTENT = """
[project]
name = "test-venv"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = []
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
def mock_session_details_windows(monkeypatch):
    """Configure SESSION_DETAILS for Windows VM execution."""
    from unity.session_details import SESSION_DETAILS

    monkeypatch.setattr(SESSION_DETAILS.assistant, "desktop_mode", "windows")
    monkeypatch.setattr(SESSION_DETAILS.assistant, "agent_id", 999_001)
    monkeypatch.setattr(
        SESSION_DETAILS.assistant,
        "desktop_url",
        "https://test-vm.unify.ai",
    )
    monkeypatch.setattr(SESSION_DETAILS, "unify_key", "test-api-key")

    # `_execute_python_function_on_remote_windows` waits on the
    # `_vm_ready` `threading.Event` (set by either ConversationManager
    # startup or the ComputerPrimitives mock path). In a pure
    # FunctionManager test those code paths never run, so the wait
    # times out after 5 min with
    # `RuntimeError: Managed VM did not become ready within 5 minutes`.
    # Pre-set the event so the wait returns immediately.
    from unity.function_manager.primitives.runtime import _vm_ready as _runtime_vm_ready

    _was_set = _runtime_vm_ready.is_set()
    _runtime_vm_ready.set()

    yield SESSION_DETAILS

    if not _was_set:
        _runtime_vm_ready.clear()


@pytest.fixture
def mock_session_details_ubuntu(monkeypatch):
    """Configure SESSION_DETAILS for Ubuntu VM execution (managed VM, not Windows)."""
    from unity.session_details import SESSION_DETAILS

    monkeypatch.setattr(SESSION_DETAILS.assistant, "desktop_mode", "ubuntu")
    monkeypatch.setattr(SESSION_DETAILS.assistant, "agent_id", 999_002)

    # Same _vm_ready pre-set rationale as in
    # mock_session_details_windows above.
    from unity.function_manager.primitives.runtime import _vm_ready as _runtime_vm_ready

    _runtime_vm_ready.set()
    monkeypatch.setattr(
        SESSION_DETAILS.assistant,
        "desktop_url",
        "https://test-ubuntu-vm.unify.ai",
    )

    yield SESSION_DETAILS


@pytest.fixture
def windows_local_root(tmp_path, monkeypatch):
    """Point the Windows-exec local sync root at a temp dir.

    Inputs (wrapper script, venv pyproject) are staged here and the result
    file is read back from here after the (mocked) bisync.
    """
    root = tmp_path / "Unity" / "Local"
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        FunctionManager,
        "_windows_exec_local_root",
        lambda self: root,
    )
    return root


@pytest.fixture
def mock_bisync(monkeypatch):
    """Simulate FileSync bisync for the Windows-exec flow.

    The exec path now (a) hard-requires a non-None SyncManager and (b) relies
    on the post-exec bisync to materialise the result file locally. We fake
    both: ``_sync_to_remote`` is a no-op that records the call, and
    ``_sync_from_remote`` writes a result file for each staged script,
    mirroring the VM having run the wrapper. The result payload is
    configurable via ``state.result_payload``.
    """
    calls: List[str] = []
    state = SimpleNamespace(result_payload={"result": None, "error": None})

    monkeypatch.setattr(FunctionManager, "_get_sync_manager", lambda self: object())

    async def _to_remote(self):
        calls.append("sync_to_remote")
        return True

    async def _from_remote(self):
        calls.append("sync_from_remote")
        root = self._windows_exec_local_root()
        scripts_dir = root / "scripts"
        if scripts_dir.is_dir():
            for script in scripts_dir.glob("_exec_*.py"):
                exec_id = script.stem[len("_exec_") :]
                (root / f"_result_{exec_id}.json").write_text(
                    json.dumps(state.result_payload),
                    encoding="utf-8",
                )
        return True

    monkeypatch.setattr(FunctionManager, "_sync_to_remote", _to_remote)
    monkeypatch.setattr(FunctionManager, "_sync_from_remote", _from_remote)

    return SimpleNamespace(calls=calls, state=state)


class MockResponse:
    """Mock aiohttp response."""

    def __init__(self, payload: Any, status: int = 200):
        self._payload = payload
        self.status = status
        self.ok = status < 400

    def raise_for_status(self):
        # Mirrors aiohttp's response API, which the shared exec client invokes.
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def json(self):
        return self._payload

    async def text(self):
        return (
            json.dumps(self._payload)
            if isinstance(self._payload, dict)
            else str(self._payload)
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class MockClientSession:
    """Mock aiohttp.ClientSession that records requests.

    Only ``/api/exec`` is exercised now (venv install + script run); file
    movement is handled by bisync, so ``/api/files`` is never called.
    """

    def __init__(self):
        self.requests: List[Dict[str, Any]] = []
        self._responses: Dict[str, MockResponse] = {}

    def set_response(self, url_pattern: str, response: MockResponse):
        """Set a response for a URL pattern."""
        self._responses[url_pattern] = response

    def _find_response(self, url: str, method: str, kwargs: Dict) -> MockResponse:
        """Find matching response or return default."""
        for pattern, response in self._responses.items():
            if pattern in url:
                return response

        if "/api/exec" in url:
            return MockResponse(
                {
                    "exitCode": 0,
                    "stdout": "__EXECUTION_COMPLETE__",
                    "stderr": "",
                    "duration": 100,
                },
            )

        return MockResponse({})

    def post(self, url: str, **kwargs):
        """Record POST request and return mock response (sync - returns context manager)."""
        self.requests.append({"method": "POST", "url": url, **kwargs})
        return self._find_response(url, "POST", kwargs)

    def get(self, url: str, **kwargs):
        """Record GET request and return mock response (sync - returns context manager)."""
        self.requests.append({"method": "GET", "url": url, **kwargs})
        return self._find_response(url, "GET", kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@pytest.fixture
def mock_aiohttp_session(monkeypatch):
    """Mock aiohttp.ClientSession for HTTP request testing."""
    mock_session = MockClientSession()

    def patched_client_session(*args, **kwargs):
        return mock_session

    import aiohttp

    monkeypatch.setattr(aiohttp, "ClientSession", patched_client_session)

    yield mock_session


def _files_requests(session: MockClientSession) -> List[Dict[str, Any]]:
    return [r for r in session.requests if "/api/files" in r["url"]]


def _exec_requests(session: MockClientSession) -> List[Dict[str, Any]]:
    return [r for r in session.requests if "/api/exec" in r["url"]]


# ────────────────────────────────────────────────────────────────────────────
# 1. Routing Logic Tests
# ────────────────────────────────────────────────────────────────────────────


class TestRemoteWindowsRoutingLogic:
    """Tests for _should_execute_python_function_on_remote_windows."""

    @_handle_project
    def test_returns_false_when_windows_os_required_is_false(
        self,
        function_manager_factory,
        mock_session_details_windows,
    ):
        """Standard functions don't route to Windows."""
        fm = function_manager_factory()
        func_data = {"windows_os_required": False}
        assert fm._should_execute_python_function_on_remote_windows(func_data) is False

    @_handle_project
    def test_returns_false_when_windows_os_required_missing(
        self,
        function_manager_factory,
        mock_session_details_windows,
    ):
        """Missing windows_os_required defaults to False."""
        fm = function_manager_factory()
        func_data = {}
        assert fm._should_execute_python_function_on_remote_windows(func_data) is False

    @_handle_project
    def test_returns_false_when_desktop_mode_is_ubuntu(
        self,
        function_manager_factory,
        mock_session_details_ubuntu,
    ):
        """Ubuntu VM mode stays local - windows_os_required functions execute on the Ubuntu VM."""
        fm = function_manager_factory()
        func_data = {"windows_os_required": True}
        assert fm._should_execute_python_function_on_remote_windows(func_data) is False

    @_handle_project
    def test_returns_true_when_all_conditions_met(
        self,
        function_manager_factory,
        mock_session_details_windows,
    ):
        """All conditions met → remote execution."""
        fm = function_manager_factory()
        func_data = {"windows_os_required": True}
        assert fm._should_execute_python_function_on_remote_windows(func_data) is True


# ────────────────────────────────────────────────────────────────────────────
# 2. Venv Staging + Preparation Tests
# ────────────────────────────────────────────────────────────────────────────


class TestWriteVenvPyprojectLocal:
    """Tests for _write_venv_pyproject_local (local staging of pyproject.toml)."""

    @_handle_project
    def test_raises_on_venv_not_found(
        self,
        function_manager_factory,
        mock_session_details_windows,
        windows_local_root,
    ):
        """ValueError when venv_id doesn't exist."""
        fm = function_manager_factory()

        with pytest.raises(ValueError, match="not found"):
            fm._write_venv_pyproject_local(venv_id=99999)

    @_handle_project
    def test_writes_pyproject_into_sync_root(
        self,
        function_manager_factory,
        mock_session_details_windows,
        windows_local_root,
    ):
        """pyproject.toml is staged at the VM-mirroring relative path."""
        fm = function_manager_factory()
        venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

        try:
            fm._write_venv_pyproject_local(venv_id=venv_id)

            dest = (
                windows_local_root
                / "Local"
                / "venvs"
                / f"venv_{venv_id}"
                / "pyproject.toml"
            )
            assert dest.is_file()
            assert dest.read_text(encoding="utf-8") == MINIMAL_VENV_CONTENT
        finally:
            try:
                fm.delete_venv(venv_id=venv_id)
            except Exception:
                pass


class TestPrepareVenvOnRemoteWindows:
    """Tests for _prepare_venv_on_remote_windows (remote install via uv sync)."""

    @_handle_project
    @pytest.mark.asyncio
    async def test_runs_uv_sync_without_files_api(
        self,
        function_manager_factory,
        mock_session_details_windows,
        mock_aiohttp_session,
    ):
        """Venv preparation installs uv + runs uv sync over /exec, never /api/files."""
        from unity.actor.execution.targets.assistant_desktop import (
            AssistantDesktopTarget,
        )

        fm = function_manager_factory()
        venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

        target = AssistantDesktopTarget(
            fm,
            api_url="https://test-vm.unify.ai",
            os="windows",
        )

        try:
            result = await fm._prepare_venv_on_remote_windows(
                target,
                venv_id=venv_id,
            )

            # Returns the venv's python.exe under the Local\venvs path.
            assert "Local\\venvs\\venv_" in result
            assert ".venv\\Scripts\\python.exe" in result

            # pip install uv + uv sync, and no file-movement over HTTP.
            assert len(_exec_requests(mock_aiohttp_session)) >= 2
            assert _files_requests(mock_aiohttp_session) == []
        finally:
            try:
                fm.delete_venv(venv_id=venv_id)
            except Exception:
                pass


# ────────────────────────────────────────────────────────────────────────────
# 3. Full Execution Flow Tests
# ────────────────────────────────────────────────────────────────────────────


class TestExecutePythonFunctionOnRemoteWindows:
    """End-to-end tests for _execute_python_function_on_remote_windows."""

    @_handle_project
    @pytest.mark.asyncio
    async def test_executes_simple_function(
        self,
        function_manager_factory,
        mock_session_details_windows,
        mock_aiohttp_session,
        windows_local_root,
        mock_bisync,
    ):
        """Result is captured from the locally-pulled result file."""
        fm = function_manager_factory()
        mock_bisync.state.result_payload = {"result": 42, "error": None}

        func_data = {"name": "test_func", "windows_os_required": True}

        result = await fm._execute_python_function_on_remote_windows(
            func_data=func_data,
            implementation=SIMPLE_WINDOWS_FUNC,
            call_kwargs={"input_path": "/test/path"},
        )

        assert result["result"] == 42
        assert result["error"] is None

        # Script ran over /exec; file movement went through bisync, not /files.
        assert len(_exec_requests(mock_aiohttp_session)) >= 1
        assert _files_requests(mock_aiohttp_session) == []
        assert mock_bisync.calls == ["sync_to_remote", "sync_from_remote"]

    @_handle_project
    @pytest.mark.asyncio
    async def test_stages_script_in_sync_root(
        self,
        function_manager_factory,
        mock_session_details_windows,
        mock_aiohttp_session,
        windows_local_root,
        monkeypatch,
    ):
        """The wrapper script is written into the local sync root before bisync."""
        seen_scripts: List[str] = []

        monkeypatch.setattr(FunctionManager, "_get_sync_manager", lambda self: object())

        async def _to_remote(self):
            # Capture what was staged at push time.
            scripts_dir = self._windows_exec_local_root() / "scripts"
            if scripts_dir.is_dir():
                seen_scripts.extend(p.name for p in scripts_dir.glob("_exec_*.py"))
            return True

        async def _from_remote(self):
            root = self._windows_exec_local_root()
            for script in (root / "scripts").glob("_exec_*.py"):
                exec_id = script.stem[len("_exec_") :]
                (root / f"_result_{exec_id}.json").write_text(
                    json.dumps({"result": None, "error": None}),
                    encoding="utf-8",
                )
            return True

        monkeypatch.setattr(FunctionManager, "_sync_to_remote", _to_remote)
        monkeypatch.setattr(FunctionManager, "_sync_from_remote", _from_remote)

        fm = function_manager_factory()
        func_data = {"name": "test_func", "windows_os_required": True}

        await fm._execute_python_function_on_remote_windows(
            func_data=func_data,
            implementation=SIMPLE_WINDOWS_FUNC,
            call_kwargs={"input_path": "/test/path"},
        )

        # A wrapper script was staged before the push bisync ran.
        assert len(seen_scripts) == 1
        # Temp files are cleaned up locally after the result is read.
        assert list((windows_local_root / "scripts").glob("_exec_*.py")) == []
        assert list(windows_local_root.glob("_result_*.json")) == []

    @_handle_project
    @pytest.mark.asyncio
    async def test_uses_powershell_shell_mode(
        self,
        function_manager_factory,
        mock_session_details_windows,
        mock_aiohttp_session,
        windows_local_root,
        mock_bisync,
    ):
        """Execution uses PowerShell shell mode."""
        fm = function_manager_factory()

        func_data = {"name": "test_func", "windows_os_required": True}

        await fm._execute_python_function_on_remote_windows(
            func_data=func_data,
            implementation=SIMPLE_WINDOWS_FUNC,
            call_kwargs={"input_path": "/test/path"},
        )

        exec_requests = _exec_requests(mock_aiohttp_session)
        assert len(exec_requests) >= 1
        assert exec_requests[-1].get("json", {}).get("shell_mode") == "powershell"


# ────────────────────────────────────────────────────────────────────────────
# 4. Integration Tests (execute_function routing)
# ────────────────────────────────────────────────────────────────────────────


class TestExecuteFunctionRemoteRouting:
    """Tests that execute_function correctly routes to remote execution."""

    @_handle_project
    @pytest.mark.asyncio
    async def test_routes_to_remote_when_conditions_met(
        self,
        function_manager_factory,
        mock_session_details_windows,
        mock_aiohttp_session,
        windows_local_root,
        mock_bisync,
        monkeypatch,
    ):
        """execute_function dispatches to remote execution when conditions met."""
        fm = function_manager_factory()

        fm.add_functions(
            implementations=SIMPLE_WINDOWS_FUNC,
            language="python",
        )

        monkeypatch.setattr(
            fm,
            "_should_execute_python_function_on_remote_windows",
            lambda func_data: True,
        )

        await fm.execute_function(
            function_name="process_data",
            call_kwargs={"input_path": "/test/path"},
        )

        # Routed remotely: the script ran over /exec via bisync staging.
        assert len(_exec_requests(mock_aiohttp_session)) >= 1
        assert mock_bisync.calls == ["sync_to_remote", "sync_from_remote"]

    @_handle_project
    @pytest.mark.asyncio
    async def test_stays_local_when_windows_os_required_false(
        self,
        function_manager_factory,
        mock_session_details_windows,
        mock_aiohttp_session,
    ):
        """execute_function stays local when windows_os_required=False."""
        fm = function_manager_factory()

        fm.add_functions(
            implementations=SIMPLE_WINDOWS_FUNC,
            language="python",
        )

        result = await fm.execute_function(
            function_name="process_data",
            call_kwargs={"input_path": "/test/path"},
        )

        # Local execution doesn't hit the agent service.
        agent_requests = [
            r for r in mock_aiohttp_session.requests if "test-vm.unify.ai" in r["url"]
        ]
        assert len(agent_requests) == 0
        assert "result" in result or "error" in result


# ────────────────────────────────────────────────────────────────────────────
# 5. FileSync Integration Tests
# ────────────────────────────────────────────────────────────────────────────


class TestSyncIntegration:
    """Tests for FileSync integration with remote execution."""

    @_handle_project
    @pytest.mark.asyncio
    async def test_sync_called_before_and_after_execution(
        self,
        function_manager_factory,
        mock_session_details_windows,
        mock_aiohttp_session,
        windows_local_root,
        mock_bisync,
    ):
        """Verify sync_to_remote and sync_from_remote are called, in order."""
        fm = function_manager_factory()

        func_data = {"name": "test_func", "windows_os_required": True}

        await fm._execute_python_function_on_remote_windows(
            func_data=func_data,
            implementation=SIMPLE_WINDOWS_FUNC,
            call_kwargs={"input_path": "/Unity/data/file.txt"},
        )

        assert mock_bisync.calls == ["sync_to_remote", "sync_from_remote"]

    @_handle_project
    @pytest.mark.asyncio
    async def test_execution_raises_without_sync_manager(
        self,
        function_manager_factory,
        mock_session_details_windows,
        mock_aiohttp_session,
        windows_local_root,
        monkeypatch,
    ):
        """Windows exec hard-requires FileSync; absence raises before any work."""
        fm = function_manager_factory()

        monkeypatch.setattr(fm, "_get_sync_manager", lambda: None)

        func_data = {"name": "test_func", "windows_os_required": True}

        with pytest.raises(RuntimeError, match="FileSync"):
            await fm._execute_python_function_on_remote_windows(
                func_data=func_data,
                implementation=SIMPLE_WINDOWS_FUNC,
                call_kwargs={"input_path": "/Unity/data/file.txt"},
            )

        # Bailed out before touching the agent service.
        assert _exec_requests(mock_aiohttp_session) == []

    @_handle_project
    def test_get_sync_manager_returns_none_without_file_manager(
        self,
        function_manager_factory,
    ):
        """_get_sync_manager returns None when no FileManager."""
        fm = function_manager_factory()
        fm._fm = None
        assert fm._get_sync_manager() is None
