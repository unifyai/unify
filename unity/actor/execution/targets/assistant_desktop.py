"""The assistant's managed VM as an execution surface.

Commands run over the VM's agent-service ``/api/exec``; file movement reuses the
FileSync bisync (one root, ``~/Droid/Local`` ↔ the VM's workspace) so there is a
single file-transfer implementation shared with the function-manager flow.
"""

from __future__ import annotations

import asyncio
import base64
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from ..surface import ExecutionSurface
from .base import ExecResult, ExecutionTarget, TargetUnavailableError
from .exec_client import AgentServiceExecClient

if TYPE_CHECKING:
    from unity.function_manager.function_manager import FunctionManager

_DEFAULT_TIMEOUT_MS = 3_600_000


def _python_exe(os_name: str) -> str:
    return "python" if (os_name or "").lower() == "windows" else "python3"


def _inline_python_command(code: str, os_name: str) -> str:
    """Build a one-line interpreter invocation that runs ``code`` verbatim.

    base64 carries arbitrary multi-line source through a single shell argument
    without quoting hazards, and works identically under POSIX sh and PowerShell.
    """
    blob = base64.b64encode(code.encode("utf-8")).decode("ascii")
    return (
        f"{_python_exe(os_name)} -c "
        f"\"import base64;exec(base64.b64decode('{blob}').decode())\""
    )


class AssistantDesktopTarget(ExecutionTarget):
    """Runs on the assistant's managed VM (agent-service + FileSync bisync)."""

    surface = ExecutionSurface.ASSISTANT_DESKTOP

    def __init__(
        self,
        function_manager: "FunctionManager",
        *,
        api_url: str | None,
        os: str | None = None,
    ) -> None:
        self._fm = function_manager
        self._api_url = (api_url or "").rstrip("/")
        self._os = (os or "").lower()
        self._client = AgentServiceExecClient(self._api_url, self.surface)

    def _timeout_ms(self, timeout: float | None) -> int:
        return int(timeout * 1000) if timeout else _DEFAULT_TIMEOUT_MS

    def _shell_mode(self) -> str | None:
        """PowerShell on the Windows VM; the agent-service default otherwise."""
        if self._os == "windows":
            return self._fm.REMOTE_WINDOWS_SHELL_MODE
        return None

    def _assert_sync_manager(self) -> None:
        if self._fm._get_sync_manager() is None:
            raise TargetUnavailableError(
                "File movement to the managed desktop requires an active "
                "FileSync SyncManager, but none is available.",
            )

    async def ensure_ready(self) -> None:
        if not self._api_url:
            raise TargetUnavailableError(
                "No managed desktop agent-service URL is configured",
            )
        # The VM boots asynchronously; block on the shared readiness signal so
        # both ad-hoc execute_code and the function-exec flow wait in one place.
        from unity.function_manager.primitives.runtime import _vm_ready

        if not _vm_ready.is_set():
            ready = await asyncio.to_thread(_vm_ready.wait, 300)
            if not ready:
                raise TargetUnavailableError(
                    "Managed VM did not become ready within 5 minutes",
                )

    async def run_shell(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        return await self._client.exec(
            command,
            cwd=cwd,
            shell_mode=self._shell_mode(),
            timeout_ms=self._timeout_ms(timeout),
        )

    async def run_python(
        self,
        code: str,
        *,
        venv_id: int | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        if venv_id is not None:
            raise ValueError(
                "venv-backed python on the assistant desktop runs through the "
                "function-manager flow; pass venv_id=None for ad-hoc python",
            )
        return await self._client.exec(
            _inline_python_command(code, self._os),
            shell_mode=self._shell_mode(),
            timeout_ms=self._timeout_ms(timeout),
        )

    async def put_file(self, local_path: str | Path, remote_rel: str) -> None:
        self._assert_sync_manager()
        dest = self._fm._windows_exec_local_root() / remote_rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(Path(local_path).expanduser(), dest)
        await self._fm._sync_to_remote()

    async def get_file(self, remote_rel: str, local_path: str | Path) -> None:
        self._assert_sync_manager()
        await self._fm._sync_from_remote()
        src = self._fm._windows_exec_local_root() / remote_rel
        dest = Path(local_path).expanduser()
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
