"""A user's own linked machine as an execution surface.

Commands run over the user desktop's agent-service ``/api/exec``; file movement
uses on-demand SFTP to the user's ``$HOME`` (:class:`UserHomeSFTP`), gated by the
user's live filesystem consent. Writebacks land as versioned copies and never
overwrite the user's originals.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional

from droid.file_manager.sync.user_sftp import UserHomeSFTP
from droid.session_details import UserDesktopLink

from ..surface import ExecutionSurface
from .assistant_desktop import _inline_python_command
from .base import ExecResult, ExecutionTarget, TargetUnavailableError
from .exec_client import AgentServiceExecClient

_DEFAULT_TIMEOUT_MS = 3_600_000


class UserDesktopTarget(ExecutionTarget):
    """Runs on a user's own machine (agent-service + on-demand home SFTP)."""

    surface = ExecutionSurface.USER_DESKTOP

    def __init__(
        self,
        user_id: str,
        link: UserDesktopLink,
        *,
        os: str | None = None,
    ) -> None:
        self._user_id = user_id
        self._link = link
        self._os = (os or link.os or "").lower()
        self._api_url = (link.url or "").rstrip("/")
        self._client = AgentServiceExecClient(self._api_url, self.surface)
        self._sftp: Optional[UserHomeSFTP] = None

    def _timeout_ms(self, timeout: float | None) -> int:
        return int(timeout * 1000) if timeout else _DEFAULT_TIMEOUT_MS

    async def ensure_ready(self) -> None:
        if not self._api_url:
            raise TargetUnavailableError(
                "User desktop has no reachable agent-service tunnel",
            )

    async def _get_sftp(self) -> UserHomeSFTP:
        """Lazily establish the on-demand SFTP link (asserts live file consent)."""
        if self._sftp is None:
            # Constructor raises if filesystem access is not currently consented.
            sftp = UserHomeSFTP(self._user_id, self._link)
            if not await sftp.setup():
                raise TargetUnavailableError(
                    f"Could not establish SFTP to {self._user_id}'s home",
                )
            self._sftp = sftp
        return self._sftp

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
                "venv-backed python is not supported on a user desktop; "
                "pass venv_id=None for ad-hoc python",
            )
        return await self._client.exec(
            _inline_python_command(code, self._os),
            timeout_ms=self._timeout_ms(timeout),
        )

    async def put_file(self, local_path: str | Path, remote_rel: str) -> None:
        sftp = await self._get_sftp()
        await sftp.push(str(Path(local_path).expanduser()), remote_rel)

    async def get_file(self, remote_rel: str, local_path: str | Path) -> None:
        sftp = await self._get_sftp()
        staged = await sftp.pull(remote_rel)
        dest = Path(local_path).expanduser()
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(staged, dest)

    async def aclose(self) -> None:
        if self._sftp is not None:
            self._sftp.cleanup()
            self._sftp = None
