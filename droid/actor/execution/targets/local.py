"""The local execution surface: in-process Python and host shell."""

from __future__ import annotations

import shlex
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from ..surface import ExecutionSurface
from .base import ExecResult, ExecutionTarget, coerce_output

if TYPE_CHECKING:
    from ..session import SessionExecutor


class LocalTarget(ExecutionTarget):
    """Runs code on the machine hosting droid via the shared SessionExecutor.

    Shell and Python both execute statelessly through the executor, so the local
    target carries no session lifecycle of its own. File moves are plain copies
    relative to the configured local root.
    """

    surface = ExecutionSurface.LOCAL

    def __init__(
        self,
        session_executor: "SessionExecutor",
        *,
        shell_language: str = "bash",
        local_root: str | Path | None = None,
    ) -> None:
        self._executor = session_executor
        self._shell_language = shell_language
        self._local_root: Optional[Path] = (
            Path(local_root).expanduser() if local_root is not None else None
        )

    def _root(self) -> Path:
        if self._local_root is not None:
            return self._local_root
        from droid.file_manager.settings import get_local_root

        self._local_root = Path(get_local_root()).expanduser()
        return self._local_root

    async def ensure_ready(self) -> None:
        return

    async def run_shell(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        code = f"cd {shlex.quote(cwd)}\n{command}" if cwd else command
        out = await self._executor.execute(
            code=code,
            language=self._shell_language,  # type: ignore[arg-type]
            state_mode="stateless",
            session_id=None,
            venv_id=None,
        )
        exit_code = out.get("result")
        return ExecResult(
            surface=self.surface,
            stdout=coerce_output(out.get("stdout")),
            stderr=coerce_output(out.get("stderr")),
            returncode=exit_code if isinstance(exit_code, int) else None,
            error=out.get("error"),
        )

    async def run_python(
        self,
        code: str,
        *,
        venv_id: int | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        out = await self._executor.execute(
            code=code,
            language="python",
            state_mode="stateless",
            session_id=None,
            venv_id=venv_id,
        )
        return ExecResult(
            surface=self.surface,
            stdout=coerce_output(out.get("stdout")),
            stderr=coerce_output(out.get("stderr")),
            result=out.get("result"),
            error=out.get("error"),
        )

    async def put_file(self, local_path: str | Path, remote_rel: str) -> None:
        dest = self._root() / remote_rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(Path(local_path).expanduser(), dest)

    async def get_file(self, remote_rel: str, local_path: str | Path) -> None:
        src = self._root() / remote_rel
        dest = Path(local_path).expanduser()
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
