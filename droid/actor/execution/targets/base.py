"""Target contract and the normalized execution result."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..surface import ExecutionSurface


def coerce_output(value: Any) -> str:
    """Normalize a SessionExecutor stdout/stderr field to plain text.

    Python execution returns structured output parts (``TextPart``/``ImagePart``);
    shell execution returns a string. Targets surface a single text stream, so
    image parts are dropped here (they remain available via the raw executor).
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(
            part.text for part in value if getattr(part, "text", None) is not None
        )
    return str(value)


@dataclass
class ExecResult:
    """Uniform result of running code/commands on any surface."""

    surface: ExecutionSurface
    stdout: str = ""
    stderr: str = ""
    returncode: int | None = None
    result: Any = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.returncode in (None, 0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "surface": self.surface.value,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "returncode": self.returncode,
            "result": self.result,
            "error": self.error,
        }

    @classmethod
    def from_agent_payload(
        cls,
        payload: dict[str, Any],
        surface: ExecutionSurface,
    ) -> "ExecResult":
        """Build a result from an agent-service ``/api/exec`` JSON response."""
        return cls(
            surface=surface,
            stdout=payload.get("stdout", "") or "",
            stderr=payload.get("stderr", "") or "",
            returncode=payload.get("exitCode"),
        )


class ExecutionTarget(ABC):
    """One execution surface behind a uniform async interface."""

    surface: ExecutionSurface

    @abstractmethod
    async def ensure_ready(self) -> None:
        """Block until the surface is usable, or raise if it cannot be reached.

        Implementations assert live consent, await VM readiness, or establish a
        transport as appropriate for the surface.
        """

    @abstractmethod
    async def run_shell(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult: ...

    @abstractmethod
    async def run_python(
        self,
        code: str,
        *,
        venv_id: int | None = None,
        timeout: float | None = None,
    ) -> ExecResult: ...

    @abstractmethod
    async def put_file(self, local_path: str | Path, remote_rel: str) -> None:
        """Move a local file onto the surface at a surface-relative path."""

    @abstractmethod
    async def get_file(self, remote_rel: str, local_path: str | Path) -> None:
        """Fetch a surface-relative file to a local path."""
