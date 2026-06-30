"""Shared HTTP client for a desktop agent-service ``/api/exec`` endpoint.

Both remote surfaces (the assistant's managed VM and a user's linked desktop)
run the same agent-service, so command execution is shared here. File movement
differs per surface and is owned by the individual targets.
"""

from __future__ import annotations

import aiohttp

from unify.session_details import SESSION_DETAILS

from ..surface import ExecutionSurface
from .base import ExecResult

# One hour, matching the remote-execution budget used elsewhere.
_DEFAULT_TIMEOUT_MS = 3_600_000
# A small grace period over the command budget for the HTTP round-trip.
_DEFAULT_HTTP_TIMEOUT_S = 3660.0


class AgentServiceExecClient:
    """Runs commands on a remote desktop over its agent-service ``/api/exec``."""

    def __init__(self, api_url: str, surface: ExecutionSurface) -> None:
        self._api_url = api_url.rstrip("/")
        self._surface = surface

    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        shell_mode: str | None = None,
        timeout_ms: int = _DEFAULT_TIMEOUT_MS,
        http_timeout_s: float = _DEFAULT_HTTP_TIMEOUT_S,
    ) -> ExecResult:
        payload: dict[str, object] = {"command": command, "timeout": timeout_ms}
        if cwd is not None:
            payload["cwd"] = cwd
        if shell_mode is not None:
            payload["shell_mode"] = shell_mode

        headers = {"Authorization": f"Bearer {SESSION_DETAILS.unify_key}"}

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self._api_url}/api/exec",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=http_timeout_s),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

        return ExecResult.from_agent_payload(data, self._surface)
