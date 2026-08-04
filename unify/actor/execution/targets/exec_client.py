"""Shared HTTP client for a desktop agent-service ``/api/exec`` endpoint.

Both remote surfaces (the assistant's managed VM and a user's linked desktop)
run the same agent-service, so command execution is shared here. File movement
differs per surface and is owned by the individual targets.

Remote runs are steerable at the process level. ``/api/exec`` blocks until
the command finishes, so the client supplies its own exec id and, while a
steering session is in flight, watches for corrections alongside the request:
a stop request or a pause-state change becomes a ``/api/exec/signal`` call
addressed at that id. There is no primitives bridge on the remote side, so
there is nothing to memoise, patch, or replay — a correction that authors
patches is recorded but cannot be applied; only stop and pause reach the run.
An agent-service too old to know the signal route answers 404 and the run
simply proceeds unsteered, which is the pre-signal behaviour.
"""

from __future__ import annotations

import asyncio
import logging
import uuid

import aiohttp

from unify.function_manager.steering import (
    ExecutionStopped,
    InterruptionRequest,
    active_session,
)
from unify.session_details import SESSION_DETAILS

from ..surface import ExecutionSurface
from .base import ExecResult

logger = logging.getLogger(__name__)

# One hour, matching the remote-execution budget used elsewhere.
_DEFAULT_TIMEOUT_MS = 3_600_000
# A small grace period over the command budget for the HTTP round-trip.
_DEFAULT_HTTP_TIMEOUT_S = 3660.0


class AgentServiceExecClient:
    """Runs commands on a remote desktop over its agent-service ``/api/exec``."""

    def __init__(self, api_url: str, surface: ExecutionSurface) -> None:
        self._api_url = api_url.rstrip("/")
        self._surface = surface

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {SESSION_DETAILS.unify_key}"}

    async def _signal(self, exec_id: str, action: str) -> bool:
        """Deliver one steering action to a running exec.

        Returns whether the agent acknowledged it. 404 means the run already
        finished or the agent predates the signal route; either way the
        blocking request tells the rest of the story, so the caller must not
        report an outcome the agent never confirmed.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self._api_url}/api/exec/signal",
                    json={"exec_id": exec_id, "action": action},
                    headers=self._headers(),
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 200:
                        return True
                    if resp.status != 404:
                        logger.debug(
                            "exec signal %s for %s answered %s",
                            action,
                            exec_id,
                            resp.status,
                        )
        except aiohttp.ClientError:
            logger.debug("exec signal %s for %s failed", action, exec_id, exc_info=True)
        return False

    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        shell_mode: str | None = None,
        timeout_ms: int = _DEFAULT_TIMEOUT_MS,
        http_timeout_s: float = _DEFAULT_HTTP_TIMEOUT_S,
        source: str | None = None,
    ) -> ExecResult:
        """Run *command* remotely, steered while a session is in flight.

        ``source`` is what the patch author reads when deciding what a
        correction means — the code as written, not the shipped one-liner
        that carries it.
        """
        exec_id = uuid.uuid4().hex[:16]
        payload: dict[str, object] = {
            "command": command,
            "timeout": timeout_ms,
            "exec_id": exec_id,
        }
        if cwd is not None:
            payload["cwd"] = cwd
        if shell_mode is not None:
            payload["shell_mode"] = shell_mode

        async def _request() -> dict:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self._api_url}/api/exec",
                    json=payload,
                    headers=self._headers(),
                    timeout=aiohttp.ClientTimeout(total=http_timeout_s),
                ) as resp:
                    resp.raise_for_status()
                    return await resp.json()

        steering = active_session()
        if steering is None:
            return ExecResult.from_agent_payload(await _request(), self._surface)

        steering.bind_source(source or command)
        stopped: ExecutionStopped | None = None

        async def _stop(request: InterruptionRequest) -> None:
            nonlocal stopped
            if await self._signal(exec_id, "stop"):
                stopped = ExecutionStopped(request.reason or "steered")

        async def _set_paused(paused: bool) -> None:
            await self._signal(exec_id, "pause" if paused else "resume")

        # Targeting uses empty source deliberately: with no dispatch record
        # to replay against, re-running patched source remotely would repeat
        # side effects, so only stop requests may fire here.
        watcher = asyncio.create_task(steering.relay_corrections("", _stop))
        pause_watcher = asyncio.create_task(steering.relay_pause(_set_paused))

        try:
            data = await _request()
        finally:
            for task in (watcher, pause_watcher):
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.debug("remote steering watcher failed", exc_info=True)

        if stopped is not None:
            return ExecResult(
                surface=self._surface,
                stdout=data.get("stdout", "") or "",
                stderr=data.get("stderr", "") or "",
                returncode=data.get("exitCode"),
                result=stopped.outcome,
            )
        return ExecResult.from_agent_payload(data, self._surface)
