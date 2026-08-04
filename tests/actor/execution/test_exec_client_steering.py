"""Steering a remote exec through the agent-service signal route.

The remote surface has no primitives bridge, so nothing can be memoised,
patched, or replayed — steering there is the two process-level verbs, stop
and pause, delivered to ``/api/exec/signal`` at a client-chosen exec id while
``/api/exec`` is still blocking. These tests drive the real client against a
stub agent and pin the acknowledgment contract: a stop only becomes the
run's outcome when the agent confirmed it, so an agent too old to know the
route (404) leaves the run — and its reported result — untouched.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import pytest
from aiohttp import web

from unify.actor.execution.surface import ExecutionSurface
from unify.actor.execution.targets.exec_client import AgentServiceExecClient
from unify.function_manager.steering import (
    InterruptionRequest,
    SteeringSession,
    use_session,
)


async def _stop_author(*, interjections, session):
    return InterruptionRequest(reason=interjections[0], stop=True)


class _StubAgent:
    """A minimal agent-service: /api/exec blocks until killed or finished."""

    def __init__(self, *, signal_route: bool = True) -> None:
        self.signals: List[Dict[str, Any]] = []
        self.exec_ids: List[str] = []
        self._killed: asyncio.Event = asyncio.Event()
        self._signal_route = signal_route
        self.run_seconds = 30.0
        self._runner: Optional[web.AppRunner] = None
        self.url = ""

    async def _exec(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.exec_ids.append(body.get("exec_id"))
        try:
            await asyncio.wait_for(self._killed.wait(), timeout=self.run_seconds)
            exit_code = 143
            stdout = "partial output\n"
        except asyncio.TimeoutError:
            exit_code = 0
            stdout = "completed\n"
        return web.json_response(
            {
                "status": "success" if exit_code == 0 else "error",
                "exitCode": exit_code,
                "stdout": stdout,
                "stderr": "",
                "duration": 1,
                "cwd": "/",
                "execId": body.get("exec_id"),
            },
        )

    async def _signal(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.signals.append(body)
        if body.get("exec_id") not in self.exec_ids:
            return web.json_response({"error": "not_found"}, status=404)
        if body.get("action") == "stop":
            self._killed.set()
        return web.json_response(
            {"status": "ok", "exec_id": body.get("exec_id"), "action": body["action"]},
        )

    async def __aenter__(self) -> "_StubAgent":
        app = web.Application()
        app.router.add_post("/api/exec", self._exec)
        if self._signal_route:
            app.router.add_post("/api/exec/signal", self._signal)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._runner is not None:
            await self._runner.cleanup()


@pytest.mark.asyncio
async def test_unsteered_exec_is_one_plain_request():
    async with _StubAgent() as agent:
        agent.run_seconds = 0.05
        client = AgentServiceExecClient(agent.url, ExecutionSurface.ASSISTANT_DESKTOP)
        result = await client.exec("echo hi", timeout_ms=5_000)

    assert result.returncode == 0
    assert result.stdout == "completed\n"
    assert agent.signals == []


@pytest.mark.asyncio
async def test_stop_request_cancels_the_remote_run():
    async with _StubAgent() as agent:
        client = AgentServiceExecClient(agent.url, ExecutionSurface.ASSISTANT_DESKTOP)
        queue: asyncio.Queue = asyncio.Queue()
        queue.put_nowait("wrong machine, stop it")
        session = SteeringSession(interject_q=queue, patch_author=_stop_author)

        with use_session(session):
            result = await client.exec("sleep 30", timeout_ms=60_000)

    assert result.result == {
        "status": "stopped",
        "reason": "wrong machine, stop it",
    }
    assert result.error is None
    assert result.stdout == "partial output\n"
    stop_signals = [s for s in agent.signals if s["action"] == "stop"]
    assert len(stop_signals) == 1
    assert stop_signals[0]["exec_id"] == agent.exec_ids[0]


@pytest.mark.asyncio
async def test_pause_transitions_reach_the_agent():
    async with _StubAgent() as agent:
        agent.run_seconds = 1.0
        client = AgentServiceExecClient(agent.url, ExecutionSurface.ASSISTANT_DESKTOP)
        session = SteeringSession()

        async def _toggle() -> None:
            await asyncio.sleep(0.15)
            session.runtime.pause()
            await asyncio.sleep(0.15)
            session.runtime.resume()

        with use_session(session):
            toggler = asyncio.create_task(_toggle())
            result = await client.exec("sleep 1", timeout_ms=60_000)
            await toggler

    assert result.returncode == 0
    assert [s["action"] for s in agent.signals] == ["pause", "resume"]


@pytest.mark.asyncio
async def test_agent_without_signal_route_runs_unsteered():
    """An unacknowledged stop must never be reported as the run's outcome."""
    async with _StubAgent(signal_route=False) as agent:
        agent.run_seconds = 0.4
        client = AgentServiceExecClient(agent.url, ExecutionSurface.ASSISTANT_DESKTOP)
        queue: asyncio.Queue = asyncio.Queue()
        queue.put_nowait("stop it")
        session = SteeringSession(interject_q=queue, patch_author=_stop_author)

        with use_session(session):
            result = await client.exec("sleep 30", timeout_ms=60_000)

    assert result.returncode == 0
    assert result.stdout == "completed\n"
    assert result.result is None, "reported stopped without acknowledgment"


@pytest.mark.asyncio
async def test_patch_corrections_do_not_touch_the_remote_run():
    """No dispatch record exists remotely, so a patched re-run would repeat
    side effects: patch requests must neither signal nor stop anything."""

    async def _patch_author(*, interjections, session):
        from unify.function_manager.steering import Patch

        return InterruptionRequest(
            reason=interjections[0],
            patches=[
                Patch(function_name="whatever", source="def whatever():\n    pass"),
            ],
        )

    async with _StubAgent() as agent:
        agent.run_seconds = 0.4
        client = AgentServiceExecClient(agent.url, ExecutionSurface.ASSISTANT_DESKTOP)
        queue: asyncio.Queue = asyncio.Queue()
        queue.put_nowait("tweak it")
        session = SteeringSession(interject_q=queue, patch_author=_patch_author)

        with use_session(session):
            result = await client.exec(
                "python3 -c ...",
                timeout_ms=60_000,
                source="def whatever():\n    return 1\n",
            )

    assert result.returncode == 0
    assert agent.signals == []
