"""
tests/conversation_manager/voice/test_subprocess_ipc.py
=======================================================

REAL integration test for subprocess IPC communication.

This test actually spawns a subprocess and verifies bidirectional IPC works.
It does NOT mock the subprocess or socket - it tests the real code paths.

This would have caught Ved's bug where the IPC socket was unidirectional
(child → parent only) and call guidance events couldn't reach the voice agent.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest
import pytest_asyncio

from unify.conversation_manager.domains.ipc_socket import (
    CallEventSocketServer,
    CM_EVENT_SOCKET_ENV,
)
from unify.conversation_manager.in_memory_event_broker import (
    create_in_memory_event_broker,
    reset_in_memory_event_broker,
)

# Path to the minimal test subprocess script
TEST_SUBPROCESS_SCRIPT = Path(__file__).parent / "ipc_test_subprocess.py"

# Cold-importing unify in a fresh child can take several seconds under a loaded
# CI shard; keep budgets generous and fail with child stdout/stderr attached.
_READY_TIMEOUT_S = 30.0
_ACK_TIMEOUT_S = 30.0
_POLL_INTERVAL_S = 0.05


@pytest_asyncio.fixture
async def event_broker():
    """Real in-memory event broker."""
    reset_in_memory_event_broker()
    broker = create_in_memory_event_broker()
    yield broker
    await broker.aclose()
    reset_in_memory_event_broker()


async def _drain_stream(
    stream: asyncio.StreamReader | None,
    chunks: list[bytes],
) -> None:
    """Read a subprocess stream to completion so PIPE buffers cannot stall the child."""
    if stream is None:
        return
    while True:
        data = await stream.read(65536)
        if not data:
            return
        chunks.append(data)


def _decode_chunks(chunks: list[bytes]) -> str:
    return b"".join(chunks).decode(errors="replace")


class TestRealSubprocessIPC:
    """
    Integration tests that spawn REAL subprocesses to verify IPC works.

    These tests do NOT mock:
    - process spawning (real asyncio subprocess)
    - Unix sockets (real socket communication)
    - IPC protocol (real message passing)

    They only skip LiveKit (which requires audio hardware).
    """

    @pytest.mark.asyncio
    async def test_bidirectional_ipc_roundtrip(self, event_broker):
        """
        REAL integration test: Full bidirectional IPC roundtrip.

        This test spawns a REAL subprocess and verifies:
        1. Child → Parent: Subprocess can send events to parent
        2. Parent → Child: Parent can send events to subprocess

        The test flow:
        1. Parent starts socket server
        2. Subprocess connects and sends "ready" to parent
        3. Parent receives "ready", sends "guidance" to subprocess
        4. Subprocess receives "guidance", sends "ack" with content back
        5. Parent verifies full roundtrip completed

        WHY THIS TEST MATTERS:
        Before Ved's fix (commit c34270dc), the IPC socket was unidirectional.
        Events could only flow child→parent. The parent→child direction
        (step 3-4) was completely broken, which meant:
        - Call guidance from ConversationManager never reached the voice agent
        - Proactive speech prompts were lost
        - The voice agent couldn't receive status updates

        This single test would have caught that bug because step 4 would
        have timed out - the subprocess would never receive the guidance.
        """
        received_from_child: list[tuple[str, str]] = []

        async def on_event(channel: str, event_json: str):
            received_from_child.append((channel, event_json))

        server = CallEventSocketServer(
            event_broker,
            on_event=on_event,
            forward_channels=["app:call:*"],
        )
        proc: asyncio.subprocess.Process | None = None
        stdout_chunks: list[bytes] = []
        stderr_chunks: list[bytes] = []
        drain_tasks: list[asyncio.Task] = []

        def child_output() -> str:
            return (
                f"returncode={proc.returncode if proc else None}, "
                f"stdout={_decode_chunks(stdout_chunks)!r}, "
                f"stderr={_decode_chunks(stderr_chunks)!r}"
            )

        async def wait_for_channel(channel: str, timeout_s: float) -> str | None:
            """Wait for a child event, failing fast if the subprocess exits early."""
            deadline = asyncio.get_running_loop().time() + timeout_s
            while asyncio.get_running_loop().time() < deadline:
                for ch, event_json in received_from_child:
                    if ch == channel:
                        return event_json
                if proc is not None and proc.returncode is not None:
                    raise AssertionError(
                        f"Subprocess exited before {channel!r} "
                        f"(exit={proc.returncode}). {child_output()}",
                    )
                await asyncio.sleep(_POLL_INTERVAL_S)
            return None

        try:
            socket_path = await server.start()

            env = os.environ.copy()
            env[CM_EVENT_SOCKET_ENV] = socket_path

            # Ensure PYTHONPATH includes workspace root so the child can import unify
            workspace_root = str(Path(__file__).parent.parent.parent.parent)
            existing_pythonpath = env.get("PYTHONPATH", "")
            env["PYTHONPATH"] = (
                f"{workspace_root}:{existing_pythonpath}"
                if existing_pythonpath
                else workspace_root
            )

            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                str(TEST_SUBPROCESS_SCRIPT),
                "full_roundtrip",
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            drain_tasks = [
                asyncio.create_task(_drain_stream(proc.stdout, stdout_chunks)),
                asyncio.create_task(_drain_stream(proc.stderr, stderr_chunks)),
            ]

            ready_event = await wait_for_channel("app:call:ready", _READY_TIMEOUT_S)
            assert ready_event is not None, (
                f"Subprocess never sent 'ready' event within {_READY_TIMEOUT_S:.0f}s. "
                f"{child_output()}"
            )

            # Clear and send guidance (using production channel name)
            received_from_child.clear()
            from unify.conversation_manager.events import FastBrainNotification

            await event_broker.publish(
                "app:call:notification",
                FastBrainNotification(
                    contact={},
                    message="Ask about their day",
                    source="slow_brain",
                ).to_json(),
            )

            ack_event = await wait_for_channel("app:call:ack", _ACK_TIMEOUT_S)
            assert ack_event is not None, (
                f"Full roundtrip failed - subprocess didn't acknowledge guidance "
                f"within {_ACK_TIMEOUT_S:.0f}s. This means parent→child IPC is broken. "
                f"{child_output()}"
            )
            data = json.loads(ack_event)
            assert (
                data["received_message"] == "Ask about their day"
            ), f"Subprocess received wrong message: {data}. {child_output()}"

        finally:
            if proc is not None and proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
            if drain_tasks:
                await asyncio.gather(*drain_tasks, return_exceptions=True)
            await server.stop()
