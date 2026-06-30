"""Persistent LiveKit agent worker.

Started once at pod startup by the ConversationManager.  Maintains a pool
of pre-warmed child processes (STT, VAD, inference runner already
initialised) so that incoming calls skip the ~22 s cold-start overhead.

Each call is dispatched as a LiveKit job via ``CreateAgentDispatchRequest``.
The warm child process runs ``call.entrypoint``, reads per-call config from
``ctx.job.metadata``, and handles the conversation.  When the call ends the
child exits and the pool automatically pre-warms a replacement.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import threading

os.environ["UNITY_TERMINAL_LOG"] = "true"

from dotenv import load_dotenv

load_dotenv()

from livekit import agents
from livekit.agents.cli.log import setup_logging
from livekit.agents.worker import AgentServer

from unify.conversation_manager.medium_scripts.call import (
    entrypoint,
    prewarm as _base_prewarm,
)
from unify.conversation_manager.medium_scripts.common import FastBrainLogger

_log = FastBrainLogger()

WORKER_READY_PATH = "/tmp/unity_worker_ready"
WORKER_REGISTERED_PATH = "/tmp/unity_worker_registered"


def clear_worker_signal_files() -> None:
    """Remove readiness/registration markers (parent or child may call)."""
    for path in (WORKER_READY_PATH, WORKER_REGISTERED_PATH):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def mark_worker_busy() -> None:
    """Clear the idle-ready marker when a prewarmed process is consumed by a job.

    The marker is re-created by ``_prewarm_and_signal`` once the worker has
    warmed a replacement idle process, so ``WORKER_READY_PATH`` existing means
    "a fresh idle process is available to accept a new call right now" rather
    than the looser "a process was warmed at some point".
    """
    try:
        os.remove(WORKER_READY_PATH)
    except FileNotFoundError:
        pass


def _touch_registered_file() -> None:
    try:
        with open(WORKER_REGISTERED_PATH, "w", encoding="utf-8") as f:
            f.write("")
    except OSError:
        pass


def _prewarm_and_signal(ctx=None):
    """Run the standard prewarm, then touch a ready-file so the parent knows."""
    _base_prewarm(ctx)
    try:
        with open(WORKER_READY_PATH, "w", encoding="utf-8") as f:
            f.write("")
    except OSError:
        pass


def _run_worker_with_registration_signal(
    opts: agents.WorkerOptions,
    *,
    log_level: str,
    devmode: bool,
    register: bool,
) -> None:
    """Run the LiveKit agent server and signal when it has registered."""
    setup_logging(log_level, devmode, console=False)
    opts.validate_config(devmode)

    server = AgentServer.from_server_options(opts)

    @server.once("worker_registered")
    def _on_worker_registered(*_args) -> None:
        _touch_registered_file()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.set_debug(False)
    loop.slow_callback_duration = 0.1

    try:

        def _signal_handler() -> None:
            raise KeyboardInterrupt

        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, _signal_handler)
    except NotImplementedError:
        pass

    async def _worker_run() -> None:
        try:
            await server.run(devmode=devmode, unregistered=not register)
        except Exception:
            from livekit.agents.log import logger

            logger.exception("worker failed")

    try:
        main_task = loop.create_task(_worker_run(), name="agent_runner")
        try:
            loop.run_until_complete(main_task)
        except KeyboardInterrupt:
            pass

        try:
            if not devmode:
                loop.run_until_complete(server.drain(timeout=opts.drain_timeout))
            loop.run_until_complete(server.aclose())
        except KeyboardInterrupt:
            from livekit.agents.log import logger

            logger.warning("exiting forcefully")
            os._exit(1)
    finally:
        loop.close()


def main() -> None:
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} dev <agent_name>")
        sys.exit(1)

    agent_name = sys.argv.pop(2)
    _log.dispatch(f"Starting persistent worker: {agent_name}")

    clear_worker_signal_files()

    log_level = os.environ.get("UNITY_VOICE_WORKER_LOG_LEVEL", "INFO")
    opts = agents.WorkerOptions(
        entrypoint_fnc=entrypoint,
        agent_name=agent_name,
        worker_type=agents.WorkerType.PUBLISHER,
        prewarm_fnc=_prewarm_and_signal,
        num_idle_processes=1,
        initialize_process_timeout=60,
    )
    opts.drain_timeout = 0

    _run_worker_with_registration_signal(
        opts,
        log_level=log_level,
        devmode=True,
        register=True,
    )


if __name__ == "__main__":
    main()
