"""
ConversationManager main entry point.

Supports two modes:
1. Subprocess mode: run via `python -m unify.conversation_manager.main`
2. In-process mode: call `run_conversation_manager()` directly from async code

The mode is determined by how this module is invoked:
- As __main__: subprocess mode with signal handling
- Via run_conversation_manager(): in-process mode, returns the ConversationManager
"""

from __future__ import annotations

from datetime import datetime
import os
import signal
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv

load_dotenv()
import asyncio

from unify.logger import LOGGER
from unify.common.hierarchical_logger import ICONS
from unify.session_details import SESSION_DETAILS
from unify.workspace import get_local_root
from unify.conversation_manager.event_broker import get_event_broker
from unify.conversation_manager.domains import managers_utils
from unify.conversation_manager.domains.utils import log_task_exc
from unify.conversation_manager.conversation_manager import ConversationManager

if TYPE_CHECKING:
    from unify.conversation_manager.event_broker import EventBroker


# Global state for subprocess mode
_stop: asyncio.Event | None = None
_conversation_manager: ConversationManager | None = None


def _signal_handler(signum, frame):
    """Handle shutdown signals gracefully (subprocess mode only)"""
    LOGGER.info(
        f"{ICONS['lifecycle']} "
        + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        + " - [MAIN.PY] Received signal "
        + str(signum)
        + ", shutting down gracefully...",
    )
    if _stop:
        _stop.set()


def create_conversation_manager(
    event_broker: "EventBroker",
    stop_event: asyncio.Event,
    project_name: str = "Assistants",
) -> ConversationManager:
    """
    Create a ConversationManager instance.

    The factory for a ConversationManager built from the current
    ``SESSION_DETAILS``. Usable in both subprocess and in-process modes.

    Args:
        event_broker: The event broker
        stop_event: Event to signal shutdown
        project_name: Project name for logging

    Returns:
        Configured ConversationManager instance
    """
    return ConversationManager(event_broker, stop_event, project_name=project_name)


async def run_conversation_manager(
    *,
    project_name: str = "Assistants",
    event_broker: "EventBroker | None" = None,
    stop_event: asyncio.Event | None = None,
) -> ConversationManager:
    """
    Run ConversationManager in-process (async entry point).

    This is the preferred way to run ConversationManager when you want it
    to share the same process as other components. It sets up all the
    background tasks and returns the ConversationManager instance.

    Args:
        project_name: Project name for logging
        event_broker: Optional event broker. If None, uses get_event_broker()
        stop_event: Optional stop event. If None, creates a new one

    Returns:
        The running ConversationManager instance. Call cm.stop.set() to
        trigger shutdown, or await cm.cleanup() when done.

    Example:
        async def my_app():
            cm = await run_conversation_manager()
            try:
                # Do stuff with cm
                await some_task()
            finally:
                cm.stop.set()
                await cm.cleanup()
    """
    SESSION_DETAILS.populate_from_env()

    # Set the process working directory to the workspace root so that relative
    # file paths in CodeActActor-generated code (e.g. "Attachments/abc123_report.pdf")
    # and on chat attachments resolve against the same root.  This must
    # happen after settings/env are loaded but before any concurrent tasks are
    # created, since os.chdir() is process-global.
    local_root = Path(get_local_root())
    local_root.mkdir(parents=True, exist_ok=True)
    os.chdir(local_root)

    if event_broker is None:
        event_broker = get_event_broker()
    if stop_event is None:
        stop_event = asyncio.Event()

    cm = create_conversation_manager(event_broker, stop_event, project_name)

    asyncio.create_task(cm.wait_for_events()).add_done_callback(log_task_exc)
    asyncio.create_task(managers_utils.init_conv_manager(cm))
    asyncio.create_task(managers_utils.listen_to_operations(cm))

    LOGGER.debug(f"{ICONS['lifecycle']} ConversationManager is running...")
    return cm


async def main(project_name: str = "Assistants"):
    """
    Main entry point for subprocess mode.

    Sets up signal handlers, runs the ConversationManager, and handles
    graceful shutdown.
    """
    global _conversation_manager, _stop

    # Set up signal handlers (subprocess mode only)
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    _stop = asyncio.Event()

    _conversation_manager = await run_conversation_manager(
        project_name=project_name,
        stop_event=_stop,
    )

    LOGGER.debug(f"{ICONS['lifecycle']} Server is Running...")
    await _stop.wait()

    LOGGER.debug(f"{ICONS['lifecycle']} Cleaning up conversation manager...")
    await _conversation_manager.cleanup()
    LOGGER.debug(f"{ICONS['lifecycle']} Cleanup finished")

    # Flush buffered EventBus writes to the store before exit.
    from unify.events.event_bus import EVENT_BUS

    if EVENT_BUS:
        LOGGER.info(f"{ICONS['lifecycle']} Final EventBus flush...")
        EVENT_BUS.flush()

    LOGGER.debug(f"{ICONS['lifecycle']} Shutdown finished")


if __name__ == "__main__":
    asyncio.run(main())
