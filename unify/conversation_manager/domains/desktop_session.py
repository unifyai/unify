"""Coalesced ensure path for the managed-desktop agent-service session."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from unify.conversation_manager.conversation_manager import ConversationManager

_desktop_ensure_lock = asyncio.Lock()
_desktop_ensure_task: asyncio.Task[None] | None = None

_MANAGED_DESKTOP_MODES = frozenset({"ubuntu", "windows"})


def has_managed_desktop_runtime() -> bool:
    """Return whether this assistant is entitled to a managed VM desktop."""
    from unify.session_details import SESSION_DETAILS

    assistant = SESSION_DETAILS.assistant
    return (
        assistant.desktop_mode in _MANAGED_DESKTOP_MODES
        and assistant.managed_desktop_status == "active"
    )


def desktop_agent_session_cached() -> bool:
    """Return whether the backend already holds a cached desktop session."""
    from unify.function_manager.primitives.runtime import ComputerPrimitives
    from unify.manager_registry import ManagerRegistry

    cp = ManagerRegistry.get_instance(ComputerPrimitives)
    if cp is None:
        return False
    backend = cp.backend
    sessions = getattr(backend, "_sessions", None)
    return isinstance(sessions, dict) and "desktop" in sessions


def desktop_session_ensure_in_flight() -> bool:
    """Return whether a coalesced desktop-session ensure task is running."""
    return _desktop_ensure_task is not None and not _desktop_ensure_task.done()


def schedule_ensure_desktop_session(cm: "ConversationManager") -> None:
    """Start a coalesced desktop-session ensure, or no-op if one is in flight."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    asyncio.ensure_future(_schedule_ensure_desktop_session(cm))


async def _schedule_ensure_desktop_session(cm: "ConversationManager") -> None:
    global _desktop_ensure_task
    async with _desktop_ensure_lock:
        if desktop_agent_session_cached():
            return
        if _desktop_ensure_task is not None and not _desktop_ensure_task.done():
            return
        _desktop_ensure_task = asyncio.create_task(
            _run_ensure_desktop_session_guarded(cm),
            name="ensure_desktop_session",
        )


async def _run_ensure_desktop_session_guarded(cm: "ConversationManager") -> None:
    global _desktop_ensure_task
    try:
        await _run_ensure_desktop_session(cm)
    finally:
        async with _desktop_ensure_lock:
            if _desktop_ensure_task is asyncio.current_task():
                _desktop_ensure_task = None


async def ensure_desktop_session(cm: "ConversationManager") -> None:
    """Await the coalesced desktop-session ensure to completion."""
    if desktop_agent_session_cached():
        return

    async with _desktop_ensure_lock:
        if desktop_agent_session_cached():
            return
        if _desktop_ensure_task is not None and not _desktop_ensure_task.done():
            task = _desktop_ensure_task
        else:
            _desktop_ensure_task = asyncio.create_task(
                _run_ensure_desktop_session_guarded(cm),
                name="ensure_desktop_session",
            )
            task = _desktop_ensure_task
    await task


async def _notify_desktop_session_ready(cm: "ConversationManager") -> None:
    """Tell the slow brain that the agent-service desktop session is usable."""
    from unify.common.prompt_helpers import now as prompt_now

    cm.notifications_bar.push_notif(
        "System",
        "Desktop session is ready — computer actions are now available.",
        prompt_now(as_string=False),
    )
    await cm.request_llm_run(delay=0)
    # Background ensure tasks are not EventHandlers, so nothing else flushes
    # the queue after this notify — without this the My Computer ring turn
    # can sit until an unrelated pub/sub event arrives.
    await cm.flush_llm_requests()


def schedule_desktop_session_ready_notify(cm: "ConversationManager") -> None:
    """Wake a follow-up slow-brain turn after the current tool turn finishes.

    The slow brain is single-shot: it cannot branch on ``prepare_desktop``'s
    return value in the same turn. When the desktop session is already cached,
    schedule the same ready notification used by the warming path so a later
    turn can ring or start the demo. Deferred so ``flush_llm_requests`` does
    not re-enter ``_run_llm`` from inside the current tool; the debouncer
    queues behind the running turn.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    asyncio.ensure_future(_notify_desktop_session_ready(cm))


async def _run_ensure_desktop_session(cm: "ConversationManager") -> None:
    """Create a desktop session in agent-service if one doesn't already exist.

    Sessions are lazy (created on first ``get_session`` call), so this must be
    called explicitly to guarantee the ``/screenshot`` endpoint has an active
    session to fall back to.  ``get_session`` is idempotent — calling it when a
    session already exists returns the cached instance.

    Retries with exponential backoff because the VM's Caddy reverse proxy may
    still be starting up or obtaining its TLS certificate from Let's Encrypt
    even after the Communication service reports the VM as "ready".
    """
    from unify.function_manager.primitives.runtime import ComputerPrimitives
    from unify.manager_registry import ManagerRegistry

    cp = ManagerRegistry.get_instance(ComputerPrimitives)
    if cp is None:
        return

    max_attempts = 12
    base_delay = 5.0
    max_delay = 30.0
    delay = base_delay

    for attempt in range(1, max_attempts + 1):
        if desktop_agent_session_cached():
            return
        try:
            session = await cp.backend.get_session("desktop")
            cm._session_logger.info(
                "desktop_session",
                f"Desktop session ready: {session._session_id}",
            )
            await _notify_desktop_session_ready(cm)
            return
        except Exception as e:
            if attempt == max_attempts:
                cm._session_logger.warning(
                    "desktop_session",
                    f"Failed to create desktop session after {max_attempts} attempts: "
                    f"{type(e).__name__}: {e}",
                )
                return
            cm._session_logger.debug(
                "desktop_session",
                f"Attempt {attempt}/{max_attempts} failed ({type(e).__name__}), "
                f"retrying in {delay:.0f}s",
            )
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, max_delay)
