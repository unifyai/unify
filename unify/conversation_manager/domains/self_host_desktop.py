"""Self-host compose helpers for managed desktop readiness."""

from __future__ import annotations

import asyncio
import os
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import TYPE_CHECKING

from unify.common.prompt_helpers import now as prompt_now
from unify.common.startup_timing import log_startup_timing
from unify.conversation_manager import assistant_jobs
from unify.conversation_manager.domains import comms_utils, managers_utils
from unify.conversation_manager.events import FileSyncComplete
from unify.function_manager.primitives.runtime import ComputerPrimitives, _vm_ready
from unify.logger import LOGGER
from unify.manager_registry import ManagerRegistry
from unify.session_details import SESSION_DETAILS

if TYPE_CHECKING:
    from unify.conversation_manager.conversation_manager import ConversationManager


def resolve_desktop_urls(browser_url: str | None = None) -> tuple[str, str]:
    """Return ``(browser_base_url, api_base_url)`` for the managed desktop."""
    browser = (
        browser_url
        or os.environ.get("SELF_HOST_DESKTOP_URL", "")
        or SESSION_DETAILS.assistant.desktop_url
        or ""
    ).rstrip("/")
    internal = os.environ.get("SELF_HOST_DESKTOP_INTERNAL_URL", "").strip().rstrip("/")
    api = internal or browser
    return browser, api


async def desktop_proxy_healthy(api_base_url: str) -> bool:
    """Return whether the desktop reverse proxy serves noVNC."""
    probe_url = f"{api_base_url.rstrip('/')}/desktop/vnc.html"

    def _probe() -> bool:
        try:
            with urllib.request.urlopen(probe_url, timeout=5) as resp:
                return resp.status < 500
        except urllib.error.HTTPError as exc:
            return exc.code < 500
        except Exception:
            return False

    return await asyncio.to_thread(_probe)


async def apply_managed_desktop_ready(
    cm: ConversationManager,
    *,
    binding_id: str,
    browser_desktop_url: str,
    api_desktop_url: str,
    vm_type: str,
    timestamp: datetime | None = None,
    publish_console_ready: bool = True,
    request_llm: bool = True,
) -> None:
    """Mark the managed desktop ready and wire primitives, liveview, and sync state."""
    from unify.conversation_manager.domains.event_handlers import (
        _ensure_desktop_session,
    )

    event_time = timestamp if timestamp is not None else prompt_now(as_string=False)
    liveview_base = browser_desktop_url or api_desktop_url
    liveview_url = f"{liveview_base}/desktop/custom.html"

    if api_desktop_url:
        SESSION_DETAILS.assistant.desktop_url = api_desktop_url

    _t0 = time.perf_counter()
    await asyncio.to_thread(
        assistant_jobs.update_liveview_url,
        cm.assistant_id,
        cm.user_id,
        liveview_url,
    )
    log_startup_timing(
        LOGGER,
        "⏱️ [StartupTiming] desktop_ready.update_liveview_url duration=%.2fs",
        time.perf_counter() - _t0,
    )

    _vm_ready.set()

    if api_desktop_url:
        from urllib.parse import urlparse

        cp = ManagerRegistry.get_instance(ComputerPrimitives)
        if cp is not None and cp._backend is not None:
            _t0 = time.perf_counter()
            parsed = urlparse(api_desktop_url)
            cp._backend.update_container_url(f"{parsed.scheme}://{parsed.netloc}/api")
            log_startup_timing(
                LOGGER,
                "⏱️ [StartupTiming] desktop_ready.update_computer_backend_url duration=%.2fs",
                time.perf_counter() - _t0,
            )

    cm.vm_ready = True
    cm.notifications_bar.push_notif(
        "System",
        "Desktop VM is ready — computer actions are now available.",
        event_time,
    )

    asyncio.ensure_future(_ensure_desktop_session(cm))

    _t0 = time.perf_counter()
    try:
        file_sync_started = await managers_utils._start_file_sync()
    except Exception as exc:
        LOGGER.warning(
            "Desktop ready file sync skipped after error: %s: %s",
            type(exc).__name__,
            exc,
        )
        file_sync_started = False
    log_startup_timing(
        LOGGER,
        "⏱️ [StartupTiming] desktop_ready.start_file_sync duration=%.2fs success=%s",
        time.perf_counter() - _t0,
        file_sync_started,
    )

    shared_desktop_mount = os.environ.get("UNITY_DESKTOP_SHARED_MOUNT") == "1"
    if file_sync_started:
        _t0 = time.perf_counter()
        await cm.event_broker.publish(
            FileSyncComplete.topic,
            FileSyncComplete().to_json(),
        )
        log_startup_timing(
            LOGGER,
            "⏱️ [StartupTiming] desktop_ready.publish_file_sync_complete duration=%.2fs",
            time.perf_counter() - _t0,
        )
    elif shared_desktop_mount or os.environ.get("SELF_HOST", "0") != "1":
        cm.file_sync_complete = True
    else:
        cm._session_logger.info(
            "file_sync",
            "Self-host file sync did not start — deferring FileSyncComplete",
        )

    if api_desktop_url:
        from unify.conversation_manager.medium_scripts.common import (
            notify_voice_worker_agent_service_url,
        )

        await notify_voice_worker_agent_service_url(cm)

    if publish_console_ready and liveview_base:
        _t0 = time.perf_counter()
        await comms_utils.publish_assistant_desktop_ready(
            binding_id,
            liveview_base,
            liveview_url,
            vm_type,
        )
        log_startup_timing(
            LOGGER,
            "⏱️ [StartupTiming] desktop_ready.publish_assistant_ready duration=%.2fs",
            time.perf_counter() - _t0,
        )

    if request_llm:
        _t0 = time.perf_counter()
        await cm.request_llm_run(delay=0)
        log_startup_timing(
            LOGGER,
            "⏱️ [StartupTiming] desktop_ready.request_llm_run duration=%.2fs",
            time.perf_counter() - _t0,
        )


async def bootstrap_managed_desktop_on_startup(cm: ConversationManager) -> bool:
    """Probe the compose desktop proxy and mark the VM ready without Pub/Sub."""
    if os.environ.get("SELF_HOST", "0") != "1":
        return False
    if cm.vm_ready:
        if (
            not cm.file_sync_complete
            and os.environ.get("UNITY_DESKTOP_SHARED_MOUNT") == "1"
        ):
            cm.file_sync_complete = True
        return True

    browser_desktop_url, api_desktop_url = resolve_desktop_urls()
    if not api_desktop_url:
        return False
    if not await desktop_proxy_healthy(api_desktop_url):
        cm._session_logger.debug(
            "desktop_ready",
            "Desktop proxy not healthy at startup; waiting for desktop_ready event",
        )
        return False

    cm._session_logger.info(
        "desktop_ready",
        f"VM ready at startup: ubuntu at {api_desktop_url}",
    )
    binding_id = SESSION_DETAILS.assistant.binding_id or ""
    await apply_managed_desktop_ready(
        cm,
        binding_id=binding_id,
        browser_desktop_url=browser_desktop_url,
        api_desktop_url=api_desktop_url,
        vm_type=SESSION_DETAILS.assistant.desktop_mode or "ubuntu",
        publish_console_ready=True,
        request_llm=False,
    )
    return True
