from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import aiohttp
from livekit.api import CreateAgentDispatchRequest, LiveKitAPI

from unify.contact_manager.types.contact import UNASSIGNED
from unify.conversation_manager.events import *
from unify.conversation_manager.domains.ipc_socket import (
    CallEventSocketServer,
    CM_EVENT_SOCKET_ENV,
)
from unify.conversation_manager.tracing import trace_kv
from unify.logger import LOGGER
from unify.common.hierarchical_logger import DEFAULT_ICON, ICONS
from unify.helpers import (
    run_script,
    terminate_process,
)

if TYPE_CHECKING:
    from unify.conversation_manager.in_memory_event_broker import InMemoryEventBroker


def _resolve_agent_service_url() -> str:
    """Resolve agent-service base URL (same logic as common.py)."""
    from unify.session_details import SESSION_DETAILS

    desktop_url = SESSION_DETAILS.assistant.desktop_url
    if desktop_url:
        from urllib.parse import urlparse

        parsed = urlparse(desktop_url)
        return f"{parsed.scheme}://{parsed.netloc}/api"
    return "http://localhost:3000"


def make_room_name(assistant_id: str, medium: str) -> str:
    """Canonical LiveKit room name for a given assistant and medium.

    Format: unity_{assistant_id}_{medium}
    Examples: unity_25_phone, unity_25_meet, unity_25_teams
    """
    return f"unity_{assistant_id}_{medium}"


@dataclass
class CallConfig:
    assistant_id: str
    user_id: str
    assistant_bio: str
    assistant_number: str
    voice_provider: str
    voice_id: str
    assistant_name: str = ""
    job_name: str = ""
    is_coordinator: bool = False


_BASE_FORWARD_CHANNELS = [
    "app:call:*",
    "app:comms:*",
]

DISPATCH_ACTIVATION_TIMEOUT_S = 90.0
# Upper bound on how long we await a freshly prewarmed idle worker process
# before starting an assistant-initiated outbound call. Prewarm normally
# completes in well under this; the cap exists so a wedged worker surfaces as a
# failure rather than hanging.
OUTBOUND_CALL_READINESS_TIMEOUT_S = 30.0

# Dispatch should only wait briefly for a running worker to publish its
# registration marker. Inbound sessions have already been accepted by the user,
# and outbound sessions reached this point only after the stricter readiness
# gate, so an unregistered worker should fall back to a per-call subprocess.
WORKER_DISPATCH_REGISTERED_TIMEOUT_S = 2.0

# How long the worker may stay alive-but-unwarmed while the manager is fully
# idle before the watchdog force-restarts it to recover. Post-job re-warm usually
# completes in seconds, but a cold container prewarm can take the full LiveKit
# initialize window (~30-60s). The stall threshold must exceed that window or
# the watchdog kills the worker before WORKER_READY is written and voice stays
# wedged forever. Keep this aligned with ``initialize_process_timeout`` in
# ``medium_scripts/worker.py``.
WORKER_REWARM_STALL_S = 60.0

# Fallback briefing for an outbound call that somehow reaches dispatch without a
# mission context. An agent-initiated call must never open "blind", so even
# here the agent is told to open with purpose rather than wait in silence.
_OUTBOUND_OPENER_FALLBACK_CONTEXT = (
    "You are placing this outbound call. Open by greeting the person warmly, "
    "saying who you are, and giving the reason you are reaching out, using what "
    "you already know about this contact and your relationship with them."
)


class LivekitCallManager:
    def __init__(
        self,
        config: CallConfig,
        event_broker: "InMemoryEventBroker | None" = None,
    ):
        self.job_name: str = ""
        self.set_config(config=config)
        self.call_exchange_id = UNASSIGNED
        self.unify_meet_exchange_id = UNASSIGNED
        self.call_start_timestamp = None
        self.unify_meet_start_timestamp = None
        self.call_contact = None
        self._call_proc: subprocess.Popen | None = None
        self._worker_proc: subprocess.Popen | None = None
        self._active_job: bool = False
        self.conference_name = ""
        self.room_name = ""
        self.call_session_id = ""
        self.unify_meet_call_session_id = ""
        self.provider_call_sid = ""
        self._event_broker = event_broker
        self._socket_server: CallEventSocketServer | None = None
        self.is_outbound: bool = False
        self.initial_notification: str = ""
        self.on_screenshot: Callable[[str], None] | None = None
        self.on_fast_brain_generating: Callable[[], dict[str, Any] | None] | None = None
        self.on_pipeline_quiescent: Callable[[bool], None] | None = None
        # Pulled at the top of every dispatch so a call always carries the
        # assistant's current voice/config rather than a snapshot taken at
        # construction time (which can go stale, e.g. self-host bootstrap).
        self._config_provider: Callable[[], CallConfig] | None = None
        self._call_channel: str | None = None
        self._disconnect_contact: dict | None = None
        self._boss_notification_task: asyncio.Task | None = None
        self._worker_watchdog_task: asyncio.Task | None = None
        self._dispatch_watchdog_task: asyncio.Task | None = None
        self._dispatch_lock = asyncio.Lock()
        # WhatsApp call joining state
        self._whatsapp_call_joining: bool = False
        # Browser-meet shared state (Google Meet / Teams Meet).  Only one
        # browser meeting can be active at a time; the channel is tracked via
        # ``self._call_channel`` so per-channel public properties remain stable
        # while the underlying state is consolidated.
        self._meet_session_id: str | None = None
        self._meet_joining: bool = False
        self._meet_presenting: bool = False
        self.google_meet_start_timestamp = None
        self.google_meet_exchange_id = UNASSIGNED
        self.teams_meet_start_timestamp = None
        self.teams_meet_exchange_id = UNASSIGNED

    def set_config(self, config: CallConfig):
        self.assistant_id = config.assistant_id
        self.user_id = config.user_id
        self.assistant_bio = config.assistant_bio
        self.assistant_number = config.assistant_number
        self.voice_provider = config.voice_provider
        self.voice_id = config.voice_id
        self.assistant_name = config.assistant_name
        self.is_coordinator = config.is_coordinator
        if config.job_name:
            self.job_name = config.job_name

    def set_config_provider(
        self,
        provider: "Callable[[], CallConfig]",
    ) -> None:
        """Register a callback that yields the current call config.

        Invoked just before each dispatch so voice/config reflect the latest
        runtime state instead of the value captured at construction time.
        """
        self._config_provider = provider

    def _refresh_config(self) -> None:
        if self._config_provider is not None:
            self.set_config(self._config_provider())

    def set_event_broker(self, event_broker: "InMemoryEventBroker") -> None:
        """Set the event broker for socket server to publish to."""
        self._event_broker = event_broker

    @property
    def worker_agent_name(self) -> str:
        return f"unity_{self.job_name}"

    @property
    def has_active_call(self) -> bool:
        return self._active_job or self._call_proc is not None

    @property
    def is_ready_for_outbound_call(self) -> bool:
        """Whether the voice worker can safely host an outbound call right now.

        True only when the persistent worker is alive with a freshly prewarmed
        idle process available (``WORKER_READY_PATH``), the previous job has
        fully disconnected from the IPC socket, and no dispatch is in flight.
        This gate is for assistant-initiated calls only. Inbound phone,
        WhatsApp, and Unify Meet sessions are accepted first and then dispatch
        through the best available path.
        """
        if not os.environ.get("LIVEKIT_URL"):
            return True
        if self._worker_proc is None or self._worker_proc.poll() is not None:
            return False
        # One voice session at a time: any live call/meeting (or a WhatsApp call
        # mid-setup) means a new call is not safe yet.
        if (
            self.has_active_call
            or self.has_active_meet()
            or self._whatsapp_call_joining
        ):
            return False
        if (
            self._socket_server is not None
            and self._socket_server.has_connected_clients
        ):
            return False
        from unify.conversation_manager.medium_scripts.worker import (
            WORKER_READY_PATH,
        )

        return os.path.exists(WORKER_READY_PATH)

    async def await_ready_for_outbound_call(
        self,
        timeout: float = OUTBOUND_CALL_READINESS_TIMEOUT_S,
        poll_interval: float = 0.25,
    ) -> bool:
        """Await until an outbound call can be safely started, or until timeout.

        Polls the real resource signals (no fixed sleep): a freshly prewarmed
        idle worker process, the IPC socket draining the previous job, and
        dispatch state. This is deliberately not used for inbound call
        acceptance. A stale dispatch that LiveKit never activated is cleared
        opportunistically so a genuinely-idle worker is not reported busy by a
        leftover flag. Returns True once ready, False if the timeout elapses.
        """
        deadline = time.monotonic() + max(0.0, float(timeout))
        while True:
            if self._active_job and self._call_proc is None:
                self._clear_stale_dispatch_state()
            if self.is_ready_for_outbound_call:
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(poll_interval)

    # ------------------------------------------------------------------
    # Persistent worker lifecycle
    # ------------------------------------------------------------------

    def start_persistent_worker(self) -> None:
        """Start the persistent LiveKit agent worker subprocess.

        Called once during pod initialisation.  The worker registers with
        LiveKit and maintains a pool of pre-warmed child processes.
        Skips silently when LiveKit is not configured (e.g. in tests).
        """
        if not os.environ.get("LIVEKIT_URL"):
            return
        if self._worker_proc is not None and self._worker_proc.poll() is None:
            return

        from unify.helpers import cleanup_dangling_call_processes

        cleanup_dangling_call_processes()

        from unify.conversation_manager.medium_scripts.worker import (
            clear_worker_signal_files,
        )

        clear_worker_signal_files()

        target = Path(__file__).parent.parent.resolve() / "medium_scripts" / "worker.py"
        self._worker_proc = run_script(str(target), "dev", self.worker_agent_name)
        LOGGER.info(
            f"{ICONS['ipc']} [LivekitCallManager] Persistent worker started "
            f"(pid={self._worker_proc.pid}, agent_name={self.worker_agent_name})",
        )
        if self._worker_watchdog_task is None or self._worker_watchdog_task.done():
            self._worker_watchdog_task = asyncio.create_task(self._worker_watchdog())

    async def refresh_persistent_worker_after_key_change(
        self,
        previous_key: str,
        current_key: str,
    ) -> None:
        """Respawn the LiveKit worker when UNIFY_KEY changes after idle-pool pre-warm.

        Idle containers start the persistent worker at boot with the image-baked
        UNIFY_KEY. Assignment updates os.environ in this process, but the worker
        subprocess tree keeps the stale key until it is restarted.
        """
        if not os.environ.get("LIVEKIT_URL"):
            return

        worker_running = (
            self._worker_proc is not None and self._worker_proc.poll() is None
        )
        key_changed = bool(current_key) and current_key != previous_key

        if key_changed and worker_running:
            if self.has_active_call:
                LOGGER.warning(
                    f"{ICONS['ipc']} [LivekitCallManager] Skipping persistent "
                    "worker restart while a voice call is active",
                )
            else:
                LOGGER.info(
                    f"{ICONS['ipc']} [LivekitCallManager] Restarting persistent "
                    "worker so voice subprocesses inherit updated UNIFY_KEY",
                )
                await self.cleanup_persistent_worker()

        self.start_persistent_worker()

    def _is_idle_pending_rewarm(self) -> bool:
        """Worker is alive and the manager is fully idle, yet no freshly prewarmed
        idle process is available.

        This is the recoverable "alive but never re-warmed" state: nothing is in
        progress (no live call/meet, no WhatsApp-call setup, no connected IPC
        client) so a new call *should* be placeable, but ``WORKER_READY_PATH`` is
        missing — the idle pool is wedged. Left alone this strands the
        call-starting tools forever, so the watchdog force-restarts the worker
        once it has persisted (see ``WORKER_REWARM_STALL_S``).
        """
        if not os.environ.get("LIVEKIT_URL"):
            return False
        if self._worker_proc is None or self._worker_proc.poll() is not None:
            return False
        if (
            self.has_active_call
            or self.has_active_meet()
            or self._whatsapp_call_joining
        ):
            return False
        if (
            self._socket_server is not None
            and self._socket_server.has_connected_clients
        ):
            return False
        if self._dispatch_lock.locked():
            return False
        from unify.conversation_manager.medium_scripts.worker import (
            WORKER_READY_PATH,
        )

        return not os.path.exists(WORKER_READY_PATH)

    async def _restart_worker(self) -> None:
        """Terminate the live worker and start a fresh one to re-warm the pool."""
        if self._dispatch_lock.locked():
            return
        proc = self._worker_proc
        self._worker_proc = None
        if proc is not None and proc.poll() is None:
            try:
                await asyncio.to_thread(terminate_process, proc, 5)
            except Exception as exc:
                LOGGER.warning(
                    f"{ICONS['ipc']} [LivekitCallManager] Failed to terminate "
                    f"wedged worker during re-warm: {exc}",
                )
        from unify.conversation_manager.medium_scripts.worker import (
            clear_worker_signal_files,
        )

        clear_worker_signal_files()
        self.start_persistent_worker()

    async def _worker_watchdog(self) -> None:
        """Restart the persistent worker if it exits unexpectedly, recover a
        wedged idle pool, and emit an INFO log when the warm pool is ready."""
        ready_logged = False
        unwarmed_since: float | None = None
        while True:
            await asyncio.sleep(2)
            proc = self._worker_proc
            if proc is None:
                continue
            if proc.poll() is not None:
                LOGGER.warning(
                    f"{ICONS['ipc']} [LivekitCallManager] Persistent worker exited "
                    f"(code={proc.returncode}), restarting…",
                )
                if self._worker_proc is proc:
                    self._worker_proc = None
                ready_logged = False
                unwarmed_since = None
                from unify.conversation_manager.medium_scripts.worker import (
                    clear_worker_signal_files,
                )

                clear_worker_signal_files()
                self.start_persistent_worker()
                continue
            if self._is_idle_pending_rewarm():
                now = time.monotonic()
                if unwarmed_since is None:
                    unwarmed_since = now
                elif now - unwarmed_since >= WORKER_REWARM_STALL_S:
                    LOGGER.warning(
                        f"{ICONS['ipc']} [LivekitCallManager] Worker alive but idle "
                        f"pool unwarmed for {WORKER_REWARM_STALL_S:.0f}s; "
                        "force-restarting to recover call readiness",
                    )
                    ready_logged = False
                    unwarmed_since = None
                    await self._restart_worker()
                continue
            unwarmed_since = None
            if not ready_logged:
                from unify.conversation_manager.medium_scripts.worker import (
                    WORKER_READY_PATH,
                )

                if os.path.exists(WORKER_READY_PATH):
                    LOGGER.info(
                        "🎙️ [LivekitCallManager] Voice agent ready",
                    )
                    ready_logged = True

    def _clear_stale_dispatch_state(self) -> bool:
        """Drop a dispatch flag left behind when LiveKit never ran the job."""
        if not self._active_job or self._call_proc is not None:
            return False
        if self._socket_server and self._socket_server.has_connected_clients:
            return False
        self._active_job = False
        return True

    def _cancel_dispatch_watchdog(self) -> None:
        task = self._dispatch_watchdog_task
        if task is not None and not task.done():
            task.cancel()
        self._dispatch_watchdog_task = None

    def _schedule_dispatch_watchdog(self) -> None:
        self._cancel_dispatch_watchdog()
        self._dispatch_watchdog_task = asyncio.create_task(
            self._watch_dispatch_activation(),
        )

    async def _watch_dispatch_activation(self) -> None:
        """Clear orphaned dispatch state when no voice agent joins the room."""
        try:
            await asyncio.sleep(DISPATCH_ACTIVATION_TIMEOUT_S)
        except asyncio.CancelledError:
            return

        if not self._active_job or self._call_proc is not None:
            return
        if self._socket_server and self._socket_server.has_connected_clients:
            return

        LOGGER.warning(
            f"{ICONS['ipc']} [LivekitCallManager] Dispatch never activated; "
            "clearing stale active-job state",
        )
        self._active_job = False

    async def _wait_for_worker_registered(
        self,
        worker_proc: subprocess.Popen,
        timeout: float = WORKER_DISPATCH_REGISTERED_TIMEOUT_S,
    ) -> bool:
        """Wait until this worker process registers with LiveKit."""
        if worker_proc.poll() is not None:
            return False

        from unify.conversation_manager.medium_scripts.worker import (
            WORKER_REGISTERED_PATH,
        )

        deadline = time.monotonic() + max(0.0, float(timeout))
        while time.monotonic() < deadline:
            if self._worker_proc is not worker_proc:
                return False
            if worker_proc.poll() is not None:
                return False
            if os.path.exists(WORKER_REGISTERED_PATH):
                return True
            await asyncio.sleep(0.5)

        LOGGER.warning(
            f"{ICONS['ipc']} [LivekitCallManager] Worker registration timeout "
            f"after {timeout:.0f}s; using subprocess fallback",
        )
        return False

    async def _dispatch_job(
        self,
        room_name: str,
        channel: str,
        contact: dict,
        boss: dict,
        outbound: bool,
        *,
        extra_metadata: dict | None = None,
        registration_timeout: float = WORKER_DISPATCH_REGISTERED_TIMEOUT_S,
    ) -> bool:
        """Dispatch a LiveKit job to the persistent worker."""
        self._refresh_config()
        async with self._dispatch_lock:
            worker_proc = self._worker_proc
            if worker_proc is None or worker_proc.poll() is not None:
                return False
            if not await self._wait_for_worker_registered(
                worker_proc,
                timeout=registration_timeout,
            ):
                return False
            if self._worker_proc is not worker_proc or worker_proc.poll() is not None:
                return False

            socket_path = await self._ensure_socket_server()

            meta_dict = {
                "voice_provider": self.voice_provider or "cartesia",
                "voice_id": self.voice_id or "",
                "outbound": outbound,
                "channel": channel,
                "contact": contact,
                "boss": boss,
                "assistant_bio": self.assistant_bio,
                "assistant_id": self.assistant_id,
                "user_id": self.user_id,
                "assistant_name": self.assistant_name,
                "is_coordinator": self.is_coordinator,
                "ipc_socket_path": socket_path or "",
            }
            if extra_metadata:
                meta_dict.update(extra_metadata)
            metadata = json.dumps(meta_dict)

            lk = LiveKitAPI(
                url=os.environ.get("LIVEKIT_URL", ""),
                api_key=os.environ.get("LIVEKIT_API_KEY", ""),
                api_secret=os.environ.get("LIVEKIT_API_SECRET", ""),
            )
            try:
                dispatch = await lk.agent_dispatch.create_dispatch(
                    CreateAgentDispatchRequest(
                        agent_name=self.worker_agent_name,
                        room=room_name,
                        metadata=metadata,
                    ),
                )
                self._active_job = True
                self._schedule_dispatch_watchdog()
                LOGGER.info(
                    f"{ICONS['ipc']} [LivekitCallManager] Dispatched job "
                    f"(dispatch_id={dispatch.id}, room={room_name}, "
                    f"call_session_id={meta_dict.get('call_session_id', '')})",
                )
                return True
            finally:
                await lk.aclose()

    async def _ensure_socket_server(self) -> str | None:
        """Start the socket server if not running, return socket path."""
        if self._event_broker is None:
            LOGGER.error(
                f"{ICONS['ipc']} [LivekitCallManager] Warning: No event broker set, socket IPC disabled",
            )
            return None

        if self._socket_server is None:

            async def _on_ipc_event(channel: str, event_json: str) -> None:
                if channel == "app:comms:screenshot" and self.on_screenshot is not None:
                    self.on_screenshot(event_json)
                elif (
                    channel == "app:comms:fast_brain_generating"
                    and self.on_fast_brain_generating is not None
                ):
                    response = self.on_fast_brain_generating()
                    if response is not None and self._socket_server is not None:
                        await self._socket_server.queue_for_clients(
                            "app:call:idle_smalltalk_state",
                            json.dumps(response),
                        )
                elif (
                    channel == "app:comms:pipeline_quiescent"
                    and self.on_pipeline_quiescent is not None
                ):
                    import json as _json

                    payload = _json.loads(event_json)
                    self.on_pipeline_quiescent(payload["quiescent"])
                else:
                    await self._event_broker.publish(channel, event_json)

            self._socket_server = CallEventSocketServer(
                self._event_broker,
                on_event=_on_ipc_event,
            )
            self._socket_server.on_client_disconnected = (
                self._on_ipc_client_disconnected
            )

        if self._socket_server.socket_path is None:
            socket_path = await self._socket_server.start()
            return socket_path

        return self._socket_server.socket_path

    async def start_call(
        self,
        contact: dict,
        boss: dict,
        outbound: bool = False,
        channel: str = "phone_call",
        room_name: str | None = None,
        opening_config: dict | None = None,
    ):
        if self.has_active_call:
            if self._clear_stale_dispatch_state():
                LOGGER.warning(
                    f"{ICONS['ipc']} [LivekitCallManager] Cleared stale dispatch "
                    "state before start_call",
                )
            else:
                LOGGER.warning(
                    f"{ICONS['ipc']} [LivekitCallManager] start_call ignored: "
                    "call already active",
                )
                return

        self._whatsapp_call_joining = False
        self.is_outbound = outbound
        self._call_channel = channel
        self._disconnect_contact = contact

        await self._ensure_socket_server()
        if self._socket_server:
            await self._socket_server.set_forward_channels(list(_BASE_FORWARD_CHANNELS))

        if contact.get("is_system", False):
            self._start_boss_notification_rendering()

        medium = "whatsapp_call" if channel == "whatsapp_call" else "phone"
        room_name = room_name or make_room_name(self.assistant_id, medium)
        self.room_name = room_name

        # An agent-initiated (outbound) call must never open "blind". The mission
        # context becomes a briefed opener the agent speaks the moment the callee
        # answers — and is injected as a durable system briefing for the rest of
        # the call — rather than a reactive fast-brain notification. This is the
        # single outbound choke point, so every outbound path is covered.
        if outbound and opening_config is None:
            briefing = (self.initial_notification or "").strip()
            opening_config = {
                "mode": "briefed",
                "system_context": briefing or _OUTBOUND_OPENER_FALLBACK_CONTEXT,
                "source": "outbound_call_opening",
            }
            # Delivered as the opener (and the in-call system briefing), so don't
            # also queue it as a separate notification that would race/double it.
            self.initial_notification = ""

        extra_metadata = {"opening_config": opening_config} if opening_config else None
        extra_env = (
            {"opening_config": json.dumps(opening_config)} if opening_config else None
        )

        dispatched = False
        if self._worker_proc is not None and self._worker_proc.poll() is None:
            dispatched = await self._dispatch_job(
                room_name,
                channel,
                contact,
                boss,
                outbound,
                extra_metadata=extra_metadata,
            )
        if not dispatched:
            await self._start_call_subprocess(
                room_name,
                channel,
                contact,
                boss,
                outbound,
                extra_env=extra_env,
            )

        if self.initial_notification:
            notification_event = FastBrainNotification(
                contact=contact,
                message=self.initial_notification,
                source="initial_call",
            )
            await self._socket_server.queue_for_clients(
                "app:call:notification",
                notification_event.to_json(),
            )
            await self._event_broker.publish(
                "app:comms:assistant_notification",
                notification_event.to_json(),
            )
            LOGGER.debug(
                f"{ICONS['ipc']} {trace_kv('CALL_MANAGER_INITIAL_NOTIFICATION', content_preview=self.initial_notification[:80])}",
            )
            self.initial_notification = ""

    async def start_unify_meet(
        self,
        contact: dict | None,
        boss: dict | None,
        room_name: str | None,
        *,
        opening_config: dict | None = None,
        call_session_id: str | None = None,
    ):
        if self.has_active_call:
            if self._clear_stale_dispatch_state():
                LOGGER.warning(
                    f"{ICONS['ipc']} [LivekitCallManager] Cleared stale dispatch "
                    "state before start_unify_meet",
                )
            else:
                LOGGER.warning(
                    f"{ICONS['ipc']} [LivekitCallManager] start_unify_meet ignored: "
                    "call already active",
                )
                return

        self.is_outbound = False
        self._call_channel = "unify_meet"
        self._disconnect_contact = contact

        await self._ensure_socket_server()
        if self._socket_server:
            await self._socket_server.set_forward_channels(list(_BASE_FORWARD_CHANNELS))

        if contact and contact.get("is_system", False):
            self._start_boss_notification_rendering()

        room_name = room_name or make_room_name(self.assistant_id, "meet")
        self.room_name = room_name
        self.unify_meet_call_session_id = call_session_id or ""
        extra_metadata = {}
        if opening_config:
            extra_metadata["opening_config"] = opening_config
        if call_session_id:
            extra_metadata["call_session_id"] = call_session_id
        extra_env = {
            key: value
            for key, value in {
                "opening_config": (
                    json.dumps(opening_config) if opening_config else None
                ),
                "CALL_SESSION_ID": call_session_id,
            }.items()
            if value
        } or None

        dispatched = False
        if self._worker_proc is not None and self._worker_proc.poll() is None:
            dispatched = await self._dispatch_job(
                room_name,
                "unify_meet",
                contact,
                boss,
                False,
                extra_metadata=extra_metadata or None,
            )
        if not dispatched:
            await self._start_call_subprocess(
                room_name,
                "unify_meet",
                contact,
                boss,
                False,
                extra_env=extra_env,
            )

    # ------------------------------------------------------------------
    # Browser-meet lifecycle (Google Meet / Teams Meet)
    # ------------------------------------------------------------------

    # Per-channel mapping of agent-service URL prefix and short room suffix.
    _MEET_PATHS: dict[str, dict[str, str]] = {
        "google_meet": {"path": "googlemeet", "room": "gmeet"},
        "teams_meet": {"path": "teamsmeet", "room": "teams"},
    }

    def has_active_meet(self, channel: str | None = None) -> bool:
        """Whether a browser meeting is active.

        With ``channel`` omitted, returns True for any active meeting.  When a
        specific channel is passed, returns True only if the active meeting
        matches that channel.
        """
        active = self._meet_session_id is not None or self._meet_joining
        if not active:
            return False
        if channel is None:
            return True
        return self._call_channel == channel

    @property
    def has_active_google_meet(self) -> bool:
        return self.has_active_meet("google_meet")

    @property
    def has_active_teams_meet(self) -> bool:
        return self.has_active_meet("teams_meet")

    @property
    def has_meet_presenting(self) -> bool:
        return self._meet_presenting

    @property
    def has_gmeet_presenting(self) -> bool:
        return self._meet_presenting and self._call_channel == "google_meet"

    @property
    def has_teams_presenting(self) -> bool:
        return self._meet_presenting and self._call_channel == "teams_meet"

    async def _start_meet(
        self,
        channel: str,
        meet_url: str,
        contact: dict,
        boss: dict,
        display_name: str = "",
    ) -> bool:
        """Join a browser meeting (Google Meet or Teams) via agent-service.

        1. POST /{path}/join on agent-service to launch browser + automation.
        2. Start audio bridge (PulseAudio <-> LiveKit).
        3. Dispatch a fast brain job into the same LiveKit room.
        4. Kick off a background monitor that polls /{path}/state and
           publishes the channel-specific *Ended event when the meeting
           terminates.
        """
        if self.has_active_call or self.has_active_meet():
            LOGGER.warning(
                f"{ICONS['ipc']} [LivekitCallManager] _start_meet ignored: "
                "session already active",
            )
            return False

        path_info = self._MEET_PATHS[channel]
        meet_path = path_info["path"]
        room_suffix = path_info["room"]

        self._meet_joining = True
        self._call_channel = channel
        self._disconnect_contact = contact

        display_name = display_name or self.assistant_name or "Unity Assistant"

        from unify.session_details import SESSION_DETAILS

        base_url = "http://localhost:3000"
        auth_key = SESSION_DETAILS.unify_key

        room_name = make_room_name(self.assistant_id, room_suffix)
        self.room_name = room_name

        # Pre-create the LiveKit room with long empty_timeout and departure_timeout.
        # Browser-meet audio flows through sounddevice/PulseAudio — no "real"
        # LiveKit participant ever joins, so:
        #   - empty_timeout (default 300s) would auto-delete the room after 5
        #     minutes of it being empty.
        #   - departure_timeout (default 20s) would auto-delete the room 20s
        #     after the agent participant disconnects, making recovery into the
        #     same room impossible if the child process is respawned.
        # Both are raised to 3h so the room survives for the full session.
        from livekit.api import CreateRoomRequest

        lk = LiveKitAPI(
            url=os.environ.get("LIVEKIT_URL", ""),
            api_key=os.environ.get("LIVEKIT_API_KEY", ""),
            api_secret=os.environ.get("LIVEKIT_API_SECRET", ""),
        )
        try:
            await lk.room.create_room(
                CreateRoomRequest(
                    name=room_name,
                    empty_timeout=10800,
                    departure_timeout=3600,
                ),
            )
        finally:
            await lk.aclose()

        # Dispatch fast brain first so it initializes (models, history, greeting)
        # while the browser navigates the slow LLM-guided join flow.
        await self._ensure_socket_server()
        if self._socket_server:
            await self._socket_server.set_forward_channels(list(_BASE_FORWARD_CHANNELS))

        if contact.get("is_system", False):
            self._start_boss_notification_rendering()

        meet_extra = {
            "meet_session_id": "",
            "meet_url": meet_url,
            "meet_display_name": display_name,
            "agent_service_url": "http://localhost:3000",
        }

        dispatched = False
        if self._worker_proc is not None and self._worker_proc.poll() is None:
            dispatched = await self._dispatch_job(
                room_name,
                channel,
                contact,
                boss,
                False,
                extra_metadata=meet_extra,
            )
        if not dispatched:
            await self._start_call_subprocess(
                room_name,
                channel,
                contact,
                boss,
                False,
                extra_env=meet_extra,
            )

        # Browser join runs after dispatch — fast brain initializes in parallel.
        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                f"{base_url}/{meet_path}/join",
                json={"meetUrl": meet_url, "displayName": display_name},
                headers={"authorization": f"Bearer {auth_key}"},
                timeout=aiohttp.ClientTimeout(total=300),
            )
            body = await resp.json()

        if resp.status != 200:
            LOGGER.error(
                f"{ICONS['ipc']} [LivekitCallManager] {channel} join failed: {body}",
            )
            self._meet_joining = False
            await self._cleanup_meet(channel)
            return False

        self._meet_session_id = body.get("sessionId")
        self._meet_joining = False
        LOGGER.info(
            f"{ICONS['ipc']} [LivekitCallManager] {channel} joined "
            f"(session={self._meet_session_id})",
        )

        if self._socket_server and self._meet_session_id:
            await self._socket_server.queue_for_clients(
                "app:call:status",
                json.dumps(
                    {"type": "meet_session_id", "session_id": self._meet_session_id},
                ),
            )

        return True

    async def _cleanup_meet(self, channel: str) -> None:
        """Leave the browser meeting and tear down the audio bridge."""
        path_info = self._MEET_PATHS[channel]
        meet_path = path_info["path"]

        session_id = self._meet_session_id
        room_name = self.room_name
        self._meet_session_id = None
        self._meet_joining = False
        self._meet_presenting = False
        if channel == "google_meet":
            self.google_meet_start_timestamp = None
            self.google_meet_exchange_id = UNASSIGNED
        elif channel == "teams_meet":
            self.teams_meet_start_timestamp = None
            self.teams_meet_exchange_id = UNASSIGNED

        if session_id:
            from unify.session_details import SESSION_DETAILS

            base_url = "http://localhost:3000"
            auth_key = SESSION_DETAILS.unify_key
            try:
                async with aiohttp.ClientSession() as session:
                    await session.post(
                        f"{base_url}/{meet_path}/leave",
                        json={"sessionId": session_id},
                        headers={"authorization": f"Bearer {auth_key}"},
                        timeout=aiohttp.ClientTimeout(total=30),
                    )
            except Exception as exc:
                LOGGER.warning(
                    f"{ICONS['ipc']} [LivekitCallManager] "
                    f"Error leaving {channel}: {exc}",
                )

        if room_name:
            from unify.conversation_manager.medium_scripts.common import (
                delete_livekit_room,
            )

            await delete_livekit_room(room_name)

        await self.cleanup_call_proc()

    async def _start_meet_screenshare(self, channel: str) -> bool:
        """Start presenting the assistant desktop in the active browser meeting."""
        session_id = self._meet_session_id
        if not session_id or self._call_channel != channel:
            return False

        from unify.session_details import SESSION_DETAILS

        desktop_url = SESSION_DETAILS.assistant.desktop_url
        if not desktop_url:
            return False

        from urllib.parse import urlparse

        parsed = urlparse(desktop_url)
        liveview_url = (
            f"{parsed.scheme}://{parsed.netloc}/desktop/custom.html"
            f"?password={SESSION_DETAILS.unify_key}"
        )

        meet_path = self._MEET_PATHS[channel]["path"]
        base_url = "http://localhost:3000"
        auth_key = SESSION_DETAILS.unify_key
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(
                    f"{base_url}/{meet_path}/present",
                    json={"sessionId": session_id, "desktopUrl": liveview_url},
                    headers={"authorization": f"Bearer {auth_key}"},
                    timeout=aiohttp.ClientTimeout(total=120),
                )
                if resp.status == 200:
                    self._meet_presenting = True
                    return True
                body = await resp.json()
                LOGGER.warning(
                    f"{ICONS['ipc']} [LivekitCallManager] "
                    f"{channel} present failed: {body}",
                )
        except Exception as exc:
            LOGGER.warning(
                f"{ICONS['ipc']} [LivekitCallManager] "
                f"Error starting {channel} screenshare: {exc}",
            )
        return False

    async def _stop_meet_screenshare(self, channel: str) -> bool:
        """Stop presenting the assistant desktop in the active browser meeting."""
        session_id = self._meet_session_id
        if not session_id or self._call_channel != channel:
            return False

        from unify.session_details import SESSION_DETAILS

        meet_path = self._MEET_PATHS[channel]["path"]
        base_url = "http://localhost:3000"
        auth_key = SESSION_DETAILS.unify_key
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(
                    f"{base_url}/{meet_path}/stop-present",
                    json={"sessionId": session_id},
                    headers={"authorization": f"Bearer {auth_key}"},
                    timeout=aiohttp.ClientTimeout(total=60),
                )
                if resp.status == 200:
                    self._meet_presenting = False
                    return True
                body = await resp.json()
                LOGGER.warning(
                    f"{ICONS['ipc']} [LivekitCallManager] "
                    f"{channel} stop-present failed: {body}",
                )
        except Exception as exc:
            LOGGER.warning(
                f"{ICONS['ipc']} [LivekitCallManager] "
                f"Error stopping {channel} screenshare: {exc}",
            )
        return False

    # Channel-specific public wrappers (kept for call-site stability).

    async def start_google_meet(
        self,
        meet_url: str,
        contact: dict,
        boss: dict,
        display_name: str = "",
    ) -> bool:
        return await self._start_meet(
            "google_meet",
            meet_url,
            contact,
            boss,
            display_name,
        )

    async def cleanup_google_meet(self) -> None:
        await self._cleanup_meet("google_meet")

    async def start_gmeet_screenshare(self) -> bool:
        return await self._start_meet_screenshare("google_meet")

    async def stop_gmeet_screenshare(self) -> bool:
        return await self._stop_meet_screenshare("google_meet")

    async def start_teams_meet(
        self,
        meet_url: str,
        contact: dict,
        boss: dict,
        display_name: str = "",
    ) -> bool:
        return await self._start_meet(
            "teams_meet",
            meet_url,
            contact,
            boss,
            display_name,
        )

    async def cleanup_teams_meet(self) -> None:
        await self._cleanup_meet("teams_meet")

    async def start_teams_meet_screenshare(self) -> bool:
        return await self._start_meet_screenshare("teams_meet")

    async def stop_teams_meet_screenshare(self) -> bool:
        return await self._stop_meet_screenshare("teams_meet")

    async def _start_call_subprocess(
        self,
        room_name: str,
        channel: str,
        contact: dict,
        boss: dict,
        outbound: bool,
        *,
        extra_env: dict | None = None,
    ) -> None:
        """Legacy path: spawn a fresh subprocess per call."""
        self._refresh_config()
        socket_path = await self._ensure_socket_server()
        if extra_env:
            for k, v in extra_env.items():
                os.environ[k.upper()] = str(v)
        if socket_path:
            os.environ[CM_EVENT_SOCKET_ENV] = socket_path
            LOGGER.debug(
                f"{ICONS['ipc']} [LivekitCallManager] Socket server at {socket_path}",
            )
        target_path = (
            Path(__file__).parent.parent.resolve() / "medium_scripts" / "call.py"
        )
        args = [
            str(a)
            for a in [
                room_name,
                self.voice_provider,
                self.voice_id,
                outbound,
                channel,
                json.dumps(contact),
                json.dumps(boss),
                self.assistant_bio,
                self.assistant_id,
                self.user_id,
            ]
        ]
        LOGGER.debug(f"{DEFAULT_ICON} target_path: {target_path}, args: {args}")
        self._call_proc = run_script(str(target_path), "dev", *args)

    # -- IPC disconnect fallback (safety net for lost call-ended events) --
    async def _on_ipc_client_disconnected(self) -> None:
        """Called by the socket server when the last IPC client disconnects.

        If ``cleanup_call_proc`` hasn't already run (meaning the call-ended
        event was lost), wait a short grace period then publish a synthetic
        call-ended event so the normal event-handler path runs the cleanup.
        """
        if not self.has_active_call:
            return

        await asyncio.sleep(1)

        if not self.has_active_call:
            return

        contact = self._disconnect_contact or {}
        channel = self._call_channel or "phone_call"
        if channel == "whatsapp_call":
            event = WhatsAppCallEnded(contact=contact)
        elif channel == "google_meet":
            event = GoogleMeetEnded(contact=contact)
        elif channel == "teams_meet":
            event = TeamsMeetEnded(contact=contact)
        elif channel == "phone_call":
            event = PhoneCallEnded(contact=contact)
        else:
            event = UnifyMeetEnded(
                contact=contact,
                call_session_id=self.unify_meet_call_session_id or None,
            )
        LOGGER.debug(
            f"{ICONS['ipc']} [LivekitCallManager] IPC client disconnected without cleanup, "
            f"publishing fallback {event.__class__.__name__}",
        )
        if self._event_broker:
            await self._event_broker.publish(
                event.topic,
                event.to_json(),
            )

    async def cleanup_persistent_worker(self) -> None:
        """Stop the persistent worker process and its watchdog."""
        if self._worker_watchdog_task and not self._worker_watchdog_task.done():
            self._worker_watchdog_task.cancel()
            try:
                await self._worker_watchdog_task
            except asyncio.CancelledError:
                pass
        self._worker_watchdog_task = None

        proc = self._worker_proc
        self._worker_proc = None
        if proc is None:
            return
        if proc.poll() is not None:
            return
        LOGGER.debug(
            f"{ICONS['ipc']} [LivekitCallManager] Terminating persistent worker {proc.pid}...",
        )
        await asyncio.to_thread(terminate_process, proc, 5)
        LOGGER.debug(
            f"{ICONS['ipc']} [LivekitCallManager] Persistent worker terminated",
        )

    async def end_call(self, reason: str = "assistant_hangup") -> None:
        """Tear down an active phone / WhatsApp / Unify Meet voice session.

        Best-effort drops the carrier leg for telephony — ending the Twilio
        conference for inbound calls, or completing the tracked call SID for
        outbound calls — then signals the running voice agent to stop via the IPC
        ``app:call:status`` channel. The agent shuts down,
        deletes the LiveKit room (which also ends the user's Unify Meet window
        since the Console tears down on ``RoomEvent.Disconnected``), and
        publishes the channel-appropriate ``*Ended`` event that drives the
        normal cleanup pipeline.

        Browser meetings (Google Meet / Teams) are not handled here — they tear
        down via ``_cleanup_meet`` instead.
        """
        channel = self._call_channel

        if channel in ("phone_call", "whatsapp_call"):
            from unify.conversation_manager.domains import comms_utils

            # Inbound calls bridge the remote party through a Twilio conference
            # (``conference_name`` populated); ending the conference cleanly drops
            # everyone. Outbound calls have no conference — they are a direct
            # ``<Dial>`` off the SIP leg — so completing the tracked call SID
            # collapses the dial and hangs up the remote party deterministically.
            try:
                if self.conference_name:
                    await comms_utils.end_phone_conference(self.conference_name)
                elif self.provider_call_sid:
                    await comms_utils.hang_up_call(self.provider_call_sid)
            except Exception as exc:
                LOGGER.warning(
                    f"{ICONS['ipc']} [LivekitCallManager] carrier hangup "
                    f"failed: {exc}",
                )

        if self._event_broker is not None:
            await self._event_broker.publish(
                "app:call:status",
                json.dumps({"type": "stop", "reason": reason}),
            )

    async def cleanup_call_proc(self) -> None:
        """Stop any running voice agent job/subprocess and socket server."""
        proc = self._call_proc
        self._call_proc = None
        self._active_job = False
        self._cancel_dispatch_watchdog()
        self._whatsapp_call_joining = False

        self.is_outbound = False
        self.initial_notification = ""
        self._call_channel = None
        self._disconnect_contact = None
        self.unify_meet_call_session_id = ""

        if self._boss_notification_task and not self._boss_notification_task.done():
            self._boss_notification_task.cancel()
            try:
                await self._boss_notification_task
            except asyncio.CancelledError:
                pass
        self._boss_notification_task = None

        if self._socket_server:
            await self._socket_server.stop()
            self._socket_server = None

        if CM_EVENT_SOCKET_ENV in os.environ:
            del os.environ[CM_EVENT_SOCKET_ENV]

        if proc is None:
            return

        if proc.poll() is not None:
            LOGGER.debug(
                f"{ICONS['ipc']} [LivekitCallManager] Process already exited with code {proc.returncode}",
            )
            return

        LOGGER.debug(
            f"{ICONS['ipc']} [LivekitCallManager] Terminating voice agent process {proc.pid}...",
        )
        await asyncio.to_thread(terminate_process, proc, 5)
        LOGGER.debug(
            f"{ICONS['ipc']} [LivekitCallManager] Voice agent process terminated",
        )

    # ------------------------------------------------------------------
    # Symbolic event forwarding for system contact calls
    # ------------------------------------------------------------------

    def _start_boss_notification_rendering(self) -> None:
        """Start an async task that forwards actor events to the fast brain."""
        if self._boss_notification_task and not self._boss_notification_task.done():
            return
        self._boss_notification_task = asyncio.create_task(
            self._render_boss_notifications(),
        )

    async def _render_boss_notifications(self) -> None:
        """Subscribe to actor events and publish rendered notifications.

        Runs for system contact calls only. Converts raw actor lifecycle
        events into FastBrainNotification messages on
        ``app:call:notification`` so the fast brain receives them as
        immediate silent context — guaranteed delivery, zero LLM latency.
        The slow brain separately decides whether to speak via
        ``guide_voice_agent``.
        """
        from unify.conversation_manager.medium_scripts.common import (
            render_event_for_fast_brain,
        )

        try:
            async with self._event_broker.pubsub() as pubsub:
                await pubsub.psubscribe("app:actor:*")
                while True:
                    msg = await pubsub.get_message(
                        timeout=1.0,
                        ignore_subscribe_messages=True,
                    )
                    if msg is None:
                        continue
                    data = msg.get("data", "")
                    if not data:
                        continue
                    text = render_event_for_fast_brain(data)
                    if not text:
                        continue
                    notification = FastBrainNotification(
                        message=text,
                        source="system",
                        contact={},
                    )
                    await self._event_broker.publish(
                        "app:call:notification",
                        notification.to_json(),
                    )
        except asyncio.CancelledError:
            pass
