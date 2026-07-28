from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from livekit.api import CreateAgentDispatchRequest, LiveKitAPI

from unify.contact_manager.types.contact import UNASSIGNED
from unify.conversation_manager.events import *
from unify.conversation_manager.domains.ipc_socket import (
    CallEventSocketServer,
    CM_EVENT_SOCKET_ENV,
)
from unify.conversation_manager.speaker_id import VOICE_PROFILES_ENV
from unify.logger import LOGGER
from unify.common.hierarchical_logger import DEFAULT_ICON, ICONS
from unify.helpers import (
    run_script,
    terminate_process,
)

if TYPE_CHECKING:
    from unify.conversation_manager.in_memory_event_broker import InMemoryEventBroker

from unify.conversation_manager.medium_scripts.common import (
    _resolve_agent_service_url,
)


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

# How often to ask the meeting backend for its view of the session. Only the
# roster and a bot that never arrived depend on this cadence -- the fast brain
# detects a normal end itself, without waiting for a poll -- so it is set for a
# modest request rate against Recall rather than for latency. Started after
# dispatch and the join, so it cannot delay the assistant reaching the room.
MEET_STATE_POLL_INTERVAL_S = 10.0
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


def _opener_opening_config(opener: str, *, source: str, briefing: str = "") -> dict:
    config = {
        "mode": "opener",
        "opener_text": opener.strip(),
        "source": source,
    }
    if briefing.strip():
        config["briefing"] = briefing.strip()
    return config


def _opening_config_is_outbound(opening_config: dict | None) -> bool:
    if not opening_config:
        return False
    return opening_config.get("mode") == "opener"


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
        self.unify_meet_participants: list[dict] = []
        self.provider_call_sid = ""
        self._event_broker = event_broker
        self._socket_server: CallEventSocketServer | None = None
        self.is_outbound: bool = False
        self.pending_opener: str = ""
        # Optional unspoken briefing queued alongside the opener by the
        # call-start tools; travels in the opening config and is injected into
        # the voice agent's context (never spoken).
        self.pending_briefing: str = ""
        # Briefing of the currently dispatched call, if any. The slow brain's
        # turn guidance uses it to defer briefed play to the voice agent.
        self.active_call_briefing: str = ""
        # Pre-armed hang-up gate reason queued by the call-start tools for
        # calls expected to be short; consumed into dispatch metadata (and the
        # CM-side ``hang_up_gate_reason`` mirror) when the session starts.
        self.pending_hang_up_gate: str = ""
        self.on_screenshot: Callable[[str], None] | None = None
        self.on_fast_brain_generating: Callable[[], dict[str, Any] | None] | None = None
        self.on_pipeline_quiescent: Callable[[bool], None] | None = None
        # Returns {contact_id: voice embedding} for enrolled contacts, injected
        # into job dispatch metadata so the voice agent can pin diarized
        # speakers to known voices. Set by the ConversationManager.
        self.voice_profile_provider: (
            Callable[[list[int]], dict[int, list[float]]] | None
        ) = None
        # Pulled at the top of every dispatch so a call always carries the
        # assistant's current voice/config rather than a snapshot taken at
        # construction time (which can go stale, e.g. self-host bootstrap).
        self._config_provider: Callable[[], CallConfig] | None = None
        self._call_channel: str | None = None
        self._disconnect_contact: dict | None = None
        # Hang-up gate: while armed (non-None), the slow brain has sanctioned
        # ending the active voice session and the fast brain may close the call
        # at a natural point. Holds the slow brain's stated reason. Armed and
        # disarmed via ``set_hang_up_gate``; cleared on session end.
        self.hang_up_gate_reason: str | None = None
        self._boss_notification_task: asyncio.Task | None = None
        self._worker_watchdog_task: asyncio.Task | None = None
        self._dispatch_watchdog_task: asyncio.Task | None = None
        # Subprocess-shaped respawn params for the pending dispatch, so the
        # watchdog can self-heal a dispatch that never activates by launching a
        # self-contained fast brain into the already-live room.
        self._pending_dispatch_fallback: dict | None = None
        self._dispatch_lock = asyncio.Lock()
        # WhatsApp call joining state
        self._whatsapp_call_joining: bool = False
        # Browser-meet shared state (Google Meet / Teams Meet).  Only one
        # browser meeting can be active at a time; the channel is tracked via
        # ``self._call_channel`` so per-channel public properties remain stable
        # while the underlying state is consolidated.
        self._meet_session_id: str | None = None
        # Lazily built by the ``meet_provider`` property and cached for the
        # session; see that property for why it is not rebuilt per join.
        self._meet_provider = None
        # Polls the meeting backend for lifecycle + roster; see
        # ``_watch_meet_state``.
        self._meet_state_task: asyncio.Task | None = None
        self._meet_joining: bool = False
        self._meet_presenting: bool = False
        # True while the browser is admitted-pending: it has knocked on the
        # meeting (clicked "Ask to join") and is sitting in the waiting room
        # for the host to let it in. This is a *successful* join in progress,
        # not a failure — the event handler uses it to tell the user we're in
        # the lobby rather than that we're already in the call.
        self._meet_lobby_waiting: bool = False
        # Reason string from the most recent failed browser-meet join (agent
        # service ``reason``/``message``), consumed by the event handler to
        # tell the user *why* the join failed rather than a generic retry line.
        self.meet_join_failure_reason: str | None = None
        self.google_meet_start_timestamp = None
        self.google_meet_exchange_id = UNASSIGNED
        self.teams_meet_start_timestamp = None
        self.teams_meet_exchange_id = UNASSIGNED
        # Parent-side mirror of the voice agent's engaged-speaker set: the
        # permanently engaged call participants (contact_id -> display name),
        # labels the slow brain has engaged, and every anonymous speaker label
        # heard on the call so far (for tool-docstring status rendering).
        self.engaged_contacts: dict[int, str] = {}
        self.engaged_labels: set[str] = set()
        self.known_speaker_labels: set[str] = set()

    def reset_speaker_engagement(
        self,
        contact: dict | None,
        boss: dict | None,
    ) -> None:
        """Initialize the per-call engagement mirror at call start."""
        self.engaged_contacts = {}
        for cand in (contact, boss):
            if not cand or cand.get("contact_id") is None:
                continue
            name = (
                f"{cand.get('first_name', '')} {cand.get('surname', '')}".strip()
                or f"contact {cand['contact_id']}"
            )
            self.engaged_contacts[int(cand["contact_id"])] = name
        self.engaged_labels = set()
        self.known_speaker_labels = set()

    def note_speaker_label(self, label: str | None) -> None:
        """Record an anonymous speaker label heard on the active call."""
        if label:
            self.known_speaker_labels.add(label.strip())

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

        Test mocks may set ``_outbound_ready_override`` when ``start_call`` is
        stubbed so flow tests still expose ``make_call`` without LiveKit warm-up.
        """
        override = getattr(self, "_outbound_ready_override", None)
        if override is not None:
            return bool(override)
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
        """Ensure the persistent worker is running after UNIFY_KEY changes.

        Idle containers start the worker at boot with an image-baked UNIFY_KEY.
        Assignment updates this process's environment, but each LiveKit job carries
        the assigned key in dispatch metadata so entrypoints authenticate as the
        tenant without restarting the pre-warmed worker pool.
        """
        if not os.environ.get("LIVEKIT_URL"):
            return
        if current_key and current_key != previous_key:
            LOGGER.debug(
                f"{ICONS['ipc']} [LivekitCallManager] UNIFY_KEY changed; "
                "keeping persistent worker warm (key passed via dispatch metadata)",
            )
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
        self._pending_dispatch_fallback = None

    def _schedule_dispatch_watchdog(
        self,
        activation_timeout: float = DISPATCH_ACTIVATION_TIMEOUT_S,
    ) -> None:
        self._cancel_dispatch_watchdog()
        self._dispatch_watchdog_task = asyncio.create_task(
            self._watch_dispatch_activation(activation_timeout),
        )

    async def _watch_dispatch_activation(
        self,
        activation_timeout: float = DISPATCH_ACTIVATION_TIMEOUT_S,
    ) -> None:
        """Self-heal a dispatch that LiveKit accepts but never assigns.

        The persistent worker can accept a dispatch it cannot immediately give
        to a process (its single prewarmed slot is still re-warming), so the
        room goes live with no fast brain ever joining. When that window
        elapses with no IPC client connected, spawn a self-contained subprocess
        into the same room rather than merely clearing the flag — otherwise the
        call is silently deaf and mute.
        """
        try:
            await asyncio.sleep(activation_timeout)
        except asyncio.CancelledError:
            return

        if not self._active_job or self._call_proc is not None:
            return
        if self._socket_server and self._socket_server.has_connected_clients:
            return

        fallback = self._pending_dispatch_fallback
        self._pending_dispatch_fallback = None
        self._active_job = False
        if fallback is None:
            LOGGER.warning(
                f"{ICONS['ipc']} [LivekitCallManager] Dispatch never activated; "
                "cleared stale active-job state (no respawn params)",
            )
            return

        LOGGER.warning(
            f"{ICONS['ipc']} [LivekitCallManager] Dispatch never activated; "
            "spawning subprocess fallback into live room "
            f"{fallback.get('room_name')}",
        )
        await self._start_call_subprocess(
            fallback["room_name"],
            fallback["channel"],
            fallback["contact"],
            fallback["boss"],
            fallback["outbound"],
            extra_env=fallback.get("extra_env"),
        )

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

    def _get_voice_profiles(
        self,
        contact: dict | None,
        boss: dict | None,
        extra_contact_ids: list[int] | None = None,
    ) -> dict[str, list[float]]:
        """Fetch enrolled voice embeddings for the call participants.

        ``extra_contact_ids`` carries the full multi-party roster so every
        enrolled human on an org call can be voice-identified, not just the
        primary contact and boss.

        Best-effort: a backend hiccup here must never block call dispatch, so
        failures degrade to "no profiles" (speaker attribution disabled).
        """
        if self.voice_profile_provider is None:
            return {}
        contact_ids = {
            int(c["contact_id"])
            for c in (contact, boss)
            if c and c.get("contact_id") is not None
        }
        contact_ids.update(int(cid) for cid in (extra_contact_ids or []))
        if not contact_ids:
            return {}
        try:
            profiles = self.voice_profile_provider(sorted(contact_ids))
        except Exception as e:  # noqa: BLE001
            LOGGER.warning(
                f"{ICONS['ipc']} [LivekitCallManager] voice profile lookup "
                f"failed: {e}",
            )
            return {}
        return {str(cid): vec for cid, vec in (profiles or {}).items()}

    async def _dispatch_job(
        self,
        room_name: str,
        channel: str,
        contact: dict,
        boss: dict,
        outbound: bool,
        *,
        extra_metadata: dict | None = None,
        fallback_env: dict | None = None,
        registration_timeout: float = WORKER_DISPATCH_REGISTERED_TIMEOUT_S,
        activation_timeout: float = DISPATCH_ACTIVATION_TIMEOUT_S,
    ) -> bool:
        """Dispatch a LiveKit job to the persistent worker.

        ``fallback_env`` is the subprocess-shaped env the dispatch watchdog uses
        to respawn a self-contained fast brain into the room if the dispatched
        worker never activates.
        """
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

            from unify.session_details import SESSION_DETAILS

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
                "unify_key": SESSION_DETAILS.unify_key,
                "agent_service_url": _resolve_agent_service_url(),
            }
            roster_contact_ids = [
                int(p["contact_id"])
                for p in (extra_metadata or {}).get("participants", [])
                if isinstance(p, dict)
                and p.get("contact_id") is not None
                and p.get("kind") != "assistant"
            ]
            meta_dict["voice_profiles"] = self._get_voice_profiles(
                contact,
                boss,
                extra_contact_ids=roster_contact_ids,
            )
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
                self._schedule_dispatch_watchdog(activation_timeout)
                self._pending_dispatch_fallback = {
                    "room_name": room_name,
                    "channel": channel,
                    "contact": contact,
                    "boss": boss,
                    "outbound": outbound,
                    "extra_env": fallback_env,
                }
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
        self.reset_speaker_engagement(contact, boss)

        await self._ensure_socket_server()
        if self._socket_server:
            await self._socket_server.set_forward_channels(list(_BASE_FORWARD_CHANNELS))

        if contact.get("is_system", False):
            self._start_boss_notification_rendering()

        medium = "whatsapp_call" if channel == "whatsapp_call" else "phone"
        room_name = room_name or make_room_name(self.assistant_id, medium)
        self.room_name = room_name

        # A queued opener always becomes a spoken ``opener`` opening — even on
        # an inbound-shaped leg of an agent-initiated call (e.g. the WhatsApp
        # permission-callback call, where the contact's "Call now" tap dials
        # us back but the opener was decided when we tried to place the call).
        if opening_config is None and (self.pending_opener or "").strip():
            opening_config = _opener_opening_config(
                self.pending_opener,
                source="outbound_call_opening",
                briefing=self.pending_briefing,
            )
            self.pending_opener = ""
            self.pending_briefing = ""
        if outbound and opening_config is None:
            raise RuntimeError(
                "Outbound call refused: no verbatim opener was queued "
                "(call-start tools must set pending_opener before dialing)",
            )
        self.active_call_briefing = (opening_config or {}).get("briefing", "")

        # A queued pre-armed hang-up gate rides the dispatch metadata so the
        # voice agent starts already sanctioned to close (no IPC race on short
        # calls); the CM-side mirror drives tool registration, the standing
        # prompt note, and proactive-speech suppression from call start.
        gate_reason = (self.pending_hang_up_gate or "").strip()
        self.pending_hang_up_gate = ""
        if gate_reason:
            self.hang_up_gate_reason = gate_reason

        extra_metadata: dict = {}
        if opening_config:
            extra_metadata["opening_config"] = opening_config
        if gate_reason:
            extra_metadata["hang_up_gate_reason"] = gate_reason
        extra_env: dict = {}
        if opening_config:
            extra_env["opening_config"] = json.dumps(opening_config)
        if gate_reason:
            extra_env["hang_up_gate_reason"] = gate_reason

        dispatched = False
        if self._worker_proc is not None and self._worker_proc.poll() is None:
            dispatched = await self._dispatch_job(
                room_name,
                channel,
                contact,
                boss,
                outbound,
                extra_metadata=extra_metadata or None,
                fallback_env=extra_env or None,
            )
        if not dispatched:
            await self._start_call_subprocess(
                room_name,
                channel,
                contact,
                boss,
                outbound,
                extra_env=extra_env or None,
            )

    async def refresh_unify_meet_roster(self, participants: list[dict] | None) -> None:
        """Update in-memory org-call roster and push it to the voice agent."""
        roster = list(participants or [])
        self.unify_meet_participants = roster
        if self._event_broker is None:
            return
        # Profiles otherwise only ride the initial dispatch, so anyone joining
        # after the call started can never be voice-pinned. Off-thread: the
        # lookup hits the backend and this runs on the event loop.
        roster_contact_ids = [
            int(p["contact_id"])
            for p in roster
            if isinstance(p, dict)
            and p.get("contact_id") is not None
            and p.get("kind") != "assistant"
        ]
        profiles = await asyncio.to_thread(
            self._get_voice_profiles,
            None,
            None,
            roster_contact_ids,
        )
        await self._event_broker.publish(
            "app:call:status",
            json.dumps(
                {
                    "type": "unify_meet_roster",
                    "participants": roster,
                    "voice_profiles": profiles,
                },
            ),
        )

    async def start_unify_meet(
        self,
        contact: dict | None,
        boss: dict | None,
        room_name: str | None,
        *,
        opening_config: dict | None = None,
        call_session_id: str | None = None,
        participants: list[dict] | None = None,
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

        outbound = _opening_config_is_outbound(opening_config)
        if opening_config is None and (self.pending_opener or "").strip():
            opening_config = _opener_opening_config(
                self.pending_opener,
                source="outbound_unify_meet_opening",
                briefing=self.pending_briefing,
            )
            self.pending_opener = ""
            self.pending_briefing = ""
            outbound = True
        elif (
            opening_config is not None
            and opening_config.get("mode") == "opener"
            and not opening_config.get("briefing")
            and (self.pending_briefing or "").strip()
        ):
            # A Unify Meet ring answer round-trips the opener through the
            # Console, but the briefing stays queued CM-side — reattach it.
            opening_config = {
                **opening_config,
                "briefing": self.pending_briefing.strip(),
            }
            self.pending_briefing = ""
        self.active_call_briefing = (opening_config or {}).get("briefing", "")

        # The pre-armed gate never round-trips the Console either — consume the
        # CM-side stash regardless of which branch produced the opening config.
        gate_reason = (self.pending_hang_up_gate or "").strip()
        self.pending_hang_up_gate = ""
        if gate_reason:
            self.hang_up_gate_reason = gate_reason

        self.is_outbound = outbound
        self._call_channel = "unify_meet"
        self._disconnect_contact = contact
        self.reset_speaker_engagement(contact, boss)

        await self._ensure_socket_server()
        if self._socket_server:
            await self._socket_server.set_forward_channels(list(_BASE_FORWARD_CHANNELS))

        if contact and contact.get("is_system", False):
            self._start_boss_notification_rendering()

        room_name = room_name or make_room_name(self.assistant_id, "meet")
        self.room_name = room_name
        self.unify_meet_call_session_id = call_session_id or ""
        self.unify_meet_participants = list(participants or [])
        extra_metadata = {}
        if opening_config:
            extra_metadata["opening_config"] = opening_config
        if call_session_id:
            extra_metadata["call_session_id"] = call_session_id
        if gate_reason:
            extra_metadata["hang_up_gate_reason"] = gate_reason
        if self.unify_meet_participants:
            extra_metadata["participants"] = self.unify_meet_participants
        extra_env = {
            key: value
            for key, value in {
                "opening_config": (
                    json.dumps(opening_config) if opening_config else None
                ),
                "CALL_SESSION_ID": call_session_id,
                "hang_up_gate_reason": gate_reason or None,
                "UNIFY_MEET_PARTICIPANTS": (
                    json.dumps(self.unify_meet_participants)
                    if self.unify_meet_participants
                    else None
                ),
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
                outbound,
                extra_metadata=extra_metadata or None,
                fallback_env=extra_env,
            )
        if not dispatched:
            await self._start_call_subprocess(
                room_name,
                "unify_meet",
                contact,
                boss,
                outbound,
                extra_env=extra_env,
            )

    # ------------------------------------------------------------------
    # Browser-meet lifecycle (Google Meet / Teams Meet)
    # ------------------------------------------------------------------

    # Short room suffix per channel, used to name the LiveKit room.
    _MEET_ROOM_SUFFIX: dict[str, str] = {
        "google_meet": "gmeet",
        "teams_meet": "teams",
    }

    @property
    def meet_provider(self):
        """The backend that joins browser meetings, per ``MEET_PROVIDER``.

        Built once and cached: a provider carries a Recall client and derived
        configuration, and rebuilding it per call would re-read the environment
        mid-session. A pod asking for ``recall`` without credentials falls back
        to the browser rather than refusing to join at all -- during the
        transition there is still something to fall back to, and a loud log line
        is more useful than a meeting nobody attends.
        """
        if self._meet_provider is None:
            from unify.conversation_manager.domains.browser_meeting import (
                build_meet_provider,
            )

            self._meet_provider = build_meet_provider(self)
        return self._meet_provider

    @property
    def meet_session_id(self) -> str:
        """Backend session id for the active browser meeting.

        Doubles as the call-utterance key for browser meets, so a transcript
        row can be traced back to the exact bot that produced it.
        """
        return self._meet_session_id or ""

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
    def meet_lobby_waiting(self) -> bool:
        """Whether the browser meet is admitted-pending in the waiting room.

        True after a successful join that landed in the lobby (host has not
        yet let us in). Distinguishes "present, waiting to be admitted" from
        "already in the call" for user-facing messaging.
        """
        return self._meet_lobby_waiting

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
        """Join a browser meeting (Google Meet or Teams).

        1. Pre-create the LiveKit room the conversation lives in.
        2. Dispatch a fast brain job into it.
        3. Ask the configured backend (``MEET_PROVIDER``) to join the meeting.

        Only step 3 differs between backends. Teardown is not polled from here:
        the channel's *Ended event is published when the fast brain's IPC client
        disconnects (see ``_on_ipc_client_disconnected``), which is why no
        backend needs to report the meeting ending.
        """
        if self.has_active_call or self.has_active_meet():
            LOGGER.warning(
                f"{ICONS['ipc']} [LivekitCallManager] _start_meet ignored: "
                "session already active",
            )
            return False

        room_suffix = self._MEET_ROOM_SUFFIX[channel]

        self._meet_joining = True
        self.meet_join_failure_reason = None
        self._call_channel = channel
        self._disconnect_contact = contact
        self.reset_speaker_engagement(contact, boss)

        opener = (self.pending_opener or "").strip()
        meet_opening_config = None
        if opener:
            meet_opening_config = _opener_opening_config(
                opener,
                source="outbound_meet_opening",
            )
            self.pending_opener = ""
        meet_outbound = meet_opening_config is not None

        display_name = display_name or self.assistant_name or "Unity Assistant"

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

        # ``agent_service_url`` is deliberately absent: meets used to pin it to
        # the pod-local service so the browser shared this pod's PulseAudio
        # server. Nothing about a meeting is pod-local now, so the general
        # dispatch metadata's resolved URL applies like it does for every other
        # channel.
        meet_extra = {
            "meet_session_id": "",
            "meet_url": meet_url,
            "meet_display_name": display_name,
        }
        if meet_opening_config:
            meet_extra["opening_config"] = meet_opening_config

        self.is_outbound = meet_outbound

        # Gate on the backend being able to accept a join before dispatching a
        # worker. The fast brain is expensive to start and, once in the room,
        # sits talking to nobody if no browser ever arrives.
        preflight_failure = await self.meet_provider.preflight()
        if preflight_failure:
            LOGGER.error(
                f"{ICONS['ipc']} [LivekitCallManager] {channel} join aborted: "
                f"{preflight_failure}",
            )
            self.meet_join_failure_reason = preflight_failure
            self._meet_joining = False
            self._meet_lobby_waiting = False
            await self._cleanup_meet(channel)
            return False

        # Fast brain into the room on exactly the same terms as voice calls:
        # prefer the co-located prewarmed worker, fall back to a self-contained
        # subprocess only if it never activates.
        #
        # Meets used to cut the activation wait to five seconds, because a
        # browser join was slow enough that a subprocess's model load hid inside
        # it and the prewarmed slot was rarely ready that early. That trade is
        # gone: audio arrives over LiveKit like every other channel, so there is
        # no browser cold start to hide behind, and a five-second window is
        # short enough that the worker activates *after* the fallback has been
        # spawned -- putting two fast brains in one room, both replying.
        meet_metadata = dict(meet_extra)
        meet_env = dict(meet_extra)
        if meet_opening_config:
            # Metadata carries the opening config as a dict (LiveKit job metadata
            # is json-encoded whole); the subprocess env carries it as a JSON
            # string it decodes from os.environ.
            meet_metadata["opening_config"] = meet_opening_config
            meet_env["opening_config"] = json.dumps(meet_opening_config)

        dispatched = False
        if self._worker_proc is not None and self._worker_proc.poll() is None:
            dispatched = await self._dispatch_job(
                room_name,
                channel,
                contact,
                boss,
                meet_outbound,
                extra_metadata=meet_metadata,
                fallback_env=meet_env,
            )
        if not dispatched:
            await self._start_call_subprocess(
                room_name,
                channel,
                contact,
                boss,
                meet_outbound,
                extra_env=meet_env,
            )

        # The join runs after dispatch so the fast brain initializes in
        # parallel with it. It can be slow -- a browser cold start and an
        # LLM-guided click-through, or a hosted bot working through a lobby --
        # and the backend owns that ceiling. What matters here is that a
        # failure comes back as a clean False rather than escaping into the
        # event loop: an unhandled exception in the meet-join handler leaves
        # ``_meet_joining`` stuck True with no teardown, surfacing as
        # "Unhandled error processing GoogleMeetReceived".
        result = await self.meet_provider.join(
            channel=channel,
            meeting_url=meet_url,
            display_name=display_name,
            room_name=room_name,
        )
        if not result.ok:
            LOGGER.error(
                f"{ICONS['ipc']} [LivekitCallManager] {channel} join failed: "
                f"{result.failure_reason}",
            )
            self.meet_join_failure_reason = result.failure_reason
            self._meet_joining = False
            self._meet_lobby_waiting = False
            await self._cleanup_meet(channel)
            return False

        # Both shapes of success are recorded: in the call, or knocked and
        # waiting for a host to admit us. Only the wording differs downstream,
        # so the event handler can say "in the lobby" rather than "joined".
        self._meet_session_id = result.session_id
        self._meet_lobby_waiting = result.lobby
        self._meet_joining = False
        LOGGER.info(
            f"{ICONS['ipc']} [LivekitCallManager] {channel} joined "
            f"(session={self._meet_session_id}, lobby={result.lobby})",
        )

        if self._socket_server and self._meet_session_id:
            await self._socket_server.queue_for_clients(
                "app:call:status",
                json.dumps(
                    {"type": "meet_session_id", "session_id": self._meet_session_id},
                ),
            )

        if meet_outbound and self._event_broker:
            await self._event_broker.publish(
                "app:call:status",
                json.dumps({"type": "call_answered"}),
            )

        # Started last, after dispatch and the join, so it can never delay the
        # fast brain reaching the room or speaking.
        self._meet_state_task = asyncio.create_task(self._watch_meet_state(channel))

        return True

    async def send_meet_chat(self, text: str, to: str | None = None) -> bool:
        """Post into the active browser meeting's chat.

        Unlike speech, chat survives the call: a link or a spelling stays
        readable after the moment it was said, which is the point of offering it
        alongside the voice channel.
        """
        session_id = self._meet_session_id
        channel = self._call_channel or ""
        if not session_id or channel not in self._MEET_ROOM_SUFFIX:
            return False
        if not (text or "").strip():
            return False
        return await self.meet_provider.send_chat(
            channel=channel,
            session_id=session_id,
            text=text,
            to=to,
        )

    async def _watch_meet_state(self, channel: str) -> None:
        """Track the meeting backend's own view of the session.

        The fast brain notices the meeting ending on its own (the bot's bridge
        page drops out of the LiveKit room), but only once the bot has *been*
        there. A bot that never arrives -- denied entry, a bad link, or nobody
        ever joining -- would otherwise leave the session marked active forever,
        refusing every later call with "session already active".

        Polling covers that, and pays for itself twice over: the same response
        carries the participant roster, which is the only source of
        platform-attributed speaker names now that nothing scrapes a DOM.
        """
        while True:
            await asyncio.sleep(MEET_STATE_POLL_INTERVAL_S)
            session_id = self._meet_session_id
            if not session_id or self._call_channel != channel:
                return

            state = await self.meet_provider.state(
                channel=channel,
                session_id=session_id,
            )
            if state is None:
                # A failed poll is not evidence the meeting ended.
                continue

            self._meet_lobby_waiting = state.lobby
            await self._push_meet_roster(state.participants)

            if state.ended:
                LOGGER.info(
                    f"{ICONS['ipc']} [LivekitCallManager] {channel} ended per "
                    f"backend (status={state.status}, "
                    f"reason={state.failure_reason})",
                )
                if state.failure_reason:
                    self.meet_join_failure_reason = state.failure_reason
                await self._publish_meet_ended(channel)
                return

    async def _push_meet_roster(self, participants) -> None:
        """Hand the current roster to the fast brain.

        It runs in another process and has no Recall credentials, so the roster
        can only reach it from here. Same transport as the org-call roster.
        """
        if not self._socket_server:
            return
        await self._socket_server.queue_for_clients(
            "app:call:status",
            json.dumps(
                {
                    "type": "meet_roster",
                    "participants": [
                        {
                            "id": p.id,
                            "name": p.name,
                            "email": p.email,
                            "is_host": p.is_host,
                        }
                        for p in participants
                    ],
                },
            ),
        )

    async def _publish_meet_ended(self, channel: str) -> None:
        """Publish the channel's *Ended event so the normal teardown path runs.

        Idempotent by way of ``has_active_meet``: the fast brain usually gets
        there first via its own shutdown, and a second event would put the
        session through cleanup twice.
        """
        if not self.has_active_meet(channel):
            return
        if self._event_broker is None:
            return
        contact = self._disconnect_contact or {}
        event = (
            GoogleMeetEnded(contact=contact)
            if channel == "google_meet"
            else TeamsMeetEnded(contact=contact)
        )
        await self._event_broker.publish(event.topic, event.to_json())

    async def _cleanup_meet(self, channel: str) -> None:
        """Leave the browser meeting and tear down the LiveKit room."""
        if self._meet_state_task is not None:
            self._meet_state_task.cancel()
            self._meet_state_task = None

        session_id = self._meet_session_id
        room_name = self.room_name
        self._meet_session_id = None
        self._meet_joining = False
        self._meet_lobby_waiting = False
        self._meet_presenting = False
        if channel == "google_meet":
            self.google_meet_start_timestamp = None
            self.google_meet_exchange_id = UNASSIGNED
        elif channel == "teams_meet":
            self.teams_meet_start_timestamp = None
            self.teams_meet_exchange_id = UNASSIGNED

        if session_id:
            await self.meet_provider.leave(channel=channel, session_id=session_id)

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

        if await self.meet_provider.present(
            channel=channel,
            session_id=session_id,
            view_url=liveview_url,
        ):
            self._meet_presenting = True
            return True
        return False

    async def _stop_meet_screenshare(self, channel: str) -> bool:
        """Stop presenting the assistant desktop in the active browser meeting."""
        session_id = self._meet_session_id
        if not session_id or self._call_channel != channel:
            return False

        if await self.meet_provider.stop_present(
            channel=channel,
            session_id=session_id,
        ):
            self._meet_presenting = False
            return True
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
        # Voice profiles ride the dispatch metadata on the worker path; this
        # path has no metadata, so without an env equivalent speaker pinning is
        # silently off for every env-configured call. Cleared rather than left
        # set when empty, or a previous call's profiles leak into this one.
        # A 512-float embedding is ~10 KB of JSON, so contact + boss stays well
        # inside the per-variable environment limit.
        profiles = self._get_voice_profiles(contact, boss)
        if profiles:
            os.environ[VOICE_PROFILES_ENV] = json.dumps(profiles)
        else:
            os.environ.pop(VOICE_PROFILES_ENV, None)
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

    async def set_hang_up_gate(self, reason: str | None) -> None:
        """Arm or disarm the fast brain's hang-up gate on the live voice agent.

        Arming (``reason`` set) exposes the ``hang_up`` classification to the
        fast brain so it can end the call at a natural close; disarming
        (``reason=None``) withdraws it. The state is mirrored to the voice
        agent child over the ``app:call:status`` IPC channel.
        """
        self.hang_up_gate_reason = reason
        if self._event_broker is None:
            return
        await self._event_broker.publish(
            "app:call:status",
            json.dumps(
                {
                    "type": "hang_up_gate",
                    "armed": reason is not None,
                    "reason": reason or "",
                },
            ),
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
        self.pending_opener = ""
        self.pending_briefing = ""
        self.active_call_briefing = ""
        self.pending_hang_up_gate = ""
        self.hang_up_gate_reason = None
        self._call_channel = None
        self._disconnect_contact = None
        self.unify_meet_call_session_id = ""
        self.unify_meet_participants = []
        self.engaged_contacts = {}
        self.engaged_labels = set()
        self.known_speaker_labels = set()

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
