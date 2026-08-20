import os
import sys
import json
import asyncio
import re
import time
from dataclasses import dataclass
from importlib import resources

os.environ["UNIFY_TERMINAL_LOG"] = "true"

from dotenv import load_dotenv

from livekit import agents, rtc
from livekit.agents import (
    AgentSession,
    Agent,
    RoomInputOptions,
    utils,
    stt,
)
from livekit.plugins import (
    cartesia,
    deepgram,
    elevenlabs,
    silero,
)
from livekit.agents.voice.io import TimedString

from unify.conversation_manager.livekit_unify_adapter import UnifyLLM

if sys.platform == "darwin":
    from livekit.plugins import noise_cancellation

from livekit.plugins.turn_detector.english import EnglishModel
from livekit.agents import ChatContext, ChatMessage
from livekit.agents import ModelSettings, llm
from livekit.agents.llm import ChatChunk, ChoiceDelta

from typing import AsyncIterable, Callable

load_dotenv()

from unify.conversation_manager.events import *
from unify.common.prompt_helpers import now as prompt_now
from unify.conversation_manager import speaker_id
from unify.conversation_manager.utils import dispatch_livekit_agent
from unify.conversation_manager.prompt_builders import (
    build_opening_greeting_messages,
    build_voice_agent_prompt,
)
from unify.conversation_manager.tracing import (
    content_trace_id,
    monotonic_ms,
    now_utc_iso,
)
from unify.session_details import SESSION_DETAILS

# Shared helpers
from unify.conversation_manager.medium_scripts.common import (
    event_broker,
    CALL_DESKTOP_SHARE_SURFACE,
    create_end_call,  # kept for test monkeypatch compatibility
    match_say_meta,
    setup_participant_disconnect_handler,  # kept for test monkeypatch compatibility
    publish_call_started,
    publish_call_ended,
    delete_livekit_room,
    configure_from_cli,
    should_dispatch_livekit_agent,
    start_event_broker_receive,
    UserTrackCaptureManager,
    MAX_VISUALS_PER_SOURCE,
    ScreenshotHistory,
    capture_assistant_screenshot,
    fetch_meet_screenshare_frame,
    render_participant_comms,
    publish_meet_interaction_from_track,
    FastBrainLogger,
    hydrate_fast_brain_history,
)
from unify.conversation_manager.medium_scripts.meet_floor import (
    FLOOR_TOPIC,
    MeetFloor,
)
from unify.conversation_manager.domains.recall.client import RECALL_EVENT_TOPIC
from unify.conversation_manager.domains.recall.events import (
    EVENT_CHAT_MESSAGE,
    EVENT_LEAVE,
    EVENT_SCREENSHARE_OFF,
    EVENT_SCREENSHARE_ON,
    EVENT_SPEECH_OFF,
    EVENT_SPEECH_ON,
    ROSTER_EVENTS,
    parse_relayed_event,
)
from unify.conversation_manager.meet_speaker_map import (
    MeetSpeakerVotes,
    MeetSpeakerWindows,
)
from unify.conversation_manager.cm_types.screenshot import (
    ScreenshotEntry,
    generate_screenshot_path,
    write_screenshot_to_disk,
)
from unify.conversation_manager.domains.fast_brain_turn import (
    PendingContinuation,
    compute_resume_text,
    pick_resume_lead_in,
    select_fast_brain_turn,
)

# Globals initialized lazily or via prewarm to avoid duplicate heavy init
STT = None
VAD = None
SPEAKER_EMBEDDER = None


# Module-level logger created early for prewarm (before entrypoint runs).
_log = FastBrainLogger()

# Channels where many people share one audio stream with no single primary, so
# every distinct voice needs its own "Speaker N" label even when nobody on the
# call is enrolled. Deliberately wider than the ("google_meet", "teams_meet")
# checks elsewhere in this module: those mean a meeting on someone else's
# platform, reached through a hosted bot, whereas Unify Meet is our own room —
# but both are just as multi-party.
MULTI_PARTY_CHANNELS = ("google_meet", "teams_meet", "unify_meet")

# How often to re-read the screen a meeting participant is sharing. Recall sends
# frames at 2fps and the relay stores one a second, so polling faster only costs
# round trips; polling much slower would leave the frame behind the conversation
# it is meant to explain. Runs only while somebody is actually presenting.
_MEET_SCREENSHARE_POLL_INTERVAL_S = 1.0

# How often a shared screen is fed to the brains while nobody is speaking.
# Deliberately far slower than the poll: every push is a screenshot written to
# disk, buffered for the slow brain, and registered as an image downstream, so
# pushing each polled frame would file hundreds of images per meeting. Ten
# seconds keeps an idle share current without that.
_MEET_AMBIENT_PUSH_INTERVAL_S = 10.0

# Consecutive empty polls before saying so. Somebody is presenting whenever this
# loop runs, so no frame is a fault; a couple of misses is just the first frame
# not having landed yet.
_MEET_FETCH_MISS_ALERT_AFTER = 5

DEPLETED_CREDITS_FAST_BRAIN_RESPONSE = (
    "Your credits are depleted, so I can't continue helping with setup or tasks "
    "until you top up. Please add credits in billing, then I'll pick this back up."
)
ELEVENLABS_TWIN_PRONUNCIATION_SOURCE = "t-w1n"
ELEVENLABS_TWIN_PRONUNCIATION_TARGET = "Twin"
IDLE_SMALLTALK_STATE_TIMEOUT_S = 0.2


# The path each voice plugin is pointed at on the broker sidecar. Deepgram takes
# its full listen endpoint; Cartesia appends ``/tts/websocket`` to the host it is
# given; ElevenLabs appends to a ``/v1`` base. All three convert an http(s) base
# to ws(s) themselves, so a loopback http base yields a loopback ws connection.
_VOICE_BROKER_PATHS = {
    "deepgram": "/voice/deepgram/v1/listen",
    "cartesia": "/voice/cartesia",
    "elevenlabs": "/voice/elevenlabs/v1",
}


def _voice_broker_kwargs(provider: str) -> dict:
    """base_url/api_key that route a voice plugin through the broker sidecar.

    The plugins otherwise read their provider key from the environment. In a pod
    that key is no longer there -- it lives only in the sidecar -- so point the
    plugin at the sidecar over loopback and hand it the pod's UNIFY_KEY as the
    nonce the sidecar checks; the sidecar swaps in the real key. Returns ``{}``
    where no sidecar is configured (self-host / local dev), leaving the plugin's
    own env-key behaviour intact.
    """
    from unify.common.broker import broker_origin

    origin = broker_origin()
    if not origin:
        return {}
    unify_key = os.environ["UNIFY_KEY"]
    return {
        "base_url": f"{origin}{_VOICE_BROKER_PATHS[provider]}",
        "api_key": unify_key,
    }


def _drain_elevenlabs_twin_pronunciation_buffer(
    pending: str,
    emitted: list[str],
) -> str:
    while pending:
        lower_pending = pending.lower()
        if ELEVENLABS_TWIN_PRONUNCIATION_SOURCE.startswith(lower_pending):
            if len(pending) == len(ELEVENLABS_TWIN_PRONUNCIATION_SOURCE):
                emitted.append(ELEVENLABS_TWIN_PRONUNCIATION_TARGET)
                return ""
            return pending

        emitted.append(pending[0])
        pending = pending[1:]

    return pending


async def _normalize_elevenlabs_twin_pronunciation_stream(
    text: AsyncIterable[str],
) -> AsyncIterable[str]:
    pending = ""

    async for chunk in text:
        emitted: list[str] = []
        for char in chunk:
            pending += char
            pending = _drain_elevenlabs_twin_pronunciation_buffer(
                pending,
                emitted,
            )
        if emitted:
            yield "".join(emitted)

    if pending:
        yield pending


class FastBrainBillingGateMonitor:
    """Polls billing state off the voice response path."""

    def __init__(self, refresh_interval_s: float = 5.0) -> None:
        from unify.spending_limits import BillingGateState

        self._refresh_interval_s = refresh_interval_s
        self._state = BillingGateState()

    @property
    def state(self):
        return self._state

    async def refresh_once(self) -> None:
        from unify.spending_limits import check_billing_gate_state

        next_state = await check_billing_gate_state()
        if next_state.allowed != self._state.allowed:
            if next_state.allowed:
                _log.info("Billing gate cleared")
            else:
                _log.warning(next_state.reason or "Billing gate active")
        self._state = next_state

    async def run(self) -> None:
        while True:
            await self.refresh_once()
            await asyncio.sleep(self._refresh_interval_s)


def prewarm(_ctx=None):
    global STT, VAD, SPEAKER_EMBEDDER
    try:
        _log.info("Prewarm: initializing STT, VAD and turn detector…")
        STT = deepgram.STT(
            model="nova-3",
            language="en-GB",
            enable_diarization=True,
            **_voice_broker_kwargs("deepgram"),
        )
        VAD = silero.VAD.load(min_speech_duration=0.15, min_silence_duration=1.0)
        _log.info("Prewarm complete")
    except Exception as e:  # noqa: BLE001
        _log.error(f"Prewarm failed: {e}")
        STT = None
        VAD = None
    try:
        model_path = speaker_id.ensure_speaker_model()
        if model_path is not None:
            SPEAKER_EMBEDDER = speaker_id.SpeakerEmbedder(model_path)
            _log.info("Prewarm: speaker-embedding model ready")
        else:
            _log.warning(
                "Prewarm: speaker-embedding model unavailable — "
                "speaker attribution disabled",
            )
    except Exception as e:  # noqa: BLE001
        _log.error(f"Prewarm speaker model failed: {e}")
        SPEAKER_EMBEDDER = None


class Assistant(Agent):
    """
    TTS Fast Brain - handles real-time conversation independently.

    Uses a lightweight LLM (gpt-5.4-mini via UnifyLLM adapter) for fast
    conversational responses. Routes through unillm.AsyncUnify for local
    caching (CI) and usage tracking.
    Communicates with the Main CM Brain (slow brain) via Unix domain socket IPC.
    """

    def __init__(
        self,
        contact: dict,
        boss: dict,
        channel: str,
        instructions: str,
        outbound: bool = False,
        normalize_elevenlabs_twin_pronunciation: bool = False,
        speaker_tracker: "speaker_id.SpeakerTracker | None" = None,
    ) -> None:
        self.contact = contact
        self.boss = boss
        self.channel = channel
        self.speaker_tracker = speaker_tracker
        # Speaking-floor coordination for multi-assistant org meets; None on
        # every other channel (no gating overhead).
        self.meet_floor: MeetFloor | None = None
        # Live peer-assistant names on this call (multi-assistant etiquette).
        # A closure over the meet roster so mid-call additions are seen.
        self._peer_assistants_provider: Callable[[], list[str]] | None = None
        # Live names of everyone else on this call (group-call etiquette). Also a
        # closure rather than a snapshot: a call that starts 1:1 and becomes a
        # group when someone joins has to pick the etiquette up mid-call, and one
        # that empties back out has to drop it again.
        self._other_participants_provider: Callable[[], list[str]] | None = None
        self.normalize_elevenlabs_twin_pronunciation = (
            normalize_elevenlabs_twin_pronunciation
        )
        if channel == "phone_call":
            self.utterance_event = InboundPhoneUtterance
            self.assistant_utterance_event = OutboundPhoneUtterance
        elif channel == "whatsapp_call":
            self.utterance_event = InboundWhatsAppCallUtterance
            self.assistant_utterance_event = OutboundWhatsAppCallUtterance
        elif channel == "google_meet":
            self.utterance_event = InboundGoogleMeetUtterance
            self.assistant_utterance_event = OutboundGoogleMeetUtterance
        elif channel == "teams_meet":
            self.utterance_event = InboundTeamsMeetUtterance
            self.assistant_utterance_event = OutboundTeamsMeetUtterance
        else:
            self.utterance_event = InboundUnifyMeetUtterance
            self.assistant_utterance_event = OutboundUnifyMeetUtterance
        self.call_received = not outbound
        self._user_speech_logged = False
        self.user_turn_generating = False
        self._credit_gate_state_provider: Callable | None = None
        # On agent-initiated calls the verbatim opener is held until the
        # callee's first utterance or a short silence window. While pending,
        # the opener is the sole response to that first turn: the fast-brain
        # filler and the slow-brain turn are suppressed for it. The entrypoint
        # overrides this from the opening config (an ``opener`` opening can
        # also arrive on an inbound-shaped leg, e.g. the WhatsApp
        # permission-callback call); cleared once the opener is dispatched.
        self._opening_pending = outbound
        self._first_user_turn = asyncio.Event()
        self._first_turn_speaking_started_at: float | None = None
        self._first_turn_duration_s: float | None = None
        # Optional short note the slow brain bundles with a spoken line for the
        # fast brain to use on the caller's next message (e.g. confirm a fact).
        # Replaced/cleared on each slow-brain spoken turn; never spoken aloud.
        self._fast_brain_guidance = ""
        # Monotonic user-turn counter and the latest turn the slow brain has
        # already produced spoken output for. A buffer filler is only useful as a
        # lead-in: if the slow brain has already responded to this turn, the
        # filler is dropped so it never plays AFTER the real answer.
        self._user_turn_seq = 0
        self._slow_brain_responded_turn = -1
        # Count of consecutive fast replies emitted since the slow brain last
        # spoke. After the first reaction, subsequent ones are marked as repeated
        # deferrals so they reassure ("bear with me") rather than starting a
        # fresh lookup. Reset when the slow brain delivers a real reply.
        self._buffers_since_slow_reply = 0
        # Armed when the recorded opener is interrupted before its static-removal
        # transition. Schedules a bridge recording at the start of the next turn.
        # The callable enqueues the bridge synchronously (no playout await) so
        # the fast-brain reply generates concurrently and queues behind it.
        self._pending_opening_bridge: Callable[[], None] | None = None
        # The in-flight TTS say handle (slow-brain speech or a spoken opener)
        # registered for resumption, and the claimable resume candidate
        # produced when it is interrupted. Pre-recorded audio is never registered
        # here, so its hand-crafted tone is never continued by the live voice.
        self._active_tts: dict | None = None
        self._pending_continuation: dict | None = None
        self._tts_seq = 0
        self._publish_voice_interrupt: Callable | None = None
        # The speech handle of the in-flight ``generate_reply`` speech, recorded
        # by the ``speech_created`` observer. That event fires when the reply
        # speech is scheduled — before ``llm_node`` streams its content — so the
        # turn logic reads this to attach continuation registration or a gated
        # hang-up finalizer to the exact speech it is producing.
        self._active_reply_handle: object | None = None
        # Entrypoint closure registering a reply handle for barge-in resumption.
        self._register_reply_continuation: Callable | None = None
        # Entrypoint closure ending the call after a reply's farewell plays out.
        self._finalize_reply_hang_up: Callable | None = None
        # Schedules the slow-brain run after the fast brain finishes a user turn.
        self._publish_fast_brain_turn_completed: Callable | None = None
        # Latest user turn for which the slow brain was already scheduled.
        self._fast_brain_completed_turn = -1
        self._fast_brain_system_prompt: str = ""
        self._fast_brain_history_provider: Callable[[], list[dict]] | None = None
        self._idle_smalltalk_allowed = False
        self._idle_smalltalk_state_event = asyncio.Event()
        # Hang-up gate: while the slow brain has sanctioned ending the call,
        # this holds its stated reason (None = disarmed). Exposes the extra
        # ``hang_up`` classification to the fast brain, which then picks the
        # natural close. Set/cleared via ``hang_up_gate`` status messages.
        self._hang_up_gate_reason: str | None = None
        # Unspoken call briefing supplied by the slow brain when it placed the
        # call (``briefing`` on the call-start tools). Injected into every
        # fast-brain turn so the live voice fully owns the briefed interaction
        # without slow-brain round-trips. Never spoken aloud.
        self._call_briefing: str = ""

        super().__init__(instructions=instructions)

    def set_credit_gate_state_provider(self, provider: Callable) -> None:
        self._credit_gate_state_provider = provider

    def set_idle_smalltalk_allowed(self, allowed: bool) -> None:
        self._idle_smalltalk_allowed = allowed
        self._idle_smalltalk_state_event.set()

    async def _request_idle_smalltalk_state(self) -> bool:
        self._idle_smalltalk_allowed = False
        self._idle_smalltalk_state_event.clear()
        publish_task = asyncio.create_task(
            event_broker.publish("app:comms:fast_brain_generating", "{}"),
        )
        if getattr(event_broker, "_socket_client", None) is None:
            return False
        await publish_task
        try:
            await asyncio.wait_for(
                self._idle_smalltalk_state_event.wait(),
                timeout=IDLE_SMALLTALK_STATE_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            return False
        return self._idle_smalltalk_allowed

    def set_call_received(self):
        self.call_received = True

    @staticmethod
    def _latest_user_text(chat_ctx: llm.ChatContext) -> str:
        for item in reversed(chat_ctx.items):
            if getattr(item, "role", None) == "user":
                return item.text_content or ""
        return ""

    async def _finalize_fast_brain_user_turn(
        self,
        *,
        turn_id: int,
        user_content: str,
        classification: str,
        intended_speech: str,
    ) -> None:
        if turn_id <= self._fast_brain_completed_turn:
            return
        self._fast_brain_completed_turn = turn_id
        if self._publish_fast_brain_turn_completed is None:
            return
        await self._publish_fast_brain_turn_completed(
            turn_id=turn_id,
            user_content=user_content,
            classification=classification,
            intended_speech=intended_speech,
        )

    async def on_user_turn_completed(
        self,
        turn_ctx: ChatContext,
        new_message: ChatMessage,
    ) -> None:
        """Hook called when user finishes speaking — before LLM generation starts.

        The opener static-removal bridge (if armed) is *scheduled* here but not
        awaited: enqueueing it synchronously, before this hook returns, keeps it
        ahead of the reply the framework generates next (same speech priority,
        FIFO), while letting the fast brain think during the bridge playout
        instead of after it. The reply queues behind the bridge rather than
        interrupting it, and the bridge text stays in the in-flight speech queue
        so the concurrent generation sees it and continues naturally from it.
        """
        # New user turn: a fresh buffer filler is now warranted (until the slow
        # brain responds to this turn).
        self._user_turn_seq += 1
        # On an outbound call, the callee's first completed utterance triggers the
        # held opener (the opener answers it). Signalling on turn-completed means
        # we respond after their "Hello?", never over it.
        if self._opening_pending and not self._first_user_turn.is_set():
            if self._first_turn_duration_s is None:
                self._first_turn_duration_s = 0.0
            self._first_user_turn.set()
        if self._pending_opening_bridge is not None:
            schedule_bridge = self._pending_opening_bridge
            self._pending_opening_bridge = None
            schedule_bridge()
        text = new_message.text_content or ""
        if text:
            _log.user_speech(text)
            self._user_speech_logged = True

    async def llm_node(
        self,
        chat_ctx: llm.ChatContext,
        tools: list[llm.FunctionTool | llm.RawFunctionTool],
        model_settings: ModelSettings,
    ) -> AsyncIterable[llm.ChatChunk]:
        """Wait for call connection, then emit a single buffer filler phrase.

        The fast brain does not free-generate substantive replies; it selects one
        short, safe filler phrase to cover latency while the slow brain composes
        the real (verbatim-spoken) response.
        """
        self.user_turn_generating = True
        my_turn = self._user_turn_seq
        user_text = self._latest_user_text(chat_ctx)
        turn_classification: str | None = None
        intended_speech = ""
        try:
            _log.info("Waiting for call to be received…")
            while not self.call_received:
                await asyncio.sleep(0.1)
            _log.call_status("call_received")

            # While the outbound opener is still pending, the callee's first turn
            # triggers the opener itself (the opener is the reply). Emit no filler
            # so it does not precede or race the opener.
            if self._opening_pending:
                _log.info("Filler suppressed: outbound opener still pending")
                return

            credit_gate_state = (
                self._credit_gate_state_provider()
                if self._credit_gate_state_provider is not None
                else None
            )
            if credit_gate_state is not None and not credit_gate_state.allowed:
                _log.info("Credit gate response served from cached state")
                turn_classification = FAST_BRAIN_TURN_DEFER
                intended_speech = DEPLETED_CREDITS_FAST_BRAIN_RESPONSE
                yield ChatChunk(
                    id=f"credit-gate-{monotonic_ms()}",
                    delta=ChoiceDelta(
                        role="assistant",
                        content=DEPLETED_CREDITS_FAST_BRAIN_RESPONSE,
                    ),
                )
                return

            # The buffer is only useful as a lead-in. If the slow brain has
            # already produced spoken output for this turn (e.g. this is a
            # notification-triggered re-generation, or its answer landed first),
            # emit nothing - a filler must never play AFTER the real answer.
            if self._slow_brain_responded_turn >= my_turn:
                _log.info("Buffer suppressed: slow brain already responded this turn")
                return

            idle_status_smalltalk = await self._request_idle_smalltalk_state()

            # The fast brain does not compose the real answer (the slow brain
            # does, spoken verbatim). It gives one brief, natural reaction to
            # cover the gap. The first reply since the slow brain last spoke is a
            # fresh reaction; subsequent ones (the caller spoke again before the
            # real reply landed) are marked as repeated deferrals so they reassure
            # rather than starting a fresh lookup.
            already_deferred = self._buffers_since_slow_reply >= 1
            recent_assistant_text = ""
            for item in reversed(chat_ctx.items):
                role = getattr(item, "role", None)
                if role == "assistant" and not recent_assistant_text:
                    recent_assistant_text = item.text_content or ""
                    break

            pending = await self._claim_pending_continuation(user_text)

            if pending is not None and not (user_text or "").strip():
                if self._slow_brain_responded_turn >= my_turn:
                    _log.info("Continuation suppressed: slow brain already responded")
                    return
                self._buffers_since_slow_reply += 1
                if pending.heard_prefix:
                    continuation = (
                        f"{pick_resume_lead_in()} {pending.resume_text}".strip()
                    )
                else:
                    continuation = pending.resume_text.strip()
                self._attach_reply_continuation(continuation)
                turn_classification = FAST_BRAIN_TURN_CONTINUATION
                intended_speech = continuation
                yield ChatChunk(
                    id=f"fast-brain-continuation-{monotonic_ms()}",
                    delta=ChoiceDelta(role="assistant", content=continuation),
                )
                return

            if not already_deferred:
                await self._capture_screenshots_for_llm(chat_ctx)

            history_provider = self._fast_brain_history_provider
            history_messages = (
                history_provider() if history_provider is not None else []
            )
            peers_provider = self._peer_assistants_provider
            others_provider = self._other_participants_provider
            resolved = await select_fast_brain_turn(
                user_text=user_text,
                system_prompt=self._fast_brain_system_prompt,
                history_messages=history_messages,
                pending_continuation=pending,
                already_deferred=already_deferred,
                guidance=self._fast_brain_guidance,
                idle_status_smalltalk=idle_status_smalltalk,
                recent_assistant_text=recent_assistant_text,
                hang_up_gate_reason=self._hang_up_gate_reason,
                briefing=self._call_briefing,
                peer_assistants=(
                    peers_provider() if peers_provider is not None else ()
                ),
                other_participants=(
                    others_provider() if others_provider is not None else ()
                ),
                own_name=SESSION_DETAILS.assistant.name or "Assistant",
            )

            if (
                resolved.declined_continuation
                and pending is not None
                and self._publish_voice_interrupt is not None
                and pending.remainder
            ):
                await self._publish_voice_interrupt(
                    pending.spoken_prefix,
                    pending.remainder,
                )

            if resolved.classification == FAST_BRAIN_TURN_SILENCE:
                _log.info("Fast brain: staying silent on bare acknowledgement")
                return

            if self._slow_brain_responded_turn >= my_turn:
                _log.info("Fast reply suppressed: slow brain already responded")
                return

            speech = resolved.intended_speech
            if not speech:
                return

            self._buffers_since_slow_reply += 1
            turn_classification = resolved.classification
            intended_speech = speech
            if turn_classification == FAST_BRAIN_TURN_CONTINUATION:
                self._attach_reply_continuation(speech)
                chunk_id = f"fast-brain-continuation-{monotonic_ms()}"
            elif turn_classification == FAST_BRAIN_TURN_SMALLTALK:
                chunk_id = f"fast-brain-smalltalk-{monotonic_ms()}"
            elif turn_classification == FAST_BRAIN_TURN_HANG_UP:
                # The farewell is spoken like any reply; the finalizer ends the
                # call once this reply's line plays out uninterrupted. It is
                # deliberately NOT registered as resumable — a barge-in aborts
                # the cut rather than resuming the goodbye.
                self._schedule_reply_hang_up(speech)
                chunk_id = f"fast-brain-hangup-{monotonic_ms()}"
            else:
                chunk_id = f"fast-brain-buffer-{monotonic_ms()}"
            yield ChatChunk(
                id=chunk_id,
                delta=ChoiceDelta(role="assistant", content=speech),
            )
        finally:
            self.user_turn_generating = False
            if (
                turn_classification is not None
                and turn_classification != FAST_BRAIN_TURN_SILENCE
            ):
                await self._finalize_fast_brain_user_turn(
                    turn_id=my_turn,
                    user_content=user_text,
                    classification=turn_classification,
                    intended_speech=intended_speech,
                )

    async def _claim_pending_continuation(
        self,
        user_text: str,
    ) -> PendingContinuation | None:
        """Claim a stashed interrupted-line candidate for this user turn."""
        # Barge-in vs user-turn race: if a line was just interrupted but its
        # remainder hasn't been computed yet, wait briefly for it.
        active = self._active_tts
        if (
            self._pending_continuation is None
            and active is not None
            and getattr(active.get("handle"), "interrupted", False)
        ):
            for _ in range(6):  # ~300ms
                await asyncio.sleep(0.05)
                if self._pending_continuation is not None:
                    break

        pending = self._pending_continuation
        if not pending or pending.get("consumed"):
            return None

        pending["consumed"] = True
        resume_text = (pending.get("resume_text") or "").strip()
        remainder = (pending.get("remainder") or "").strip()
        spoken_prefix = (pending.get("spoken_prefix") or "").strip()
        self._pending_continuation = None
        self._active_tts = None
        if not resume_text:
            return None

        return PendingContinuation(
            resume_text=resume_text,
            remainder=remainder,
            spoken_prefix=spoken_prefix,
        )

    def _attach_reply_continuation(self, full_text: str) -> None:
        """Register the in-flight reply speech for barge-in resumption.

        ``speech_created`` fires when the reply speech is scheduled — before
        ``llm_node`` streams — so the recorded handle is always this turn's own
        speech, never a later one.
        """
        handle = self._active_reply_handle
        register = self._register_reply_continuation
        if handle is None or register is None:
            return
        register(handle, full_text)

    def _schedule_reply_hang_up(self, farewell: str) -> None:
        """Arrange the gated call cut once this reply's farewell plays out."""
        handle = self._active_reply_handle
        finalize = self._finalize_reply_hang_up
        if handle is None or finalize is None:
            _log.warning(
                "Gated hang-up dropped: no reply speech handle to finalize",
            )
            return
        finalize(handle, farewell)

    def _tee_frames_to_speaker_tracker(
        self,
        audio: AsyncIterable[rtc.AudioFrame],
    ) -> AsyncIterable[rtc.AudioFrame]:
        """Copy inbound audio frames into the speaker tracker's ring buffer.

        Purely observational: frames are forwarded to STT unchanged and the
        tracker only appends to an in-memory buffer, so the live pipeline
        incurs no added latency.
        """
        tracker = self.speaker_tracker
        if tracker is None:
            return audio

        async def _tee():
            async for frame in audio:
                tracker.add_audio(
                    bytes(frame.data),
                    frame.sample_rate,
                    frame.num_channels,
                )
                yield frame

        return _tee()

    async def stt_node(
        self,
        audio: AsyncIterable[rtc.AudioFrame],
        model_settings: ModelSettings,
    ):
        audio = self._tee_frames_to_speaker_tracker(audio)
        async for event in super().stt_node(audio, model_settings):
            if (
                getattr(event, "type", None) == stt.SpeechEventType.FINAL_TRANSCRIPT
                and event.alternatives
            ):
                # The tracker observes every final so attribution windows stay
                # aligned with the transcript; nothing is gated on the result.
                tracker = self.speaker_tracker
                if tracker is not None:
                    tracker.observe_final_transcript(
                        event.alternatives[0].speaker_id,
                        end_ts=time.time(),
                    )
            yield event

    async def _tts_node_unlocked(
        self,
        text: AsyncIterable[str],
        model_settings: ModelSettings,
    ) -> AsyncIterable:
        if self.normalize_elevenlabs_twin_pronunciation:
            text = _normalize_elevenlabs_twin_pronunciation_stream(text)

        async for frame in super().tts_node(text, model_settings):
            yield frame


def _load_config_from_metadata(ctx: agents.JobContext) -> dict | None:
    """Parse call config from job dispatch metadata (persistent worker path).

    Returns the parsed dict, or None when no metadata is present (legacy
    subprocess path).
    """
    raw = getattr(ctx.job, "metadata", None)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def _hydrate_session_details_from_metadata(meta: dict) -> None:
    """Apply assistant identity fields carried by LiveKit job metadata."""
    assistant_bio = meta.get("assistant_bio", "")
    SESSION_DETAILS.assistant.about = assistant_bio
    SESSION_DETAILS.assistant.is_coordinator = meta.get("is_coordinator", False) is True
    if meta.get("assistant_id"):
        try:
            SESSION_DETAILS.assistant.agent_id = int(meta["assistant_id"])
        except (ValueError, TypeError):
            pass
    if meta.get("user_id"):
        SESSION_DETAILS.user.id = meta["user_id"]
    if meta.get("assistant_name"):
        parts = meta["assistant_name"].split(None, 1)
        SESSION_DETAILS.assistant.first_name = parts[0] if parts else ""
        SESSION_DETAILS.assistant.surname = parts[1] if len(parts) > 1 else ""
    if meta.get("unify_key"):
        SESSION_DETAILS.unify_key = str(meta["unify_key"])
    # Credit gate / UniLLM deduct still read os.environ["UNIFY_KEY"]. Without
    # this, the voice worker keeps the image-baked key after metadata hydrate.
    SESSION_DETAILS.export_to_env()


def _voice_call_channel_defers_desktop_binding(channel: str) -> bool:
    """Return whether this LiveKit call channel uses deferred desktop binding."""
    return channel in (
        "phone_call",
        "whatsapp_call",
        "unify_meet",
        "google_meet",
        "teams_meet",
    )


_CALL_OPENING_MODES = {"speak", "opener", "simulated", "silent", "recorded"}

# On an agent-initiated call, the verbatim opener is held until the earlier of
# the callee's first completed utterance or this much silence after they answer
# (with the audio pipeline live). Triggering on their speech ensures the opener
# lands when they are actually listening; the silence fallback covers callees
# who answer and wait for the caller to speak first.
OPENER_SILENCE_TRIGGER_S = 3.0

# A callee first turn at or above this speaking duration is substantive: the
# held opener goes to the fast brain to decide (deliver verbatim vs respond to
# what they said) instead of being spoken blindly as though they had only said
# "Hello?".
OPENER_SHORT_TURN_MAX_S = 5.0

# Durable system note carrying the slow brain's unspoken call briefing into the
# voice context (chat ctx + session history) so every voice surface — not just
# the structured turn selector — knows why the call was placed.
_CALL_BRIEFING_SYSTEM_NOTE = (
    "[system] Call briefing — context, not script. This call was placed for "
    "the reason below. NEVER read the briefing aloud or quote it verbatim; "
    "speak naturally in your own words. You fully own everything it covers.\n"
    "\n"
    "{briefing}"
)

# After a gated hang-up farewell finishes playing out, wait this long for a
# barge-in before actually ending the call. A caller who starts speaking in
# this window ("oh wait, one more thing") aborts the cut entirely — the
# farewell becomes an ordinary turn and the conversation continues.
HANG_UP_GRACE_S = 1.0


# Ceiling on a fast-brain small-talk reply lives in fast_brain_turn.py.

# When the assistant ends a Unify Meet, we first tell the Console to leave the
# room itself (so its WebRTC peer connection and SCTP data channels close
# cleanly) before the agent shuts down and the room is deleted server-side.
# Without this lead time the server-side DeleteRoom force-evicts the still-
# connected browser, which surfaces as benign but noisy "Unknown DataChannel
# error" logs in the Next.js console. This is the grace given for the client to
# disconnect gracefully on its own.
MEET_GRACEFUL_LEAVE_GRACE_S = 0.6

# Sentence-level timings for the coordinator onboarding intro (walkie + clean).
# Boundaries were derived from the original per-sentence audio slices aligned via
# Whisper word timestamps; playback uses one continuous recording with TimedString
# chunks so LiveKit can commit only the heard prefix on interruption.
#
# ``twin-onboarding-intro.mp3`` is spliced from the original recording:
#   1. Part A: trim 0→18.35s (through "Much better." + natural silence).
#   2. Part B: trim 20.77→21.27s (0.5s natural silence before unmute in the
#      source) concat 21.27s→end ("By the way…") — one continuous part B.
#   3. Concat part A + part B once with a 2ms ``acrossfade`` at that join.
# Extra pause comes from prefixing part B, not from padding part A or adding a
# third segment after "Much better." — those extra joins caused the blip.
# The "Any questions…" block (≈18.36–20.76s) stays removed.
# Radio static-removal SFX (≈16.65–16.95s intro, ≈2.05–3.85s bridge) is
# level-matched on ``twin-onboarding-intro.mp3`` and
# ``twin-onboarding-static-bridge.mp3`` (~8× intro, ~⅙ bridge).
_COORDINATOR_ONBOARDING_CLEAN_START_TIME = 17.160000
_COORDINATOR_ONBOARDING_TIMED_CHUNKS: list[dict[str, object]] = [
    {"text": "Hey, great to meet you.", "start_time": 0.000000, "end_time": 1.140000},
    {
        "text": "I'm T-W1N, and I'll be acting as your digital twin.",
        "start_time": 1.140000,
        "end_time": 3.780000,
    },
    {"text": "Inventive name, I know.", "start_time": 3.780000, "end_time": 5.720000},
    {
        "text": "Should we start with the onboarding, or would you rather just dive in and get help with some of the tasks on your plate?",
        "start_time": 5.720000,
        "end_time": 12.740000,
    },
    {
        "text": "Also, let me remove this voice static.",
        "start_time": 12.740000,
        "end_time": 17.160000,
    },
    {"text": "Much better.", "start_time": 17.160000, "end_time": 17.900000},
    {
        "text": "By the way, you'll probably want to unmute yourself first. Click the microphone at the bottom of the meet window, and then I'll be able to hear you.",
        "start_time": 18.850000,
        "end_time": 25.800000,
    },
]

# Scripted chat intro when the user picks text over the onboarding call.
# Matches the recorded opener minus walkie static, unmute guidance, and the
# removed "Any questions before we start with the onboarding?" line.
COORDINATOR_ONBOARDING_CHAT_INTRO = (
    "Hey, great to meet you. I'm T-W1N, and I'll be acting as your digital twin. "
    "Inventive name, I know. Should we start with the onboarding, or would you "
    "rather just dive in and get help with some of the tasks on your plate?"
)

_RECORDED_OPENING_ASSETS = {
    "coordinator_onboarding_intro": "twin-onboarding-intro.mp3",
    "coordinator_onboarding_static_bridge": "twin-onboarding-static-bridge.mp3",
}
_RECORDED_OPENING_TRANSCRIPTS: dict[str, str] = {}

_WALKIE_OPENER_BRIDGE_TRANSCRIPT = """\
Hang on, let me just remove this voice static.

Much better."""

# Recorded openers played as one continuous audio stream with TimedString
# transcript chunks. If the caller interrupts before the clean voice transition,
# ``bridge`` is armed for the next assistant turn.
_RECORDED_OPENINGS = {
    "coordinator_onboarding_intro": {
        "asset": "coordinator_onboarding_intro",
        "timed_chunks": _COORDINATOR_ONBOARDING_TIMED_CHUNKS,
        "clean_start_time": _COORDINATOR_ONBOARDING_CLEAN_START_TIME,
        "bridge": {
            "asset": "coordinator_onboarding_static_bridge",
            "transcript": _WALKIE_OPENER_BRIDGE_TRANSCRIPT,
        },
    },
}


def _recorded_opening_timed_transcript(chunks: list[dict[str, object]]) -> str:
    return " ".join(str(chunk["text"]) for chunk in chunks)


async def _timed_opening_text(
    chunks: list[dict[str, object]],
) -> AsyncIterable[str]:
    for chunk in chunks:
        yield TimedString(
            str(chunk["text"]),
            start_time=float(chunk["start_time"]),
            end_time=float(chunk["end_time"]),
        )


@dataclass(frozen=True)
class _PreloadedAudio:
    pcm: bytes
    sample_rate: int
    num_channels: int


def _load_recorded_asset_pcm(asset_key: str) -> _PreloadedAudio:
    import numpy as _np
    import soundfile as _sf

    filename = _RECORDED_OPENING_ASSETS.get(asset_key)
    if not filename:
        raise ValueError(f"unknown recorded opening asset: {asset_key}")
    asset = resources.files("unify.assets.audio").joinpath(filename)
    with resources.as_file(asset) as recording_path:
        with _sf.SoundFile(str(recording_path)) as recording:
            data = recording.read(dtype="int16", always_2d=True)
            sample_rate = recording.samplerate
            num_channels = recording.channels
    return _PreloadedAudio(
        pcm=_np.ascontiguousarray(data).tobytes(),
        sample_rate=sample_rate,
        num_channels=num_channels,
    )


def _preload_recorded_opening_pcm(config: dict) -> dict[str, _PreloadedAudio]:
    asset_key = config.get("recording_asset", "").strip()
    spec = _RECORDED_OPENINGS.get(asset_key)
    if spec is None:
        source = _recorded_opening_source(config)
        if source.startswith("asset://"):
            key = source.removeprefix("asset://")
            if key in _RECORDED_OPENING_ASSETS:
                return {key: _load_recorded_asset_pcm(key)}
        return {}

    preloaded: dict[str, _PreloadedAudio] = {}
    asset = spec["asset"]
    preloaded[asset] = _load_recorded_asset_pcm(asset)
    if bridge := spec.get("bridge"):
        key = bridge["asset"]
        if key not in preloaded:
            preloaded[key] = _load_recorded_asset_pcm(key)
    return preloaded


def _pcm_audio_frames(
    pcm: bytes,
    *,
    sample_rate: int,
    num_channels: int,
    frame_duration_ms: int = 20,
) -> AsyncIterable[rtc.AudioFrame]:
    async def _frames() -> AsyncIterable[rtc.AudioFrame]:
        import numpy as _np

        samples_per_chunk = max(1, int(sample_rate * frame_duration_ms / 1000))
        bytes_per_chunk = samples_per_chunk * num_channels * 2
        offset = 0
        while offset < len(pcm):
            end = min(offset + bytes_per_chunk, len(pcm))
            chunk = pcm[offset:end]
            block = _np.frombuffer(chunk, dtype="int16").reshape(-1, num_channels)
            yield rtc.AudioFrame(
                data=_np.ascontiguousarray(block).tobytes(),
                sample_rate=sample_rate,
                num_channels=num_channels,
                samples_per_channel=len(block),
            )
            offset = end
            await asyncio.sleep(0)

    return _frames()


def _preloaded_audio_frames(
    audio: _PreloadedAudio,
    *,
    frame_duration_ms: int = 20,
) -> AsyncIterable[rtc.AudioFrame]:
    return _pcm_audio_frames(
        audio.pcm,
        sample_rate=audio.sample_rate,
        num_channels=audio.num_channels,
        frame_duration_ms=frame_duration_ms,
    )


def _recorded_opening_audio(
    source: str,
    preloaded: dict[str, _PreloadedAudio] | None = None,
    *,
    frame_duration_ms: int = 20,
) -> AsyncIterable[rtc.AudioFrame]:
    if source.startswith("asset://"):
        asset_key = source.removeprefix("asset://")
        cached = (preloaded or {}).get(asset_key)
        if cached is not None:
            return _preloaded_audio_frames(cached, frame_duration_ms=frame_duration_ms)
    return _recording_audio_frames(source, frame_duration_ms=frame_duration_ms)


def _recording_audio_frames(
    source: str,
    *,
    frame_duration_ms: int = 20,
) -> AsyncIterable[rtc.AudioFrame]:
    async def _frames() -> AsyncIterable[rtc.AudioFrame]:
        import io as _io

        import httpx as _httpx
        import numpy as _np
        import soundfile as _sf

        async def _yield_file(recording_source: str | _io.BytesIO):
            with _sf.SoundFile(recording_source) as recording:
                samples_per_chunk = max(
                    1,
                    int(recording.samplerate * frame_duration_ms / 1000),
                )
                while True:
                    block = recording.read(
                        samples_per_chunk,
                        dtype="int16",
                        always_2d=True,
                    )
                    if len(block) == 0:
                        break
                    pcm = _np.ascontiguousarray(block).tobytes()
                    yield rtc.AudioFrame(
                        data=pcm,
                        sample_rate=recording.samplerate,
                        num_channels=recording.channels,
                        samples_per_channel=len(block),
                    )
                    await asyncio.sleep(0)

        if source.startswith("asset://"):
            asset_key = source.removeprefix("asset://")
            filename = _RECORDED_OPENING_ASSETS.get(asset_key)
            if not filename:
                raise ValueError(f"unknown recorded opening asset: {asset_key}")
            asset = resources.files("unify.assets.audio").joinpath(filename)
            with resources.as_file(asset) as recording_path:
                async for frame in _yield_file(str(recording_path)):
                    yield frame
            return

        recording_source: str | _io.BytesIO = os.path.expanduser(source)
        if source.startswith(("http://", "https://")):
            async with _httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(source)
                response.raise_for_status()
                recording_source = _io.BytesIO(response.content)

        async for frame in _yield_file(recording_source):
            yield frame

    return _frames()


def _recorded_opening_source(config: dict) -> str:
    asset = config.get("recording_asset", "").strip()
    if asset:
        return f"asset://{asset}"
    path = config.get("recording_path", "").strip()
    url = config.get("recording_url", "").strip()
    return path or url


def _recorded_opening_transcript(config: dict) -> str:
    transcript = config.get("transcript", "").strip()
    if transcript:
        return transcript
    asset = config.get("recording_asset", "").strip()
    if asset:
        spec = _RECORDED_OPENINGS.get(asset)
        if spec is not None:
            return _recorded_opening_timed_transcript(spec["timed_chunks"])
        transcript = _RECORDED_OPENING_TRANSCRIPTS.get(asset, "").strip()
    if not transcript:
        raise ValueError("recorded opening requires transcript")
    return transcript


def _recorded_opening_source_count(config: dict) -> int:
    return sum(
        bool(config.get(key, "").strip())
        for key in ("recording_asset", "recording_path", "recording_url")
    )


def _strip_chat_html(raw: str) -> str:
    """Flatten a meeting-chat message to plain text.

    Recall returns formatted messages as HTML, and this text goes into a prompt
    and a durable record -- neither of which wants markup. Entities are decoded
    after tag removal so an escaped ``&lt;`` in the original survives as a
    literal rather than being re-read as a tag.
    """
    import html
    import re

    without_tags = re.sub(r"<[^>]*>", " ", raw)
    return re.sub(r"\s+", " ", html.unescape(without_tags)).strip()


def _describe_call_opening_config(raw: object) -> str:
    """Summarise an opening config for the log, without its copy.

    Names the fields that decide how the call opens — and, for a recorded
    opening, whether the asset actually resolves against
    ``_RECORDED_OPENINGS``. The spoken text itself is the boss's own words
    and is reported only as present or absent.
    """
    if raw in (None, ""):
        return "absent"
    parsed: object = raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except ValueError:
            return f"unparseable-json len={len(raw)}"
    if not isinstance(parsed, dict):
        return f"non-object {type(parsed).__name__}"

    def _val(key: str) -> str:
        return str(parsed.get(key, "") or "").strip()

    asset = _val("recording_asset")
    carried = sorted(
        key
        for key in ("opener_text", "briefing", "simulated_utterance", "transcript")
        if _val(key)
    )
    return (
        f"mode={_val('mode') or '-'} source={_val('source') or '-'} "
        f"recording_asset={asset or '-'} asset_resolves={asset in _RECORDED_OPENINGS} "
        f"recording_path={bool(_val('recording_path'))} "
        f"recording_url={bool(_val('recording_url'))} carried={carried}"
    )


#: Splits ``recordingAsset`` into ``recording_Asset`` so a camelCase spelling
#: can be matched against the snake_case field names below.
_CAMEL_BOUNDARY = re.compile(r"([a-z0-9])([A-Z])")

_CALL_OPENING_FIELDS = frozenset(
    {
        "mode",
        "opener_text",
        "briefing",
        "simulated_utterance",
        "source",
        "transcript",
        "recording_asset",
        "recording_path",
        "recording_url",
    },
)


def _reject_camel_cased_opening_fields(raw: dict) -> None:
    """Refuse a config whose fields were never converted to snake_case.

    Every field is read by its snake_case name, so a camelCase spelling is
    simply invisible: the mode still says ``recorded`` while the asset naming
    what to play is silently absent, and the failure surfaces as a missing
    transcript several frames away from the sender that caused it. Name the
    offending keys instead. Fields this function does not recognise at all are
    left alone — an unrelated extra key is not a casing bug.
    """
    unconverted = sorted(
        key
        for key in raw
        if key not in _CALL_OPENING_FIELDS
        and _CAMEL_BOUNDARY.sub(r"\1_\2", key).lower() in _CALL_OPENING_FIELDS
    )
    if unconverted:
        raise ValueError(
            "opening_config fields must be snake_case; received "
            f"{unconverted} — the sender did not convert the nested object",
        )


def _call_opening_or_spoken(raw: object) -> dict:
    """Normalise the opening, falling back to a spoken greeting if unusable.

    A rejected opening raised straight out of the job entrypoint, killing the
    voice agent before it made a sound: the caller sat in silence over a config
    problem they could neither see nor do anything about, and the only trace was
    a traceback in a subprocess. A generated greeting is a worse opening than the
    one that was asked for and a far better one than none, so the call proceeds
    and the rejection is logged loudly against the shape that caused it.
    """
    try:
        return _normalize_call_opening_config(raw)
    except ValueError as exc:
        _log.error(
            f"Opening config rejected ({exc}); opening with a generated greeting "
            f"instead of failing the call. Arrived as: "
            f"{_describe_call_opening_config(raw)}",
        )
        return {"mode": "speak"}


def _normalize_call_opening_config(raw: object) -> dict:
    # Logged before validation: a rejected config raises out of here, and the
    # shape it arrived in is the only way to tell a caller sending the wrong
    # thing from a call opening on stale state.
    _log.info(f"Opening config: {_describe_call_opening_config(raw)}")
    if raw in (None, ""):
        return {"mode": "speak"}
    if isinstance(raw, str):
        raw = json.loads(raw)
    if not isinstance(raw, dict):
        raise ValueError("opening_config must be an object")

    _reject_camel_cased_opening_fields(raw)

    mode = str(raw.get("mode", "speak")).strip()
    if mode not in _CALL_OPENING_MODES:
        raise ValueError(
            "opening_config.mode must be one of speak, opener, simulated, silent, recorded",
        )

    config = {"mode": mode}
    if raw.get("opener_text") is not None:
        config["opener_text"] = str(raw["opener_text"])
    if raw.get("briefing") is not None:
        config["briefing"] = str(raw["briefing"])
    if raw.get("simulated_utterance") is not None:
        config["simulated_utterance"] = str(raw["simulated_utterance"])
    if raw.get("source") is not None:
        config["source"] = str(raw["source"])
    if raw.get("transcript") is not None:
        config["transcript"] = str(raw["transcript"])
    if raw.get("recording_asset") is not None:
        config["recording_asset"] = str(raw["recording_asset"])
    if raw.get("recording_path") is not None:
        config["recording_path"] = str(raw["recording_path"])
    if raw.get("recording_url") is not None:
        config["recording_url"] = str(raw["recording_url"])

    if mode == "opener" and not config.get("opener_text", "").strip():
        raise ValueError("opener opening requires opener_text")
    if mode == "recorded" and config.get("recording_asset", "").strip() not in (
        _RECORDED_OPENINGS
    ):
        _recorded_opening_transcript(config)
        if _recorded_opening_source_count(config) != 1:
            raise ValueError(
                "recorded opening requires exactly one of recording_asset, recording_path, or recording_url",
            )
    return config


def _configure_child_logging() -> None:
    """Ensure Unity's LOGGER works in LiveKit's pre-warmed child processes.

    LiveKit agents (v1.2.x) uses ``forkserver`` on Linux.  Child processes
    are forked from a lean server process, not from the worker, so the
    ``unity`` logger's handlers may point to stale file descriptors.

    The framework routes child logs through a ``LogQueueHandler`` on the
    **root** logger, which serialises records back to the worker process.
    We enable propagation so Unity records flow through that channel, and
    remove any direct handlers that could double-emit or silently fail.
    """
    import logging as _logging

    from unify.logger import LOGGER as _L

    _L.propagate = True
    for h in list(_L.handlers):
        _L.removeHandler(h)

    for name in ("livekit", "livekit.agents", "livekit.plugins"):
        lg = _logging.getLogger(name)
        lg.propagate = True
        for h in list(lg.handlers):
            lg.removeHandler(h)


async def entrypoint(ctx: agents.JobContext):
    global STT, VAD, SPEAKER_EMBEDDER

    _configure_child_logging()

    # This prewarmed process is now being consumed by a job, so clear the
    # idle-ready marker. The worker re-creates it once a replacement idle
    # process has finished warming, which is what gates starting the next call.
    from unify.conversation_manager.medium_scripts.worker import mark_worker_busy

    mark_worker_busy()

    # Wire the module-level logger into the shared event broker.
    event_broker.set_logger(_log)

    # --- Config: persistent worker (job metadata) or legacy subprocess (env) ---
    meta = _load_config_from_metadata(ctx)
    _log.info(f"Entrypoint started (has_metadata={meta is not None})")
    if meta:
        from unify.conversation_manager.domains.ipc_socket import init_socket_for_job

        ipc_path = meta.get("ipc_socket_path", "")
        if ipc_path:
            init_socket_for_job(ipc_path)
            event_broker.reinit_socket()
            _log.info(f"IPC socket initialised: {ipc_path}")
        else:
            _log.warning("No ipc_socket_path in job metadata — IPC disabled")

        voice_provider = meta.get("voice_provider") or "cartesia"
        voice_id = meta.get("voice_id") or ""
        outbound = meta.get("outbound", False)
        channel = meta.get("channel", "phone")
        assistant_bio = meta.get("assistant_bio", "")
        contact = meta.get("contact", {})
        boss = meta.get("boss", {})
        opening_config = _call_opening_or_spoken(meta.get("opening_config"))
        pre_armed_hang_up_gate = str(meta.get("hang_up_gate_reason") or "").strip()
        _hydrate_session_details_from_metadata(meta)
    else:
        # Per-call subprocess path: no LiveKit job metadata, but the CM still
        # exports its IPC socket via CM_EVENT_SOCKET. Connect to it so this
        # worker is a real IPC client — steerable (hang-up gate, guide_voice_agent)
        # and, crucially, counted by the CM's socket server. The CM's
        # last-client-disconnect fallback (_on_ipc_client_disconnected) is what
        # publishes the synthetic *-Ended event; without this connection the meet
        # has no client of its own, so an unrelated client disconnecting is treated
        # as the last one and tears the live meet down with a spurious end.
        from unify.conversation_manager.domains.ipc_socket import (
            CM_EVENT_SOCKET_ENV,
            init_socket_for_job,
        )

        ipc_path = os.environ.get(CM_EVENT_SOCKET_ENV, "")
        if ipc_path:
            init_socket_for_job(ipc_path)
            event_broker.reinit_socket()
            _log.info(f"IPC socket initialised from env: {ipc_path}")
        else:
            _log.warning(
                "No job metadata and no CM_EVENT_SOCKET — env-based config "
                "(IPC disabled)",
            )
        SESSION_DETAILS.populate_from_env()
        voice_provider = SESSION_DETAILS.voice.provider
        voice_id = SESSION_DETAILS.voice.id
        outbound = SESSION_DETAILS.voice_call.outbound
        channel = SESSION_DETAILS.voice_call.channel
        assistant_bio = SESSION_DETAILS.assistant.about
        contact = json.loads(SESSION_DETAILS.voice_call.contact_json or "{}")
        boss = json.loads(SESSION_DETAILS.voice_call.boss_json or "{}")
        opening_config = _call_opening_or_spoken(
            os.environ.get("OPENING_CONFIG"),
        )
        pre_armed_hang_up_gate = os.environ.get("HANG_UP_GATE_REASON", "").strip()

    from unify.coordinator_voice import resolve_runtime_voice

    is_coordinator = (
        bool(meta.get("is_coordinator")) if meta else SESSION_DETAILS.is_coordinator
    )
    voice_provider, voice_id = resolve_runtime_voice(
        is_coordinator=is_coordinator,
        voice_provider=voice_provider,
        voice_id=voice_id,
    )

    # Browser-meet diarization config (Google Meet / Teams Meet)
    meet_session_id: str = ""
    call_session_id: str = ""
    meet_url: str = ""
    if channel in ("google_meet", "teams_meet"):
        if meta:
            meet_session_id = meta.get("meet_session_id", "")
            meet_url = meta.get("meet_url", "")
        else:
            meet_session_id = os.environ.get("MEET_SESSION_ID", "")
            meet_url = os.environ.get("MEET_URL", "")
    call_session_id = (
        str(meta.get("call_session_id", "")).strip()
        if meta
        else os.environ.get("CALL_SESSION_ID", "").strip()
    )
    unify_meet_roster: list[dict] = []
    if meta:
        raw_roster = meta.get("participants") or []
        if isinstance(raw_roster, list):
            unify_meet_roster = [p for p in raw_roster if isinstance(p, dict)]
    else:
        raw_roster_env = os.environ.get("UNIFY_MEET_PARTICIPANTS", "").strip()
        if raw_roster_env:
            loaded = json.loads(raw_roster_env)
            if isinstance(loaded, list):
                unify_meet_roster = [p for p in loaded if isinstance(p, dict)]
    _unify_meet_active_identity: str | None = None

    _log.config(
        f"voice_provider={voice_provider} voice_id={voice_id} outbound={outbound} channel={channel} opening_mode={opening_config['mode']}",
    )

    _log.session_start("Connecting to room…")
    await ctx.connect()
    _log.session_start("Connected to room")

    # Console call tiles resolve this attribute to map the agent participant
    # onto the right assistant cell in multi-party meets (the agents framework
    # separately publishes `lk.agent.state` for the talking/working pose).
    if SESSION_DETAILS.assistant.agent_id:
        try:
            await ctx.room.local_participant.set_attributes(
                {"unify_assistant_id": str(SESSION_DETAILS.assistant.agent_id)},
            )
        except Exception as exc:
            _log.session_start(f"Could not set assistant participant attributes: {exc}")

    # User screen share and webcam capture (subscribe to LiveKit room tracks automatically)
    screen_capture = UserTrackCaptureManager(
        ctx.room,
        track_source="screenshare",
        on_track_change=publish_meet_interaction_from_track,
        fb_logger=_log,
    )
    webcam_capture = UserTrackCaptureManager(
        ctx.room,
        track_source="camera",
        on_track_change=publish_meet_interaction_from_track,
        fb_logger=_log,
    )

    # Flag for call_answered that may arrive during initialization
    call_answered_flag = asyncio.Event()
    user_joined_event = asyncio.Event()
    joined_gate_required = channel in ("phone_call", "whatsapp_call")
    speech_gate_open = not joined_gate_required
    if speech_gate_open:
        user_joined_event.set()

    # Start receiving events from parent (callbacks registered later)
    await start_event_broker_receive()

    # Fallback for whenever pre-loading fails
    if STT is None:
        STT = deepgram.STT(
            model="nova-3",
            language="en-GB",
            enable_diarization=True,
            **_voice_broker_kwargs("deepgram"),
        )
        VAD = silero.VAD.load(min_speech_duration=0.15, min_silence_duration=1.0)

    stt_instance = STT

    # --- Browser-meet speaker + participant tracker (Meet / Teams) ---
    # Speaker identity uses two complementary signals:
    # 1. Deepgram diarization (enable_diarization=True) for precise per-utterance
    #    anonymous speaker IDs (S0, S1, ...).
    # 2. The meeting platform's own roster and speech events, relayed in.
    # ``_meet_speech_windows`` and ``_meet_speaker_votes`` link the two, by
    # overlapping each finalised utterance with the platform's speaking spans.
    _meet_auth_key = SESSION_DETAILS.unify_key
    _meet_cached_active_speaker: str | None = None
    _meet_cached_participants: list[dict] = []
    _meet_prev_participant_names: set[str] = set()
    # The screen somebody is sharing into the meeting, refreshed by
    # ``_meet_screenshare_poller`` while at least one person is presenting.
    _meet_latest_screenshot: str | None = None
    _meet_screenshare_sharer: str = ""
    # Presenter ids in the order they started, so the label follows whoever the
    # store has focused. Empty means nobody is sharing and the frame is dropped.
    _meet_sharing: list[str] = []
    _meet_screenshare_task: asyncio.Task | None = None
    # What was last handed to the brains, and when. A share is mostly static --
    # somebody's slide sits there for a minute -- so re-filing an identical frame
    # every few seconds buys nothing and costs a stored image each time.
    _meet_last_pushed_frame: str | None = None
    _meet_last_push_at: float = 0.0
    _meet_fetch_misses: int = 0
    _meet_display_name: str = ""
    _meet_last_speaker_id: str | None = None
    _meet_speech_windows = MeetSpeakerWindows()
    _meet_speaker_votes = MeetSpeakerVotes()
    if channel in ("google_meet", "teams_meet"):
        if meta:
            _meet_display_name = meta.get("meet_display_name", "")
        if not _meet_display_name:
            _meet_display_name = SESSION_DETAILS.assistant.name or "Unity Assistant"

    def _meet_participant_email(display_name: str) -> str:
        """The meeting platform's email for a participant, by display name.

        Frequently empty. Platforms disclose an address to a bot created through
        the Create Bot API only sometimes, so this is a bonus signal rather than
        one anything may depend on -- name matching has to carry the rest.
        """
        if not display_name:
            return ""
        wanted = display_name.strip().lower()
        for participant in _meet_cached_participants:
            if str(participant.get("name") or "").strip().lower() == wanted:
                return str(participant.get("email") or "").strip()
        return ""

    def _merge_meet_participant(entry: dict) -> None:
        """Merge one participant into the cached roster, keyed on platform id.

        Realtime events and the ten-second roster poll describe the same person
        from different sources, and either may know an email the other does not.
        An absent name or address therefore never overwrites a known one, so
        whichever source saw it wins regardless of which arrived last. Host
        status is taken from the newer report, being the kind of thing that
        genuinely changes mid-meeting.
        """
        participant_id = str(entry.get("id") or "")
        for existing in _meet_cached_participants:
            if str(existing.get("id") or "") != participant_id:
                continue
            existing["is_host"] = bool(entry.get("is_host"))
            if entry.get("name"):
                existing["name"] = entry["name"]
            if entry.get("email"):
                existing["email"] = entry["email"]
            return
        _meet_cached_participants.append(
            {
                "id": participant_id,
                "name": str(entry.get("name") or ""),
                "email": entry.get("email") or None,
                "is_host": bool(entry.get("is_host")),
            },
        )

    def _drop_meet_participant(participant_id: str) -> None:
        """Drop one participant from the cached roster by platform id."""
        for index, existing in enumerate(_meet_cached_participants):
            if str(existing.get("id") or "") == participant_id:
                del _meet_cached_participants[index]
                return

    def _reconcile_meet_roster(incoming: list[dict]) -> None:
        """Fold a polled roster into the cache, dropping whoever is gone.

        Merged rather than swapped in wholesale: the poll is authoritative about
        *who* is present, but not about every field of them, and replacing the
        list would discard an email only a realtime join had carried.
        """
        for entry in incoming:
            _merge_meet_participant(entry)
        present = {str(entry.get("id") or "") for entry in incoming}
        for existing in list(_meet_cached_participants):
            if str(existing.get("id") or "") not in present:
                _drop_meet_participant(str(existing.get("id") or ""))

    def _resolve_contact_by_name(display_name: str) -> dict | None:
        """Best-effort contact resolution from a Meet display name.

        Email first where the platform gave us one: two people called "Dan" are
        indistinguishable by name, and a display name is whatever the
        participant typed, whereas the address is the account they are signed in
        as. Falls back to matching first_name+surname across the known contacts
        (the caller and the boss), then None, letting the caller keep the
        original contact dict.
        """
        if not display_name:
            return None

        email = _meet_participant_email(display_name).lower()
        if email:
            for candidate in (contact, boss):
                candidate_email = str(candidate.get("email_address") or "").strip()
                if candidate_email and candidate_email.lower() == email:
                    return candidate

        dn_lower = display_name.strip().lower()
        for candidate in (contact, boss):
            full = f"{candidate.get('first_name', '')} {candidate.get('surname', '')}".strip()
            if full.lower() == dn_lower:
                return candidate
            if candidate.get("first_name", "").lower() == dn_lower:
                return candidate
        return None

    # --- Voice-embedding speaker tracker (all voice channels) ---
    # Voice-enrollment capture only: accumulates an auto-enrollment on
    # single-voice calls and suggests manual enrollment when multiple voices
    # are heard. Enrolled profiles arrive as dispatch metadata on the worker
    # path (an env var on the legacy per-call subprocess path) and gate capture
    # — they are never matched against live audio to attribute turns.
    _raw_profiles = (meta or {}).get("voice_profiles")
    if not _raw_profiles:
        try:
            _raw_profiles = json.loads(
                os.environ.get(speaker_id.VOICE_PROFILES_ENV, "") or "{}",
            )
        except json.JSONDecodeError:
            _log.error("Malformed VOICE_PROFILES env var; speaker pinning disabled")
            _raw_profiles = {}
    voice_profiles: dict[int, list[float]] = {}
    for _cid, _vec in (_raw_profiles or {}).items():
        try:
            voice_profiles[int(_cid)] = [float(x) for x in _vec]
        except (TypeError, ValueError):
            continue

    if SPEAKER_EMBEDDER is None:
        _speaker_model_path = speaker_id.ensure_speaker_model(download=False)
        if _speaker_model_path is not None:
            SPEAKER_EMBEDDER = speaker_id.SpeakerEmbedder(_speaker_model_path)

    speaker_tracker: speaker_id.SpeakerTracker | None = None
    # Publishes spawned by tracker callbacks; awaited during job shutdown so a
    # call-end enrollment is never dropped by process teardown.
    speaker_event_tasks: set[asyncio.Task] = set()

    def _publish_speaker_event(event) -> None:
        task = asyncio.create_task(
            event_broker.publish(event.topic, event.to_json()),
        )
        speaker_event_tasks.add(task)
        task.add_done_callback(speaker_event_tasks.discard)

    if SPEAKER_EMBEDDER is not None:

        def _on_enrollment_captured(
            embedding,
            wav_path: str,
            duration_s: float,
        ) -> None:
            event = VoiceEnrollmentCaptured(
                contact=contact,
                embedding=[float(x) for x in embedding],
                wav_path=wav_path,
                duration_s=float(duration_s),
                channel=channel,
            )
            _log.info(
                f"Voice enrollment captured ({duration_s:.0f}s) for "
                f"contact {contact.get('contact_id')}",
            )
            _publish_speaker_event(event)

        def _on_enrollment_suggested(num_speakers: int) -> None:
            event = VoiceEnrollmentSuggested(
                contact=contact,
                num_speakers=num_speakers,
                channel=channel,
            )
            _log.info(
                f"Voice enrollment suggested: {num_speakers} distinct voices "
                "and call contact has no enrollment",
            )
            _publish_speaker_event(event)

        speaker_tracker = speaker_id.SpeakerTracker(
            embedder=SPEAKER_EMBEDDER,
            enrolled_profiles=voice_profiles,
            call_contact_id=contact.get("contact_id"),
            multi_party=channel in MULTI_PARTY_CHANNELS,
            on_enrollment_captured=_on_enrollment_captured,
            on_enrollment_suggested=_on_enrollment_suggested,
        )

    def _user_id_from_livekit_identity(identity: str) -> str | None:
        """Parse ``user-{userId}-{suffix}`` LiveKit identities from Console tokens."""
        if not identity.startswith("user-"):
            return None
        rest = identity[len("user-") :]
        if not rest:
            return None
        # Suffix is a short random token without hyphens; user ids may contain them.
        if "-" not in rest:
            return rest
        return rest.rsplit("-", 1)[0] or None

    def _roster_member_for_user_id(user_id: str) -> dict | None:
        for member in unify_meet_roster:
            if member.get("kind") == "human" and str(
                member.get("user_id") or "",
            ) == str(
                user_id,
            ):
                return member
        return None

    def _contact_from_roster_member(member: dict) -> dict:
        display = (member.get("display_name") or "").strip()
        first, _, surname = display.partition(" ")
        return {
            "contact_id": member.get("contact_id"),
            "first_name": first or display,
            "surname": surname.strip(),
            "is_system": True,
        }

    def _sharer_name_for_identity(identity: str) -> str:
        """Who a captured video frame belongs to, or "" when unknown.

        Tried against the org-call roster first, since that carries the names
        the rest of the call already uses. The room's own participant name is
        the fallback for surfaces with no roster behind them (the LiveKit
        Playground, a dev client), and an empty result is honest: a frame
        labelled with a guessed owner is worse than one labelled with none,
        because the brains will repeat the guess back to the room.
        """
        if not identity:
            return ""
        user_id = _user_id_from_livekit_identity(identity)
        if user_id:
            member = _roster_member_for_user_id(user_id)
            if member is not None:
                name = (member.get("display_name") or "").strip()
                # The roster self-heals from LiveKit identities, so a member
                # discovered that way carries the identity as its name; that is
                # not a display name and should not be shown as one.
                if name and name != identity:
                    return name
        remotes = getattr(ctx.room, "remote_participants", {}) or {}
        for participant in remotes.values():
            if getattr(participant, "identity", "") == identity:
                return (getattr(participant, "name", "") or "").strip()
        return ""

    def _merge_unify_meet_roster_from_identity(identity: str) -> None:
        """Append a human roster entry discovered via LiveKit when missing."""
        user_id = _user_id_from_livekit_identity(identity)
        if not user_id or _roster_member_for_user_id(user_id) is not None:
            return
        unify_meet_roster.append(
            {
                "kind": "human",
                "user_id": user_id,
                "assistant_id": None,
                "display_name": identity,
                "contact_id": None,
                "email": None,
            },
        )

    def _unify_meet_stamp(
        *,
        exclude_contact_id: int | None = None,
    ) -> tuple[list[str] | None, list[int] | None]:
        """Names + contact_ids for Unify Meet transcript receiver expansion."""
        if channel != "unify_meet" or not unify_meet_roster:
            return None, None
        names: list[str] = []
        ids: list[int] = []
        for member in unify_meet_roster:
            cid = member.get("contact_id")
            if cid is not None:
                cid_int = int(cid)
                if exclude_contact_id is not None and cid_int == int(
                    exclude_contact_id,
                ):
                    continue
                ids.append(cid_int)
            name = (member.get("display_name") or "").strip()
            if name:
                names.append(name)
        # Also surface LiveKit remotes not yet in the wake roster.
        remotes = getattr(ctx.room, "remote_participants", {}) or {}
        for participant in remotes.values():
            identity = getattr(participant, "identity", "") or ""
            _merge_unify_meet_roster_from_identity(identity)
        return (names or None), (ids or None)

    def _resolve_speaker() -> tuple[dict, str | None, str | None, str | None]:
        """Resolve the current speaker.

        Returns (contact_dict, display_name, speaker_id, label_source).
        ``label_source`` is a ``speaker_id.LABEL_SOURCE_*`` tag recording which
        of the signals below produced ``display_name`` (None when no name was
        resolved). The order these are consumed in *is* the authority ordering:
        1. LiveKit identity → org-call roster (Unify Meet multi-party).
        2. Diarization speaker_id → name correlation, accumulated by overlapping
           finalised utterances with the platform's speaking spans (browser
           meets).
        3. Platform-reported active speaker (browser meets, relayed in near
           real time from participant speech events) — weaker than 2 because it
           is a single reading rather than accumulated evidence.

        Voice embeddings are deliberately not consulted: measured on production
        audio they conflate distinct speakers (including the assistant's own
        TTS voice) as often as they separate them, so an unresolved turn falls
        back to the call contact and the slow brain infers the true speaker
        from the conversation itself.
        """
        sid = _meet_last_speaker_id

        # 1. Unify-meet roster identity.
        if channel == "unify_meet" and unify_meet_roster:
            identity = _unify_meet_active_identity
            if identity:
                user_id = _user_id_from_livekit_identity(identity)
                if user_id:
                    member = _roster_member_for_user_id(user_id)
                    if member is not None and member.get("contact_id") is not None:
                        resolved = _contact_from_roster_member(member)
                        label = (member.get("display_name") or "").strip() or None
                        return (
                            resolved,
                            label,
                            sid,
                            speaker_id.LABEL_SOURCE_MEET_ROSTER,
                        )

        # 2-3. Browser-meet name resolution, from the meeting platform's own
        # participant events rather than anything read off a screen.
        if channel in ("google_meet", "teams_meet"):
            # 2. Diarization speaker_id → correlated display name.
            correlated = _meet_speaker_votes.resolve(sid)
            if correlated:
                resolved = _resolve_contact_by_name(correlated)
                if resolved:
                    label = f"{resolved.get('first_name', '')} {resolved.get('surname', '')}".strip()
                    return (
                        resolved,
                        label or None,
                        sid,
                        speaker_id.LABEL_SOURCE_RECALL_PARTICIPANT,
                    )
                return (
                    contact,
                    correlated,
                    sid,
                    speaker_id.LABEL_SOURCE_RECALL_PARTICIPANT,
                )

            # 3. Whoever the platform says is speaking right now.
            active_name = _meet_cached_active_speaker
            if active_name:
                resolved = _resolve_contact_by_name(active_name)
                if resolved:
                    label = f"{resolved.get('first_name', '')} {resolved.get('surname', '')}".strip()
                    return (
                        resolved,
                        label or None,
                        sid,
                        speaker_id.LABEL_SOURCE_RECALL_PARTICIPANT,
                    )
                return (
                    contact,
                    active_name,
                    sid,
                    speaker_id.LABEL_SOURCE_RECALL_PARTICIPANT,
                )

        return contact, None, sid, None

    def _get_meet_participant_names() -> list[str]:
        """Return display names of all human participants (excluding the assistant)."""
        return [
            p["name"]
            for p in _meet_cached_participants
            if p.get("name") and p["name"] != _meet_display_name
        ]

    # Channel-specific event classes for participant join/leave events.
    if channel == "teams_meet":
        _ParticipantJoinedEvent = TeamsMeetParticipantJoined
        _ParticipantLeftEvent = TeamsMeetParticipantLeft
        _participant_topic = "app:comms:teamsmeet_participant"
    else:
        _ParticipantJoinedEvent = GoogleMeetParticipantJoined
        _ParticipantLeftEvent = GoogleMeetParticipantLeft
        _participant_topic = "app:comms:googlemeet_participant"

    def _publish_meet_roster_changes() -> None:
        """Emit join/leave for the difference since the last roster push."""
        nonlocal _meet_prev_participant_names
        current = {p["name"] for p in _meet_cached_participants if p.get("name")}
        joined = current - _meet_prev_participant_names
        left = _meet_prev_participant_names - current
        _meet_prev_participant_names = current

        for name, event_cls in (
            *((n, _ParticipantJoinedEvent) for n in joined),
            *((n, _ParticipantLeftEvent) for n in left),
        ):
            if name == _meet_display_name:
                continue
            evt = event_cls(contact=contact, participant_name=name)
            asyncio.create_task(
                event_broker.publish(_participant_topic, evt.to_json()),
            )

    # How long the bridge may be absent before the meeting is treated as over.
    # It has to outlast a LiveKit reconnect: the page retries, and shutting down
    # on a transient blip would drop the assistant out of a live meeting. It
    # also has to cover the gap before the bridge first arrives, which is why
    # the watch waits to see it once before it starts counting.
    _RECALL_BRIDGE_ABSENT_GRACE_S = 20.0

    def _recall_bridge_present() -> bool:
        """Whether the Recall bot's bridge page is in the room."""
        remotes = getattr(ctx.room, "remote_participants", {}) or {}
        for participant in remotes.values():
            identity = getattr(participant, "identity", "") or ""
            if identity.startswith("recall-bridge-"):
                return True
        return False

    async def _recall_end_watch() -> None:
        """End the session once the Recall bridge leaves the room for good.

        The bot's page is a real LiveKit participant, so its departure is the
        meeting ending -- no Recall API call and no webhook needed. Ending here
        is what ultimately publishes the channel's *Ended event: shutdown drops
        this process's IPC client, and the CM turns that into the event.
        """
        seen = False
        absent_since: float | None = None
        try:
            while True:
                await asyncio.sleep(1)
                if _recall_bridge_present():
                    seen = True
                    absent_since = None
                    continue
                if not seen:
                    # Still waiting for the bot to arrive; a bot that never
                    # arrives is the CM's join timeout to report, not ours.
                    continue
                now = asyncio.get_event_loop().time()
                if absent_since is None:
                    absent_since = now
                elif now - absent_since >= _RECALL_BRIDGE_ABSENT_GRACE_S:
                    _log.info(f"{channel} ended (recall bridge left the room)")
                    ctx.shutdown(reason="meet_ended")
                    return
        except asyncio.CancelledError:
            pass

    if channel in ("google_meet", "teams_meet"):
        asyncio.create_task(_recall_end_watch())

    from unify.settings import SETTINGS

    # Fast brain LLM - lightweight model for responsive conversation
    # Uses UnifyLLM adapter for local caching (CI) and usage tracking
    llm_model = UnifyLLM(
        model=SETTINGS.conversation.FAST_BRAIN_MODEL,
        reasoning_effort=SETTINGS.conversation.FAST_BRAIN_REASONING_EFFORT,
    )

    assistant_name = SESSION_DETAILS.assistant.name
    # The acting user on this call is the person we're talking with when they map
    # to a system user (boss or provisioned org member), else the workspace owner.
    # Drives per-speaker linked-desktop resolution so the guardrail relaxes only
    # for someone who has actually linked their machine to this assistant.
    call_acting_user_id = (
        contact.get("user_id") if contact.get("is_system") else None
    ) or SESSION_DETAILS.user.id
    call_has_linked_user_desktop = (
        SESSION_DETAILS.assistant.user_desktop_for(call_acting_user_id) is not None
    )

    system_prompt = build_voice_agent_prompt(
        bio=assistant_bio,
        assistant_name=assistant_name or None,
        boss_first_name=boss.get("first_name", ""),
        boss_surname=boss.get("surname", ""),
        boss_email_address=boss.get("email_address", ""),
        boss_phone_number=boss.get("phone_number", ""),
        boss_bio=boss.get("bio") or None,
        contact_first_name=contact.get("first_name", ""),
        contact_surname=contact.get("surname", ""),
        contact_phone_number=contact.get("phone_number", ""),
        contact_email=contact.get("email_address", ""),
        contact_bio=contact.get("bio") or None,
        is_boss_user=bool(contact.get("is_system", False)),
        contact_rolling_summary=contact.get("rolling_summary", ""),
        channel=channel,
        has_linked_user_desktop=call_has_linked_user_desktop,
        is_coordinator=SESSION_DETAILS.is_coordinator,
        is_multiplayer=SESSION_DETAILS.is_multiplayer,
        is_org_workspace=SESSION_DETAILS.org_id is not None,
        console_ui_present=SETTINGS.UNIFY_CONSOLE_UI,
    ).flatten()
    _log.config(f"System prompt ({len(system_prompt)} chars)")

    if voice_provider == "elevenlabs":
        tts_instance = elevenlabs.TTS(
            voice_id=voice_id or elevenlabs.DEFAULT_VOICE_ID,
            model="eleven_multilingual_v2",
            **_voice_broker_kwargs("elevenlabs"),
        )
    else:
        tts_instance = cartesia.TTS(
            voice=voice_id or cartesia.tts.TTSDefaultVoiceId,
            **_voice_broker_kwargs("cartesia"),
        )

    session = AgentSession(
        llm=llm_model,
        stt=stt_instance,
        tts=tts_instance,
        vad=VAD,
        turn_handling={
            "turn_detection": EnglishModel(),
            "endpointing": {"min_delay": 0.75},
            "interruption": {"enabled": True},
        },
        preemptive_generation=False,
    )

    user_is_speaking = False
    # When each side's current line became audible, so an utterance can be
    # placed in the recording by its start rather than by the commit that
    # follows it. Consumed when the utterance is published: a turn that
    # finalises into several items must not reuse one span's start for all of
    # them, which would place the later ones far too early.
    user_speech_started_at = None
    agent_speech_started_at = None
    _queued_speech: list[tuple[str, str, str, str, str]] = []
    _say_meta_queue: list[dict] = []
    _recorded_opening_preloaded: dict[str, _PreloadedAudio] = {}
    generation_seq = 0
    user_state_seq = 0
    _was_quiescent = True
    _pending_reply_timer: asyncio.TimerHandle | None = None
    _NOTIFY_COALESCE_S = 0.05

    def _log_reply_task(task: asyncio.Task) -> None:
        try:
            task.result()
            _log.llm_completed()
        except asyncio.CancelledError:
            _log.llm_cancelled()
        except Exception as exc:  # noqa: BLE001
            _log.llm_error(str(exc))

    def _say_opening(text: str) -> None:
        speech_handle = session.say(
            text,
            allow_interruptions=True,
            add_to_chat_ctx=True,
        )
        # A speak-mode opener is live TTS (not pre-recorded), so a barge-in
        # can be resumed by the fast brain just like any slow-brain line.
        _register_interruptible_tts(speech_handle, lambda: text, "opening")

    def _say_verbatim_opener(text: str) -> None:
        """Speak the slow-brain's pre-decided opener word-for-word."""
        _say_meta_queue.append(
            {
                "source": opening_config.get("source", "call_opener"),
                "text": text,
                "llm_log_path": "",
            },
        )
        speech_handle = session.say(
            text,
            allow_interruptions=True,
            add_to_chat_ctx=True,
        )
        _register_interruptible_tts(speech_handle, lambda: text, "opening")

    def _say_recorded_opening(text: str, recording_source: str):
        _say_meta_queue.append(
            {
                "source": opening_config.get("source", "recorded_opening"),
                "text": text,
                "llm_log_path": "",
            },
        )
        return session.say(
            text,
            audio=_recorded_opening_audio(
                recording_source,
                _recorded_opening_preloaded,
            ),
            allow_interruptions=True,
            add_to_chat_ctx=True,
        )

    def _schedule_opening_bridge(segment: dict) -> None:
        """Enqueue the opener static-removal bridge without blocking on playout.

        Scheduling (``session.say``) happens synchronously so the bridge is
        queued ahead of the user-turn reply the framework generates next. We
        deliberately do NOT await playout: the reply is generated concurrently
        while the bridge plays, then plays after it (same speech priority,
        FIFO — a newly scheduled reply does not interrupt in-progress speech).
        The bridge text remains in ``_say_meta_queue`` until its playout
        commits to history, so the concurrent generation sees it as in-flight
        speech and continues naturally from it.
        """
        text = segment["transcript"]
        _say_meta_queue.append(
            {
                "source": "recorded_opening_bridge",
                "text": text,
                "llm_log_path": "",
            },
        )
        session.say(
            text,
            audio=_recorded_opening_audio(
                f"asset://{segment['asset']}",
                _recorded_opening_preloaded,
            ),
            allow_interruptions=True,
            add_to_chat_ctx=True,
        )

    async def _run_recorded_opening(config: dict) -> None:
        asset_key = config.get("recording_asset", "").strip()
        spec = _RECORDED_OPENINGS.get(asset_key)
        if spec is None:
            handle = _say_recorded_opening(
                _recorded_opening_transcript(config),
                _recorded_opening_source(config),
            )
            await handle.wait_for_playout()
            return

        bridge = spec.get("bridge")
        timed_chunks = spec["timed_chunks"]
        full_transcript = _recorded_opening_timed_transcript(timed_chunks)
        _say_meta_queue.append(
            {
                "source": opening_config.get("source", "recorded_opening"),
                "text": full_transcript,
                "llm_log_path": "",
            },
        )
        handle = session.say(
            _timed_opening_text(timed_chunks),
            audio=_recorded_opening_audio(
                f"asset://{spec['asset']}",
                _recorded_opening_preloaded,
            ),
            allow_interruptions=True,
            add_to_chat_ctx=True,
        )
        await handle.wait_for_playout()
        if handle.interrupted:
            spoken = _spoken_text_from_handle(handle).strip()
            if "Much better." not in spoken and bridge is not None:
                assistant._pending_opening_bridge = (
                    lambda b=bridge: _schedule_opening_bridge(b)
                )

    def _fire_generate_reply(
        reason: str,
        source_id: str,
        allow_interruptions: bool = True,
        user_input: str | None = None,
    ):
        nonlocal generation_seq, _pending_reply_timer
        _pending_reply_timer = None
        generation_seq += 1
        generation_id = f"gen-{generation_seq:06d}"
        last_role = (
            assistant._chat_ctx.items[-1].role if assistant._chat_ctx.items else "none"
        )
        trigger = {
            "generation_id": generation_id,
            "reason": reason,
            "source_id": source_id,
            "user_is_speaking": user_is_speaking,
            "last_chat_role": last_role,
            "ts_utc": now_utc_iso(),
            "monotonic_ms": monotonic_ms(),
        }
        enqueue_trace_context = getattr(llm_model, "enqueue_trace_context", None)
        if callable(enqueue_trace_context):
            enqueue_trace_context(trigger)
        _log.llm_thinking(
            reason=reason,
            queued_speech=len(_queued_speech),
        )
        reply_kwargs = {"allow_interruptions": allow_interruptions}
        if user_input is not None:
            reply_kwargs["user_input"] = user_input
        maybe_result = session.generate_reply(**reply_kwargs)
        if isinstance(maybe_result, asyncio.Task):
            maybe_result.add_done_callback(_log_reply_task)
        return maybe_result

    def trigger_generate_reply(
        reason: str,
        source_id: str,
        *,
        allow_interruptions: bool = True,
        wait_for_completion: bool = False,
        user_input: str | None = None,
    ):
        nonlocal _pending_reply_timer
        if _pending_reply_timer is not None:
            _pending_reply_timer.cancel()
            _pending_reply_timer = None

        if wait_for_completion:
            return _fire_generate_reply(
                reason,
                source_id,
                allow_interruptions,
                user_input,
            )

        loop = asyncio.get_event_loop()
        _pending_reply_timer = loop.call_later(
            _NOTIFY_COALESCE_S,
            _fire_generate_reply,
            reason,
            source_id,
            allow_interruptions,
            user_input,
        )

    def _invalidate_current_generation(reason: str, source_id: str) -> None:
        """Cancel in-flight FastBrain generation and re-trigger with updated context.

        Called when a significant IPC event (slow brain notification, outbound
        message confirmation) arrives while the FastBrain LLM is mid-generation.
        The 50 ms coalescence in ``trigger_generate_reply`` naturally collapses
        bursts (e.g. notification + message_sent arriving ~100 ms apart) into a
        single regeneration.
        """
        if not assistant.user_turn_generating:
            return
        _log.info(f"Invalidating in-flight generation: {reason}")
        session.interrupt()
        trigger_generate_reply(reason=reason, source_id=source_id)

    if channel == "phone_call":
        user_utterance_event = InboundPhoneUtterance
        assistant_utterance_event = OutboundPhoneUtterance
    elif channel == "whatsapp_call":
        user_utterance_event = InboundWhatsAppCallUtterance
        assistant_utterance_event = OutboundWhatsAppCallUtterance
    elif channel == "google_meet":
        user_utterance_event = InboundGoogleMeetUtterance
        assistant_utterance_event = OutboundGoogleMeetUtterance
    elif channel == "teams_meet":
        user_utterance_event = InboundTeamsMeetUtterance
        assistant_utterance_event = OutboundTeamsMeetUtterance
    else:
        user_utterance_event = InboundUnifyMeetUtterance
        assistant_utterance_event = OutboundUnifyMeetUtterance

    def _consume_speech_start(*, assistant_side: bool) -> str | None:
        """The captured start for this side's current line, cleared on read.

        Clearing matters: one speaking span can finalise into several items, and
        reusing the span's start for each would place the later ones at the
        moment the whole turn began. Whatever is left unmatched falls back to the
        commit timestamp, which is the pre-existing behaviour.
        """
        nonlocal user_speech_started_at, agent_speech_started_at
        started = agent_speech_started_at if assistant_side else user_speech_started_at
        if assistant_side:
            agent_speech_started_at = None
        else:
            user_speech_started_at = None
        return started.isoformat() if started else None

    async def _publish_assistant_utterance(text: str) -> None:
        if channel == "google_meet":
            event = OutboundGoogleMeetUtterance(
                contact=contact,
                content=text,
                participant_names=_get_meet_participant_names() or None,
            )
        elif channel == "teams_meet":
            event = OutboundTeamsMeetUtterance(
                contact=contact,
                content=text,
                participant_names=_get_meet_participant_names() or None,
            )
        elif channel == "unify_meet":
            names, cids = _unify_meet_stamp()
            event = OutboundUnifyMeetUtterance(
                contact=contact,
                content=text,
                participant_names=names,
                participant_contact_ids=cids,
            )
        else:
            event = assistant_utterance_event(contact, content=text)
        event.speech_started_at = _consume_speech_start(assistant_side=True)
        await event_broker.publish(
            f"app:comms:{channel}_utterance",
            event.to_json(),
        )

    credit_gate_task: asyncio.Task | None = None
    explicit_stop_requested = False
    shutdown_completed = False

    # Register cleanup as a LiveKit shutdown callback so it runs on any
    # exit path: participant disconnect or explicit stop.
    async def _on_job_shutdown():
        nonlocal shutdown_completed
        if shutdown_completed:
            return
        shutdown_completed = True
        if speaker_tracker is not None:
            # Flush pending embeddings and fire a partial auto-enrollment for
            # single-voice calls that ended before reaching the full target.
            # finalize() also emits the attribution summary.
            await speaker_tracker.finalize()
            if speaker_event_tasks:
                await asyncio.gather(
                    *list(speaker_event_tasks),
                    return_exceptions=True,
                )
        if credit_gate_task is not None:
            await utils.aio.cancel_and_wait(credit_gate_task)
        if _meet_screenshare_task is not None:
            await utils.aio.cancel_and_wait(_meet_screenshare_task)
        await screen_capture.close()
        await webcam_capture.close()
        # Unify Meet rooms belong to the call session, never to one agent:
        # deleting on agent exit would kick remaining humans/co-assistants.
        # Session end drives departures; LiveKit's empty timeout reaps rooms.
        if channel != "unify_meet":
            await delete_livekit_room(ctx.room.name)
        await publish_call_ended(contact, channel, call_session_id=call_session_id)

    ctx.add_shutdown_callback(_on_job_shutdown)

    # Bridge AgentSession close → job shutdown.  close_on_disconnect
    # (RoomInputOptions, default True) closes the AgentSession when the
    # linked participant leaves, but does NOT resolve the JobContext's
    # shutdown future — so our shutdown callbacks never fire.  Listening
    # for the session "close" event completes the chain.
    @session.on("close")
    def _on_session_close(ev):
        from livekit.agents.voice.events import CloseReason

        if ev.reason == CloseReason.PARTICIPANT_DISCONNECTED:
            ctx.shutdown(reason="participant_disconnected")

    def _check_quiescence_transition() -> None:
        nonlocal _was_quiescent
        now_quiescent = _is_pipeline_quiescent()
        if now_quiescent != _was_quiescent:
            _was_quiescent = now_quiescent
            import json as _json

            asyncio.create_task(
                event_broker.publish(
                    "app:comms:pipeline_quiescent",
                    _json.dumps({"quiescent": now_quiescent}),
                ),
            )

    @session.on("user_state_changed")
    def _on_user_state_changed(ev):
        nonlocal user_is_speaking, user_state_seq, user_speech_started_at
        user_state_seq += 1
        state_id = f"usrstate-{user_state_seq:06d}"
        user_is_speaking = ev.new_state == "speaking"
        if user_is_speaking:
            # VAD onset: the moment their line became audible in the room.
            user_speech_started_at = prompt_now(as_string=False)
        if not user_is_speaking:
            # The user just freed the floor: a queued slow-brain line should play
            # at the next silent moment, not wait for the next agent-state cycle.
            maybe_speak_queued()
        _log.user_state(ev.new_state, state_id=state_id)
        _check_quiescence_transition()

    @session.on("agent_state_changed")
    def _on_agent_state_changed(ev):
        """Try queued speech only after the agent settles into a quiescent state.

        We intentionally do NOT trigger from user_state_changed because there is
        a gap between VAD silence detection and the turn detector confirming the
        turn. During that gap, agent_state is still "listening" and current_speech
        is None — firing then would race ahead of the fast brain's reply.

        Triggering here guarantees the full thinking → speaking → listening cycle
        has completed before queued notification speech plays.
        """
        nonlocal agent_speech_started_at
        if ev.new_state == "speaking":
            # Playback start, not `speech_created` -- that fires when the reply
            # is scheduled, which can precede audio by a noticeable margin.
            agent_speech_started_at = prompt_now(as_string=False)
        if ev.new_state in ("listening", "idle"):
            maybe_speak_queued()
        _check_quiescence_transition()

    # -- Diarization: last-speaker tracking + name correlation (meets) --
    # The speaker tracker itself is fed from the STT filter (which sees every
    # final, including gated background speech); this handler only sees
    # forwarded finals, so it tracks the current *turn's* speaker.
    @session.on("user_input_transcribed")
    def _on_user_input_transcribed(ev):
        nonlocal _meet_last_speaker_id
        if not ev.is_final or not ev.speaker_id:
            return
        _meet_last_speaker_id = ev.speaker_id
        if channel in ("google_meet", "teams_meet"):
            # Matched against the platform's speaking spans over this turn's own
            # span, not against whoever is speaking at this instant: a final
            # arrives *after* its speaker stopped, by which point the platform
            # has already reported speech_off and nobody is speaking at all.
            turn_ended_at = prompt_now(as_string=False).timestamp()
            turn_started_at = (
                user_speech_started_at.timestamp()
                if user_speech_started_at is not None
                else turn_ended_at
            )
            speaking_name = _meet_speech_windows.speaker_during(
                turn_started_at,
                turn_ended_at,
            )
            if speaking_name and speaking_name != _meet_display_name:
                _meet_speaker_votes.observe(ev.speaker_id, speaking_name)

    # -- Screenshot state --
    screenshot_history = ScreenshotHistory()
    assistant_screen_share_active = False
    # Whether the desktop belongs on *this call's* stage, which is a different
    # question from whether anyone is watching it: a Desktop tab open beside the
    # call watches without the room having anything to show. Kept apart so that
    # capturing frames for the brain and mounting an iframe on every
    # participant's screen cannot be decided by one another's answer.
    assistant_desktop_on_stage = False
    user_remote_control_active = False
    _agent_service_url: str | None = (
        (meta.get("agent_service_url") if meta else None)
        or os.environ.get("AGENT_SERVICE_URL")
        or None
    )
    _visual_ctx_msg_id: str | None = None
    import aiohttp as _aiohttp

    _screenshot_http_session = _aiohttp.ClientSession()

    def _publish_assistant_screenshare_state() -> None:
        """Tell every Console client in the room whether to mount the desktop.

        The assistant's desktop is not a published video track -- each
        participant mounts the VM's own liveview -- so the room has to be told
        about it out of band, and every participant needs the same answer.
        Keyed by assistant: a room call can carry several, each presenting its
        own desktop.

        Only a Unify Meet has Console participants listening. A browser meet
        reaches its audience through the bot's own screenshare instead.

        Defined here, alongside the flag it reports, rather than beside the
        other room publishers further down: the participant-join and
        notification callbacks both reach it, and they can fire during
        ``session.start`` -- before a definition placed after that await would
        have been bound.
        """
        if channel != "unify_meet":
            return
        state = {
            "type": "assistant_screenshare",
            "assistantId": str(SESSION_DETAILS.assistant.agent_id or ""),
            "active": assistant_desktop_on_stage,
        }

        async def _send() -> None:
            await ctx.room.local_participant.publish_data(
                json.dumps(state).encode(),
                topic="agent_status",
                reliable=True,
            )

        asyncio.create_task(_send())

    def _clear_visual_context(source: str | None = None) -> None:
        """Remove visual context from chat contexts and clear screenshot history."""
        nonlocal _visual_ctx_msg_id
        screenshot_history.clear(source=source)
        if not screenshot_history.build_visual_context_content():
            for ctx in (assistant._chat_ctx, session.history):
                if _visual_ctx_msg_id is not None:
                    idx = ctx.index_by_id(_visual_ctx_msg_id)
                    if idx is not None:
                        ctx.items.pop(idx)
            _visual_ctx_msg_id = None

    def _inject_visual_context() -> None:
        """Replace the visual context system message in the chat context."""
        nonlocal _visual_ctx_msg_id
        content = screenshot_history.build_visual_context_content()
        if not content:
            return
        # Remove the previous visual context message if present.
        for ctx in (assistant._chat_ctx, session.history):
            if _visual_ctx_msg_id is not None:
                idx = ctx.index_by_id(_visual_ctx_msg_id)
                if idx is not None:
                    ctx.items.pop(idx)
        msg = assistant._chat_ctx.add_message(role="user", content=content)
        session.history.add_message(
            role="user",
            content=content,
            id=msg.id,
        )
        _visual_ctx_msg_id = msg.id

    def _publish_screenshot(entry: ScreenshotEntry, filepath: str) -> None:
        """Fire-and-forget: write to disk and publish to slow brain via IPC."""

        async def _background():
            await asyncio.to_thread(write_screenshot_to_disk, entry, filepath)
            await event_broker.publish(
                "app:comms:screenshot",
                json.dumps(
                    {
                        "b64": entry.b64,
                        "utterance": entry.utterance,
                        "timestamp": entry.timestamp.isoformat(),
                        "source": entry.source,
                        "filepath": filepath,
                        "attribution": entry.attribution,
                    },
                ),
            )

        asyncio.create_task(_background())

    def _handle_screenshot(entry: ScreenshotEntry) -> None:
        """Process a captured screenshot: history, visual context, disk, IPC."""
        filepath = generate_screenshot_path(entry)
        screenshot_history.add(entry, filepath)
        _inject_visual_context()
        if entry.source != "assistant":
            _publish_screenshot(entry, filepath)

    def _push_meet_frame(utterance: str = "") -> None:
        """Hand the current shared screen to both brains.

        The slot on its own reaches nobody: the fast brain reads it from its
        visual context and the slow brain from its screenshot buffer, and both are
        filled here. Without this the frame sits in the slot unseen -- which is
        exactly what happened in a meeting held entirely over Meet chat, where a
        screen was shared, stored, and never once looked at.
        """
        from datetime import datetime, timezone

        nonlocal _meet_last_pushed_frame, _meet_last_push_at

        if not _meet_latest_screenshot:
            return
        _meet_last_pushed_frame = _meet_latest_screenshot
        _meet_last_push_at = time.monotonic()
        _handle_screenshot(
            ScreenshotEntry(
                b64=_meet_latest_screenshot,
                utterance=utterance,
                timestamp=datetime.now(timezone.utc),
                source=channel,
                attribution=_meet_screenshare_sharer,
            ),
        )

    async def _refresh_screenshots() -> None:
        """Capture fresh screenshots from all active sources and update visual context.

        Called before any module that needs the latest visual state (e.g., the
        notification reply evaluator, the fast-brain LLM).  Sync captures
        (user screen, webcam) are ~1 ms.  The assistant capture reads from the
        agent-service screenshot cache (~0 ms) unless the user has remote
        control, in which case a live capture (~500 ms) is used.

        Every publisher of a source is filed, not just the focused one: a group
        call has several screens and several faces, and taking one frame per
        source made the others invisible to both brains. Bounded by the same
        limit the fast brain applies, since a frame filed beyond it is written
        and published only to be dropped again.
        """
        from datetime import datetime, timezone

        for source, capture_manager in (
            ("user", screen_capture),
            ("webcam", webcam_capture),
        ):
            for b64, identity in capture_manager.capture_screenshots(
                limit=MAX_VISUALS_PER_SOURCE,
            ):
                _handle_screenshot(
                    ScreenshotEntry(
                        b64=b64,
                        utterance="",
                        timestamp=datetime.now(timezone.utc),
                        source=source,
                        attribution=_sharer_name_for_identity(identity),
                    ),
                )

        if assistant_screen_share_active:
            entry = await capture_assistant_screenshot(
                utterance="",
                cached=not user_remote_control_active,
                fb_logger=_log,
                agent_service_url=_agent_service_url,
                http_session=_screenshot_http_session,
            )
            if entry and assistant_screen_share_active:
                _handle_screenshot(entry)

        if channel in ("google_meet", "teams_meet"):
            _push_meet_frame()

    async def _meet_screenshare_poll_loop() -> None:
        """Keep the shared screen fresh, and keep feeding it to both brains.

        Polled rather than pushed: the frames live in comms, and the LiveKit data
        channel that carries participant events is no place for a screenshot. The
        loop runs only between ``screenshare_on`` and the last ``screenshare_off``,
        so an ordinary meeting costs nothing.

        It also pushes, not just polls. Every other visual source in this file is
        sampled on a spoken user turn, which silently made a shared screen
        invisible in any meeting conducted over chat, or with a screen up while
        nobody happens to be talking. A share is ambient context, so it is fed on
        its own schedule -- rate-limited, and only when the picture has actually
        changed, because each push costs a stored and registered image downstream.
        """
        nonlocal _meet_latest_screenshot, _meet_screenshare_sharer
        nonlocal _meet_fetch_misses

        while True:
            frame = await fetch_meet_screenshare_frame(
                ctx.room.name,
                http_session=_screenshot_http_session,
                fb_logger=_log,
            )
            if frame is None:
                # Nobody sharing yet, or the frame aged out. Drop what we hold
                # rather than describing a screen that is no longer up.
                _meet_latest_screenshot = None
                _meet_screenshare_sharer = ""
                _meet_fetch_misses += 1
                # Somebody is presenting (or this loop would not be running) and
                # yet no frame arrives: that is a fault, not a quiet meeting.
                # Logged once per outage because the alternative -- staying silent
                # -- is indistinguishable from working, which is what made an
                # unprovisioned bucket invisible from this side for a whole call.
                if _meet_fetch_misses == _MEET_FETCH_MISS_ALERT_AFTER:
                    _log.screenshot(
                        f"Meet screenshare: presenter active but no frame after "
                        f"{_meet_fetch_misses} polls (room={ctx.room.name}) -- "
                        f"the relay or its store is not delivering",
                    )
            else:
                if _meet_fetch_misses >= _MEET_FETCH_MISS_ALERT_AFTER:
                    _log.screenshot(
                        f"Meet screenshare: frames recovered after "
                        f"{_meet_fetch_misses} missed polls",
                    )
                _meet_fetch_misses = 0
                _meet_latest_screenshot = frame.b64
                _meet_screenshare_sharer = frame.participant_name

                changed = _meet_latest_screenshot != _meet_last_pushed_frame
                due = (
                    time.monotonic() - _meet_last_push_at
                    >= _MEET_AMBIENT_PUSH_INTERVAL_S
                )
                if changed and due:
                    _push_meet_frame()

            await asyncio.sleep(_MEET_SCREENSHARE_POLL_INTERVAL_S)

    def _sync_meet_screenshare_poller() -> None:
        """Start or stop the poller to match whether anyone is presenting.

        Also announces the transition to the slow brain, which otherwise has no
        way to know: it sees only frames arriving, so the absence of a frame is
        indistinguishable from a turn that happened not to include one.
        """
        nonlocal _meet_screenshare_task
        nonlocal _meet_latest_screenshot, _meet_screenshare_sharer

        if _meet_sharing and _meet_screenshare_task is None:
            _meet_screenshare_task = asyncio.create_task(
                _meet_screenshare_poll_loop(),
            )
            evt = MeetScreenShareStarted(
                contact=contact,
                reason="participant started sharing",
            )
            asyncio.create_task(event_broker.publish(evt.topic, evt.to_json()))
            return
        if not _meet_sharing and _meet_screenshare_task is not None:
            _meet_screenshare_task.cancel()
            _meet_screenshare_task = None
            _meet_latest_screenshot = None
            _meet_screenshare_sharer = ""
            # The shared screen is gone from the prompt as well as from the slot:
            # a stale snapshot in the fast brain's visual context outlives the
            # share and gets described as if it were still up.
            _clear_visual_context(source=channel)
            evt = MeetScreenShareStopped(
                contact=contact,
                reason="participant stopped sharing",
            )
            asyncio.create_task(event_broker.publish(evt.topic, evt.to_json()))

    @session.on("conversation_item_added")
    def _on_chat_item_added(ev):
        """Publish both user and assistant utterances from a single location."""
        role = getattr(ev.item, "role", None)
        if role not in ("user", "assistant"):
            return
        text = ev.item.text_content or ""
        utterance_id = content_trace_id("utt", f"{role}:{text}")
        say_meta: dict | None = None
        if role == "assistant" and _say_meta_queue:
            for i, candidate in enumerate(_say_meta_queue):
                if match_say_meta(candidate, text):
                    say_meta = _say_meta_queue.pop(i)
                    break
        if role == "user":
            if not assistant._user_speech_logged:
                _log.user_speech(text)
            assistant._user_speech_logged = False
        else:
            source = (say_meta or {}).get("source", "reply")
            if say_meta and say_meta.get("llm_log_path"):
                log_path = say_meta["llm_log_path"]
            elif source == "reply":
                log_path = getattr(llm_model, "last_log_path", "")
            else:
                log_path = ""
            _log.assistant_speech(text, source=source, llm_log_path=log_path)
        if role == "user":
            from datetime import datetime, timezone

            # Paired with the utterance, one entry per publisher: "what am I
            # looking at here?" asked in a room of several shares needs all of
            # them, each named, rather than whichever happened to be focused.
            for shot_source, capture_manager in (
                ("user", screen_capture),
                ("webcam", webcam_capture),
            ):
                for b64, identity in capture_manager.capture_screenshots(
                    limit=MAX_VISUALS_PER_SOURCE,
                ):
                    _handle_screenshot(
                        ScreenshotEntry(
                            b64=b64,
                            utterance=text,
                            timestamp=datetime.now(timezone.utc),
                            source=shot_source,
                            attribution=_sharer_name_for_identity(identity),
                        ),
                    )
            if channel in ("google_meet", "teams_meet"):
                _push_meet_frame(utterance=text)

            async def _publish_user_utterance(text: str) -> None:
                nonlocal _meet_last_speaker_id
                (
                    resolved_contact,
                    speaker_label,
                    dia_sid,
                    speaker_label_source,
                ) = _resolve_speaker()
                _meet_last_speaker_id = None
                # Stamp the current turn so the slow-brain run scheduled after
                # the fast brain completes can be correlated precisely.
                turn_id = assistant._user_turn_seq
                if channel == "google_meet":
                    event = InboundGoogleMeetUtterance(
                        contact=resolved_contact,
                        content=text,
                        speaker_label=speaker_label,
                        participant_names=_get_meet_participant_names() or None,
                        diarization_speaker_id=dia_sid,
                        turn_id=turn_id,
                        speaker_label_source=speaker_label_source,
                    )
                elif channel == "teams_meet":
                    event = InboundTeamsMeetUtterance(
                        contact=resolved_contact,
                        content=text,
                        speaker_label=speaker_label,
                        participant_names=_get_meet_participant_names() or None,
                        diarization_speaker_id=dia_sid,
                        turn_id=turn_id,
                        speaker_label_source=speaker_label_source,
                    )
                elif channel == "unify_meet":
                    names, cids = _unify_meet_stamp(
                        exclude_contact_id=resolved_contact.get("contact_id"),
                    )
                    event = InboundUnifyMeetUtterance(
                        contact=resolved_contact,
                        content=text,
                        turn_id=turn_id,
                        speaker_label=speaker_label,
                        diarization_speaker_id=dia_sid,
                        speaker_label_source=speaker_label_source,
                        participant_names=names,
                        participant_contact_ids=cids,
                    )
                else:
                    event = user_utterance_event(
                        resolved_contact,
                        content=text,
                        turn_id=turn_id,
                        speaker_label=speaker_label,
                        diarization_speaker_id=dia_sid,
                        speaker_label_source=speaker_label_source,
                    )
                event.speech_started_at = _consume_speech_start(assistant_side=False)
                await event_broker.publish(
                    f"app:comms:{channel}_utterance",
                    event.to_json(),
                )

            # Opener-pending turns are still published for the durable transcript.
            # Slow-brain scheduling is gated separately: llm_node returns before
            # classification while _opening_pending, so no FastBrainTurnCompleted
            # is emitted and the utterance handler never calls handle_voice_user_turn.
            asyncio.create_task(
                _publish_user_utterance(text),
            )
        else:
            asyncio.create_task(_publish_assistant_utterance(text))

    # Browser meets reach this process one of two ways.
    #
    # agent_service: the browser is a sibling process in this pod with no way
    # into the LiveKit room, so audio is shuttled through PulseAudio null sinks
    # and the bridge below pumps them.
    #
    # recall: the bot renders our bridge page, which joins the room as an
    # ordinary participant. Audio is then plain LiveKit tracks, exactly as for
    # phone, whatsapp_call and unify_meet -- so no bridge, and none of
    # PortAudio's thread affinity or ring-buffer leak to work around. Opening
    # one anyway would capture silence from an unconnected sink while the real
    # audio sat unheard on the room's tracks.
    assistant = Assistant(
        contact=contact,
        boss=boss,
        channel=channel,
        instructions=system_prompt,
        outbound=outbound,
        normalize_elevenlabs_twin_pronunciation=voice_provider == "elevenlabs",
        speaker_tracker=speaker_tracker,
    )

    # --- Group-call etiquette (multi-party channels only) ---
    # Telephony carries exactly one other person, so every turn there is
    # necessarily addressed to the assistant and no provider is wired: the 1:1
    # path keeps replying to everything, as it should.
    if channel in MULTI_PARTY_CHANNELS:

        def _other_participant_names() -> list[str]:
            if channel == "unify_meet":
                return [
                    name
                    for name in (
                        (member.get("display_name") or "").strip()
                        for member in unify_meet_roster
                        if member.get("kind") == "human"
                    )
                    if name
                ]
            # Browser meets: the platform reports one roster and does not mark
            # which entries are bots, so every other participant counts. A stray
            # notetaker inflating the count is harmless — it only means the
            # assistant reads the room before speaking.
            return _get_meet_participant_names()

        assistant._other_participants_provider = _other_participant_names

    # --- Multi-assistant speaking floor (org meets only) ---
    # Assistants in a shared org room coordinate playout over the data channel
    # so they never talk over each other. Solo calls skip the claim window via
    # the peer probe, so 1:1 latency is unchanged.
    if call_session_id and channel == "unify_meet":

        def _peer_assistant_names() -> list[str]:
            """Assistants other than this one, by display name.

            Self-exclusion is by id, falling back to display name when the
            roster crossed the wire without one. Listing itself here is the
            failure that matters: a peer being present is what puts the fast
            brain's unusable-decision fallbacks on silence, so an assistant that
            counts itself would start dropping turns on a call where every turn
            is its own.
            """
            own_id = SESSION_DETAILS.assistant.agent_id
            own_name = (SESSION_DETAILS.assistant.name or "").strip()
            names: list[str] = []
            for member in unify_meet_roster:
                if member.get("kind") != "assistant":
                    continue
                member_id = member.get("assistant_id")
                if member_id is not None and own_id is not None:
                    if str(member_id) == str(own_id):
                        continue
                name = (member.get("display_name") or "").strip()
                if name and name != own_name:
                    names.append(name)
            return names

        assistant._peer_assistants_provider = _peer_assistant_names

        def _floor_peers_present() -> bool:
            remotes = getattr(ctx.room, "remote_participants", {}) or {}
            for participant in remotes.values():
                attrs = getattr(participant, "attributes", None) or {}
                if attrs.get("unify_assistant_id") or attrs.get("lk.agent.state"):
                    return True
            return False

        async def _publish_floor(payload: dict) -> None:
            await ctx.room.local_participant.publish_data(
                json.dumps(payload).encode(),
                topic=FLOOR_TOPIC,
                reliable=True,
            )

        assistant.meet_floor = MeetFloor(
            local_id=str(SESSION_DETAILS.assistant.agent_id or os.getpid()),
            publish=_publish_floor,
            peer_probe=_floor_peers_present,
            log=_log.info,
        )

        @ctx.room.on("data_received")
        def _on_floor_data(packet) -> None:
            if getattr(packet, "topic", "") != FLOOR_TOPIC:
                return
            if assistant.meet_floor is not None:
                assistant.meet_floor.handle_message(bytes(packet.data))

    if channel in ("google_meet", "teams_meet"):
        # The relay republishes the meeting platform's participant events into
        # this room. Roster changes are applied from here as well as from the
        # CM's ten-second poll: the poll is what proves who is present, but a
        # join or leave is worth acting on the moment it happens, and the
        # join/leave publish downstream diffs against the previous set, so the
        # poll re-reporting the same person emits nothing a second time.
        @ctx.room.on("data_received")
        def _on_recall_event(packet) -> None:
            nonlocal _meet_cached_active_speaker
            if getattr(packet, "topic", "") != RECALL_EVENT_TOPIC:
                return
            try:
                message = json.loads(bytes(packet.data).decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                return

            event = parse_relayed_event(message)
            if event is None or event.participant is None:
                return
            participant = event.participant

            # Ahead of the name guard below, and keyed on id rather than name: a
            # platform that reports a presenter without a display name would
            # otherwise drop the event, and with it every frame of the share.
            # The label comes from the frame itself, so only the id is needed.
            if event.name == EVENT_SCREENSHARE_ON:
                if participant.id not in _meet_sharing:
                    _meet_sharing.append(participant.id)
                _sync_meet_screenshare_poller()
                return
            if event.name == EVENT_SCREENSHARE_OFF:
                if participant.id in _meet_sharing:
                    _meet_sharing.remove(participant.id)
                _sync_meet_screenshare_poller()
                return

            name = participant.name.strip()
            if not name or name == _meet_display_name:
                return

            at = prompt_now(as_string=False).timestamp()
            if event.name in ROSTER_EVENTS:
                if event.name == EVENT_LEAVE:
                    _drop_meet_participant(participant.id)
                else:
                    _merge_meet_participant(
                        {
                            "id": participant.id,
                            "name": name,
                            "email": participant.email,
                            "is_host": participant.is_host,
                        },
                    )
                _publish_meet_roster_changes()
            elif event.name == EVENT_SPEECH_ON:
                _meet_cached_active_speaker = name
                _meet_speech_windows.speech_on(name, at)
            elif event.name == EVENT_SPEECH_OFF:
                # Only clear if this speaker is still the one on record, or a
                # trailing speech_off would blank whoever started next.
                if _meet_cached_active_speaker == name:
                    _meet_cached_active_speaker = None
                _meet_speech_windows.speech_off(name, at)
            elif event.name == EVENT_CHAT_MESSAGE:
                text = _strip_chat_html(event.chat_text or "").strip()
                if not text:
                    return
                # Paired with the message, exactly as a spoken turn is: someone
                # typing "what do you make of this?" is pointing at the screen
                # they are sharing, and the reply is built from this buffer. A
                # whole meeting can happen in chat with nobody speaking, which is
                # how a shared screen went unseen despite being stored.
                _push_meet_frame(utterance=text)
                chat_cls = (
                    TeamsMeetChatMessage
                    if channel == "teams_meet"
                    else GoogleMeetChatMessage
                )
                evt = chat_cls(
                    contact=contact,
                    sender_name=name,
                    content=text,
                    sender_email=participant.email,
                )
                asyncio.create_task(event_broker.publish(evt.topic, evt.to_json()))

    # The opener hold applies whenever an ``opener`` opening config is present,
    # including inbound-shaped legs of agent-initiated calls (e.g. the WhatsApp
    # permission-callback call), where ``outbound`` is False.
    assistant._opening_pending = opening_config["mode"] == "opener"
    # A pre-armed hang-up gate (slow brain sanctioned the close at call
    # placement, e.g. an expected-short call) starts armed from the first turn
    # — no IPC round trip needed before the fast brain can end the call.
    if pre_armed_hang_up_gate:
        assistant._hang_up_gate_reason = pre_armed_hang_up_gate
        _log.info(f"Hang-up gate pre-armed at dispatch: {pre_armed_hang_up_gate}")
    opening_briefing = str(opening_config.get("briefing", "")).strip()
    if opening_briefing:
        assistant._call_briefing = opening_briefing
        briefing_note = _CALL_BRIEFING_SYSTEM_NOTE.format(briefing=opening_briefing)
        assistant._chat_ctx.add_message(role="system", content=[briefing_note])
        session.history.add_message(role="system", content=[briefing_note])
        _log.info("Injected call briefing into voice context")
    credit_gate_monitor = FastBrainBillingGateMonitor()
    assistant.set_credit_gate_state_provider(lambda: credit_gate_monitor.state)
    # In-flight says (proactive/guidance still playing, not yet committed) live
    # in _say_meta_queue until their playout commits them to history. Set as a
    # direct attribute so it works uniformly on the real Assistant and the test
    # fakes without each needing a setter.
    credit_gate_task = asyncio.create_task(
        credit_gate_monitor.run(),
        name="fast_brain_credit_gate_monitor",
    )

    async def _capture_screenshots_for_llm(chat_ctx) -> None:
        """Capture fresh screenshots and inject them into the LLM's chat_ctx.

        The LiveKit pipeline passes a **copy** of the chat context to
        ``llm_node``.  ``_refresh_screenshots`` updates the live
        ``session.history`` (for subsequent turns and IPC), but that copy
        is stale.  After refreshing, we rebuild the visual context content
        and inject it directly into the ``chat_ctx`` parameter so the
        current LLM call sees the screenshot.
        """
        try:
            copy_visual_id = _visual_ctx_msg_id

            await _refresh_screenshots()

            content = screenshot_history.build_visual_context_content()
            if content:
                if copy_visual_id is not None:
                    idx = chat_ctx.index_by_id(copy_visual_id)
                    if idx is not None:
                        chat_ctx.items.pop(idx)
                msg = chat_ctx.add_message(role="user", content=content)
                chat_ctx.items.pop()
                chat_ctx.items.insert(-1, msg)
        except Exception as e:
            print(f"[llm_node] screenshot capture error (non-fatal): {e}")

    assistant._capture_screenshots_for_llm = _capture_screenshots_for_llm

    @session.on("user_state_changed")
    def _on_outbound_first_turn_speaking_duration(ev) -> None:
        if not assistant._opening_pending:
            return
        if assistant._first_user_turn.is_set():
            return
        if ev.new_state == "speaking":
            if assistant._first_turn_speaking_started_at is None:
                assistant._first_turn_speaking_started_at = time.monotonic()
        elif assistant._first_turn_speaking_started_at is not None:
            assistant._first_turn_duration_s = (
                time.monotonic() - assistant._first_turn_speaking_started_at
            )

    rio = RoomInputOptions(
        noise_cancellation=(
            noise_cancellation.BVC() if sys.platform == "darwin" else None
        ),
        close_on_disconnect=(
            channel not in ("google_meet", "teams_meet", "unify_meet")
        ),
    )

    # Publish call started (shared helper)
    await publish_call_started(contact, channel, call_session_id=call_session_id)

    pending_notifications: list[tuple[str, str, bool, str, str, str]] = (
        []
    )  # (message, spoken_message, should_speak, notification_id, notification_source, llm_log_path)
    session_ready = False

    def _mark_user_joined(reason: str) -> None:
        nonlocal speech_gate_open
        if user_joined_event.is_set():
            return
        _log.call_status(f"user_joined:{reason}")
        speech_gate_open = True
        user_joined_event.set()
        assistant.set_call_received()

    async def _graceful_meet_stop() -> None:
        """End a Unify Meet by letting the Console disconnect itself first.

        Publishing ``call_ended`` prompts the browser to call ``room.disconnect()``
        — a clean, client-initiated WebRTC teardown that closes its data channels
        via ``onclose`` — before the agent shuts down and the room is deleted.
        That avoids the abrupt server-side eviction that otherwise fires the
        browser's ``RTCDataChannel.onerror`` ("Unknown DataChannel error").
        """
        try:
            await ctx.room.local_participant.publish_data(
                json.dumps({"type": "call_ended"}).encode(),
                topic="agent_status",
                reliable=True,
            )
        except Exception as exc:
            _log.call_status(f"call_ended publish failed: {exc}")
        await asyncio.sleep(MEET_GRACEFUL_LEAVE_GRACE_S)
        ctx.shutdown(reason="stopped")

    def on_status(data: dict) -> None:
        """Handle status events (call_answered, stop, meet_session_id)."""
        nonlocal explicit_stop_requested, meet_session_id, speech_gate_open
        nonlocal _unify_meet_active_identity
        event_type = data.get("type", "")
        _log.call_status(event_type)

        if event_type == "call_answered":
            call_answered_flag.set()
            # Answered implies received even when the joined gate was already
            # open (e.g. unify_meet, where the gate is pre-set at startup and
            # _mark_user_joined would early-return without flipping this).
            assistant.set_call_received()
            _mark_user_joined("call_answered")
        elif event_type == "hang_up_gate":
            if data.get("armed"):
                assistant._hang_up_gate_reason = str(data.get("reason") or "")
                _log.info(
                    "Hang-up gate armed: "
                    f"{assistant._hang_up_gate_reason or '(no reason given)'}",
                )
            else:
                assistant._hang_up_gate_reason = None
                _log.info("Hang-up gate disarmed")
        elif event_type in ("meet_session_id", "gmeet_session_id"):
            meet_session_id = data.get("session_id", "")
        elif event_type == "meet_roster":
            # Browser-meet roster, pushed by the CM because only it holds the
            # backend credentials. Publishes join/leave so the brain knows who
            # is in the room, and feeds _get_meet_participant_names, which is
            # what names a diarized voice. Realtime join/leave events already
            # maintain the same cache; this is the reconciliation pass that
            # corrects it if one was missed.
            incoming = data.get("participants") or []
            if isinstance(incoming, list):
                _reconcile_meet_roster([p for p in incoming if isinstance(p, dict)])
                _publish_meet_roster_changes()
        elif event_type == "unify_meet_roster":
            incoming = data.get("participants") or []
            if isinstance(incoming, list):
                unify_meet_roster.clear()
                unify_meet_roster.extend(
                    [p for p in incoming if isinstance(p, dict)],
                )
                _log.info(
                    f"Unify Meet roster refreshed ({len(unify_meet_roster)} members)",
                )
        elif event_type == "stop":
            explicit_stop_requested = True
            if channel == "unify_meet":
                asyncio.create_task(_graceful_meet_stop())
            else:
                ctx.shutdown(reason="stopped")

    @ctx.room.on("participant_connected")
    def _on_room_participant_connected(participant):
        identity = getattr(participant, "identity", "") or ""
        if channel == "unify_meet":
            _merge_unify_meet_roster_from_identity(identity)
            # A client that joins mid-share starts with nothing mounted, so the
            # state has to be restated for it. Only when active: "not sharing"
            # is what a fresh client already assumes.
            if assistant_desktop_on_stage:
                _publish_assistant_screenshare_state()
        if joined_gate_required and not outbound:
            _mark_user_joined("participant_connected")

    @ctx.room.on("active_speakers_changed")
    def _on_active_speakers_changed(speakers):
        nonlocal _unify_meet_active_identity
        if channel != "unify_meet":
            return
        for speaker in speakers or []:
            identity = getattr(speaker, "identity", "") or ""
            if identity.startswith("user-"):
                _unify_meet_active_identity = identity
                _merge_unify_meet_roster_from_identity(identity)
                return

    def _is_pipeline_quiescent() -> bool:
        """True when the voice pipeline is completely idle (no speech in flight)."""
        if user_is_speaking:
            return False
        if session.agent_state not in ("listening", "idle"):
            return False
        current = session.current_speech
        if current is not None and not current.done:
            return False
        return True

    def _queued_speech_block_reason() -> str:
        """Why a queued slow-brain line cannot play yet, or "" if the floor is free.

        Deliberately narrower than ``_is_pipeline_quiescent``: a ready line is held
        ONLY while someone occupies the floor (the user speaking, or assistant audio
        actually playing). The agent merely "thinking" (generating a reply) does not
        block it - filler/answer ordering is handled by the ``_slow_brain_responded_turn``
        suppression in ``llm_node``, not by withholding the real line.
        """
        if user_is_speaking:
            return "user_speaking"
        current = session.current_speech
        if current is not None and not current.done:
            return "assistant_speaking"
        return ""

    def _spoken_text_from_handle(handle: object) -> str:
        """Concatenate the assistant text actually persisted for a say handle.

        On interruption LiveKit records only the synchronized (actually-spoken)
        transcript, so this is the prefix the caller really heard.
        """
        items = getattr(handle, "chat_items", None) or []
        texts: list[str] = []
        for item in items:
            if getattr(item, "role", None) != "assistant":
                continue
            content = getattr(item, "text_content", None)
            if content:
                texts.append(content)
        return " ".join(texts)

    async def _publish_voice_interrupt(spoken: str, remainder: str) -> None:
        """Hand an unheard remainder to the slow brain to re-surface next turn."""
        await event_broker.publish(
            VoiceInterrupt.topic,
            VoiceInterrupt(
                contact=contact,
                spoken_prefix=spoken,
                unheard_remainder=remainder,
            ).to_json(),
        )
        from unify.logger import LOGGER

        LOGGER.info(
            "⬥ [FastBrain] Reported unheard remainder to the slow brain.",
        )

    # Expose to Assistant.llm_node (a class method, outside this closure) so it
    # can hand off when it decides not to resume an interrupted line itself.
    assistant._publish_voice_interrupt = _publish_voice_interrupt

    async def _publish_fast_brain_turn_completed(
        *,
        turn_id: int,
        user_content: str,
        classification: str,
        intended_speech: str,
    ) -> None:
        """Schedule the slow-brain run after the fast brain finishes a user turn."""
        await event_broker.publish(
            FastBrainTurnCompleted.topic,
            FastBrainTurnCompleted(
                contact=contact,
                turn_id=turn_id,
                user_content=user_content,
                classification=classification,
                intended_speech=intended_speech,
            ).to_json(),
        )

    assistant._publish_fast_brain_turn_completed = _publish_fast_brain_turn_completed

    def _register_interruptible_tts(
        handle: object,
        full_text_getter,
        notification_source: str,
    ) -> None:
        """Register an in-flight TTS line so a barge-in can be resumed.

        The slow brain owns all substantive speech; when the caller interrupts,
        only the spoken prefix lands in the transcript, so the missed remainder
        would otherwise be lost. On interruption we stash the remainder as a
        claimable continuation candidate. The fast brain is the single front
        door: the next ``llm_node`` turn always decides what happens to it -
        resume it verbatim (CONTINUE, the heavy default), or hand it to the slow
        brain (DEFER -> ``VoiceInterrupt``). A barge-in that produced no
        transcript is resumed automatically. There is no timer: the candidate
        simply waits for that decision, so the fast brain can never lose the race.

        A fast-brain continuation is itself registered here (the ``speech_created``
        observer hands its reply handle to this function), so interrupting a
        resumed line re-stashes a fresh candidate, making continuation recursive
        to arbitrary depth.

        Pre-recorded openings are never passed here, so their hand-crafted tone
        is never continued by the live voice. Proactive silence-filler is never
        resumed or reported.
        """
        if notification_source == "proactive_speech":
            return

        assistant._tts_seq += 1
        seq = assistant._tts_seq
        assistant._active_tts = {
            "handle": handle,
            "source": notification_source,
            "seq": seq,
        }

        async def _after_playout() -> None:
            try:
                await handle.wait_for_playout()
            except Exception:
                return
            if not getattr(handle, "interrupted", False):
                return
            full = (full_text_getter() or "").strip()
            if not full:
                return
            spoken = _spoken_text_from_handle(handle).strip()
            remainder = full
            if spoken and full.startswith(spoken):
                remainder = full[len(spoken) :].strip()
            if not remainder:
                return
            resume_text = compute_resume_text(full, spoken) or remainder
            assistant._pending_continuation = {
                "resume_text": resume_text,
                "remainder": remainder,
                "spoken_prefix": spoken,
                "source": notification_source,
                "seq": seq,
                "consumed": False,
            }

        asyncio.ensure_future(_after_playout())

    async def _finalize_gated_hang_up(handle, farewell: str, trigger: str) -> None:
        """End the call after a gated farewell plays out uninterrupted.

        The cut is aborted (and the conversation simply continues) if the
        caller barges in on the farewell, speaks during the grace window, or
        the slow brain disarms the gate before the window closes.
        """
        try:
            await handle.wait_for_playout()
        except Exception as exc:
            _log.warning(f"Gated hang-up aborted: farewell playout failed: {exc}")
            return
        if getattr(handle, "interrupted", False):
            _log.info("Gated hang-up aborted: farewell was interrupted")
            return
        await asyncio.sleep(HANG_UP_GRACE_S)
        if user_is_speaking or assistant.user_turn_generating:
            _log.info("Gated hang-up aborted: caller spoke during the grace window")
            return
        gate_reason = assistant._hang_up_gate_reason
        if gate_reason is None:
            _log.info("Gated hang-up aborted: gate was disarmed")
            return
        assistant._hang_up_gate_reason = None
        _log.call_status(f"gated_hang_up:{trigger}")
        await event_broker.publish(
            FastBrainHangUp.topic,
            FastBrainHangUp(
                contact=contact,
                farewell=farewell,
                trigger=trigger,
                gate_reason=gate_reason,
            ).to_json(),
        )

    # NOTE: there is intentionally no silence-based auto-close for an armed
    # hang-up gate. A call ends only explicitly: the caller hangs up, or the
    # agent classifies a ``hang_up`` turn (whose farewell is finalized above).

    @session.on("speech_created")
    def _on_speech_created(ev) -> None:
        """Record the reply speech handle for the turn logic to attach to.

        Slow-brain lines and live openings register themselves at ``session.say``
        time. A ``generate_reply`` speech is created (and this fires) before
        ``llm_node`` streams its content, so the handle is recorded here and
        ``llm_node`` — which alone knows the turn's classification — attaches
        continuation registration or the gated hang-up finalizer to it. Claiming
        markers here instead would race: this observer runs before the turn sets
        them, handing them to the wrong (next) speech.
        """
        if getattr(ev, "source", "") != "generate_reply":
            return
        assistant._active_reply_handle = ev.speech_handle

    def _register_reply_continuation(handle: object, full_text: str) -> None:
        _register_interruptible_tts(handle, lambda t=full_text: t, "continuation")

    def _finalize_reply_hang_up(handle: object, farewell: str) -> None:
        asyncio.create_task(
            _finalize_gated_hang_up(handle, farewell, trigger="user_turn"),
        )

    assistant._register_reply_continuation = _register_reply_continuation
    assistant._finalize_reply_hang_up = _finalize_reply_hang_up

    # Console moves for the line about to be spoken, set when the slow brain's
    # notification arrives. A single slot, because a newer slow-brain line
    # supersedes an older one (see ``_queued_speech.clear()``) and its moves
    # must be superseded with it rather than fire against the wrong words.
    _pending_console_steps: list = []

    def _publish_console_script(script_id: str, spoken_text: str) -> None:
        """Hand Console the moves to make while this line plays.

        Sent before playout begins and complete in one message, so the whole
        sequence runs with no further round trip. Console aligns each move
        against the synchronized transcript it is already receiving, which is
        also what makes a barge-in drop the moves not yet reached.
        """
        nonlocal _pending_console_steps
        steps = _pending_console_steps
        _pending_console_steps = []
        if not steps:
            return

        async def _send() -> None:
            await ctx.room.local_participant.publish_data(
                json.dumps(
                    {
                        "type": "console_script",
                        "scriptId": script_id,
                        "spokenText": spoken_text,
                        "steps": steps,
                    },
                ).encode(),
                topic="console_actions",
            )

        asyncio.create_task(_send())

    def _speak_now(
        text: "str | AsyncIterable[str]",
        notification_id: str,
        notification_source: str,
        notification_content: str,
        llm_log_path: str,
    ) -> None:
        if isinstance(text, str):
            _say_meta_queue.append(
                {
                    "notification_id": notification_id,
                    "source": notification_source,
                    "text": text,
                    "llm_log_path": llm_log_path,
                },
            )
            _log.notification_say(text, notification_source=notification_source)
            _publish_console_script(notification_id, text)
            handle = session.say(text, allow_interruptions=True, add_to_chat_ctx=True)
            _register_interruptible_tts(
                handle,
                lambda: text,
                notification_source,
            )
            return

        # Streaming path: ``text`` is an async iterator of token chunks (rewritten
        # speech). The chunks are forwarded to TTS as they arrive so playout starts
        # on the first token. ``say_meta["text"]`` is kept in sync so the
        # ``conversation_item_added`` handler (which fires at playout end) can match
        # the assembled utterance via its prefix.
        say_meta = {
            "notification_id": notification_id,
            "source": notification_source,
            "text": "",
            "llm_log_path": llm_log_path,
        }
        _say_meta_queue.append(say_meta)
        parts: list[str] = []

        async def _tracked_stream() -> "AsyncIterable[str]":
            try:
                async for chunk in text:
                    if not chunk:
                        continue
                    parts.append(chunk)
                    say_meta["text"] = "".join(parts)
                    yield chunk
            except Exception as e:
                from unify.logger import LOGGER

                LOGGER.error(f"⬥ Speech rewrite stream interrupted: {e}")
            final = "".join(parts)
            if final:
                _log.notification_say(final, notification_source=notification_source)

        handle = session.say(
            _tracked_stream(),
            allow_interruptions=True,
            add_to_chat_ctx=True,
        )
        _register_interruptible_tts(
            handle,
            lambda: "".join(parts),
            notification_source,
        )

    def _extract_chat_messages(
        ctx,
        *,
        strip_images: bool = False,
        tail: int | None = None,
    ) -> list[dict]:
        """Convert a LiveKit ChatContext into a list of message dicts for direct LLM calls.

        Parameters
        ----------
        strip_images : bool
            When True, image content parts are dropped and only the text portions
            of multi-modal messages are kept.  Messages that become empty after
            stripping are omitted entirely.
        tail : int | None
            When set, only the last *tail* messages are returned (after any
            image stripping).  Useful for keeping the context compact.
        """
        from livekit.agents.llm import ImageContent

        messages: list[dict] = []
        for item in ctx.items:
            role = getattr(item, "role", None)
            if role is None:
                continue
            raw_content = getattr(item, "content", None)
            if not raw_content:
                continue
            has_images = isinstance(raw_content, list) and any(
                isinstance(c, ImageContent) for c in raw_content
            )
            if has_images:
                if strip_images:
                    text_parts = [c for c in raw_content if isinstance(c, str)]
                    text = " ".join(text_parts).strip()
                    if text:
                        messages.append({"role": role, "content": text})
                else:
                    parts: list[dict] = []
                    for c in raw_content:
                        if isinstance(c, str):
                            parts.append({"type": "text", "text": c})
                        elif isinstance(c, ImageContent) and isinstance(c.image, str):
                            parts.append(
                                {"type": "image_url", "image_url": {"url": c.image}},
                            )
                    if parts:
                        messages.append({"role": role, "content": parts})
            else:
                text = getattr(item, "text_content", None)
                if not text:
                    continue
                messages.append({"role": role, "content": text})
        if tail is not None and len(messages) > tail:
            messages = messages[-tail:]
        return messages

    def apply_notification(
        message: str,
        should_speak: bool = False,
        *,
        spoken_message: str = "",
        notification_id: str = "",
        source: str = "",
        notification_source: str = "",
        llm_log_path: str = "",
    ) -> None:
        # Awareness notifications (should_speak=False) are injected into the fast
        # brain's context so it can surface them in first person. should_speak=True
        # guidance is spoken verbatim by the slow brain (the fast brain only emits
        # a filler phrase), then its spoken text lands in context as an assistant
        # turn. If the caller interrupts mid-utterance, the unheard remainder is
        # reported to the slow brain via VoiceInterrupt (see _speak_now), not the
        # fast brain. Proactive speech is fire-and-forget filler — it never
        # updates context.
        speech_text = spoken_message or message
        if notification_source != "proactive_speech" and message and not should_speak:
            notification_message = f"[notification] {message}"
            assistant._chat_ctx.add_message(
                role="system",
                content=[notification_message],
            )
            session.history.add_message(
                role="system",
                content=[notification_message],
            )

        if should_speak and speech_text:
            if notification_source == "proactive_speech":
                # Proactive speech exists purely to fill silence — never queue it.
                # Play immediately if the pipeline is fully quiescent and nothing
                # else is waiting; otherwise discard silently.
                if not _is_pipeline_quiescent() or _queued_speech:
                    return
                _speak_now(
                    speech_text,
                    notification_id,
                    notification_source,
                    message,
                    llm_log_path,
                )
            else:
                # The slow brain has produced spoken output for the current
                # turn; mark it so any in-flight / re-triggered buffer filler is
                # suppressed rather than played after this real answer, and end
                # the filler streak so the next filler is a fresh first reaction.
                assistant._slow_brain_responded_turn = assistant._user_turn_seq
                assistant._buffers_since_slow_reply = 0
                # Latest slow brain guidance supersedes older queued speech.
                _queued_speech.clear()
                _queued_speech.append(
                    (
                        speech_text,
                        notification_id,
                        notification_source,
                        message,
                        llm_log_path,
                    ),
                )
                maybe_speak_queued()

    def apply_assistant_turn_injection(content: str) -> None:
        if not content:
            return
        assistant._chat_ctx.add_message(role="assistant", content=[content])
        session.history.add_message(role="assistant", content=[content])

    def maybe_speak_queued() -> None:
        """Speak the next queued slow-brain response, verbatim, when the floor is free.

        Releases the line as soon as nobody is occupying the voice floor (the user
        speaking, or assistant audio playing). It does NOT wait for the agent to
        leave the "thinking" state, so a ready line is never stalled behind reply
        generation. Filler/answer ordering is preserved by ``llm_node`` suppressing
        a filler once the slow brain has responded for the turn.
        """
        if not speech_gate_open or not _queued_speech:
            return
        block_reason = _queued_speech_block_reason()
        if block_reason:
            _log.info(f"Queued slow-brain speech deferred: {block_reason}")
            return
        (
            text,
            notification_id,
            notification_source,
            notification_content,
            llm_log_path,
        ) = _queued_speech.pop(0)
        _speak_now(
            text,
            notification_id,
            notification_source,
            notification_content,
            llm_log_path,
        )

    def on_notification(data: dict) -> None:
        """Handle notifications from conversation manager."""
        nonlocal assistant_screen_share_active, _agent_service_url
        nonlocal _pending_console_steps, user_remote_control_active
        nonlocal assistant_desktop_on_stage
        if data.get("event_name") == "AssistantTurnInjected":
            payload = data.get("payload") or {}
            apply_assistant_turn_injection(str(payload.get("content") or ""))
            return

        payload = data.get("payload") or data
        message = payload.get("message", "")
        # Track screen share state from meet interaction notifications.
        if payload.get("source") == "meet_interaction":
            from unify.conversation_manager.medium_scripts.common import (
                update_agent_service_url_from_meet_interaction,
            )

            _agent_service_url = update_agent_service_url_from_meet_interaction(
                _agent_service_url,
                payload,
            )
            surfaces = payload.get("meet_surface_state") or {}
            if "assistant_screen_share_active" in surfaces:
                assistant_screen_share_active = bool(
                    surfaces["assistant_screen_share_active"],
                )
                if not assistant_screen_share_active:
                    _clear_visual_context(source="assistant")
            # Republished on every viewer change rather than on a transition of
            # this flag, so a room that has drifted out of step is put back.
            # Every client's copy of what is mounted lives in its own memory, and
            # an edge-triggered broadcast can only correct one that is wrong in
            # the direction the edge happens to be travelling.
            if CALL_DESKTOP_SHARE_SURFACE in surfaces:
                assistant_desktop_on_stage = bool(
                    surfaces[CALL_DESKTOP_SHARE_SURFACE],
                )
                _publish_assistant_screenshare_state()
            if surfaces.get("user_screen_share_active") is False:
                _clear_visual_context(source="user")
            if "user_remote_control_active" in surfaces:
                user_remote_control_active = bool(
                    surfaces["user_remote_control_active"],
                )

                async def _update_cache_after_remote_control():
                    entry = await capture_assistant_screenshot(
                        utterance="",
                        cached=False,
                        fb_logger=_log,
                        agent_service_url=_agent_service_url,
                        http_session=_screenshot_http_session,
                    )
                    if entry and assistant_screen_share_active:
                        _handle_screenshot(entry)

                if not user_remote_control_active and assistant_screen_share_active:
                    asyncio.create_task(_update_cache_after_remote_control())
        spoken_message = payload.get("spoken_message", "")
        should_speak = payload.get("should_speak", False)
        notification_source = payload.get("source", "")
        llm_log_path = payload.get("llm_log_path", "")
        # A slow-brain spoken turn carries (possibly empty) fast-brain guidance
        # bundled with it; set it so the fast brain can use it on the next message
        # — and so a spoken turn without guidance clears any stale note.
        if notification_source == "slow_brain" and should_speak:
            assistant._fast_brain_guidance = payload.get("fast_brain_guidance", "")
            _pending_console_steps = list(payload.get("console_steps") or [])
        notification_id = content_trace_id("guid", message or spoken_message)
        triggers_turn = notification_source not in (
            "meet_interaction",
            "proactive_speech",
        )
        _log.notification(
            notification_source,
            message,
            speak=should_speak,
            turn=triggers_turn,
        )

        if message or (should_speak and spoken_message):
            if not session_ready or (should_speak and not speech_gate_open):
                pending_notifications.append(
                    (
                        message,
                        spoken_message,
                        should_speak,
                        notification_id,
                        notification_source,
                        llm_log_path,
                    ),
                )
                _log.notification_buffered(len(pending_notifications))
            else:
                apply_notification(
                    message,
                    should_speak,
                    spoken_message=spoken_message,
                    notification_id=notification_id,
                    source="socket_callback",
                    notification_source=notification_source,
                    llm_log_path=llm_log_path,
                )
                # Only awareness notifications (should_speak=False) regenerate the
                # fast brain's filler with new context. Spoken guidance already has
                # its real content queued; regenerating a filler for it would only
                # re-enter the "thinking" state and defer that very line.
                if triggers_turn and not should_speak:
                    _invalidate_current_generation(
                        "notification_during_generation",
                        notification_id,
                    )

    event_broker.register_callback("app:call:status", on_status)
    event_broker.register_callback("app:call:notification", on_notification)

    def on_idle_smalltalk_state(data: dict) -> None:
        assistant.set_idle_smalltalk_allowed(
            bool(data.get("idle_smalltalk_allowed")),
        )

    event_broker.register_callback(
        "app:call:idle_smalltalk_state",
        on_idle_smalltalk_state,
    )

    # --- Tier 1: Comms from call participants (all calls) ---
    is_boss_user = bool(contact.get("is_system", False))
    participant_ids: set[int] = set()
    if contact.get("contact_id") is not None:
        participant_ids.add(contact["contact_id"])

    def _inject_silent_context(msg: str) -> None:
        """Inject a system message into chat context as silent background."""
        assistant._chat_ctx.add_message(role="system", content=[msg])
        session.history.add_message(role="system", content=[msg])

    def on_participant_comms(data: dict) -> None:
        raw = data.get("event") if "event" in data else json.dumps(data)
        text = render_participant_comms(
            raw if isinstance(raw, str) else json.dumps(raw),
            participant_ids,
        )
        if not text:
            return
        _log.participant_comms(text)
        if not session_ready:
            return
        _inject_silent_context(text)
        if text.startswith("[You "):
            if assistant.user_turn_generating:
                _invalidate_current_generation(
                    "outbound_action_during_generation",
                    "participant_comms",
                )
            else:
                trigger_generate_reply(
                    reason="outbound_message_acknowledgment",
                    source_id="participant_comms",
                )

    event_broker.register_callback("app:comms:*", on_participant_comms)

    # Handle call_answered that arrived during initialization
    if call_answered_flag.is_set():
        _log.call_status("call_answered (arrived during init)")
        assistant.set_call_received()

    _log.session_start("Starting AgentSession + history hydration (parallel)")
    history_task = asyncio.create_task(
        hydrate_fast_brain_history(
            participant_ids=participant_ids,
            is_boss_user=is_boss_user,
            assistant_name=assistant_name or "Assistant",
            limit=SETTINGS.conversation.FAST_BRAIN_CONTEXT_WINDOW,
        ),
    )
    await session.start(room=ctx.room, agent=assistant, room_input_options=rio)
    if joined_gate_required and not outbound and not user_joined_event.is_set():
        remote_participants = getattr(ctx.room, "remote_participants", {}) or {}
        if remote_participants:
            _mark_user_joined("existing_participant")
    history_lines = await history_task
    if history_lines:
        history_block = (
            "--- Recent conversation history ---\n"
            + "\n".join(history_lines)
            + "\n--- Current call ---"
        )
        assistant._chat_ctx.add_message(role="system", content=[history_block])
        session.history.add_message(role="system", content=[history_block])
        _log.info(f"Hydrated {len(history_lines)} historical events into context")

    # Mark session ready and process any buffered notifications BEFORE first utterance.
    # After this, the on_notification callback will apply notifications immediately.
    # Note: For outbound calls, llm_node will wait for call_received (set by on_status).
    session_ready = True
    if pending_notifications:
        _log.session_ready(
            f"Applying {len(pending_notifications)} buffered notification(s)",
        )
        still_pending_notifications: list[tuple[str, str, bool, str, str, str]] = []
        for (
            message,
            spoken_message,
            should_speak,
            notification_id,
            notification_source,
            llm_log_path,
        ) in pending_notifications:
            if should_speak and not speech_gate_open:
                still_pending_notifications.append(
                    (
                        message,
                        spoken_message,
                        should_speak,
                        notification_id,
                        notification_source,
                        llm_log_path,
                    ),
                )
                continue
            apply_notification(
                message,
                should_speak,
                spoken_message=spoken_message,
                notification_id=notification_id,
                source="pending_buffer_flush",
                notification_source=notification_source,
                llm_log_path=llm_log_path,
            )
        pending_notifications[:] = still_pending_notifications

    async def _publish_ready_to_speak() -> None:
        if channel == "phone":
            return
        await ctx.room.local_participant.publish_data(
            json.dumps({"type": "ready_to_speak"}).encode(),
            topic="agent_status",
            reliable=True,
        )

    def _schedule_deferred_desktop_binding() -> None:
        if not _voice_call_channel_defers_desktop_binding(channel):
            return
        # Entitlement, not ``has_managed_desktop``: the whole point of this call
        # is that the activation deferred VM binding, so no desktop_url exists
        # yet. Gating on one meant the promotion never fired and a deferred
        # voice session never got a desktop at all.
        if not SESSION_DETAILS.assistant.managed_desktop_entitled:
            return
        agent_id = SESSION_DETAILS.assistant.agent_id
        if agent_id is None:
            return
        from unify.conversation_manager.domains import comms_utils

        asyncio.create_task(
            comms_utils.request_deferred_desktop_binding(agent_id),
        )

    async def _generate_opening_greeting() -> str:
        """Pre-generate the opening line via a sidecar LLM call.

        Returns the cached depleted-credits response when the credit gate is
        closed, otherwise generates from the voice system prompt plus the
        current call history.
        """
        from unify.common.llm_client import new_llm_client

        if not credit_gate_monitor.state.allowed:
            _log.info("Credit gate greeting served from cached state")
            return DEPLETED_CREDITS_FAST_BRAIN_RESPONSE
        greeting_client = new_llm_client(
            model=SETTINGS.conversation.FAST_BRAIN_MODEL,
            origin="fast_brain_greeting",
            reasoning_effort=SETTINGS.conversation.FAST_BRAIN_REASONING_EFFORT,
        )
        greeting_messages = build_opening_greeting_messages(
            system_prompt=system_prompt,
            history_messages=_extract_chat_messages(session.history),
        )
        return await greeting_client.generate(messages=greeting_messages)

    assistant._fast_brain_system_prompt = system_prompt
    assistant._fast_brain_history_provider = lambda: _extract_chat_messages(
        session.history,
        tail=8,
    )

    opening_mode = opening_config["mode"]

    async def _prepare_opening() -> tuple[str, str | dict | None]:
        if opening_mode == "speak":
            return "speak", await _generate_opening_greeting()
        if opening_mode == "opener":
            return "opener", opening_config["opener_text"].strip()
        if opening_mode == "recorded":
            preloaded = await asyncio.to_thread(
                _preload_recorded_opening_pcm,
                opening_config,
            )
            return "recorded", {
                "config": opening_config,
                "preloaded": preloaded,
            }
        if opening_mode == "simulated":
            simulated_utterance = opening_config.get("simulated_utterance", "").strip()
            if not simulated_utterance:
                raise ValueError("simulated opening requires simulated_utterance")
            return "simulated", simulated_utterance
        return "silent", None

    opening_task = asyncio.create_task(_prepare_opening())
    await event_broker.publish(
        "app:call:status",
        json.dumps(
            {
                "type": "agent_ready",
                "room_name": ctx.room.name,
                "channel": channel,
            },
        ),
    )

    if outbound or (opening_mode == "opener" and channel == "unify_meet"):
        _log.info("Agent-initiated call — waiting for callee to answer…")
        await call_answered_flag.wait()
        _log.call_status("call_answered — opening turn")

    await user_joined_event.wait()
    speech_gate_open = True

    opener_trigger: str | None = None
    if opening_mode == "opener":
        # Hold the verbatim opener until the earlier of the callee's first
        # completed utterance (their "Hello?") or OPENER_SILENCE_TRIGGER_S of
        # silence — so it lands when they are actually listening, not into dead
        # air right after the line connects. The assistant says nothing until
        # then. A short first turn is logged to the transcript but does not
        # schedule the slow brain (filler suppressed via _opening_pending in
        # llm_node); a substantive first turn routes the held opener through
        # the fast brain's continuation decision instead.
        try:
            await asyncio.wait_for(
                assistant._first_user_turn.wait(),
                OPENER_SILENCE_TRIGGER_S,
            )
        except asyncio.TimeoutError:
            if user_is_speaking:
                # The silence window elapsed mid-utterance — not silence; let
                # them finish and judge the completed turn's duration.
                await assistant._first_user_turn.wait()
        if assistant._first_user_turn.is_set():
            first_turn_s = assistant._first_turn_duration_s or 0.0
            opener_trigger = (
                "short_turn" if first_turn_s < OPENER_SHORT_TURN_MAX_S else "long_turn"
            )
        else:
            opener_trigger = "silence"
        _log.call_status(f"opener_trigger:{opener_trigger}")

    try:
        prepared_mode, prepared_payload = await opening_task
    except Exception as exc:
        _log.llm_error(f"opening preload failed: {exc}")
        prepared_mode, prepared_payload = "speak", "Hello — I'm here."

    if prepared_mode == "opener":
        opener_text = str(prepared_payload or "").strip()
        await _publish_ready_to_speak()
        assistant._opening_pending = False
        if opener_trigger == "long_turn":
            # The callee opened with a substantive turn; parroting the planned
            # line as though they had only said hello would ignore them. Hold
            # the never-spoken opener as a claimable continuation and let the
            # fast brain decide: deliver it verbatim (continuation) or respond
            # to what they actually said.
            assistant._pending_continuation = {
                "resume_text": opener_text,
                "remainder": opener_text,
                "spoken_prefix": "",
                "source": "opening",
                "seq": assistant._tts_seq,
                "consumed": False,
            }
            _log.info(
                "Opener held for fast-brain decision after substantive first "
                f"turn ({assistant._first_turn_duration_s or 0.0:.1f}s)",
            )
            trigger_generate_reply(
                reason="opener_substantive_first_turn",
                source_id="held_opener",
            )
        else:
            _say_verbatim_opener(opener_text)
    elif prepared_mode == "speak":
        await _publish_ready_to_speak()
        _say_opening(str(prepared_payload or "Hello — I'm here."))
    elif prepared_mode == "recorded":
        payload = prepared_payload or opening_config
        if isinstance(payload, dict) and "config" in payload:
            recorded_config = payload["config"]
            _recorded_opening_preloaded = payload.get("preloaded") or {}
        else:
            recorded_config = payload
        await _publish_ready_to_speak()
        await _run_recorded_opening(recorded_config)
    elif prepared_mode == "simulated":
        simulated_utterance = str(prepared_payload or "")
        await _publish_ready_to_speak()
        assistant._chat_ctx.add_message(role="assistant", content=[simulated_utterance])
        session.history.add_message(role="assistant", content=[simulated_utterance])
        _log.assistant_speech(
            simulated_utterance,
            source=opening_config.get("source", "simulated_opening"),
            llm_log_path="",
        )
        await _publish_assistant_utterance(simulated_utterance)
    else:
        _log.info("Opening turn suppressed by call opening config")
        await _publish_ready_to_speak()

    _schedule_deferred_desktop_binding()

    # The opener has been dispatched or deferred; resume normal turn handling
    # (fast-brain fillers and slow-brain turns) for any subsequent user speech.
    assistant._opening_pending = False

    if pending_notifications:
        gated_notifications = list(pending_notifications)
        pending_notifications.clear()
        for (
            message,
            spoken_message,
            should_speak,
            notification_id,
            notification_source,
            llm_log_path,
        ) in gated_notifications:
            apply_notification(
                message,
                should_speak,
                spoken_message=spoken_message,
                notification_id=notification_id,
                source="user_joined_buffer_flush",
                notification_source=notification_source,
                llm_log_path=llm_log_path,
            )
    maybe_speak_queued()

    # Inject the initializing-state system message *after* the greeting has
    # been generated and spoken.  Placing it before the greeting caused the
    # LLM to proactively mention "still setting up" in the opening line,
    # which sounds odd when no action has been requested yet.  The note only
    # matters for subsequent turns where the user might ask for something
    # that requires initialized managers.
    if not os.environ.get("UNIFY_CM_INITIALIZED"):
        _init_note = (
            "[system] You have just started up and your systems are still "
            "syncing — loading your files, tools, and any conversation "
            "history. This takes a few moments. If the user asks you to do "
            "something that requires looking things up or taking action, let "
            "them know naturally that you are still getting set up (e.g. "
            "'give me just a moment to finish getting set up and I'll get "
            "right on that'). Do NOT say 'I can't do that' — frame it as a "
            "brief delay, not a limitation. You will receive a notification "
            "when everything is ready."
        )
        assistant._chat_ctx.add_message(role="system", content=[_init_note])
        session.history.add_message(role="system", content=[_init_note])
        _log.info("Injected initializing-state system message (CM not yet initialized)")


if __name__ == "__main__":
    # CLI handling
    room_name = configure_from_cli(
        extra_env=[
            ("CONTACT", True),
            ("BOSS", True),
            ("ASSISTANT_BIO", False),
            ("ASSISTANT_ID", False),
            ("USER_ID", False),
        ],
    )

    # Unify Meets always carry a call session (enforced at the adapters and
    # comms-manager boundaries) and share one LiveKit room, so each agent
    # registers a distinct per-assistant name. Channels that own their room
    # (phone/WhatsApp/browser meets) register as the room itself.
    call_session_id = os.environ.get("CALL_SESSION_ID", "").strip()
    agent_name = room_name
    if call_session_id:
        aid = SESSION_DETAILS.assistant.agent_id
        agent_name = f"unity_{aid}" if aid else f"unity_meet_{os.getpid()}"

    if should_dispatch_livekit_agent():
        _log.dispatch(f"Dispatching LiveKit agent {agent_name} into room {room_name}")
        dispatch_livekit_agent(
            room_name,
            agent_name=agent_name,
            call_session_id=call_session_id,
        )
        _log.dispatch(f"LiveKit agent {agent_name} dispatched")

    # Run the agent using the standard CLI - this is the natural way to run LiveKit agents.
    # The process will be terminated via SIGTERM when cleanup_call_proc() is called.
    agents.cli.run_app(
        agents.WorkerOptions(
            entrypoint_fnc=entrypoint,
            agent_name=agent_name,
            prewarm_fnc=prewarm,
            initialize_process_timeout=60,
        ),
    )
