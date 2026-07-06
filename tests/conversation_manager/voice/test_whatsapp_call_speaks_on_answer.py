"""Demonstration: an outbound WhatsApp call's agent speaks its verbatim opener
after the callee answers — and stays silent until then.

This drives the real ``call.entrypoint`` for ``channel="whatsapp_call"`` with
``outbound=True`` and an ``opener`` opening config, using the established fake
LiveKit / AgentSession / broker stack so no real LiveKit, Twilio, LLM, or TTS
is involved. It proves the opener contract that a real call must honour:

1. After the agent is ready it WAITS — it does not speak before the callee
   answers (no outbound utterance is emitted while unanswered).
2. After a ``call_answered`` status arrives (what the Twilio ``in-progress``
   webhook ultimately triggers via ``app:call:status``), the agent holds the
   opener through the silence window (no utterance yet), then announces it is
   ready to speak and speaks the pre-decided opener verbatim.

Together with the worker-readiness gating tests, this closes the loop: the
worker is only dispatched when ready, the agent activates, and on answer it
delivers the exact opener the slow brain queued before dialing.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

OPENING_LINE = "Hi, this is your assistant calling on WhatsApp."
CALL_BRIEFING = "Quiz answer: Dune. Confirm warmly, then wrap up."
UTTERANCE_TOPIC = "app:comms:whatsapp_call_utterance"


def _install_entrypoint_fakes(monkeypatch, sequence, extra_metadata=None):
    """Patch call.py's LiveKit / session / broker / model deps with fakes.

    Returns ``(call_script, broker, make_ctx)`` where ``broker`` exposes the
    ``app:call:status`` callback the agent registers (so the test can deliver
    ``call_answered``) and ``make_ctx`` builds the fake outbound whatsapp_call
    JobContext.
    """
    from livekit.agents import llm

    from unify.conversation_manager.medium_scripts import call as call_script

    contact = {"contact_id": 1, "first_name": "User", "surname": "Example"}
    boss = {"contact_id": 1, "first_name": "User", "surname": "Example"}

    class _ImmediateAwaitable:
        # Mimics a completed SpeechHandle: awaitable, playout resolves at
        # once, never interrupted (so gated hang-up finalization can proceed).
        interrupted = False

        def __await__(self):
            async def _done():
                return None

            return _done().__await__()

        async def wait_for_playout(self):
            return None

    class _FakeLocalParticipant:
        async def publish_data(self, payload, *, topic=None, reliable=False):
            sequence.append(("data", json.loads(payload.decode()), topic))

    class _FakeRoom:
        name = "unity_123_whatsapp_call"
        local_participant = _FakeLocalParticipant()

        def on(self, *args, **kwargs):
            return lambda fn: fn

    class _FakeJobContext:
        def __init__(self):
            self.room = _FakeRoom()
            self.job = SimpleNamespace(
                metadata=json.dumps(
                    {
                        "voice_provider": "cartesia",
                        "voice_id": "",
                        "outbound": True,
                        "channel": "whatsapp_call",
                        "contact": contact,
                        "boss": boss,
                        "assistant_bio": "Assistant bio",
                        "assistant_id": "123",
                        "user_id": "user-123",
                        "assistant_name": "Assistant",
                        "opening_config": {
                            "mode": "opener",
                            "opener_text": OPENING_LINE,
                            "briefing": CALL_BRIEFING,
                            "source": "outbound_whatsapp_opening",
                        },
                        **(extra_metadata or {}),
                    },
                ),
            )

        async def connect(self):
            return None

        def add_shutdown_callback(self, cb):
            pass

        def shutdown(self, reason=""):
            pass

    class _FakeEventBroker:
        def __init__(self):
            self.callbacks = {}

        def set_logger(self, fb_logger):
            pass

        def register_callback(self, channel, handler):
            self.callbacks[channel] = handler

        async def publish(self, channel, message):
            sequence.append(("broker", channel, message))
            return 1

    holders: dict = {}

    class _FakeSession:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.current_agent = None
            self._events = {}
            self.agent_state = "listening"
            self.current_speech = None
            self.say_calls = []
            holders["session"] = self

        @property
        def history(self):
            return self._chat_ctx

        def on(self, event_name):
            def _decorator(fn):
                self._events[event_name] = fn
                return fn

            return _decorator

        async def start(self, room, agent, room_input_options=None):
            self.current_agent = agent

        def generate_reply(self, **kwargs):
            return _ImmediateAwaitable()

        def say(self, text, **kwargs):
            self.say_calls.append((text, kwargs))
            # A real AgentSession commits the spoken line to history and emits
            # conversation_item_added, which is what publishes the utterance.
            handler = self._events.get("conversation_item_added")
            if handler is not None:
                handler(
                    SimpleNamespace(
                        item=SimpleNamespace(role="assistant", text_content=text),
                    ),
                )
            return _ImmediateAwaitable()

        def interrupt(self):
            pass

    class _FakeAssistant:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.call_received = False
            self.user_turn_generating = False
            self._user_speech_logged = False
            self._opening_pending = False
            self._first_user_turn = asyncio.Event()
            self._first_turn_speaking_started_at = None
            self._first_turn_duration_s = None
            self._pending_continuation = None
            self._active_tts = None
            self._tts_seq = 0
            self._hang_up_gate_reason = None
            self._active_reply_handle = None

        def set_call_received(self):
            self.call_received = True

        def set_credit_gate_state_provider(self, provider):
            self.credit_gate_state_provider = provider

    async def _noop_async(*args, **kwargs):
        return None

    async def _noop_end_call():
        return None

    fake_session_details = SimpleNamespace(
        user=SimpleNamespace(id=None),
        assistant=SimpleNamespace(
            about="Assistant bio",
            is_coordinator=False,
            agent_id=None,
            name="Assistant",
            first_name="",
            surname="",
            user_desktop_for=lambda user_id: None,
        ),
        voice=SimpleNamespace(provider="cartesia", id=""),
        voice_call=SimpleNamespace(outbound=True, channel="whatsapp_call"),
        is_coordinator=False,
        org_id=None,
        unify_key="",
    )

    broker = _FakeEventBroker()
    monkeypatch.setattr(call_script, "event_broker", broker)
    monkeypatch.setattr(call_script, "SESSION_DETAILS", fake_session_details)
    monkeypatch.setattr(call_script, "AgentSession", _FakeSession)
    monkeypatch.setattr(call_script, "Assistant", _FakeAssistant)
    monkeypatch.setattr(call_script, "UnifyLLM", lambda *a, **k: object())
    monkeypatch.setattr(
        call_script,
        "build_voice_agent_prompt",
        lambda **kwargs: SimpleNamespace(flatten=lambda: "system prompt"),
    )
    monkeypatch.setattr(call_script, "start_event_broker_receive", _noop_async)
    monkeypatch.setattr(call_script, "publish_call_started", _noop_async)
    monkeypatch.setattr(
        call_script,
        "create_end_call",
        lambda *a, **k: _noop_end_call,
    )
    monkeypatch.setattr(
        call_script,
        "setup_participant_disconnect_handler",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(call_script, "RoomInputOptions", lambda **kwargs: object())
    monkeypatch.setattr(call_script, "EnglishModel", lambda: object())
    monkeypatch.setattr(call_script.cartesia, "TTS", lambda **kwargs: object())
    monkeypatch.setattr(call_script.elevenlabs, "TTS", lambda **kwargs: object())
    if hasattr(call_script, "noise_cancellation"):
        monkeypatch.setattr(call_script.noise_cancellation, "BVC", lambda: object())
    monkeypatch.setattr(call_script, "STT", object())
    monkeypatch.setattr(call_script, "VAD", object())
    # Keep the silence window short so the test does not sleep for real seconds.
    monkeypatch.setattr(call_script, "OPENER_SILENCE_TRIGGER_S", 0.05)

    return call_script, broker, _FakeJobContext, holders


def _has_agent_ready(sequence) -> bool:
    return any(
        item[0] == "broker"
        and item[1] == "app:call:status"
        and '"agent_ready"' in item[2]
        for item in sequence
    )


def _utterance_emitted(sequence) -> bool:
    return any(item[0] == "broker" and item[1] == UTTERANCE_TOPIC for item in sequence)


@pytest.mark.asyncio
async def test_outbound_whatsapp_agent_speaks_only_after_answer(monkeypatch):
    sequence: list = []
    call_script, broker, make_ctx, holders = _install_entrypoint_fakes(
        monkeypatch,
        sequence,
    )

    task = asyncio.create_task(call_script.entrypoint(make_ctx()))
    try:
        # Wait until the agent has booted, registered the status callback, and
        # parked on the "callee answered" gate (agent_ready is published right
        # before that wait).
        for _ in range(500):
            if "app:call:status" in broker.callbacks and _has_agent_ready(sequence):
                break
            await asyncio.sleep(0.01)
        else:  # pragma: no cover - defensive
            raise AssertionError("agent never reached the answered gate")

        # (1) The agent must NOT speak before the callee answers.
        await asyncio.sleep(0.1)
        assert not _utterance_emitted(
            sequence,
        ), "agent spoke before the call was answered"

        # (2) Deliver the answered signal (what Twilio's in-progress webhook
        #     ultimately triggers via app:call:status) and let the agent speak.
        broker.callbacks["app:call:status"]({"type": "call_answered"})

        await asyncio.wait_for(task, timeout=5.0)
    finally:
        if not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass

    # The agent announced readiness then emitted its opening utterance, in order.
    ready_idx = next(
        (
            i
            for i, item in enumerate(sequence)
            if item[0] == "data" and item[1] == {"type": "ready_to_speak"}
        ),
        None,
    )
    utter_idx = next(
        (
            i
            for i, item in enumerate(sequence)
            if item[0] == "broker" and item[1] == UTTERANCE_TOPIC
        ),
        None,
    )
    assert ready_idx is not None, "agent never signalled ready_to_speak"
    assert utter_idx is not None, "agent never emitted its opening utterance"
    assert ready_idx < utter_idx, "ready_to_speak must precede the opening utterance"

    # The opening utterance carries the spoken line.
    utter_msg = next(
        item[2]
        for item in sequence
        if item[0] == "broker" and item[1] == UTTERANCE_TOPIC
    )
    assert OPENING_LINE in utter_msg

    # The unspoken briefing was injected into the voice context (system role,
    # never spoken) and armed on the assistant for per-turn selection.
    session = holders["session"]
    history_system_text = "\n".join(
        str(item.content)
        for item in session.history.items
        if getattr(item, "role", None) == "system"
    )
    assert CALL_BRIEFING in history_system_text
    assert "NEVER read the briefing aloud" in history_system_text
    assert session.current_agent._call_briefing == CALL_BRIEFING
    # The briefing must never have been spoken.
    assert all(CALL_BRIEFING not in text for text, _ in session.say_calls)


@pytest.mark.asyncio
async def test_speech_created_records_reply_handle(monkeypatch):
    """The observer records each generate_reply speech handle for the turn
    logic to attach to (continuation registration / gated hang-up), and
    ignores other speech sources. It must not claim anything itself: it runs
    before ``llm_node`` streams, so claiming would hand work to the wrong
    (next) speech."""
    sequence: list = []
    call_script, broker, make_ctx, holders = _install_entrypoint_fakes(
        monkeypatch,
        sequence,
    )

    task = asyncio.create_task(call_script.entrypoint(make_ctx()))
    try:
        for _ in range(500):
            if "app:call:status" in broker.callbacks and _has_agent_ready(sequence):
                break
            await asyncio.sleep(0.01)
        else:  # pragma: no cover - defensive
            raise AssertionError("agent never reached the answered gate")
        broker.callbacks["app:call:status"]({"type": "call_answered"})
        await asyncio.wait_for(task, timeout=5.0)
    finally:
        if not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass

    session = holders["session"]
    assistant = session.current_agent
    observer = session._events["speech_created"]

    reply_handle = object()
    observer(SimpleNamespace(source="generate_reply", speech_handle=reply_handle))
    assert assistant._active_reply_handle is reply_handle

    # Non-reply speeches (say / opening lines) never overwrite the record.
    observer(SimpleNamespace(source="say", speech_handle=object()))
    assert assistant._active_reply_handle is reply_handle

    # The entrypoint wired the attach callbacks the turn logic invokes.
    assert callable(assistant._register_reply_continuation)
    assert callable(assistant._finalize_reply_hang_up)


def _hang_up_events(sequence) -> list:
    return [
        item
        for item in sequence
        if item[0] == "broker" and item[1] == "app:comms:fast_brain_hang_up"
    ]


@pytest.mark.asyncio
async def test_pre_armed_gate_never_closes_before_answer(monkeypatch):
    """A pre-armed hang-up gate must not fire while the call is still ringing
    (the pipeline is quiescent pre-answer), and the sanctioned-silence close
    only runs once the call is live and the opener has been dispatched."""
    sequence: list = []
    call_script, broker, make_ctx, holders = _install_entrypoint_fakes(
        monkeypatch,
        sequence,
        extra_metadata={
            "hang_up_gate_reason": "Deliver the message, then wrap up.",
        },
    )
    # Tiny close/grace windows so the watcher (1s tick) fires fast once live.
    monkeypatch.setattr(call_script, "HANG_UP_SILENCE_CLOSE_S", 0.5)
    monkeypatch.setattr(call_script, "HANG_UP_GRACE_S", 0.01)

    task = asyncio.create_task(call_script.entrypoint(make_ctx()))
    try:
        for _ in range(500):
            if "app:call:status" in broker.callbacks and _has_agent_ready(sequence):
                break
            await asyncio.sleep(0.01)
        else:  # pragma: no cover - defensive
            raise AssertionError("agent never reached the answered gate")

        # Phase 1: still ringing (no call_answered). The gate is armed, the
        # pipeline is quiescent — the watcher must NOT close the call.
        await asyncio.sleep(2.5)
        assert not _hang_up_events(
            sequence,
        ), "gated hang-up fired while the call was still ringing"

        # Phase 2: answer the call; opener flows; the line then goes silent.
        # The sanctioned-silence close now fires and ends the call.
        broker.callbacks["app:call:status"]({"type": "call_answered"})
        await asyncio.wait_for(task, timeout=5.0)

        for _ in range(600):
            if _hang_up_events(sequence):
                break
            await asyncio.sleep(0.01)
        hang_ups = _hang_up_events(sequence)
        assert hang_ups, "sanctioned-silence close never fired after answer"
        payload = hang_ups[0][2]
        assert '"silence"' in payload
        assert "Deliver the message, then wrap up." in payload
    finally:
        if not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass
