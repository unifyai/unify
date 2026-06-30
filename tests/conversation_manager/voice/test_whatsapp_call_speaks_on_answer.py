"""Demonstration: an outbound WhatsApp call's agent speaks the moment the
callee answers — and stays silent until then.

This drives the real ``call.entrypoint`` for ``channel="whatsapp_call"`` with
``outbound=True`` (the exact configuration used after a Meet -> WhatsApp-call
handoff), using the established fake LiveKit / AgentSession / broker stack so no
real LiveKit, Twilio, LLM, or TTS is involved. It proves the speak-on-answer
contract that a real call must honour:

1. After the agent is ready it WAITS — it does not speak before the callee
   answers (no outbound utterance is emitted while unanswered).
2. The instant a ``call_answered`` status arrives (what the Twilio
   ``in-progress`` webhook ultimately triggers via ``app:call:status``), the
   agent announces it is ready to speak and then emits its opening utterance.

Together with the worker-readiness gating tests, this closes the loop: the
worker is only dispatched when ready, the agent activates, and on answer it
speaks.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

OPENING_LINE = "Hi, this is your assistant calling on WhatsApp."
UTTERANCE_TOPIC = "app:comms:whatsapp_call_utterance"


def _install_entrypoint_fakes(monkeypatch, sequence):
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
        def __await__(self):
            async def _done():
                return None

            return _done().__await__()

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
                            "mode": "simulated",
                            "simulated_utterance": OPENING_LINE,
                            "source": "outbound_whatsapp_opening",
                        },
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

    class _FakeSession:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.current_agent = None
            self._events = {}
            self.agent_state = "listening"
            self.current_speech = None
            self.say_calls = []

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
            return _ImmediateAwaitable()

        def interrupt(self):
            pass

    class _FakeAssistant:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.call_received = False
            self.user_turn_generating = False

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

    return call_script, broker, _FakeJobContext


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
    call_script, broker, make_ctx = _install_entrypoint_fakes(monkeypatch, sequence)

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
