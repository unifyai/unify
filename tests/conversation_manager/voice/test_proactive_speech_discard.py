"""
tests/conversation_manager/voice/test_proactive_speech_discard.py
=================================================================

Regression tests for proactive speech play-or-discard semantics in the call
subprocess (call.py).

Proactive speech exists purely to fill silence. It must NEVER be queued behind
other speech — if the voice pipeline is not completely quiescent at the moment
the proactive notification arrives, it should be silently discarded.

These tests exercise `apply_notification` in call.py through the full
entrypoint, verifying that proactive speech:
  - plays immediately when the pipeline is idle
  - is discarded when the user is speaking
  - is discarded when the agent is thinking or speaking
  - is discarded when other speech is already queued
  - does not block regular (non-proactive) notifications from queueing normally
"""

import asyncio
import json
from types import SimpleNamespace

import pytest


def _proactive_payload(text: str = "Still getting it open, one sec.") -> dict:
    return {
        "payload": {
            "message": text,
            "should_speak": True,
            "source": "proactive_speech",
        },
    }


def _actor_payload(
    message: str = "The meeting is at 3pm.",
    *,
    spoken_message: str = "It's at 3pm.",
) -> dict:
    return {
        "payload": {
            "message": message,
            "spoken_message": spoken_message,
            "should_speak": True,
            "source": "actor",
        },
    }


async def _boot_entrypoint(monkeypatch):
    """Boot call.py's entrypoint with fakes and return test handles.

    Returns (session, fake_broker, agent_state_cb_trigger, user_state_cb_trigger).
    """
    from livekit.agents import llm

    from unify.conversation_manager.medium_scripts import call as call_script

    contact = {
        "contact_id": 2,
        "first_name": "Caller",
        "surname": "Example",
        "phone_number": "+15550100002",
        "email_address": "caller@example.com",
    }
    boss = {
        "contact_id": 1,
        "first_name": "Manager",
        "surname": "Example",
        "phone_number": "+15550100001",
        "email_address": "manager@example.com",
    }

    class _ImmediateAwaitable:
        def __await__(self):
            async def _done():
                return None

            return _done().__await__()

    class _FakeLocalParticipant:
        async def publish_data(self, *args, **kwargs):
            pass

    class _FakeRoom:
        name = "fake-room"
        local_participant = _FakeLocalParticipant()

        def on(self, *args, **kwargs):
            return lambda fn: fn

    class _FakeJobContext:
        def __init__(self):
            self.room = _FakeRoom()
            self.job = SimpleNamespace()

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
            return 1

    fake_session_holder = {}

    class _FakeSession:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.current_agent = None
            self._events = {}
            self.generate_reply_calls = 0
            self.say_calls = []
            self.agent_state = "listening"
            self.current_speech = None
            fake_session_holder["session"] = self

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
            self.generate_reply_calls += 1
            return _ImmediateAwaitable()

        def say(self, text, **kwargs):
            self.say_calls.append(text)
            return _ImmediateAwaitable()

        def interrupt(self):
            pass

    class _FakeAssistant:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.call_received = True
            self.user_turn_generating = False
            self._user_turn_seq = 0
            self._slow_brain_responded_turn = -1
            self._buffers_since_slow_reply = 0
            self._pending_opening_bridge = None
            self._active_tts = None
            self._pending_continuation = None
            self._tts_seq = 0
            self._publish_voice_interrupt = None

        def set_call_received(self):
            self.call_received = True

        def set_credit_gate_state_provider(self, provider):
            pass

    async def _noop_async(*args, **kwargs):
        return None

    async def _noop_end_call():
        return None

    fake_broker = _FakeEventBroker()
    fake_session_details = SimpleNamespace(
        populate_from_env=lambda: None,
        voice=SimpleNamespace(provider="cartesia", id=""),
        voice_call=SimpleNamespace(
            outbound=False,
            channel="meet",
            contact_json=json.dumps(contact),
            boss_json=json.dumps(boss),
        ),
        assistant=SimpleNamespace(
            about="Assistant bio",
            name="Ava",
            first_name="Ava",
            surname="Assistant",
            agent_id="agent-1",
            is_coordinator=False,
            user_desktop_for=lambda _uid: None,
        ),
        user=SimpleNamespace(id="owner-1"),
        unify_key="",
        is_coordinator=False,
        org_id=None,
    )

    monkeypatch.setattr(call_script, "event_broker", fake_broker)
    monkeypatch.setattr(call_script, "SESSION_DETAILS", fake_session_details)
    monkeypatch.setattr(call_script, "AgentSession", _FakeSession)
    monkeypatch.setattr(call_script, "Assistant", _FakeAssistant)
    monkeypatch.setattr(call_script, "UnifyLLM", lambda *args, **kwargs: object())
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
        lambda *args, **kwargs: _noop_end_call,
    )
    monkeypatch.setattr(
        call_script,
        "setup_participant_disconnect_handler",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(call_script, "RoomInputOptions", lambda **kwargs: object())
    monkeypatch.setattr(call_script, "EnglishModel", lambda: object())
    monkeypatch.setattr(call_script.cartesia, "TTS", lambda **kwargs: object())
    monkeypatch.setattr(call_script.elevenlabs, "TTS", lambda **kwargs: object())
    if hasattr(call_script, "noise_cancellation"):
        monkeypatch.setattr(call_script.noise_cancellation, "BVC", lambda: object())
    monkeypatch.setattr(call_script, "STT", object())
    monkeypatch.setattr(call_script, "VAD", object())

    import unify.common.llm_client as _llm_mod

    class _FakeGreetingClient:
        async def generate(self, **kwargs):
            return "Hello!"

    monkeypatch.setattr(
        _llm_mod,
        "new_llm_client",
        lambda *a, **kw: _FakeGreetingClient(),
    )

    await call_script.entrypoint(_FakeJobContext())

    session = fake_session_holder["session"]
    session.say_calls.clear()
    notification_cb = fake_broker.callbacks["app:call:notification"]

    def set_user_state(state: str):
        session._events["user_state_changed"](SimpleNamespace(new_state=state))

    def set_agent_state(state: str):
        session.agent_state = state
        session._events["agent_state_changed"](SimpleNamespace(new_state=state))

    return session, notification_cb, set_agent_state, set_user_state


@pytest.mark.asyncio
class TestProactiveSpeechDiscard:
    """Proactive speech must only play in genuine silence — never queued."""

    async def test_proactive_plays_when_pipeline_idle(self, monkeypatch):
        """When the pipeline is fully quiescent, proactive speech plays
        immediately via session.say()."""
        session, notify, set_agent, set_user = await _boot_entrypoint(monkeypatch)

        assert session.agent_state == "listening"
        assert len(session.say_calls) == 0

        notify(_proactive_payload())

        assert len(session.say_calls) == 1
        assert session.say_calls[0] == "Still getting it open, one sec."

    async def test_proactive_discarded_when_user_speaking(self, monkeypatch):
        """Proactive speech arriving while the user is speaking is discarded,
        not queued."""
        session, notify, set_agent, set_user = await _boot_entrypoint(monkeypatch)

        set_user("speaking")
        notify(_proactive_payload())

        assert (
            len(session.say_calls) == 0
        ), "Proactive speech must be discarded while user is speaking."

        # Crucially: when the pipeline returns to idle, the proactive speech
        # must NOT resurface — it was discarded, not queued.
        set_user("listening")
        set_agent("listening")

        assert (
            len(session.say_calls) == 0
        ), "Discarded proactive speech must not resurface when pipeline settles."

    async def test_proactive_discarded_when_agent_thinking(self, monkeypatch):
        """Proactive speech arriving while the agent is thinking is discarded."""
        session, notify, set_agent, set_user = await _boot_entrypoint(monkeypatch)

        session.agent_state = "thinking"
        notify(_proactive_payload())

        assert (
            len(session.say_calls) == 0
        ), "Proactive speech must be discarded while agent is thinking."

        set_agent("listening")
        assert (
            len(session.say_calls) == 0
        ), "Discarded proactive speech must not resurface after agent settles."

    async def test_proactive_discarded_when_agent_speaking(self, monkeypatch):
        """Proactive speech arriving while the agent is speaking is discarded."""
        session, notify, set_agent, set_user = await _boot_entrypoint(monkeypatch)

        session.agent_state = "speaking"
        notify(_proactive_payload())

        assert len(session.say_calls) == 0

        set_agent("listening")
        assert (
            len(session.say_calls) == 0
        ), "Discarded proactive speech must not play after speaking finishes."

    async def test_proactive_discarded_when_current_speech_active(self, monkeypatch):
        """Proactive speech discarded when TTS is actively playing."""
        session, notify, set_agent, set_user = await _boot_entrypoint(monkeypatch)

        session.current_speech = SimpleNamespace(done=False)
        notify(_proactive_payload())

        assert len(session.say_calls) == 0

        session.current_speech = SimpleNamespace(done=True)
        set_agent("listening")
        assert (
            len(session.say_calls) == 0
        ), "Discarded proactive speech must not play after TTS finishes."

    async def test_proactive_discarded_when_other_speech_queued(self, monkeypatch):
        """If a regular (actor) notification is already queued, proactive speech
        arriving afterwards is discarded — it must never queue behind other speech."""
        session, notify, set_agent, set_user = await _boot_entrypoint(monkeypatch)

        # Occupy the floor (assistant speaking) so the actor notification waits.
        session.current_speech = SimpleNamespace(done=False)
        notify(_actor_payload())
        assert len(session.say_calls) == 0

        # Now send proactive speech — pipeline isn't quiescent AND there's
        # queued speech, so it must be discarded.
        notify(_proactive_payload())
        assert len(session.say_calls) == 0

        # When the floor frees, only the actor notification plays.
        session.current_speech = SimpleNamespace(done=True)
        set_agent("listening")
        await asyncio.sleep(0)
        assert len(session.say_calls) == 1
        assert session.say_calls[0] == "It's at 3pm."

    async def test_proactive_not_queued_behind_fast_brain_reply(self, monkeypatch):
        """Reproduces the exact production bug: proactive speech notification
        arrives while the fast brain is actively replying, then plays AFTER
        the reply finishes. With the fix, the proactive speech is discarded.

        Timeline from production logs:
          04:02:07  proactive notification arrives
          04:02:08  user speaks, fast brain starts thinking
          04:02:12  fast brain reply plays
          04:02:15  stale proactive speech plays (BUG)
        """
        session, notify, set_agent, set_user = await _boot_entrypoint(monkeypatch)

        # Fast brain is thinking (about to reply).
        session.agent_state = "thinking"

        # Proactive notification arrives from ConversationManager (stale by now).
        notify(_proactive_payload("Still getting it open, one sec."))
        assert len(session.say_calls) == 0

        # Fast brain speaks its reply (via direct session.say, not the queue).
        session.agent_state = "speaking"

        # Agent finishes reply, settles to listening.
        set_agent("listening")

        # The proactive speech must NOT play here.
        assert len(session.say_calls) == 0, (
            "Proactive speech must never play after a fast brain reply. "
            "It should have been discarded when it arrived during thinking."
        )

    async def test_regular_notification_still_queues_normally(self, monkeypatch):
        """Non-proactive (actor) notifications must still queue while the floor is
        occupied and play once it frees — the proactive discard logic must not
        affect them."""
        session, notify, set_agent, set_user = await _boot_entrypoint(monkeypatch)

        # Floor occupied (assistant speaking) — actor notification waits.
        session.current_speech = SimpleNamespace(done=False)
        notify(_actor_payload())
        assert len(session.say_calls) == 0

        # Floor frees — queued actor notification plays.
        session.current_speech = SimpleNamespace(done=True)
        set_agent("listening")
        await asyncio.sleep(0)
        assert len(session.say_calls) == 1
        assert session.say_calls[0] == "It's at 3pm."

    async def test_regular_notification_queued_while_user_speaking_plays_after(
        self,
        monkeypatch,
    ):
        """Actor notifications queued during user speech still play when the
        pipeline settles (regression guard: proactive discard must not break
        the regular notification path)."""
        session, notify, set_agent, set_user = await _boot_entrypoint(monkeypatch)

        set_user("speaking")
        notify(
            _actor_payload(
                "Bob's number is 555-1234.",
                spoken_message="It's 555-1234.",
            ),
        )
        assert len(session.say_calls) == 0

        set_user("listening")
        set_agent("listening")
        await asyncio.sleep(0)
        assert len(session.say_calls) == 1
        assert session.say_calls[0] == "It's 555-1234."


def _speak_payload(message: str) -> dict:
    return {
        "payload": {
            "message": message,
            "should_speak": True,
            "source": "slow_brain",
        },
    }


@pytest.mark.asyncio
async def test_speak_guidance_speaks_the_message(monkeypatch):
    """SPEAK guidance is spoken verbatim via TTS. The spoken text reaches the
    transcript through the real ``say(add_to_chat_ctx=True)`` path (not modeled by
    the fake session); a separate ``[notification]`` injection is reserved for
    awareness (should_speak=False) notifications and must NOT happen here."""
    session, notify, set_agent, set_user = await _boot_entrypoint(monkeypatch)

    spoken = "I found it: spot gold is about 4,486 US dollars per troy ounce."
    notify(_speak_payload(spoken))
    set_agent("listening")
    await asyncio.sleep(0.05)

    assert session.say_calls == [spoken]

    notification_lines = []
    for item in session._chat_ctx.items:
        raw = getattr(item, "content", None)
        if raw is None:
            continue
        text = (
            raw
            if isinstance(raw, str)
            else " ".join(c for c in raw if isinstance(c, str))
        )
        if "[notification]" in text:
            notification_lines.append(text)

    assert not notification_lines, (
        "SPEAK guidance must not inject a [notification] context line - that is "
        "only for awareness (should_speak=False) notifications."
    )
