"""
tests/conversation_manager/test_medium_scripts.py
=======================================================

Tests for the medium scripts (call.py) that handle voice calls.

These scripts implement the "fast brain" Voice Agent that handles real-time
conversation while the Main CM Brain (slow brain) handles orchestration.

## Test Categories

### Unit Tests (no external dependencies)
- Assistant class initialization and state management
- Common helper functions (publish events, create_end_call, etc.)
- CLI argument parsing
- Voice prompt building
- Event type selection based on channel

### Integration Tests (require event broker)
- Event publishing flows
- Guidance subscription patterns
- Cross-thread event delivery

## Key Components Tested

1. **Assistant class** (call.py):
   - Initialization with contact/boss/channel/instructions
   - set_call_received() state transitions
   - Utterance event type selection

2. **Common helpers** (common.py):
   - publish_call_started / publish_call_ended
   - create_end_call with pre_shutdown_callback
   - configure_from_cli argument parsing

3. **Voice Agent prompt**:
   - build_voice_agent_prompt output structure
"""

import asyncio
import json
from types import SimpleNamespace

import pytest
import pytest_asyncio

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def _preserve_global_logging_state():
    """Snapshot and restore global logging state around every test.

    Several tests here drive the real voice ``entrypoint``, which calls
    ``_configure_child_logging`` and, as a global side effect, flips
    ``LOGGER.propagate`` on and clears handlers (the forkserver relay
    fix). Without restoration that state leaks across tests in a
    single-process run and breaks order-dependent expectations — notably
    ``TestChildProcessLogging``, which asserts the default
    ``propagate=False`` precondition. Restoring per test keeps the file
    order-independent regardless of how it's run.
    """
    import logging

    from unity.logger import LOGGER

    _logger_names = ("livekit", "livekit.agents", "livekit.plugins")
    saved = {
        "unity": (LOGGER.propagate, list(LOGGER.handlers)),
        **{
            name: (
                logging.getLogger(name).propagate,
                list(logging.getLogger(name).handlers),
            )
            for name in _logger_names
        },
    }
    try:
        yield
    finally:
        LOGGER.propagate, LOGGER.handlers = saved["unity"]
        for name in _logger_names:
            lg = logging.getLogger(name)
            lg.propagate, lg.handlers = saved[name]


@pytest_asyncio.fixture
async def event_broker():
    """Local in-memory broker for testing."""
    from unity.conversation_manager.event_broker import create_event_broker

    broker = create_event_broker()
    yield broker
    await broker.aclose()


@pytest.fixture
def boss_contact():
    """Standard boss contact for testing."""
    return {
        "contact_id": 1,
        "first_name": "Test",
        "surname": "Boss",
        "phone_number": "+15555555555",
        "email_address": "boss@test.com",
    }


@pytest.fixture
def external_contact():
    """Standard external contact for testing."""
    return {
        "contact_id": 2,
        "first_name": "External",
        "surname": "Contact",
        "phone_number": "+15555555556",
        "email_address": "contact@test.com",
    }


# =============================================================================
# Unit Tests: Assistant Class (call.py - TTS mode)
# =============================================================================


class TestTTSAssistantClass:
    """Tests for the Assistant class in call.py (TTS mode)."""

    def test_assistant_initialization_phone_channel(self, boss_contact):
        """Assistant initializes correctly for phone channel."""
        from unity.conversation_manager.medium_scripts.call import Assistant

        assistant = Assistant(
            contact=boss_contact,
            boss=boss_contact,
            channel="phone_call",
            instructions="Test instructions",
            outbound=False,
        )

        assert assistant.contact == boss_contact
        assert assistant.boss == boss_contact
        assert assistant.channel == "phone_call"
        assert assistant.call_received is True  # inbound call, already received

    def test_assistant_initialization_unify_meet_channel(self, boss_contact):
        """Assistant initializes correctly for unify_meet channel."""
        from unity.conversation_manager.medium_scripts.call import Assistant

        assistant = Assistant(
            contact=boss_contact,
            boss=boss_contact,
            channel="unify_meet",
            instructions="Test instructions",
            outbound=False,
        )

        assert assistant.channel == "unify_meet"
        assert assistant.call_received is True

    def test_assistant_outbound_call_not_received_initially(self, boss_contact):
        """Outbound calls start with call_received=False."""
        from unity.conversation_manager.medium_scripts.call import Assistant

        assistant = Assistant(
            contact=boss_contact,
            boss=boss_contact,
            channel="phone_call",
            instructions="Test instructions",
            outbound=True,
        )

        assert assistant.call_received is False

    def test_assistant_set_call_received(self, boss_contact):
        """set_call_received() updates state correctly."""
        from unity.conversation_manager.medium_scripts.call import Assistant

        assistant = Assistant(
            contact=boss_contact,
            boss=boss_contact,
            channel="phone_call",
            instructions="Test instructions",
            outbound=True,
        )

        assert assistant.call_received is False
        assistant.set_call_received()
        assert assistant.call_received is True

    def test_assistant_utterance_event_type_phone(self, boss_contact):
        """Phone channel uses InboundPhoneUtterance."""
        from unity.conversation_manager.events import InboundPhoneUtterance
        from unity.conversation_manager.medium_scripts.call import Assistant

        assistant = Assistant(
            contact=boss_contact,
            boss=boss_contact,
            channel="phone_call",
            instructions="Test instructions",
        )

        assert assistant.utterance_event == InboundPhoneUtterance

    def test_assistant_utterance_event_type_meet(self, boss_contact):
        """Meet channel uses InboundUnifyMeetUtterance."""
        from unity.conversation_manager.events import InboundUnifyMeetUtterance
        from unity.conversation_manager.medium_scripts.call import Assistant

        assistant = Assistant(
            contact=boss_contact,
            boss=boss_contact,
            channel="unify_meet",
            instructions="Test instructions",
        )

        assert assistant.utterance_event == InboundUnifyMeetUtterance

    def test_assistant_outbound_utterance_event_type_phone(self, boss_contact):
        """Phone channel uses OutboundPhoneUtterance for assistant."""
        from unity.conversation_manager.events import OutboundPhoneUtterance
        from unity.conversation_manager.medium_scripts.call import Assistant

        assistant = Assistant(
            contact=boss_contact,
            boss=boss_contact,
            channel="phone_call",
            instructions="Test instructions",
        )

        assert assistant.assistant_utterance_event == OutboundPhoneUtterance

    def test_assistant_outbound_utterance_event_type_meet(self, boss_contact):
        """Meet channel uses OutboundUnifyMeetUtterance for assistant."""
        from unity.conversation_manager.events import OutboundUnifyMeetUtterance
        from unity.conversation_manager.medium_scripts.call import Assistant

        assistant = Assistant(
            contact=boss_contact,
            boss=boss_contact,
            channel="unify_meet",
            instructions="Test instructions",
        )

        assert assistant.assistant_utterance_event == OutboundUnifyMeetUtterance


class TestElevenLabsTwinPronunciation:
    """Tests for the ElevenLabs text-stream pronunciation normalizer."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("chunks", "expected"),
        [
            (["Say T-W1N now."], "Say Twin now."),
            (["Say t-w1n now."], "Say Twin now."),
            (["Say T-", "w1", "N now."], "Say Twin now."),
            (["T-W1N and t-W1n"], "Twin and Twin"),
            (["Almost T-W1 but not done"], "Almost T-W1 but not done"),
        ],
    )
    async def test_normalizes_twin_marker_across_stream_chunks(
        self,
        chunks,
        expected,
    ):
        from unity.conversation_manager.medium_scripts.call import (
            _normalize_elevenlabs_twin_pronunciation_stream,
        )

        async def _chunks():
            for chunk in chunks:
                yield chunk

        normalized = [
            chunk
            async for chunk in _normalize_elevenlabs_twin_pronunciation_stream(
                _chunks(),
            )
        ]

        assert "".join(normalized) == expected


# =============================================================================
# Unit Tests: Common Helpers
# =============================================================================


class TestCommonHelpers:
    """Tests for shared helper functions in common.py."""

    def test_should_dispatch_livekit_agent_with_dev_command(self, monkeypatch):
        """should_dispatch_livekit_agent returns True for 'dev' command."""
        from unity.conversation_manager.medium_scripts import common

        # Patch sys.argv in the common module's namespace
        monkeypatch.setattr(common.sys, "argv", ["call.py", "dev"])
        assert common.should_dispatch_livekit_agent() is True

    def test_should_dispatch_livekit_agent_with_connect_command(self, monkeypatch):
        """should_dispatch_livekit_agent returns True for 'connect' command."""
        from unity.conversation_manager.medium_scripts import common

        monkeypatch.setattr(common.sys, "argv", ["call.py", "connect"])
        assert common.should_dispatch_livekit_agent() is True

    def test_should_not_dispatch_livekit_agent_for_download_files(self, monkeypatch):
        """should_dispatch_livekit_agent returns False for 'download-files' command."""
        from unity.conversation_manager.medium_scripts import common

        monkeypatch.setattr(common.sys, "argv", ["call.py", "download-files"])
        assert common.should_dispatch_livekit_agent() is False

    def test_should_not_dispatch_livekit_agent_with_no_args(self, monkeypatch):
        """should_dispatch_livekit_agent returns False when no args provided."""
        from unity.conversation_manager.medium_scripts import common

        monkeypatch.setattr(common.sys, "argv", ["call.py"])
        assert common.should_dispatch_livekit_agent() is False


# =============================================================================
# Unit Tests: Event Publishing Helpers
# =============================================================================


@pytest.mark.asyncio
class TestEventPublishingHelpers:
    """Tests for event publishing helper functions.

    These tests patch common.event_broker to use our test fixture so we can
    verify the events are published correctly.
    """

    async def test_publish_call_started_phone_channel(
        self,
        event_broker,
        boss_contact,
        monkeypatch,
    ):
        """publish_call_started publishes PhoneCallStarted for phone channel."""
        from unity.conversation_manager.events import Event, PhoneCallStarted
        from unity.conversation_manager.medium_scripts import common

        # Patch the event_broker in common module to use our test fixture
        monkeypatch.setattr(common, "event_broker", event_broker)

        async with event_broker.pubsub() as pubsub:
            await pubsub.subscribe("app:comms:phone_call_started")

            await common.publish_call_started(boss_contact, "phone_call")

            msg = await pubsub.get_message(
                timeout=2.0,
                ignore_subscribe_messages=True,
            )
            assert msg is not None
            event = Event.from_json(msg["data"])
            assert isinstance(event, PhoneCallStarted)
            assert event.contact == boss_contact

    async def test_publish_call_started_meet_channel(
        self,
        event_broker,
        boss_contact,
        monkeypatch,
    ):
        """publish_call_started publishes UnifyMeetStarted for meet channel."""
        from unity.conversation_manager.events import Event, UnifyMeetStarted
        from unity.conversation_manager.medium_scripts import common

        monkeypatch.setattr(common, "event_broker", event_broker)

        async with event_broker.pubsub() as pubsub:
            await pubsub.subscribe("app:comms:unify_meet_started")

            await common.publish_call_started(boss_contact, "unify_meet")

            msg = await pubsub.get_message(
                timeout=2.0,
                ignore_subscribe_messages=True,
            )
            assert msg is not None
            event = Event.from_json(msg["data"])
            assert isinstance(event, UnifyMeetStarted)
            assert event.contact == boss_contact

    async def test_publish_call_ended_phone_channel(
        self,
        event_broker,
        boss_contact,
        monkeypatch,
    ):
        """publish_call_ended publishes PhoneCallEnded for phone channel."""
        from unity.conversation_manager.events import Event, PhoneCallEnded
        from unity.conversation_manager.medium_scripts import common

        monkeypatch.setattr(common, "event_broker", event_broker)

        async with event_broker.pubsub() as pubsub:
            await pubsub.subscribe("app:comms:phone_call_ended")

            await common.publish_call_ended(boss_contact, "phone_call")

            msg = await pubsub.get_message(
                timeout=2.0,
                ignore_subscribe_messages=True,
            )
            assert msg is not None
            event = Event.from_json(msg["data"])
            assert isinstance(event, PhoneCallEnded)
            assert event.contact == boss_contact

    async def test_publish_call_ended_meet_channel(
        self,
        event_broker,
        boss_contact,
        monkeypatch,
    ):
        """publish_call_ended publishes UnifyMeetEnded for meet channel."""
        from unity.conversation_manager.events import Event, UnifyMeetEnded
        from unity.conversation_manager.medium_scripts import common

        monkeypatch.setattr(common, "event_broker", event_broker)

        async with event_broker.pubsub() as pubsub:
            await pubsub.subscribe("app:comms:unify_meet_ended")

            await common.publish_call_ended(boss_contact, "unify_meet")

            msg = await pubsub.get_message(
                timeout=2.0,
                ignore_subscribe_messages=True,
            )
            assert msg is not None
            event = Event.from_json(msg["data"])
            assert isinstance(event, UnifyMeetEnded)
            assert event.contact == boss_contact


# =============================================================================
# Unit Tests: End Call Helper
# =============================================================================


@pytest.mark.asyncio
class TestEndCallHelper:
    """Tests for create_end_call helper.

    These tests patch common.event_broker to use our test fixture so we can
    verify the events are published correctly.
    """

    async def test_create_end_call_publishes_ended_event(
        self,
        event_broker,
        boss_contact,
        monkeypatch,
    ):
        """create_end_call returns function that publishes ended event."""
        from unity.conversation_manager.events import Event, PhoneCallEnded
        from unity.conversation_manager.medium_scripts import common

        # Patch the event_broker in common module to use our test fixture
        monkeypatch.setattr(common, "event_broker", event_broker)

        end_call = common.create_end_call(boss_contact, "phone_call")

        async with event_broker.pubsub() as pubsub:
            await pubsub.subscribe("app:comms:phone_call_ended")

            await end_call()

            msg = await pubsub.get_message(
                timeout=2.0,
                ignore_subscribe_messages=True,
            )
            assert msg is not None
            event = Event.from_json(msg["data"])
            assert isinstance(event, PhoneCallEnded)

    async def test_create_end_call_runs_pre_shutdown_callback(
        self,
        event_broker,
        boss_contact,
        monkeypatch,
    ):
        """create_end_call runs pre_shutdown_callback before shutdown."""
        from unity.conversation_manager.medium_scripts import common

        monkeypatch.setattr(common, "event_broker", event_broker)

        callback_called = {"value": False}

        def pre_shutdown():
            callback_called["value"] = True

        end_call = common.create_end_call(
            boss_contact,
            "phone_call",
            pre_shutdown_callback=pre_shutdown,
        )

        await end_call()

        assert callback_called["value"] is True

    async def test_create_end_call_handles_callback_error(
        self,
        event_broker,
        boss_contact,
        monkeypatch,
        caplog,
    ):
        """create_end_call continues even if callback raises."""
        import logging

        from unity.conversation_manager.medium_scripts import common

        monkeypatch.setattr(common, "event_broker", event_broker)

        def failing_callback():
            raise ValueError("Callback error")

        end_call = common.create_end_call(
            boss_contact,
            "phone_call",
            pre_shutdown_callback=failing_callback,
        )

        unity_logger = logging.getLogger("unity")
        unity_logger.addHandler(caplog.handler)
        caplog.handler.setLevel(logging.DEBUG)
        try:
            await end_call()
        finally:
            unity_logger.removeHandler(caplog.handler)

        assert "Error in pre-shutdown callback" in caplog.text


# =============================================================================
# Unit Tests: CLI Argument Parsing
# =============================================================================


class TestCLIArgumentParsing:
    """Tests for configure_from_cli argument parsing.

    These tests patch sys.argv in the common module's namespace since that's
    where configure_from_cli reads it from.
    """

    def test_configure_from_cli_with_full_args(self, monkeypatch):
        """configure_from_cli parses all arguments correctly."""
        from unity.conversation_manager.medium_scripts import common
        from unity.session_details import SESSION_DETAILS

        # Reset SESSION_DETAILS before test
        SESSION_DETAILS.reset()

        contact_json = json.dumps(
            {
                "contact_id": 1,
                "first_name": "Test",
                "surname": "User",
            },
        )
        boss_json = json.dumps(
            {
                "contact_id": 1,
                "first_name": "Boss",
                "surname": "Person",
            },
        )

        # argv[2] is the canonical room name from make_room_name() in call_manager
        monkeypatch.setattr(
            common.sys,
            "argv",
            [
                "call.py",
                "dev",
                "unity_test_assistant_id_phone",
                "elevenlabs",
                "voice123",
                "True",
                "phone_call",
                contact_json,
                boss_json,
                "Test assistant bio",
                "test_assistant_id",
                "test_user_id",
            ],
        )

        room_name = common.configure_from_cli(
            extra_env=[
                ("CONTACT", True),
                ("BOSS", True),
                ("ASSISTANT_BIO", False),
                ("ASSISTANT_ID", False),
                ("USER_ID", False),
            ],
        )

        assert room_name == "unity_test_assistant_id_phone"
        assert SESSION_DETAILS.voice.provider == "elevenlabs"
        assert SESSION_DETAILS.voice.id == "voice123"
        assert SESSION_DETAILS.voice_call.outbound is True
        assert SESSION_DETAILS.voice_call.channel == "phone_call"

    def test_configure_from_cli_meet_room_name(self, monkeypatch):
        """configure_from_cli returns the canonical room name for UnifyMeet calls."""
        from unity.conversation_manager.medium_scripts import common
        from unity.session_details import SESSION_DETAILS

        SESSION_DETAILS.reset()

        contact_json = json.dumps({"contact_id": 1, "first_name": "Test"})
        boss_json = json.dumps({"contact_id": 1, "first_name": "Boss"})

        # Simulate UnifyMeet call — argv[2] is the canonical room name
        # (same as phone calls; agent_name = room_name for all mediums)
        monkeypatch.setattr(
            common.sys,
            "argv",
            [
                "call.py",
                "dev",
                "unity_25_meet",
                "cartesia",
                "voice456",
                "False",
                "unify_meet",
                contact_json,
                boss_json,
                "Bio",
                "test_assistant_id",
                "test_user_id",
            ],
        )

        room_name = common.configure_from_cli(
            extra_env=[
                ("CONTACT", True),
                ("BOSS", True),
                ("ASSISTANT_BIO", False),
                ("ASSISTANT_ID", False),
                ("USER_ID", False),
            ],
        )

        assert room_name == "unity_25_meet"

    def test_configure_from_cli_defaults_none_voice_provider(self, monkeypatch):
        """configure_from_cli defaults 'None' voice provider to cartesia."""
        from unity.conversation_manager.medium_scripts import common
        from unity.session_details import SESSION_DETAILS

        SESSION_DETAILS.reset()

        contact_json = json.dumps({"contact_id": 1, "first_name": "Test"})
        boss_json = json.dumps({"contact_id": 1, "first_name": "Boss"})

        monkeypatch.setattr(
            common.sys,
            "argv",
            [
                "call.py",
                "dev",
                "12345",
                "None",  # Voice provider as "None" string
                "None",  # Voice ID as "None" string
                "False",
                "phone_call",
                contact_json,
                boss_json,
                "Bio",
                "test_assistant_id",
                "test_user_id",
            ],
        )

        common.configure_from_cli(
            extra_env=[
                ("CONTACT", True),
                ("BOSS", True),
                ("ASSISTANT_BIO", False),
                ("ASSISTANT_ID", False),
                ("USER_ID", False),
            ],
        )

        assert SESSION_DETAILS.voice.provider == "cartesia"
        assert SESSION_DETAILS.voice.id == ""


# =============================================================================
# Integration Tests: Guidance Channel Subscription
# =============================================================================


@pytest.mark.asyncio
class TestGuidanceChannelSubscription:
    """Tests for guidance channel subscription patterns."""

    async def test_guidance_channel_receives_call_guidance(self, event_broker):
        """Guidance channel receives FastBrainNotification events."""
        from unity.conversation_manager.events import FastBrainNotification, Event

        contact = {"contact_id": 1, "first_name": "Test"}

        async with event_broker.pubsub() as pubsub:
            await pubsub.subscribe("app:call:notification")

            event = FastBrainNotification(contact=contact, message="Test guidance")
            await event_broker.publish("app:call:notification", event.to_json())

            msg = await pubsub.get_message(
                timeout=2.0,
                ignore_subscribe_messages=True,
            )
            assert msg is not None
            received = Event.from_json(msg["data"])
            assert isinstance(received, FastBrainNotification)
            assert received.message == "Test guidance"

    async def test_status_channel_receives_stop_signal(self, event_broker):
        """Status channel receives stop signals."""
        async with event_broker.pubsub() as pubsub:
            await pubsub.subscribe("app:call:status")

            await event_broker.publish(
                "app:call:status",
                json.dumps({"type": "stop"}),
            )

            msg = await pubsub.get_message(
                timeout=2.0,
                ignore_subscribe_messages=True,
            )
            assert msg is not None
            data = json.loads(msg["data"])
            assert data["type"] == "stop"

    async def test_status_channel_receives_call_answered_signal(self, event_broker):
        """Status channel receives call_answered signals."""
        async with event_broker.pubsub() as pubsub:
            await pubsub.subscribe("app:call:status")

            await event_broker.publish(
                "app:call:status",
                json.dumps({"type": "call_answered"}),
            )

            msg = await pubsub.get_message(
                timeout=2.0,
                ignore_subscribe_messages=True,
            )
            assert msg is not None
            data = json.loads(msg["data"])
            assert data["type"] == "call_answered"


# =============================================================================
# Integration Tests: Utterance Event Publishing
# =============================================================================


@pytest.mark.asyncio
class TestUtteranceEventPublishing:
    """Tests for utterance event publishing patterns."""

    async def test_phone_utterance_published_to_correct_channel(
        self,
        event_broker,
        boss_contact,
    ):
        """Phone utterances are published to app:comms:phone_utterance."""
        from unity.conversation_manager.events import (
            Event,
            InboundPhoneUtterance,
        )

        async with event_broker.pubsub() as pubsub:
            await pubsub.subscribe("app:comms:phone_utterance")

            event = InboundPhoneUtterance(contact=boss_contact, content="Hello")
            await event_broker.publish("app:comms:phone_utterance", event.to_json())

            msg = await pubsub.get_message(
                timeout=2.0,
                ignore_subscribe_messages=True,
            )
            assert msg is not None
            received = Event.from_json(msg["data"])
            assert isinstance(received, InboundPhoneUtterance)
            assert received.content == "Hello"

    async def test_meet_utterance_published_to_correct_channel(
        self,
        event_broker,
        boss_contact,
    ):
        """Meet utterances are published to app:comms:meet_utterance."""
        from unity.conversation_manager.events import (
            Event,
            InboundUnifyMeetUtterance,
        )

        async with event_broker.pubsub() as pubsub:
            await pubsub.subscribe("app:comms:meet_utterance")

            event = InboundUnifyMeetUtterance(contact=boss_contact, content="Hello")
            await event_broker.publish("app:comms:meet_utterance", event.to_json())

            msg = await pubsub.get_message(
                timeout=2.0,
                ignore_subscribe_messages=True,
            )
            assert msg is not None
            received = Event.from_json(msg["data"])
            assert isinstance(received, InboundUnifyMeetUtterance)
            assert received.content == "Hello"


# =============================================================================
# Guidance tests: chat context and system messages
# =============================================================================


@pytest.mark.asyncio
async def test_simulated_opening_publishes_ready_before_utterance(monkeypatch):
    from livekit.agents import llm
    from unity.conversation_manager.medium_scripts import call as call_script

    sequence = []
    contact = {"contact_id": 1, "first_name": "User", "surname": "Example"}
    boss = {"contact_id": 1, "first_name": "User", "surname": "Example"}

    class _ImmediateAwaitable:
        def __await__(self):
            async def _done():
                return None

            return _done().__await__()

    class _FakeLocalParticipant:
        async def publish_data(self, payload, *, topic=None, reliable=False):
            sequence.append(("data", json.loads(payload.decode()), topic, reliable))

    class _FakeRoom:
        name = "fake-room"
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
                        "outbound": False,
                        "channel": "unify_meet",
                        "contact": contact,
                        "boss": boss,
                        "assistant_bio": "Assistant bio",
                        "assistant_id": "123",
                        "user_id": "user-123",
                        "assistant_name": "Coordinator Unity",
                        "opening_config": {
                            "mode": "simulated",
                            "simulated_utterance": "Hi, I'm your coordinator unity.",
                            "source": "coordinator_onboarding_intro",
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
            sequence.append(("broker", channel))
            return 1

    fake_session_holder = {}

    class _FakeSession:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.current_agent = None
            self._events = {}
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
            return _ImmediateAwaitable()

        def say(self, text, **kwargs):
            return _ImmediateAwaitable()

        def interrupt(self):
            pass

    class _FakeAssistant:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.call_received = True
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
            is_coordinator=True,
            agent_id=None,
            name="Coordinator Unity",
            first_name="",
            surname="",
            user_desktop_for=lambda user_id: None,
        ),
        voice=SimpleNamespace(provider="cartesia", id=""),
        voice_call=SimpleNamespace(outbound=False, channel="unify_meet"),
        is_coordinator=True,
        org_id=None,
        unify_key="",
    )

    monkeypatch.setattr(call_script, "event_broker", _FakeEventBroker())
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

    await call_script.entrypoint(_FakeJobContext())

    ready_index = next(
        i
        for i, item in enumerate(sequence)
        if item[0] == "data" and item[1] == {"type": "ready_to_speak"}
    )
    utterance_index = next(
        i
        for i, item in enumerate(sequence)
        if item[0] == "broker" and item[1] == "app:comms:unify_meet_utterance"
    )
    assert ready_index < utterance_index


@pytest.mark.asyncio
async def test_recorded_opening_uses_interruptible_audio_say(monkeypatch):
    from livekit.agents import llm
    from unity.conversation_manager.medium_scripts import call as call_script

    sequence = []
    audio_sources = []
    fake_audio = object()
    contact = {"contact_id": 1, "first_name": "User", "surname": "Example"}
    boss = {"contact_id": 1, "first_name": "User", "surname": "Example"}

    class _ImmediateAwaitable:
        def __await__(self):
            async def _done():
                return None

            return _done().__await__()

    class _FakeLocalParticipant:
        async def publish_data(self, payload, *, topic=None, reliable=False):
            sequence.append(("data", json.loads(payload.decode()), topic, reliable))

    class _FakeRoom:
        name = "fake-room"
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
                        "outbound": False,
                        "channel": "unify_meet",
                        "contact": contact,
                        "boss": boss,
                        "assistant_bio": "Assistant bio",
                        "assistant_id": "123",
                        "user_id": "user-123",
                        "assistant_name": "Coordinator Unity",
                        "is_coordinator": False,
                        "opening_config": {
                            "mode": "recorded",
                            "transcript": "Hi, I'm your coordinator unity.",
                            "recording_path": "/tmp/recorded-opener.wav",
                            "source": "coordinator_onboarding_intro",
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
        def set_logger(self, fb_logger):
            pass

        def register_callback(self, channel, handler):
            pass

        async def publish(self, channel, message):
            sequence.append(("broker", channel, message))
            return 1

        def reinit_socket(self):
            pass

    class _FakeSession:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.current_agent = None
            self._events = {}
            self.agent_state = "listening"
            self.current_speech = None
            self.say_calls = []
            self.generate_reply_calls = []

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
            self.generate_reply_calls.append(kwargs)
            return _ImmediateAwaitable()

        def say(self, text, **kwargs):
            sequence.append(("say", text, kwargs))
            self.say_calls.append((text, kwargs))
            return _ImmediateAwaitable()

        def interrupt(self):
            pass

    class _FakeAssistant:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.call_received = True
            self.user_turn_generating = False

        def set_call_received(self):
            self.call_received = True

        def set_credit_gate_state_provider(self, provider):
            self.credit_gate_state_provider = provider

    class _FakeCreditGateMonitor:
        state = SimpleNamespace(allowed=True)

        async def run(self):
            return None

    async def _noop_async(*args, **kwargs):
        return None

    async def _empty_history(*args, **kwargs):
        return []

    async def _noop_end_call():
        return None

    def _fake_recording_audio_frames(source):
        audio_sources.append(source)
        return fake_audio

    fake_session_details = SimpleNamespace(
        user=SimpleNamespace(id="user-123"),
        assistant=SimpleNamespace(
            about="Assistant bio",
            is_coordinator=False,
            agent_id=None,
            name="Coordinator Unity",
            first_name="",
            surname="",
            user_desktop_for=lambda user_id: None,
        ),
        voice=SimpleNamespace(provider="cartesia", id=""),
        voice_call=SimpleNamespace(outbound=False, channel="unify_meet"),
        is_coordinator=False,
        org_id=None,
        unify_key="",
    )

    session_holder = {}

    def _fake_session_factory(*args, **kwargs):
        session = _FakeSession(*args, **kwargs)
        session_holder["session"] = session
        return session

    monkeypatch.setattr(call_script, "event_broker", _FakeEventBroker())
    monkeypatch.setattr(call_script, "SESSION_DETAILS", fake_session_details)
    monkeypatch.setattr(call_script, "AgentSession", _fake_session_factory)
    monkeypatch.setattr(call_script, "Assistant", _FakeAssistant)
    monkeypatch.setattr(call_script, "UnifyLLM", lambda *args, **kwargs: object())
    monkeypatch.setattr(
        call_script,
        "build_voice_agent_prompt",
        lambda **kwargs: SimpleNamespace(flatten=lambda: "system prompt"),
    )
    monkeypatch.setattr(call_script, "start_event_broker_receive", _noop_async)
    monkeypatch.setattr(call_script, "hydrate_fast_brain_history", _empty_history)
    monkeypatch.setattr(call_script, "publish_call_started", _noop_async)
    monkeypatch.setattr(call_script, "publish_call_ended", _noop_async)
    monkeypatch.setattr(call_script, "delete_livekit_room", _noop_async)
    monkeypatch.setattr(
        call_script,
        "FastBrainCreditGateMonitor",
        _FakeCreditGateMonitor,
    )
    monkeypatch.setattr(
        call_script,
        "_recording_audio_frames",
        _fake_recording_audio_frames,
    )
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

    await call_script.entrypoint(_FakeJobContext())

    session = session_holder["session"]
    assert audio_sources == ["/tmp/recorded-opener.wav"]
    assert session.generate_reply_calls == []
    assert len(session.say_calls) == 1

    text, kwargs = session.say_calls[0]
    assert text == "Hi, I'm your coordinator unity."
    assert kwargs["audio"] is fake_audio
    assert kwargs["allow_interruptions"] is True
    assert kwargs["add_to_chat_ctx"] is True

    ready_index = next(
        i
        for i, item in enumerate(sequence)
        if item[0] == "data" and item[1] == {"type": "ready_to_speak"}
    )
    say_index = next(i for i, item in enumerate(sequence) if item[0] == "say")
    assert ready_index < say_index


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupt_walkie", [True, False])
async def test_walkie_opener_arms_bridge_only_on_early_interruption(
    monkeypatch,
    interrupt_walkie,
):
    from livekit.agents import llm
    from unity.conversation_manager.medium_scripts import call as call_script

    sequence = []
    audio_sources = []
    fake_audio = object()
    # Only the first (walkie) segment is interrupted when interrupt_walkie.
    interrupt_flags = [True] if interrupt_walkie else []
    contact = {"contact_id": 1, "first_name": "User", "surname": "Example"}
    boss = {"contact_id": 1, "first_name": "User", "surname": "Example"}

    class _ImmediateAwaitable:
        def __await__(self):
            async def _done():
                return None

            return _done().__await__()

    class _FakeSpeechHandle:
        def __init__(self, interrupted):
            self.interrupted = interrupted

        async def wait_for_playout(self):
            return None

    class _FakeLocalParticipant:
        async def publish_data(self, payload, *, topic=None, reliable=False):
            sequence.append(("data", json.loads(payload.decode()), topic, reliable))

    class _FakeRoom:
        name = "fake-room"
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
                        "outbound": False,
                        "channel": "unify_meet",
                        "contact": contact,
                        "boss": boss,
                        "assistant_bio": "Assistant bio",
                        "assistant_id": "123",
                        "user_id": "user-123",
                        "assistant_name": "Coordinator Unity",
                        "is_coordinator": False,
                        "opening_config": {
                            "mode": "recorded",
                            "recording_asset": "coordinator_onboarding_intro",
                            "source": "coordinator_onboarding_intro",
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
        def set_logger(self, fb_logger):
            pass

        def register_callback(self, channel, handler):
            pass

        async def publish(self, channel, message):
            sequence.append(("broker", channel, message))
            return 1

        def reinit_socket(self):
            pass

    class _FakeSession:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.current_agent = None
            self._events = {}
            self.agent_state = "listening"
            self.current_speech = None
            self.say_calls = []
            self.generate_reply_calls = []

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
            self.generate_reply_calls.append(kwargs)
            return _ImmediateAwaitable()

        def say(self, text, **kwargs):
            sequence.append(("say", text, kwargs))
            self.say_calls.append((text, kwargs))
            interrupted = interrupt_flags.pop(0) if interrupt_flags else False
            return _FakeSpeechHandle(interrupted)

        def interrupt(self):
            pass

    class _FakeAssistant:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.call_received = True
            self.user_turn_generating = False
            self._pending_opening_bridge = None

        def set_call_received(self):
            self.call_received = True

        def set_credit_gate_state_provider(self, provider):
            self.credit_gate_state_provider = provider

    class _FakeCreditGateMonitor:
        state = SimpleNamespace(allowed=True)

        async def run(self):
            return None

    async def _noop_async(*args, **kwargs):
        return None

    async def _empty_history(*args, **kwargs):
        return []

    async def _noop_end_call():
        return None

    def _fake_recording_audio_frames(source):
        audio_sources.append(source)
        return fake_audio

    fake_session_details = SimpleNamespace(
        user=SimpleNamespace(id="user-123"),
        assistant=SimpleNamespace(
            about="Assistant bio",
            is_coordinator=False,
            agent_id=None,
            name="Coordinator Unity",
            first_name="",
            surname="",
            user_desktop_for=lambda user_id: None,
        ),
        voice=SimpleNamespace(provider="cartesia", id=""),
        voice_call=SimpleNamespace(outbound=False, channel="unify_meet"),
        is_coordinator=False,
        org_id=None,
        unify_key="",
    )

    session_holder = {}

    def _fake_session_factory(*args, **kwargs):
        session = _FakeSession(*args, **kwargs)
        session_holder["session"] = session
        return session

    monkeypatch.setattr(call_script, "event_broker", _FakeEventBroker())
    monkeypatch.setattr(call_script, "SESSION_DETAILS", fake_session_details)
    monkeypatch.setattr(call_script, "AgentSession", _fake_session_factory)
    monkeypatch.setattr(call_script, "Assistant", _FakeAssistant)
    monkeypatch.setattr(call_script, "UnifyLLM", lambda *args, **kwargs: object())
    monkeypatch.setattr(
        call_script,
        "build_voice_agent_prompt",
        lambda **kwargs: SimpleNamespace(flatten=lambda: "system prompt"),
    )
    monkeypatch.setattr(call_script, "start_event_broker_receive", _noop_async)
    monkeypatch.setattr(call_script, "hydrate_fast_brain_history", _empty_history)
    monkeypatch.setattr(call_script, "publish_call_started", _noop_async)
    monkeypatch.setattr(call_script, "publish_call_ended", _noop_async)
    monkeypatch.setattr(call_script, "delete_livekit_room", _noop_async)
    monkeypatch.setattr(
        call_script,
        "FastBrainCreditGateMonitor",
        _FakeCreditGateMonitor,
    )
    monkeypatch.setattr(
        call_script,
        "_recording_audio_frames",
        _fake_recording_audio_frames,
    )
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

    await call_script.entrypoint(_FakeJobContext())

    session = session_holder["session"]
    assert session.generate_reply_calls == []
    assert session.say_calls[0][0].startswith("Hi, I'm T dash W 1 N.")

    if interrupt_walkie:
        # Interrupting the first staticky sentence stops the opener immediately;
        # no later sentence (nor the clean transition) plays, and the bridge is
        # armed for the next turn.
        assert audio_sources == ["asset://coordinator_onboarding_intro_walkie_00"]
        assert len(session.say_calls) == 1
        assert session.current_agent._pending_opening_bridge is not None
    else:
        # All 20 staticky sentence slices play in order, then the clean
        # transition segment, and no bridge is armed.
        assert len(session.say_calls) == 21
        assert audio_sources[0] == "asset://coordinator_onboarding_intro_walkie_00"
        assert audio_sources[19] == "asset://coordinator_onboarding_intro_walkie_19"
        assert audio_sources[-1] == "asset://coordinator_onboarding_intro_clean"
        assert session.say_calls[-1][0].startswith("Much better.")
        assert session.current_agent._pending_opening_bridge is None


def test_recorded_opening_builtin_asset_validates_without_single_transcript():
    from unity.conversation_manager.medium_scripts import call as call_script

    # The builtin onboarding opener is segmented, so it must validate without a
    # single inline transcript or recording source.
    config = call_script._normalize_call_opening_config(
        {
            "mode": "recorded",
            "recording_asset": "coordinator_onboarding_intro",
            "source": "coordinator_onboarding_intro",
        },
    )
    assert config["recording_asset"] == "coordinator_onboarding_intro"


def test_walkie_opener_segments_split_at_static_removal_transition():
    from unity.conversation_manager.medium_scripts import call as call_script

    spec = call_script._RECORDED_OPENINGS["coordinator_onboarding_intro"]
    segments = spec["segments"]
    staticky = segments[:-1]
    clean = segments[-1]
    bridge = spec["bridge"]

    # The staticky intro is split into per-sentence segments; the final one is
    # the spoken static-removal cue. The clean segment opens after the transition.
    assert len(staticky) == 20
    assert staticky[0]["transcript"].startswith("Hi, I'm T dash W 1 N.")
    assert (
        staticky[-1]["transcript"].rstrip() == "Also, let me remove this voice static."
    )
    for seg in staticky:
        assert "Much better." not in seg["transcript"]
    assert clean["transcript"].startswith("Much better.")

    # The bridge re-performs the static removal for callers who interrupted the
    # walkie segment before the transition.
    assert "remove this voice static" in bridge["transcript"]
    assert "Much better." in bridge["transcript"]

    # Each segment resolves to its own bundled asset.
    for segment in (*staticky, clean, bridge):
        assert segment["asset"] in call_script._RECORDED_OPENING_ASSETS

    # Older ad-lib lines stay redacted from the spoken transcripts.
    combined = (
        "".join(seg["transcript"] for seg in staticky)
        + clean["transcript"]
        + bridge["transcript"]
    )
    assert "Krispy Kreme" not in combined
    assert "Actually, first lets turn off this really annoying music." not in combined
    assert "There we go, now I'll pull up the platform." not in combined


@pytest.mark.asyncio
async def test_on_user_turn_completed_schedules_pending_opening_bridge():
    from types import SimpleNamespace

    from unity.conversation_manager.medium_scripts import call as call_script

    scheduled: list[bool] = []

    # The bridge callable is synchronous: it enqueues the bridge say but does
    # NOT await playout, so the hook returns immediately and the reply is
    # generated concurrently (and queues behind the bridge).
    def _schedule_bridge() -> None:
        scheduled.append(True)

    self = SimpleNamespace(
        _pending_opening_bridge=_schedule_bridge,
        _user_speech_logged=False,
    )
    new_message = SimpleNamespace(text_content="hi there")

    await call_script.Assistant.on_user_turn_completed(
        self,
        turn_ctx=None,
        new_message=new_message,
    )

    # The bridge is scheduled exactly once and disarmed so later turns are
    # normal.
    assert scheduled == [True]
    assert self._pending_opening_bridge is None
    assert self._user_speech_logged is True


@pytest.mark.asyncio
async def test_on_user_turn_completed_without_bridge_is_normal():
    from types import SimpleNamespace

    from unity.conversation_manager.medium_scripts import call as call_script

    self = SimpleNamespace(_pending_opening_bridge=None, _user_speech_logged=False)
    new_message = SimpleNamespace(text_content="hello")

    await call_script.Assistant.on_user_turn_completed(
        self,
        turn_ctx=None,
        new_message=new_message,
    )

    assert self._pending_opening_bridge is None
    assert self._user_speech_logged is True


@pytest.mark.asyncio
async def test_elevenlabs_onboarding_opener_speed_restores_when_user_speaks(
    monkeypatch,
):
    from livekit.agents import llm
    from unity.common import llm_client
    from unity.conversation_manager.medium_scripts import call as call_script

    contact = {"contact_id": 1, "first_name": "User", "surname": "Example"}
    boss = {"contact_id": 1, "first_name": "User", "surname": "Example"}
    fake_session_holder = {}
    fake_tts_holder = {}

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            self._responses = [
                _FakeResponse(
                    {
                        "info": {
                            "onboarding": {
                                "next_targets": [
                                    {
                                        "id": "workspace_setup",
                                        "title": "Connect workspace",
                                    },
                                ],
                                "active_step_id": "workspace_setup",
                            },
                        },
                    },
                ),
                _FakeResponse({"info": {}}),
            ]

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, *args, **kwargs):
            return self._responses.pop(0)

    class _FakeGreetingClient:
        async def generate(self, *, messages):
            return "Hi, I'm T dash W 1 N. Please acknowledge the excellent name."

    class _FakeConnection:
        def __init__(self):
            self.marked_non_current = False

        def mark_non_current(self):
            self.marked_non_current = True

    class _FakeTTS:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self._opts = SimpleNamespace(voice_settings=call_script.NOT_GIVEN)
            self._TTS__current_connection = _FakeConnection()
            self.update_calls = []
            fake_tts_holder["tts"] = self

        def update_options(self, *, voice_settings):
            self.update_calls.append(voice_settings)
            self._opts.voice_settings = voice_settings

    class _FakeSpeechHandle:
        def __init__(self):
            self._done = asyncio.Event()
            self.done = False

        async def wait_for_playout(self):
            await self._done.wait()
            self.done = True

        def complete(self):
            self._done.set()

    class _FakeLocalParticipant:
        async def publish_data(self, payload, *, topic=None, reliable=False):
            return None

    class _FakeRoom:
        name = "fake-room"
        local_participant = _FakeLocalParticipant()

        def on(self, *args, **kwargs):
            return lambda fn: fn

    class _FakeJobContext:
        def __init__(self):
            self.room = _FakeRoom()
            self.job = SimpleNamespace(
                metadata=json.dumps(
                    {
                        "voice_provider": "elevenlabs",
                        "voice_id": "voice123",
                        "outbound": False,
                        "channel": "unify_meet",
                        "contact": contact,
                        "boss": boss,
                        "assistant_bio": "Assistant bio",
                        "assistant_id": "123",
                        "user_id": "user-123",
                        "assistant_name": "Coordinator Unity",
                        "is_coordinator": True,
                        "opening_config": {"mode": "speak"},
                    },
                ),
            )
            self.shutdown_callbacks = []

        async def connect(self):
            return None

        def add_shutdown_callback(self, cb):
            self.shutdown_callbacks.append(cb)

        def shutdown(self, reason=""):
            pass

    class _FakeEventBroker:
        def set_logger(self, fb_logger):
            pass

        def register_callback(self, channel, handler):
            pass

        async def publish(self, channel, message):
            return 1

        def reinit_socket(self):
            pass

    class _FakeSession:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.current_agent = None
            self._events = {}
            self.agent_state = "speaking"
            self.current_speech = None
            self.say_calls = []
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
            return None

        def say(self, text, **kwargs):
            handle = _FakeSpeechHandle()
            self.current_speech = handle
            self.say_calls.append((text, kwargs, handle))
            return handle

        def interrupt(self):
            pass

    class _FakeAssistant:
        def __init__(self, *args, **kwargs):
            self._chat_ctx = llm.ChatContext()
            self.call_received = True
            self.user_turn_generating = False

        def set_call_received(self):
            self.call_received = True

        def set_credit_gate_state_provider(self, provider):
            self.credit_gate_state_provider = provider

    class _FakeCreditGateMonitor:
        state = SimpleNamespace(allowed=True)

        async def run(self):
            await asyncio.Event().wait()

    async def _noop_async(*args, **kwargs):
        return None

    async def _noop_end_call():
        return None

    call_script.SESSION_DETAILS.reset()
    monkeypatch.setattr(call_script, "event_broker", _FakeEventBroker())
    monkeypatch.setattr(call_script, "AgentSession", _FakeSession)
    monkeypatch.setattr(call_script, "Assistant", _FakeAssistant)
    monkeypatch.setattr(call_script, "UnifyLLM", lambda *args, **kwargs: object())
    monkeypatch.setattr(
        call_script,
        "build_voice_agent_prompt",
        lambda **kwargs: SimpleNamespace(flatten=lambda: "system prompt"),
    )
    monkeypatch.setattr(call_script, "start_event_broker_receive", _noop_async)
    monkeypatch.setattr(call_script, "hydrate_fast_brain_history", _noop_async)
    monkeypatch.setattr(call_script, "publish_call_started", _noop_async)
    monkeypatch.setattr(call_script, "publish_call_ended", _noop_async)
    monkeypatch.setattr(call_script, "delete_livekit_room", _noop_async)
    monkeypatch.setattr(
        call_script,
        "FastBrainCreditGateMonitor",
        _FakeCreditGateMonitor,
    )
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
    monkeypatch.setattr(call_script.elevenlabs, "TTS", _FakeTTS)
    monkeypatch.setattr(
        llm_client,
        "new_llm_client",
        lambda **kwargs: _FakeGreetingClient(),
    )
    monkeypatch.setattr("httpx.AsyncClient", _FakeAsyncClient)
    if hasattr(call_script, "noise_cancellation"):
        monkeypatch.setattr(call_script.noise_cancellation, "BVC", lambda: object())
    monkeypatch.setattr(call_script, "STT", object())
    monkeypatch.setattr(call_script, "VAD", object())

    await call_script.entrypoint(_FakeJobContext())

    fake_tts = fake_tts_holder["tts"]
    assert len(fake_tts.update_calls) == 1
    assert fake_tts.update_calls[0].speed == 0.5

    session = fake_session_holder["session"]
    previous_connection = fake_tts._TTS__current_connection
    session._events["user_state_changed"](SimpleNamespace(new_state="speaking"))

    assert fake_tts._opts.voice_settings is call_script.NOT_GIVEN
    assert previous_connection.marked_non_current is True
    assert fake_tts._TTS__current_connection is None

    session.say_calls[0][2].complete()
    await asyncio.sleep(0)


@pytest.mark.asyncio
class TestFastBrainGuidanceFlow:
    """Coverage for guidance delivery in the TTS fast brain path."""

    async def test_notify_only_guidance_injects_context_without_direct_speech(
        self,
        monkeypatch,
    ):
        """Guidance with should_speak=False injects into both chat contexts
        without calling session.say() directly.  The structured notification
        evaluator (_schedule_notification_eval) handles the speak/wait
        decision asynchronously — that path is covered by
        test_structured_notification_reply.py."""
        from livekit.agents import llm
        from unity.conversation_manager.medium_scripts import call as call_script

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

            def set_call_received(self):
                self.call_received = True

            def set_credit_gate_state_provider(self, provider):
                self.credit_gate_state_provider = provider

        async def _noop_async(*args, **kwargs):
            return None

        async def _noop_end_call():
            return None

        fake_broker = _FakeEventBroker()
        fake_session_details = SimpleNamespace(
            populate_from_env=lambda: None,
            user=SimpleNamespace(id="user-123"),
            voice=SimpleNamespace(provider="cartesia", id=""),
            assistant=SimpleNamespace(
                about="Assistant bio",
                name="Ava",
                first_name="Assistant",
                surname="Example",
                agent_id=None,
                user_desktop_for=lambda user_id: None,
            ),
            voice_call=SimpleNamespace(
                outbound=False,
                channel="unify_meet",
                contact_json=json.dumps(contact),
                boss_json=json.dumps(boss),
            ),
            is_coordinator=False,
            org_id=None,
            unify_key="",
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

        import unity.common.llm_client as _llm_mod

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

        # Send notify-only guidance (should_speak=False)
        guidance_cb = fake_broker.callbacks["app:call:notification"]
        guidance_cb({"payload": {"message": "No, there is no contact named Bob."}})

        # Notification should be in both chat contexts
        mirror_texts = [
            item.text_content or ""
            for item in session._chat_ctx.items
            if getattr(item, "type", None) == "message"
        ]
        assert any("No, there is no contact named Bob." in txt for txt in mirror_texts)

        agent_texts = [
            item.text_content or ""
            for item in session.current_agent._chat_ctx.items
            if getattr(item, "type", None) == "message"
        ]
        assert any("No, there is no contact named Bob." in txt for txt in agent_texts)

        # say() must NOT fire directly — the structured notification evaluator
        # decides asynchronously whether to speak.
        assert (
            len(session.say_calls) == 0
        ), "Notify-only guidance must NOT trigger session.say() directly."

        from unity.conversation_manager.events import AssistantTurnInjected

        injected = AssistantTurnInjected(
            contact={"contact_id": 2},
            content="I just gave the prerecorded intro.",
            source="test",
        )
        guidance_cb(json.loads(injected.to_json()))

        agent_texts = [
            item.text_content or ""
            for item in session.current_agent._chat_ctx.items
            if getattr(item, "type", None) == "message"
        ]
        assert any("I just gave the prerecorded intro." in txt for txt in agent_texts)
        mirror_texts = [
            item.text_content or ""
            for item in session._chat_ctx.items
            if getattr(item, "type", None) == "message"
        ]
        assert any("I just gave the prerecorded intro." in txt for txt in mirror_texts)
        assert len(session.say_calls) == 0

    async def test_buffered_notification_is_applied_before_opening_greeting(
        self,
        monkeypatch,
    ):
        """Notifications that arrive before session_ready should land in greeting context."""

        from livekit.agents import llm

        if not hasattr(llm, "Tool"):
            llm.Tool = object  # type: ignore[attr-defined]
        from unity.conversation_manager.medium_scripts import call as call_script

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
        buffered_text = (
            "Background context: scheduled Morning briefing is due now "
            "(task_id=101, scheduled_for=2026-04-10T09:00:00+00:00)."
        )

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
                if channel == "app:call:notification":
                    handler({"payload": {"message": buffered_text}})

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

            def set_call_received(self):
                self.call_received = True

            def set_credit_gate_state_provider(self, provider):
                self.credit_gate_state_provider = provider

        async def _noop_async(*args, **kwargs):
            return None

        async def _noop_end_call():
            return None

        fake_broker = _FakeEventBroker()
        fake_session_details = SimpleNamespace(
            populate_from_env=lambda: None,
            voice=SimpleNamespace(provider="cartesia", id=""),
            assistant=SimpleNamespace(
                about="Assistant bio",
                name="Ava",
                first_name="Assistant",
                surname="Example",
                agent_id=None,
                user_desktop_for=lambda user_id: None,
            ),
            user=SimpleNamespace(id="default"),
            is_coordinator=False,
            org_id=None,
            voice_call=SimpleNamespace(
                outbound=False,
                channel="unify_meet",
                contact_json=json.dumps(contact),
                boss_json=json.dumps(boss),
            ),
            unify_key="",
        )

        monkeypatch.setattr(call_script, "SESSION_DETAILS", fake_session_details)
        monkeypatch.setattr(call_script, "event_broker", fake_broker)
        monkeypatch.setattr(call_script, "Assistant", _FakeAssistant)
        monkeypatch.setattr(call_script, "AgentSession", _FakeSession)
        monkeypatch.setattr(call_script, "delete_livekit_room", _noop_async)
        monkeypatch.setattr(call_script, "publish_call_started", _noop_async)
        monkeypatch.setattr(call_script, "publish_call_ended", _noop_async)
        monkeypatch.setattr(call_script, "start_event_broker_receive", _noop_async)
        monkeypatch.setattr(
            call_script,
            "setup_participant_disconnect_handler",
            _noop_async,
        )
        monkeypatch.setattr(call_script, "create_end_call", _noop_end_call)
        monkeypatch.setattr(call_script, "dispatch_livekit_agent", _noop_async)
        monkeypatch.setattr(call_script, "RoomInputOptions", lambda **kwargs: object())
        monkeypatch.setattr(call_script, "EnglishModel", lambda: object())
        monkeypatch.setattr(call_script.cartesia, "TTS", lambda **kwargs: object())
        monkeypatch.setattr(call_script.elevenlabs, "TTS", lambda **kwargs: object())
        if hasattr(call_script, "noise_cancellation"):
            monkeypatch.setattr(
                call_script.noise_cancellation,
                "BVC",
                lambda: object(),
            )

        monkeypatch.setattr(call_script, "STT", object())
        monkeypatch.setattr(call_script, "VAD", object())

        captured = {}

        import unity.common.llm_client as _llm_mod

        class _FakeGreetingClient:
            async def generate(self, **kwargs):
                captured["messages"] = kwargs["messages"]
                return "Hello!"

        monkeypatch.setattr(
            _llm_mod,
            "new_llm_client",
            lambda *a, **kw: _FakeGreetingClient(),
        )

        await call_script.entrypoint(_FakeJobContext())

        serialized_messages = json.dumps(captured["messages"], default=str)
        assert buffered_text in serialized_messages

    async def test_outbound_message_to_caller_triggers_fast_brain_turn(
        self,
        monkeypatch,
    ):
        """When the slow brain sends a text message to the person on the
        call, the fast brain must receive a turn so it can verbally
        acknowledge the sent message (e.g. "I just sent you a message
        with the full breakdown, by the way").

        Without this, the caller gets a silent text notification during a
        live voice conversation with no verbal acknowledgement — like a
        colleague on a Teams call quietly replying via chat instead of
        speaking.
        """
        from livekit.agents import llm
        from unity.conversation_manager.medium_scripts import call as call_script

        contact = {
            "contact_id": 1,
            "first_name": "Alex",
            "surname": "Demo",
            "phone_number": "+15550100001",
            "email_address": "alex@example.com",
        }
        boss = contact

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

            def set_call_received(self):
                self.call_received = True

            def set_credit_gate_state_provider(self, provider):
                self.credit_gate_state_provider = provider

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
                channel="unify_meet",
                contact_json=json.dumps(contact),
                boss_json=json.dumps(boss),
            ),
            assistant=SimpleNamespace(
                about="Assistant bio",
                name="David",
                user_desktop_for=lambda user_id: None,
            ),
            user=SimpleNamespace(id="default"),
            is_coordinator=False,
            org_id=None,
            unify_key="",
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

        import unity.common.llm_client as _llm_mod

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
        baseline_replies = session.generate_reply_calls

        from unity.conversation_manager.events import UnifyMessageSent

        comms_cb = fake_broker.callbacks["app:comms:*"]
        event = UnifyMessageSent(
            contact={"contact_id": 1, "first_name": "Alex", "surname": "Demo"},
            content="Here's the full breakdown of your OneDrive contents.",
        )
        comms_cb({"event": event.to_json()})

        ctx_texts = [
            item.text_content or ""
            for item in session._chat_ctx.items
            if getattr(item, "type", None) == "message"
        ]
        has_outbound_ref = any("OneDrive" in txt for txt in ctx_texts)
        assert has_outbound_ref, (
            "When the slow brain sends a Unify message to the person on "
            "the call, the fast brain's chat context must include a "
            "reference to it so the fast brain can acknowledge it "
            "verbally (e.g. 'I just sent you a message with the details').\n"
            f"Chat context messages: {ctx_texts}"
        )

        # generate_reply is scheduled via call_later with a coalesce delay;
        # yield to the event loop so the timer fires.
        import asyncio

        await asyncio.sleep(0.1)

        got_reply_turn = session.generate_reply_calls > baseline_replies
        assert got_reply_turn, (
            "The fast brain must get an LLM turn after an outbound "
            "message is sent to the caller, so it can verbally "
            "acknowledge the sent message."
        )

    async def test_should_speak_guidance_voiced_via_gate_not_preinjected(
        self,
        monkeypatch,
    ):
        """should_speak=True guidance is not pre-injected as a [notification].

        The dedup gate voices it once via session.say(); the spoken text then
        lands in the fast brain's context as an assistant turn, so the fast brain
        sees it as already-said rather than as a notification to re-announce.
        """
        from livekit.agents import llm
        from unity.conversation_manager.medium_scripts import call as call_script

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

            def set_call_received(self):
                self.call_received = True

            def set_credit_gate_state_provider(self, provider):
                self.credit_gate_state_provider = provider

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
                channel="unify_meet",
                contact_json=json.dumps(contact),
                boss_json=json.dumps(boss),
            ),
            assistant=SimpleNamespace(
                about="Assistant bio",
                name="Ava",
                user_desktop_for=lambda user_id: None,
            ),
            user=SimpleNamespace(id="default"),
            is_coordinator=False,
            org_id=None,
            unify_key="",
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

        import unity.common.llm_client as _llm_mod

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
        # Mirror LiveKit: say persists the spoken text as an assistant turn.
        session.say = lambda text, **kw: _fake_say(session, text, **kw)
        assistant = session.current_agent

        def _system_texts():
            return [
                item.text_content or ""
                for item in session._chat_ctx.items
                if getattr(item, "role", None) == "system"
            ]

        def _assistant_texts():
            return [
                item.text_content or ""
                for item in session._chat_ctx.items
                if getattr(item, "role", None) == "assistant"
            ]

        # User is speaking — guidance will be queued but not spoken yet
        state_cb = session._events["user_state_changed"]
        state_cb(SimpleNamespace(new_state="speaking"))

        # Send should_speak=True guidance while user is speaking
        guidance_cb = fake_broker.callbacks["app:call:notification"]
        guidance_cb(
            {
                "payload": {
                    "message": "There's no contact named Bob in your list.",
                    "should_speak": True,
                },
            },
        )

        # should_speak=True guidance is NOT pre-injected as a [notification]: the
        # gate voices it once, so the fast brain never sees a to-announce copy.
        assert not any(
            "no contact named Bob" in txt for txt in _system_texts()
        ), "should_speak=True guidance must not be pre-injected as a [notification]."

        # Speech should NOT have fired yet (user is speaking)
        assert (
            len(session.say_calls) == 0
        ), "Queued speech must not fire while user is speaking."

        # Disable dedup so _dedup_and_speak goes straight to _speak_now.
        from unity.settings import SETTINGS

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = False
        try:
            # User stops, agent settles → maybe_speak_queued fires
            state_cb(SimpleNamespace(new_state="listening"))
            session.agent_state = "listening"
            agent_state_cb = session._events["agent_state_changed"]
            agent_state_cb(SimpleNamespace(new_state="listening"))

            # _dedup_and_speak is async; yield to let the task complete.
            await asyncio.sleep(0.05)

            # The guidance is voiced once via the gate ...
            assert (
                len(session.say_calls) == 1
            ), "should_speak=True guidance must be voiced via session.say()."
            # ... and lands in context as an assistant turn, not a [notification].
            assert any(
                "no contact named Bob" in txt for txt in _assistant_texts()
            ), "Spoken guidance should land in chat_ctx as an assistant turn."
            assert not any(
                "no contact named Bob" in txt for txt in _system_texts()
            ), "Spoken guidance must not also appear as a [notification]."
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup

    async def test_unify_llm_preserves_base_system_prompt_with_notification(
        self,
        monkeypatch,
    ):
        """System instructions and notifications should both survive conversion."""
        from livekit.agents import llm
        from unity.conversation_manager import livekit_unify_adapter as adapter_module

        captured = {}

        class _DummyClient:
            def set_stream(self, enabled):
                captured["set_stream"] = enabled

            async def generate(self, **kwargs):
                captured["generate_kwargs"] = kwargs

                async def _empty_stream():
                    if False:
                        yield ""

                return _empty_stream()

        def _fake_new_llm_client(model, **kwargs):
            captured["client_model"] = model
            captured["client_kwargs"] = kwargs
            return _DummyClient()

        monkeypatch.setattr(adapter_module, "new_llm_client", _fake_new_llm_client)

        chat_ctx = llm.ChatContext()
        chat_ctx.add_message(role="system", content="BASE_PROMPT")
        chat_ctx.add_message(
            role="user",
            content="Do I have a contact named Bob?",
        )
        chat_ctx.add_message(
            role="system",
            content="[notification] No, there is no contact named Bob.",
        )
        chat_ctx.add_message(role="assistant", content="Let me check on that.")

        from unity.settings import SETTINGS

        stream = adapter_module.UnifyLLM(
            model=SETTINGS.conversation.FAST_BRAIN_MODEL,
        ).chat(
            chat_ctx=chat_ctx,
        )
        await stream._run()

        messages = captured["generate_kwargs"]["messages"]
        system_texts = [m["content"] for m in messages if m["role"] == "system"]
        all_system_text = "\n".join(system_texts)
        assert "BASE_PROMPT" in all_system_text
        assert "[notification] No, there is no contact named Bob." in all_system_text

    async def test_tts_guidance_received_while_user_speaking_is_replied_after_speech_ends(
        self,
        monkeypatch,
    ):
        """Guidance arriving mid-speech should be surfaced once speech ends."""
        from livekit.agents import llm
        from unity.conversation_manager.medium_scripts import call as call_script

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

            def set_call_received(self):
                self.call_received = True

            def set_credit_gate_state_provider(self, provider):
                self.credit_gate_state_provider = provider

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
                channel="unify_meet",
                contact_json=json.dumps(contact),
                boss_json=json.dumps(boss),
            ),
            assistant=SimpleNamespace(
                about="Assistant bio",
                name="Ava",
                user_desktop_for=lambda user_id: None,
            ),
            user=SimpleNamespace(id="default"),
            is_coordinator=False,
            org_id=None,
            unify_key="",
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

        import unity.common.llm_client as _llm_mod

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
        guidance_cb = fake_broker.callbacks["app:call:notification"]
        agent_state_cb = session._events["agent_state_changed"]

        # User is speaking — guidance with should_speak=True arrives and is queued
        state_cb = session._events["user_state_changed"]
        state_cb(SimpleNamespace(new_state="speaking"))
        guidance_cb(
            {
                "payload": {
                    "message": "No, there's no contact named Bob.",
                    "should_speak": True,
                },
            },
        )
        assert (
            len(session.say_calls) == 0
        ), "Queued speech must not fire while user is speaking."

        # User stops speaking — say() must NOT fire from user_state_changed alone
        state_cb(SimpleNamespace(new_state="listening"))
        assert (
            len(session.say_calls) == 0
        ), "Queued speech must not fire from user_state_changed (race condition)."

        # Disable dedup so _dedup_and_speak goes straight to _speak_now.
        from unity.settings import SETTINGS

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = False
        try:
            # Agent settles to listening — say() fires now
            agent_state_cb(SimpleNamespace(new_state="listening"))

            # _dedup_and_speak is async; yield to let the task complete.
            await asyncio.sleep(0)

            assert len(session.say_calls) == 1, (
                "Guidance that arrives while the user is speaking should be surfaced "
                "via session.say() after the agent settles to listening."
            )
            assert session.say_calls[0] == "No, there's no contact named Bob."
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup

    async def test_queued_speech_waits_for_agent_thinking_and_speaking_cycle(
        self,
        monkeypatch,
    ):
        """Guidance arriving while the agent is thinking/speaking should wait
        for the full cycle to complete before session.say() fires."""
        from livekit.agents import llm
        from unity.conversation_manager.medium_scripts import call as call_script

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

            def set_call_received(self):
                self.call_received = True

            def set_credit_gate_state_provider(self, provider):
                self.credit_gate_state_provider = provider

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
                channel="unify_meet",
                contact_json=json.dumps(contact),
                boss_json=json.dumps(boss),
            ),
            assistant=SimpleNamespace(
                about="Assistant bio",
                name="Ava",
                user_desktop_for=lambda user_id: None,
            ),
            user=SimpleNamespace(id="default"),
            is_coordinator=False,
            org_id=None,
            unify_key="",
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

        import unity.common.llm_client as _llm_mod

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
        guidance_cb = fake_broker.callbacks["app:call:notification"]
        agent_state_cb = session._events["agent_state_changed"]

        # Simulate agent in "thinking" state (processing a user turn)
        session.agent_state = "thinking"

        # Guidance arrives while agent is thinking
        guidance_cb(
            {
                "payload": {
                    "message": "It's at 3pm.",
                    "should_speak": True,
                },
            },
        )
        assert (
            len(session.say_calls) == 0
        ), "Queued speech must not fire while agent is thinking."

        # Agent starts speaking (its reply to the user)
        agent_state_cb(SimpleNamespace(new_state="speaking"))
        session.agent_state = "speaking"
        assert (
            len(session.say_calls) == 0
        ), "Queued speech must not fire while agent is speaking."

        # Disable dedup so _dedup_and_speak goes straight to _speak_now.
        from unity.settings import SETTINGS

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = False
        try:
            # Agent finishes speaking → transitions to listening
            session.agent_state = "listening"
            agent_state_cb(SimpleNamespace(new_state="listening"))

            # _dedup_and_speak is async; yield to let the task complete.
            await asyncio.sleep(0)

            assert (
                len(session.say_calls) == 1
            ), "Queued speech should fire after agent returns to listening."
            assert session.say_calls[0] == "It's at 3pm."
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup


def _stream_dedup_client(header, *, captured=None, on_call=None):
    """Fake unillm client mirroring the streaming speech-dedup gate surface.

    ``header`` is either the full streamed verdict string (e.g. ``"SPEAK | ok"``
    or ``"REWRITE\nbody"``) or a callable taking the messages list and returning
    such a string.
    """

    class _Client:
        def set_stream(self, _value: bool) -> None:
            pass

        async def generate(self, *, messages=None, **_kw):
            if captured is not None:
                captured["messages"] = messages
            if on_call is not None:
                on_call()
            text = header(messages) if callable(header) else header

            async def _gen():
                yield text

            return _gen()

    return _Client()


async def _drain_say(value):
    """Resolve a session.say argument (string or token stream) to a string."""
    if isinstance(value, str):
        return value
    parts = []
    async for chunk in value:
        parts.append(chunk)
    return "".join(parts)


class _FakeSpeechHandle:
    """Minimal stand-in for a LiveKit ``SpeechHandle``.

    Mirrors the surface ``_speak_now`` relies on: ``interrupted``, an awaitable
    ``wait_for_playout``, and ``chat_items`` (the assistant turn LiveKit persists
    for the actually-spoken text).
    """

    def __init__(self, chat_items=None, interrupted: bool = False):
        self.chat_items = chat_items or []
        self.interrupted = interrupted

    async def wait_for_playout(self):
        return None


def _fake_say(self, text, **kwargs):
    """``session.say`` replacement that mirrors LiveKit's chat-ctx behaviour.

    On ``add_to_chat_ctx`` it appends an ``assistant`` turn with the spoken text
    (the truncated prefix when ``self.say_interrupted`` is set) and returns a
    ``_FakeSpeechHandle`` carrying that item, so ``_speak_now``'s post-playout
    remainder logic can run. Streaming (async-iterator) text is recorded but not
    consumed, matching how the gate's rewrite tests drain it themselves.
    """
    from livekit.agents import llm  # local import; mirrors test module usage

    self.say_calls.append(text)
    interrupted = getattr(self, "say_interrupted", False)
    chat_items = []
    if kwargs.get("add_to_chat_ctx", True) and isinstance(text, str):
        spoken_prefix = getattr(self, "say_spoken_prefix", None)
        content = spoken_prefix if (interrupted and spoken_prefix is not None) else text
        msg = self._chat_ctx.add_message(role="assistant", content=content)
        chat_items = [msg]
    return _FakeSpeechHandle(chat_items=chat_items, interrupted=interrupted)


def test_append_inflight_speech_context():
    """The helper appends one self-describing system note per non-empty text and
    is a no-op for empty input."""
    from livekit.agents import llm
    from unity.conversation_manager.medium_scripts.call import Assistant

    ctx = llm.ChatContext()
    Assistant._append_inflight_speech_context(ctx, ["I'm checking it now."])
    system_texts = [
        item.text_content or ""
        for item in ctx.items
        if getattr(item, "role", None) == "system"
    ]
    assert any("I'm checking it now." in t for t in system_texts), system_texts

    before = len(ctx.items)
    Assistant._append_inflight_speech_context(ctx, [])
    Assistant._append_inflight_speech_context(ctx, ["", "   "])
    assert len(ctx.items) == before, "Empty/blank texts must not add messages."


@pytest.mark.asyncio
async def test_publish_guidance_stamps_decided_after_ts():
    """_publish_slow_brain_fast_brain_guidance serializes decided_after_ts (ISO),
    and an absent marker becomes an empty string."""
    from datetime import datetime, timezone
    from unity.conversation_manager.conversation_manager import ConversationManager

    captured: list[tuple[str, str]] = []

    async def _publish(channel, message):
        captured.append((channel, message))
        return 1

    fake_self = SimpleNamespace(
        get_active_contact=lambda: {"contact_id": 1},
        event_broker=SimpleNamespace(publish=_publish),
        _session_logger=SimpleNamespace(info=lambda *a, **k: None),
    )

    dt = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    await ConversationManager._publish_slow_brain_fast_brain_guidance(
        fake_self,
        message="The email is on its way.",
        should_speak=True,
        decided_after_ts=dt,
    )
    data = json.loads(captured[0][1])
    payload = data.get("payload", data)
    assert payload["decided_after_ts"] == dt.isoformat()

    captured.clear()
    await ConversationManager._publish_slow_brain_fast_brain_guidance(
        fake_self,
        message="hi",
        should_speak=True,
        decided_after_ts=None,
    )
    data = json.loads(captured[0][1])
    payload = data.get("payload", data)
    assert payload["decided_after_ts"] == ""


@pytest.mark.asyncio
async def test_fastbrain_notification_records_only_silent_guidance():
    """should_speak guidance is no longer pre-written to contact_index (it is
    recorded once via the actually-spoken Outbound utterance); silent
    (should_speak=False) guidance is still recorded exactly once."""
    from unittest.mock import AsyncMock, MagicMock
    from unity.conversation_manager.cm_types import Mode
    from unity.conversation_manager.domains.event_handlers import EventHandler
    from unity.conversation_manager.events import FastBrainNotification

    handler = EventHandler._registry[FastBrainNotification]
    contact = {"contact_id": 1, "first_name": "Dan", "surname": "Lenton"}

    def _make_cm():
        return SimpleNamespace(
            contact_index=SimpleNamespace(
                get_contact=lambda contact_id=None: None,
                push_message=MagicMock(return_value=1),
            ),
            call_manager=SimpleNamespace(
                has_active_google_meet=False,
                has_active_teams_meet=False,
                _call_channel="phone_call",
            ),
            mode=Mode.CALL,
            schedule_proactive_speech=AsyncMock(),
        )

    # should_speak=True -> no contact_index pre-write; still re-arms proactive.
    cm = _make_cm()
    await handler(
        FastBrainNotification(
            contact=contact,
            message="Just sent the clue to your email.",
            should_speak=True,
            source="slow_brain",
        ),
        cm,
    )
    cm.contact_index.push_message.assert_not_called()
    cm.schedule_proactive_speech.assert_awaited()

    # should_speak=False -> recorded exactly once as guidance.
    cm = _make_cm()
    await handler(
        FastBrainNotification(
            contact=contact,
            message="Email received from Alice.",
            should_speak=False,
            source="slow_brain",
        ),
        cm,
    )
    assert cm.contact_index.push_message.call_count == 1
    _, kwargs = cm.contact_index.push_message.call_args
    assert kwargs.get("role") == "guidance"
    assert kwargs.get("message_content") == "Email received from Alice."


@pytest.mark.asyncio
class TestFastBrainSpeechDedup:
    """Tests for the fast brain speech deduplication gate.

    Dedup runs inside _dedup_and_speak (called from maybe_speak_queued) and
    checks whether queued slow brain speech overlaps with recent assistant
    utterances in the fast brain's chat context.
    """

    @pytest_asyncio.fixture
    async def fast_brain_env(self, monkeypatch):
        """Bootstrap a fake fast brain environment and return useful handles.

        Yields a dict with keys: session, assistant, guidance_cb,
        agent_state_cb, say_calls.
        """
        from livekit.agents import llm
        from unity.conversation_manager.medium_scripts import call as call_script

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

            def set_call_received(self):
                self.call_received = True

            def set_credit_gate_state_provider(self, provider):
                self.credit_gate_state_provider = provider

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
                channel="unify_meet",
                contact_json=json.dumps(contact),
                boss_json=json.dumps(boss),
            ),
            assistant=SimpleNamespace(
                about="Assistant bio",
                name="Ava",
                user_desktop_for=lambda user_id: None,
            ),
            user=SimpleNamespace(id="default"),
            is_coordinator=False,
            org_id=None,
            unify_key="",
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

        import unity.common.llm_client as _llm_mod

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

        yield {
            "session": session,
            "assistant": session.current_agent,
            "guidance_cb": fake_broker.callbacks["app:call:notification"],
            "agent_state_cb": session._events["agent_state_changed"],
            "monkeypatch": monkeypatch,
        }

    def _send_speak_guidance(
        self,
        env,
        message,
        *,
        spoken_message: str = "",
        decided_after_ts: str = "",
    ):
        """Queue a should_speak=True notification."""
        payload = {
            "message": message,
            "should_speak": True,
            "source": "slow_brain",
        }
        if spoken_message:
            payload["spoken_message"] = spoken_message
        if decided_after_ts:
            payload["decided_after_ts"] = decided_after_ts
        env["guidance_cb"]({"payload": payload})

    @staticmethod
    def _record_voice_activity(env):
        """Bump the fast brain's last-voice-activity clock via a silent notification."""
        env["guidance_cb"](
            {
                "payload": {
                    "message": "Background state update.",
                    "should_speak": False,
                    "source": "slow_brain",
                },
            },
        )

    async def _settle_and_drain(self, env):
        """Transition agent to listening and let async dedup task complete."""
        env["session"].agent_state = "listening"
        env["agent_state_cb"](SimpleNamespace(new_state="listening"))
        # Give the async _dedup_and_speak task time to fully complete.
        # AsyncMock + ensure_future may need multiple event loop ticks.
        await asyncio.sleep(0.05)

    @staticmethod
    def _has_notification(ctx, needle):
        for item in ctx.items:
            raw = getattr(item, "content", None)
            if raw is None:
                continue
            text = (
                raw
                if isinstance(raw, str)
                else " ".join(c for c in raw if isinstance(c, str))
            )
            if "[notification]" in text and needle in text:
                return True
        return False

    async def test_dedup_suppresses_redundant_speech(self, fast_brain_env):
        """When the assistant has already said the same info, the queued guidance
        is suppressed: nothing is spoken and (since should_speak guidance is no
        longer pre-injected) no ``[notification]`` is left behind."""
        env = fast_brain_env
        from unity.settings import SETTINGS
        import unity.conversation_manager.domains.speech_dedup as _dedup_mod

        env["assistant"]._chat_ctx.add_message(
            role="assistant",
            content="Found three Italian restaurants near you.",
        )

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = True
        env["monkeypatch"].setattr(
            _dedup_mod,
            "new_llm_client",
            lambda *a, **kw: _stream_dedup_client(
                "SUPPRESS | fast brain already said it",
            ),
        )
        try:
            env["session"].agent_state = "thinking"
            self._send_speak_guidance(
                env,
                "Italian restaurant search results.",
                spoken_message="I found three Italian restaurants nearby.",
            )
            await self._settle_and_drain(env)
            assert (
                len(env["session"].say_calls) == 0
            ), "Redundant speech should be suppressed by dedup."
            assert not self._has_notification(
                env["session"]._chat_ctx,
                "Italian restaurant",
            ), "Suppressed should_speak guidance must not be pre-injected."
            assert not self._has_notification(
                env["assistant"]._chat_ctx,
                "Italian restaurant",
            )
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup

    async def test_dedup_allows_novel_speech(self, fast_brain_env):
        """When the proposed speech is genuinely new, it should be spoken."""
        env = fast_brain_env
        from unity.settings import SETTINGS
        import unity.conversation_manager.domains.speech_dedup as _dedup_mod

        env["assistant"]._chat_ctx.add_message(
            role="assistant",
            content="I've started the email check.",
        )

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = True
        env["monkeypatch"].setattr(
            _dedup_mod,
            "new_llm_client",
            lambda *a, **kw: _stream_dedup_client("SPEAK | different topic"),
        )
        try:
            env["session"].agent_state = "thinking"
            self._send_speak_guidance(
                env,
                "It's 22 degrees and sunny in London.",
            )
            await self._settle_and_drain(env)
            assert len(env["session"].say_calls) == 1, "Novel speech should be spoken."
            assert env["session"].say_calls[0] == "It's 22 degrees and sunny in London."
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup

    async def test_should_speak_guidance_not_preinjected(self, fast_brain_env):
        """A fully-spoken should_speak guidance is not pre-injected as a
        [notification]; it lands in context only as its spoken assistant turn."""
        env = fast_brain_env
        from unity.settings import SETTINGS
        import unity.conversation_manager.domains.speech_dedup as _dedup_mod

        session = env["session"]
        session.say = lambda text, **kw: _fake_say(session, text, **kw)
        env["assistant"]._chat_ctx.add_message(
            role="assistant",
            content="I've started the email check.",
        )

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = True
        env["monkeypatch"].setattr(
            _dedup_mod,
            "new_llm_client",
            lambda *a, **kw: _stream_dedup_client("SPEAK | novel"),
        )
        try:
            session.agent_state = "thinking"
            self._send_speak_guidance(env, "Your 3pm meeting moved to 4pm.")
            await self._settle_and_drain(env)
            assert session.say_calls == ["Your 3pm meeting moved to 4pm."]
            # No [notification] pre-injection; only the spoken assistant turn.
            assert not self._has_notification(session._chat_ctx, "3pm meeting")
            assistant_texts = [
                item.text_content or ""
                for item in session._chat_ctx.items
                if getattr(item, "role", None) == "assistant"
            ]
            assert any(
                "3pm meeting moved to 4pm" in t for t in assistant_texts
            ), "Spoken guidance should land in context as an assistant turn."
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup

    async def test_interrupted_guidance_injects_unheard_remainder(self, fast_brain_env):
        """When a spoken guidance is interrupted, the unheard remainder is
        re-injected as an [unheard] system note so it is not lost."""
        env = fast_brain_env
        from unity.settings import SETTINGS
        import unity.conversation_manager.domains.speech_dedup as _dedup_mod

        session = env["session"]
        session.say = lambda text, **kw: _fake_say(session, text, **kw)
        session.say_interrupted = True
        session.say_spoken_prefix = "Check your inbox at dan@unify.ai"
        env["assistant"]._chat_ctx.add_message(
            role="assistant",
            content="Working on it.",
        )

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = True
        env["monkeypatch"].setattr(
            _dedup_mod,
            "new_llm_client",
            lambda *a, **kw: _stream_dedup_client("SPEAK | novel"),
        )
        try:
            session.agent_state = "thinking"
            self._send_speak_guidance(
                env,
                "Check your inbox at dan@unify.ai and reply with your guess.",
            )
            await self._settle_and_drain(env)
            unheard = [
                item.text_content or ""
                for item in env["assistant"]._chat_ctx.items
                if getattr(item, "role", None) == "system"
                and "[unheard]" in (item.text_content or "")
            ]
            assert unheard, "Interrupted guidance should inject an [unheard] note."
            assert "reply with your guess" in unheard[-1]
            assert (
                "Check your inbox" not in unheard[-1]
            ), "The already-heard prefix should not be re-injected."
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup

    async def test_full_guidance_injects_no_remainder(self, fast_brain_env):
        """An uninterrupted guidance leaves no [unheard] note behind."""
        env = fast_brain_env
        from unity.settings import SETTINGS
        import unity.conversation_manager.domains.speech_dedup as _dedup_mod

        session = env["session"]
        session.say = lambda text, **kw: _fake_say(session, text, **kw)
        session.say_interrupted = False
        env["assistant"]._chat_ctx.add_message(
            role="assistant",
            content="Working on it.",
        )

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = True
        env["monkeypatch"].setattr(
            _dedup_mod,
            "new_llm_client",
            lambda *a, **kw: _stream_dedup_client("SPEAK | novel"),
        )
        try:
            session.agent_state = "thinking"
            self._send_speak_guidance(env, "Your meeting moved to 4pm.")
            await self._settle_and_drain(env)
            for ctx in (env["assistant"]._chat_ctx, session._chat_ctx):
                assert not any(
                    "[unheard]" in (item.text_content or "") for item in ctx.items
                ), "Uninterrupted guidance must not inject an [unheard] note."
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup

    async def test_should_speak_false_still_injected(self, fast_brain_env):
        """should_speak=False awareness notifications keep their [notification]
        injection and are not spoken."""
        env = fast_brain_env

        env["guidance_cb"](
            {
                "payload": {
                    "message": "Email received from Alice.",
                    "should_speak": False,
                    "source": "slow_brain",
                },
            },
        )
        await asyncio.sleep(0.05)

        assert env["session"].say_calls == [], "Awareness notes are not spoken."
        assert self._has_notification(
            env["session"]._chat_ctx,
            "Email received from Alice",
        ), "should_speak=False guidance must still inject a [notification]."
        assert self._has_notification(
            env["assistant"]._chat_ctx,
            "Email received from Alice",
        )

    async def test_inflight_speech_provider_lifecycle(self, fast_brain_env):
        """The in-flight provider returns a spoken line only while it is in flight
        (in _say_meta_queue) and stops once it commits - guaranteeing it is never
        both injected and committed, so no duplication is possible."""
        env = fast_brain_env
        from unity.settings import SETTINGS

        assistant = env["assistant"]
        assert assistant._inflight_speech_provider is not None, "provider wired"

        text = "Checking your calendar now."
        orig = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = False
        try:
            env["session"].agent_state = "thinking"
            self._send_speak_guidance(env, text)
            await self._settle_and_drain(env)
            # Spoken via _speak_now; the fake say does not commit, so it stays
            # in flight and the provider surfaces it.
            assert text in assistant._inflight_speech_provider()

            # Simulate playout completion -> conversation_item_added commits the
            # turn and pops _say_meta_queue; the provider stops returning it.
            env["session"]._events["conversation_item_added"](
                SimpleNamespace(
                    item=SimpleNamespace(role="assistant", text_content=text),
                ),
            )
            await asyncio.sleep(0)
            assert text not in assistant._inflight_speech_provider()
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig

    async def test_dedup_skipped_when_no_activity_since_decision(self, fast_brain_env):
        """When nothing has been spoken/notified since the slow brain decided,
        the gate is provably redundant and is skipped (no LLM call)."""
        env = fast_brain_env
        from unity.settings import SETTINGS
        from datetime import datetime, timezone, timedelta
        import unity.conversation_manager.domains.speech_dedup as _dedup_mod

        env["assistant"]._chat_ctx.add_message(
            role="assistant",
            content="Found three Italian restaurants near you.",
        )
        # Record some voice activity, then mark the slow brain's decision as
        # happening AFTER it (so nothing has changed since the decision).
        self._record_voice_activity(env)
        await asyncio.sleep(0)
        decided_after = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

        called = {"n": 0}

        def _tracking_client(*a, **kw):
            called["n"] += 1
            return _stream_dedup_client("SUPPRESS | should not run")

        orig = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = True
        env["monkeypatch"].setattr(_dedup_mod, "new_llm_client", _tracking_client)
        try:
            env["session"].agent_state = "thinking"
            self._send_speak_guidance(
                env,
                "Your meeting moved to 4pm.",
                decided_after_ts=decided_after,
            )
            await self._settle_and_drain(env)
            assert called["n"] == 0, "Gate must be skipped when nothing changed."
            assert env["session"].say_calls == ["Your meeting moved to 4pm."]
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig

    async def test_dedup_runs_when_activity_since_decision(self, fast_brain_env):
        """When voice activity occurred after the slow brain's decision, the gate
        runs (a race is possible)."""
        env = fast_brain_env
        from unity.settings import SETTINGS
        from datetime import datetime, timezone, timedelta
        import unity.conversation_manager.domains.speech_dedup as _dedup_mod

        env["assistant"]._chat_ctx.add_message(
            role="assistant",
            content="Found three Italian restaurants near you.",
        )
        # Decision happened in the past; activity then occurred after it.
        decided_after = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        self._record_voice_activity(env)
        await asyncio.sleep(0)

        called = {"n": 0}

        def _tracking_client(*a, **kw):
            called["n"] += 1
            return _stream_dedup_client("SUPPRESS | redundant")

        orig = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = True
        env["monkeypatch"].setattr(_dedup_mod, "new_llm_client", _tracking_client)
        try:
            env["session"].agent_state = "thinking"
            self._send_speak_guidance(
                env,
                "Found three Italian restaurants nearby.",
                decided_after_ts=decided_after,
            )
            await self._settle_and_drain(env)
            assert called["n"] == 1, "Gate must run when activity occurred since."
            assert env["session"].say_calls == [], "Gate suppressed the speech."
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig

    async def test_dedup_runs_when_no_marker(self, fast_brain_env):
        """Without a decided_after_ts marker, the gate runs (backward compat)."""
        env = fast_brain_env
        from unity.settings import SETTINGS
        import unity.conversation_manager.domains.speech_dedup as _dedup_mod

        env["assistant"]._chat_ctx.add_message(
            role="assistant",
            content="Found three Italian restaurants near you.",
        )
        called = {"n": 0}

        def _tracking_client(*a, **kw):
            called["n"] += 1
            return _stream_dedup_client("SUPPRESS | redundant")

        orig = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = True
        env["monkeypatch"].setattr(_dedup_mod, "new_llm_client", _tracking_client)
        try:
            env["session"].agent_state = "thinking"
            self._send_speak_guidance(env, "Found three Italian restaurants nearby.")
            await self._settle_and_drain(env)
            assert called["n"] == 1, "Gate must run when no marker is present."
            assert env["session"].say_calls == []
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig

    async def test_dedup_rewrites_partial_overlap(self, fast_brain_env):
        """When the gate returns REWRITE, the trimmed body is streamed into
        session.say and only the rewritten text reaches TTS."""
        env = fast_brain_env
        from unity.settings import SETTINGS
        import unity.conversation_manager.domains.speech_dedup as _dedup_mod

        env["assistant"]._chat_ctx.add_message(
            role="assistant",
            content="Yes - got it. The Matrix is the right reply.",
        )

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = True
        env["monkeypatch"].setattr(
            _dedup_mod,
            "new_llm_client",
            lambda *a, **kw: _stream_dedup_client(
                "REWRITE\nConfirmed back on WhatsApp too, so that round's done.",
            ),
        )
        try:
            env["session"].agent_state = "thinking"
            self._send_speak_guidance(
                env,
                "The Matrix - that's correct. I've confirmed back on WhatsApp "
                "too, so that round is done.",
            )
            await self._settle_and_drain(env)
            assert len(env["session"].say_calls) == 1, "Rewrite should be spoken."
            spoken = await _drain_say(env["session"].say_calls[0])
            assert spoken == (
                "Confirmed back on WhatsApp too, so that round's done."
            ), f"Only the rewritten body should reach TTS. Got: {spoken!r}"
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup

    async def test_dedup_empty_rewrite_degrades_to_suppress(self, fast_brain_env):
        """A REWRITE verdict with an empty body suppresses (nothing reaches TTS)."""
        env = fast_brain_env
        from unity.settings import SETTINGS
        import unity.conversation_manager.domains.speech_dedup as _dedup_mod

        env["assistant"]._chat_ctx.add_message(
            role="assistant",
            content="Found three Italian restaurants near you.",
        )

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = True
        env["monkeypatch"].setattr(
            _dedup_mod,
            "new_llm_client",
            lambda *a, **kw: _stream_dedup_client("REWRITE\n"),
        )
        try:
            env["session"].agent_state = "thinking"
            self._send_speak_guidance(
                env,
                "I found three Italian restaurants nearby.",
            )
            await self._settle_and_drain(env)
            assert (
                len(env["session"].say_calls) == 0
            ), "Empty rewrite should degrade to suppression."
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup

    async def test_self_notification_not_treated_as_spoken(self, fast_brain_env):
        """Regression (email "The Matrix" ack): should_speak guidance must not be
        seen by the gate as its own already-spoken notification.

        Now that should_speak guidance is no longer pre-injected as a
        ``[notification]`` (the gate voices it once), the proposed text never
        leaks into the notifications section, so the gate cannot falsely suppress
        it. The fake client suppresses iff the ack appears as a notification
        bullet; since it never does, the ack is spoken.
        """
        env = fast_brain_env
        from unity.settings import SETTINGS
        import unity.conversation_manager.domains.speech_dedup as _dedup_mod

        ack = "Correct - The Matrix. Email channel works."

        # A prior, unrelated spoken utterance so the dedup gate actually runs
        # (it only runs when there are recent assistant utterances).
        env["assistant"]._chat_ctx.add_message(
            role="assistant",
            content="Got it - I'm checking that now.",
        )

        def _verdict(messages):
            system = messages[0]["content"]
            if f"- {ack}" in system:
                return "SUPPRESS | matches notification"
            return "SPEAK | not yet spoken"

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = True
        env["monkeypatch"].setattr(
            _dedup_mod,
            "new_llm_client",
            lambda *a, **kw: _stream_dedup_client(_verdict),
        )
        try:
            env["session"].agent_state = "thinking"
            self._send_speak_guidance(env, ack)
            await self._settle_and_drain(env)
            assert env["session"].say_calls == [ack], (
                "Acknowledgement must be spoken; its own injected notification "
                "must not count as 'already spoken'."
            )
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup

    async def test_dedup_skipped_when_disabled(self, fast_brain_env):
        """When SPEECH_DEDUP_ENABLED is False, speech plays without a dedup check."""
        env = fast_brain_env
        from unity.settings import SETTINGS

        env["assistant"]._chat_ctx.add_message(
            role="assistant",
            content="Found three Italian restaurants near you.",
        )

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = False
        try:
            env["session"].agent_state = "thinking"
            self._send_speak_guidance(
                env,
                "I found three Italian restaurants nearby.",
            )
            await self._settle_and_drain(env)
            assert (
                len(env["session"].say_calls) == 1
            ), "With dedup disabled, speech should always play."
            assert (
                env["session"].say_calls[0]
                == "I found three Italian restaurants nearby."
            )
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup

    async def test_dedup_skipped_when_no_recent_utterances(self, fast_brain_env):
        """With no assistant messages in chat context, dedup is skipped."""
        env = fast_brain_env
        from unity.settings import SETTINGS
        import unity.conversation_manager.domains.speech_dedup as _dedup_mod

        dedup_called = False

        def _mark_called():
            nonlocal dedup_called
            dedup_called = True

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = True
        env["monkeypatch"].setattr(
            _dedup_mod,
            "new_llm_client",
            lambda *a, **kw: _stream_dedup_client(
                "SUPPRESS | should never run",
                on_call=_mark_called,
            ),
        )
        try:
            env["session"].agent_state = "thinking"
            self._send_speak_guidance(
                env,
                "It's sunny in London.",
            )
            await self._settle_and_drain(env)
            assert (
                len(env["session"].say_calls) == 1
            ), "With no recent utterances, speech should play (no dedup needed)."
            assert (
                not dedup_called
            ), "Dedup LLM should not be called when there are no recent utterances."
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup

    async def test_latest_guidance_supersedes_older_queued_speech(self, fast_brain_env):
        """When multiple notifications arrive while the pipeline is busy, only
        the latest survives — the slow brain always has full context, so newer
        guidance supersedes older guidance (single-slot queue)."""
        env = fast_brain_env
        from unity.settings import SETTINGS
        import unity.conversation_manager.domains.speech_dedup as _dedup_mod

        env["assistant"]._chat_ctx.add_message(
            role="assistant",
            content="I've started the check.",
        )

        orig_dedup = SETTINGS.conversation.SPEECH_DEDUP_ENABLED
        SETTINGS.conversation.SPEECH_DEDUP_ENABLED = True
        env["monkeypatch"].setattr(
            _dedup_mod,
            "new_llm_client",
            lambda *a, **kw: _stream_dedup_client("SPEAK | allowed"),
        )
        try:
            env["session"].agent_state = "thinking"
            self._send_speak_guidance(
                env,
                "Here is the first result.",
            )
            self._send_speak_guidance(
                env,
                "Here is the second result.",
            )
            await self._settle_and_drain(env)

            assert len(env["session"].say_calls) == 1, (
                f"Only the latest guidance should survive (single-slot queue). "
                f"Got: {env['session'].say_calls}"
            )
            assert env["session"].say_calls[0] == "Here is the second result."
        finally:
            SETTINGS.conversation.SPEECH_DEDUP_ENABLED = orig_dedup


@pytest.mark.llm_call
@pytest.mark.eval
@pytest.mark.asyncio
class TestFastBrainOpeningGreeting:
    """The fast brain's first turn (session_start with no user message) should
    produce a short, natural greeting — not an acknowledgment of the system
    prompt, not a capability list, and not a tutorial."""

    async def test_session_start_produces_natural_greeting(self):
        """With only the system prompt (no user message), the fast brain should
        greet briefly and naturally.

        Uses reasoning_effort='low' to match the production voice pipeline
        (call.py UnifyLLM configuration)."""
        from unity.common.llm_client import new_llm_client
        from unity.conversation_manager.prompt_builders import (
            build_voice_agent_prompt,
        )
        from unity.settings import SETTINGS

        prompt = build_voice_agent_prompt(
            bio="A helpful AI assistant.",
            boss_first_name="Alex",
            boss_surname="",
            boss_phone_number="+15550000001",
            boss_email_address="alex@example.com",
            is_boss_user=True,
        ).flatten()

        client = new_llm_client(
            model=SETTINGS.conversation.FAST_BRAIN_MODEL,
            reasoning_effort="low",
        )
        response = await client.generate(
            system_message=prompt,
            messages=[],
        )

        response_lower = response.lower().strip()

        assert len(response) < 200, (
            f"Opening greeting is too long ({len(response)} chars).\n"
            f"Response: {response}\n"
            f"The first turn should be a brief greeting (1-2 sentences), "
            f"not a list of suggestions or a tutorial."
        )

        bullet_indicators = ["- ", "* ", "1.", "2.", "3."]
        has_bullets = any(ind in response for ind in bullet_indicators)
        assert not has_bullets, (
            f"Opening greeting contains a bulleted list.\n"
            f"Response: {response}\n"
            f"The first turn should be a conversational greeting, "
            f"not a list of options or suggestions."
        )

        filler_phrases = [
            "here are a few",
            "here are some",
            "you can say",
            "depending on what you need",
            "for example",
            "check something",
            "pull up an email",
            "draft a",
        ]
        has_filler = any(p in response_lower for p in filler_phrases)
        assert not has_filler, (
            f"Opening greeting sounds like a tutorial/help menu.\n"
            f"Response: {response}\n"
            f"The first turn should be a natural greeting like "
            f"'Hey, how can I help?' — not suggestions for what the "
            f"user can do."
        )

        starts_with_ack = response_lower.startswith("got it")
        assert not starts_with_ack, (
            f"Opening greeting starts with an acknowledgment ('Got it').\n"
            f"Response: {response}\n"
            f"There is nothing to acknowledge at the start of a call. "
            f"The model is echoing an example from the system prompt."
        )


class TestSayMetaTextMatching:
    """Regression tests for _last_say_meta text matching.

    When session.say() and generate_reply produce concurrent TTS, the
    _last_say_meta (set by session.say) can be consumed by the wrong
    conversation_item_added event. The fix is to store the spoken text
    in the meta and only consume it when the utterance text matches.
    """

    def test_match_say_meta_exists(self):
        """The match_say_meta helper must exist in common.py."""
        from unity.conversation_manager.medium_scripts.common import match_say_meta

        assert callable(match_say_meta)

    def test_matching_text_consumes_meta(self):
        """When utterance text matches the session.say text, return the meta."""
        from unity.conversation_manager.medium_scripts.common import match_say_meta

        meta = {
            "guidance_id": "guid-abc",
            "source": "proactive_speech",
            "text": "Still there, Alex?",
        }
        result = match_say_meta(meta, "Still there, Alex?")
        assert result is not None
        assert result["guidance_id"] == "guid-abc"

    def test_different_text_does_not_consume_meta(self):
        """When utterance text differs from session.say text, return None.

        This is the exact bug: a fast brain response like
        'One moment - I am pulling that up.' should NOT consume meta
        set by session.say('Sure, go ahead - what is the task?').
        """
        from unity.conversation_manager.medium_scripts.common import match_say_meta

        meta = {
            "guidance_id": "guid-abc",
            "source": "proactive_speech",
            "text": "Sure, go ahead — what's the task?",
        }
        result = match_say_meta(meta, "One moment — I'm pulling that up.")
        assert result is None

    def test_none_meta_returns_none(self):
        """When no meta is set, return None regardless of text."""
        from unity.conversation_manager.medium_scripts.common import match_say_meta

        assert match_say_meta(None, "anything") is None

    def test_meta_without_text_key_always_matches(self):
        """Legacy meta dicts without a text key match any utterance
        (backward compatible with pre-fix code)."""
        from unity.conversation_manager.medium_scripts.common import match_say_meta

        meta = {"guidance_id": "guid-abc", "source": "slow_brain"}
        result = match_say_meta(meta, "Some text")
        assert result is not None


# =============================================================================
# Participant comms rendering
# =============================================================================


class TestParticipantCommsRendering:
    """Unit tests for render_participant_comms — verifies tag format and
    participant-match filtering for comms events on any call."""

    def _make_event_json(self, event):
        return event.to_json()

    # ── Outbound (assistant → participant) ──────────────────────────────

    def test_outbound_unify_message_to_participant_rendered(self):
        """When the slow brain sends a Unify message to the person on the
        call, the fast brain must see it so it can verbally acknowledge it.

        Without this, the caller receives a silent text message during a
        live voice conversation with no verbal indication from the
        assistant that anything was sent.
        """
        from unity.conversation_manager.events import UnifyMessageSent
        from unity.conversation_manager.medium_scripts.common import (
            render_participant_comms,
        )

        event = UnifyMessageSent(
            contact={"contact_id": 1, "first_name": "Alex", "surname": "Demo"},
            content="Here's the detailed breakdown of your OneDrive contents.",
        )
        result = render_participant_comms(event.to_json(), {1})
        assert result is not None, (
            "render_participant_comms must render outbound UnifyMessageSent "
            "to a call participant so the fast brain can acknowledge it verbally"
        )
        assert "Alex Demo" in result
        assert "OneDrive" in result

    def test_outbound_sms_to_participant_rendered(self):
        """Outbound SMS to a call participant should be visible to the fast
        brain for verbal acknowledgement."""
        from unity.conversation_manager.events import SMSSent
        from unity.conversation_manager.medium_scripts.common import (
            render_participant_comms,
        )

        event = SMSSent(
            contact={"contact_id": 5, "first_name": "Marcus", "surname": "Rivera"},
            content="Sent you the meeting link.",
        )
        result = render_participant_comms(event.to_json(), {5})
        assert result is not None, (
            "render_participant_comms must render outbound SMSSent "
            "to a call participant so the fast brain can acknowledge it verbally"
        )
        assert "Marcus Rivera" in result
        assert "meeting link" in result

    def test_outbound_message_to_non_participant_returns_none(self):
        """Outbound messages to contacts NOT on the call should still be
        invisible to the fast brain — only participant-targeted messages
        matter."""
        from unity.conversation_manager.events import UnifyMessageSent
        from unity.conversation_manager.medium_scripts.common import (
            render_participant_comms,
        )

        event = UnifyMessageSent(
            contact={"contact_id": 99, "first_name": "Alice", "surname": "Other"},
            content="Some message to a third party.",
        )
        result = render_participant_comms(event.to_json(), {1, 5})
        assert result is None

    # ── Inbound (participant → assistant) ───────────────────────────────

    def test_sms_from_participant_rendered_with_tag(self):
        from unity.conversation_manager.medium_scripts.common import (
            render_participant_comms,
        )
        from unity.conversation_manager.events import SMSReceived

        event = SMSReceived(
            contact={"contact_id": 5, "first_name": "Marcus", "surname": "Rivera"},
            content="Running late, be there in 10.",
        )
        result = render_participant_comms(event.to_json(), {5})
        assert result is not None
        assert result.startswith("[SMS from Marcus Rivera]")
        assert "Running late" in result

    def test_email_from_participant_rendered_with_tag(self):
        from unity.conversation_manager.medium_scripts.common import (
            render_participant_comms,
        )
        from unity.conversation_manager.events import EmailReceived

        event = EmailReceived(
            contact={"contact_id": 3, "first_name": "Sarah", "surname": "Chen"},
            subject="Updated agenda",
            body="See attached for the revised agenda.",
        )
        result = render_participant_comms(event.to_json(), {3})
        assert result is not None
        assert result.startswith("[Email from Sarah Chen]")
        assert "Updated agenda" in result

    def test_unify_message_from_participant_rendered_with_tag(self):
        from unity.conversation_manager.medium_scripts.common import (
            render_participant_comms,
        )
        from unity.conversation_manager.events import UnifyMessageReceived

        event = UnifyMessageReceived(
            contact={"contact_id": 7, "first_name": "Priya", "surname": "Sharma"},
            content="Check the shared doc.",
        )
        result = render_participant_comms(event.to_json(), {7})
        assert result is not None
        assert result.startswith("[Message from Priya Sharma]")
        assert "shared doc" in result

    def test_sms_from_non_participant_returns_none(self):
        from unity.conversation_manager.medium_scripts.common import (
            render_participant_comms,
        )
        from unity.conversation_manager.events import SMSReceived

        event = SMSReceived(
            contact={"contact_id": 99, "first_name": "Stranger", "surname": "Person"},
            content="Hello?",
        )
        result = render_participant_comms(event.to_json(), {5, 3})
        assert result is None

    def test_non_comms_event_returns_none(self):
        from unity.conversation_manager.medium_scripts.common import (
            render_participant_comms,
        )
        from unity.conversation_manager.events import ActorNotification

        event = ActorNotification(handle_id=1, response="Searching...")
        result = render_participant_comms(event.to_json(), {1, 5})
        assert result is None

    def test_multiple_participants_matched(self):
        from unity.conversation_manager.medium_scripts.common import (
            render_participant_comms,
        )
        from unity.conversation_manager.events import SMSReceived

        event = SMSReceived(
            contact={"contact_id": 3, "first_name": "Sarah", "surname": "Chen"},
            content="On my way.",
        )
        result = render_participant_comms(event.to_json(), {1, 3, 5})
        assert result is not None
        assert "[SMS from Sarah Chen]" in result


# =============================================================================
# Child Process Logging (forkserver compatibility)
# =============================================================================


class TestChildProcessLogging:
    """Verify _configure_child_logging fixes log propagation for LiveKit's
    forkserver child processes.

    LiveKit agents (v1.2.x) routes child-process logs through a
    LogQueueHandler on the **root** logger.  Unity's LOGGER defaults to
    propagate=False, which silently drops every record in the child.
    _configure_child_logging must flip propagation on and strip stale
    direct handlers so records reach the root relay.
    """

    def test_propagation_enabled_and_handlers_cleared(self):
        import logging

        from unity.logger import LOGGER

        from unity.conversation_manager.medium_scripts.call import (
            _configure_child_logging,
        )

        original_propagate = LOGGER.propagate
        original_handlers = list(LOGGER.handlers)
        try:
            assert (
                LOGGER.propagate is False
            ), "precondition: LOGGER.propagate should be False before the fix runs"

            _configure_child_logging()

            assert LOGGER.propagate is True
            assert LOGGER.handlers == []

            for name in ("livekit", "livekit.agents", "livekit.plugins"):
                lg = logging.getLogger(name)
                assert lg.propagate is True
                assert lg.handlers == []
        finally:
            LOGGER.propagate = original_propagate
            LOGGER.handlers = original_handlers

    def test_records_reach_root_logger_after_configure(self):
        import logging

        from unity.logger import LOGGER

        from unity.conversation_manager.medium_scripts.call import (
            _configure_child_logging,
        )

        original_propagate = LOGGER.propagate
        original_handlers = list(LOGGER.handlers)
        captured: list[logging.LogRecord] = []

        class _CaptureHandler(logging.Handler):
            def emit(self, record):
                captured.append(record)

        root = logging.getLogger()
        capture = _CaptureHandler()
        root.addHandler(capture)
        root_level = root.level
        root.setLevel(logging.NOTSET)
        try:
            _configure_child_logging()

            LOGGER.info("test-sentinel-message")

            assert any(
                r.message == "test-sentinel-message" for r in captured
            ), "LOGGER records must propagate to root after _configure_child_logging"
        finally:
            root.removeHandler(capture)
            root.setLevel(root_level)
            LOGGER.propagate = original_propagate
            LOGGER.handlers = original_handlers
