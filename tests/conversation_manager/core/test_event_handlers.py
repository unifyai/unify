"""
tests/conversation_manager/test_event_handlers.py
======================================================

Unit and integration tests for the EventHandler registry and individual
event handlers in `domains/event_handlers.py`.

Tests cover:
1. EventHandler registry pattern (`@EventHandler.register`, `handle_event`)
2. The `_event_type_to_log_key` helper (CamelCase → snake_case conversion)
3. Individual event handler behavior and side effects
4. Handler error cases and edge conditions
"""

from __future__ import annotations

from typing import ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unify.conversation_manager.domains.event_handlers import (
    EventHandler,
    INITIALIZATION_COMPLETE_NO_HISTORY_NOTIFICATION,
    INITIALIZATION_COMPLETE_NOTIFICATION,
    OPEN_SLOW_BRAIN_TURN_NOTIFICATION,
    _event_type_to_log_key,
)
from unify.conversation_manager.events import (
    ActionStopRequested,
    ActorClarificationRequest,
    ActorHandleResponse,
    ActorHandleStarted,
    ActorNotification,
    ActorResult,
    ActorSessionResponse,
    BackupContactsEvent,
    DirectMessageEvent,
    Error,
    Event,
    GetChatHistory,
    InitializationComplete,
    LLMInput,
    NotificationInjectedEvent,
    NotificationUnpinnedEvent,
    OpenSlowBrainTurn,
    Ping,
    SyncContacts,
    UnifyMessageReceived,
    UnifyMessageSent,
)
from unify.contact_manager.simulated import SimulatedContactManager
from unify.conversation_manager.domains.contact_index import ContactIndex
from unify.conversation_manager.domains.notifications import NotificationBar
from unify.conversation_manager.cm_types import Medium, Mode
from unify.session_details import SESSION_DETAILS

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def mock_session_logger():
    """Create a mock session logger."""
    logger = MagicMock()
    logger.info = MagicMock()
    logger.debug = MagicMock()
    logger.error = MagicMock()
    return logger


@pytest.fixture
def mock_event_broker():
    """Create a mock event broker."""
    broker = MagicMock()
    broker.publish = AsyncMock(return_value=0)
    return broker


@pytest.fixture
def sample_contacts():
    """Standard test contacts."""
    return [
        {
            "contact_id": 0,
            "first_name": "Test",
            "surname": "Assistant",
            "email_address": "assistant@test.com",
            "phone_number": "+15555551234",
        },
        {
            "contact_id": 1,
            "first_name": "Boss",
            "surname": "User",
            "email_address": "boss@test.com",
            "phone_number": "+15555551111",
        },
        {
            "contact_id": 2,
            "first_name": "Alice",
            "surname": "Smith",
            "email_address": "alice@example.com",
            "phone_number": "+15555552222",
        },
    ]


@pytest.fixture
def mock_cm(mock_session_logger, mock_event_broker, sample_contacts):
    """Create a mock ConversationManager with minimal state for handler tests."""
    cm = MagicMock()
    cm._session_logger = mock_session_logger
    cm.event_broker = mock_event_broker
    cm.mode = Mode.TEXT
    cm.chat_history = []
    cm.in_flight_actions = {}
    cm.completed_actions = {}
    cm.memory_manager = None

    # Create a SimulatedContactManager and populate with sample contacts
    contact_manager = SimulatedContactManager()

    # Update system contacts (0 and 1) with sample data
    for contact_data in sample_contacts:
        contact_id = contact_data["contact_id"]
        contact_manager.update_contact(
            contact_id=contact_id,
            first_name=contact_data.get("first_name"),
            surname=contact_data.get("surname"),
            email_address=contact_data.get("email_address"),
            phone_number=contact_data.get("phone_number"),
        )

    # Set up contact index with SimulatedContactManager
    cm.contact_index = ContactIndex()
    cm.contact_index.set_contact_manager(contact_manager)
    cm.contact_manager = contact_manager

    # Set up notifications bar
    cm.notifications_bar = NotificationBar()

    # LLM generation counter (commissioning duplicate guard, tool execution metadata)
    cm._llm_gen = 0

    # Mock async methods
    cm.request_llm_run = AsyncMock()
    cm.stop_in_flight_action_by_calling_id = AsyncMock(return_value=True)
    cm.record_last_inbound_reply = MagicMock()
    cm.get_active_contact = MagicMock(return_value=sample_contacts[1])

    return cm


# =============================================================================
# 1. EventHandler Registry Tests
# =============================================================================


class TestEventHandlerRegistry:
    """Tests for the EventHandler registry pattern."""

    def test_registry_is_populated(self):
        """Verify that the registry contains registered event handlers."""
        assert len(EventHandler._registry) > 0, "Registry should have handlers"

    def test_known_events_are_registered(self):
        """Verify that expected event classes are in the registry."""
        expected_events = [
            Ping,
            ActionStopRequested,
            UnifyMessageReceived,
            UnifyMessageSent,
            Error,
            BackupContactsEvent,
            GetChatHistory,
            ActorHandleStarted,
            ActorHandleResponse,
            ActorResult,
            ActorClarificationRequest,
            ActorSessionResponse,
            ActorNotification,
            NotificationInjectedEvent,
            NotificationUnpinnedEvent,
            SyncContacts,
            OpenSlowBrainTurn,
            InitializationComplete,
            DirectMessageEvent,
        ]
        for event_cls in expected_events:
            assert (
                event_cls in EventHandler._registry
            ), f"{event_cls.__name__} should be registered"

    def test_unregistered_event_returns_sleep(self):
        """Verify that unregistered events return a no-op coroutine."""
        import uuid

        # A freshly-created event class has no registered handler.
        unregistered_cls = type(
            f"_UnregisteredEvent_{uuid.uuid4().hex[:8]}",
            (Event,),
            {},
        )
        result = EventHandler._registry.get(unregistered_cls)
        assert result is None, "A fresh event class should have no handler"

    def test_register_decorator_single_event(self):
        """Verify @EventHandler.register works for single event class."""
        # Use a dynamically created event class (proper subclass syntax)
        # We need to create a unique class each time to avoid registry conflicts
        import uuid

        class_name = f"_TestSingleEvent_{uuid.uuid4().hex[:8]}"
        TestEventCls = type(class_name, (Event,), {})

        test_handler_called = []

        @EventHandler.register(TestEventCls)
        async def test_handler(event, cm, *args, **kwargs):
            test_handler_called.append(True)

        assert TestEventCls in EventHandler._registry
        assert EventHandler._registry[TestEventCls] == test_handler

        # Cleanup
        del EventHandler._registry[TestEventCls]

    def test_register_decorator_multiple_events(self):
        """Verify @EventHandler.register works for tuple of event classes."""
        import uuid

        suffix = uuid.uuid4().hex[:8]
        TestEventA = type(f"_TestEventA_{suffix}", (Event,), {})
        TestEventB = type(f"_TestEventB_{suffix}", (Event,), {})

        @EventHandler.register((TestEventA, TestEventB))
        async def multi_handler(event, cm, *args, **kwargs):
            pass

        assert TestEventA in EventHandler._registry
        assert TestEventB in EventHandler._registry
        assert EventHandler._registry[TestEventA] == multi_handler
        assert EventHandler._registry[TestEventB] == multi_handler

        # Cleanup
        del EventHandler._registry[TestEventA]
        del EventHandler._registry[TestEventB]


class TestEventTypeToLogKey:
    """Tests for the _event_type_to_log_key helper function."""

    def test_simple_camel_case(self):
        """Simple CamelCase converts to snake_case."""
        assert _event_type_to_log_key(ActorResult) == "actor_result"
        assert _event_type_to_log_key(SyncContacts) == "sync_contacts"

    def test_consecutive_uppercase(self):
        """Handles consecutive uppercase letters (LLM)."""
        assert _event_type_to_log_key(LLMInput) == "llm_input"

    def test_unify_message_events(self):
        """UnifyMessage event names convert correctly."""
        assert _event_type_to_log_key(UnifyMessageReceived) == "unify_message_received"
        assert _event_type_to_log_key(UnifyMessageSent) == "unify_message_sent"

    def test_single_word(self):
        """Single-word event names convert correctly."""
        assert _event_type_to_log_key(Ping) == "ping"
        assert _event_type_to_log_key(Error) == "error"

    def test_multi_word_events(self):
        """Longer event names convert correctly."""
        assert (
            _event_type_to_log_key(NotificationInjectedEvent)
            == "notification_injected_event"
        )
        assert _event_type_to_log_key(OpenSlowBrainTurn) == "open_slow_brain_turn"


# =============================================================================
# 2. handle_event Core Behavior Tests
# =============================================================================


class TestHandleEventCore:
    """Tests for EventHandler.handle_event core behavior."""

    @pytest.mark.asyncio
    async def test_handle_event_logs_event(self, mock_cm):
        """Verify handle_event logs loggable+prominent events via session logger."""
        from dataclasses import dataclass

        @dataclass
        class StubLoggableEvent(Event):
            prominent: ClassVar[bool] = True

        mock_cm._current_event_trace = {"event_id": "evt-test"}
        event = StubLoggableEvent()
        await EventHandler.handle_event(event, mock_cm)

        mock_cm._session_logger.info.assert_called_with(
            "stub_loggable_event",
            "Event: StubLoggableEvent",
        )

    @pytest.mark.asyncio
    async def test_handle_event_publishes_loggable_events(self, mock_cm):
        """Verify loggable events are published to bus."""
        event = UnifyMessageReceived(
            contact={"contact_id": 2},
            content="Hello",
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

            # Should queue a publish operation for loggable event
            assert mock_utils.queue_operation.called

    @pytest.mark.asyncio
    async def test_handle_event_skips_non_loggable(self, mock_cm):
        """Verify non-loggable events (like Ping) don't publish to bus."""
        event = Ping(kind="keepalive")
        assert event.loggable is False

        with patch(
            "unify.conversation_manager.domains.event_handlers.asyncio.create_task",
        ) as mock_create_task:
            await EventHandler.handle_event(event, mock_cm)
            mock_create_task.assert_not_called()


# =============================================================================
# 3. Ping Event Handler Tests
# =============================================================================


class TestPingHandler:
    """Tests for the Ping event handler."""

    @pytest.mark.asyncio
    async def test_ping_logs_debug_message(self, mock_cm):
        """Ping handler logs debug message."""
        event = Ping(kind="test")
        await EventHandler.handle_event(event, mock_cm)

        mock_cm._session_logger.debug.assert_called_with(
            "ping",
            "Ping received - keeping conversation manager alive",
        )


# =============================================================================
# 4. Unify Message Event Handler Tests
# =============================================================================


class TestUnifyMessageHandlers:
    """Tests for the UnifyMessageReceived / UnifyMessageSent handlers."""

    @pytest.mark.asyncio
    async def test_received_updates_contact_index(self, mock_cm):
        """UnifyMessageReceived adds the message to the contact's thread."""
        event = UnifyMessageReceived(
            contact={"contact_id": 2},
            content="Hello there!",
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        msgs = mock_cm.contact_index.get_messages_for_contact(2)
        assert len(msgs) == 1
        assert msgs[0].content == "Hello there!"
        assert msgs[0].role == "user"

    @pytest.mark.asyncio
    async def test_received_pushes_notification(self, mock_cm):
        """UnifyMessageReceived pushes a notification naming the sender."""
        event = UnifyMessageReceived(
            contact={"contact_id": 2},
            content="Test message",
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        assert len(mock_cm.notifications_bar.notifications) == 1
        notif = mock_cm.notifications_bar.notifications[0]
        assert notif.type == "comms"
        assert notif.content == "Unify message from Alice Smith"

    @pytest.mark.asyncio
    async def test_received_requests_llm_run(self, mock_cm):
        """UnifyMessageReceived wakes the slow brain for the sender."""
        event = UnifyMessageReceived(
            contact={"contact_id": 2},
            content="Need response",
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        mock_cm.request_llm_run.assert_called_once_with(triggering_contact_id=2)

    @pytest.mark.asyncio
    async def test_received_records_last_inbound_reply_context(self, mock_cm):
        """UnifyMessageReceived records where a reply should be routed."""
        event = UnifyMessageReceived(
            contact={"contact_id": 2},
            content="Where are you?",
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        mock_cm.record_last_inbound_reply.assert_called_once_with(
            {"medium": Medium.UNIFY_MESSAGE.value, "contact_id": 2},
        )

    @pytest.mark.asyncio
    async def test_received_logs_message_via_transcript_queue(self, mock_cm):
        """UnifyMessageReceived queues the transcript write for the message."""
        event = UnifyMessageReceived(
            contact={"contact_id": 2},
            content="Log me",
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        mock_utils.queue_operation.assert_any_call(
            mock_utils.log_message,
            mock_cm,
            event,
        )

    @pytest.mark.asyncio
    async def test_received_carries_attachments_into_thread(self, mock_cm):
        """Attachments on the event are attached to the thread entry."""
        attachment = {
            "filename": "report.pdf",
            "filepath": "Files/report.pdf",
            "content_type": "application/pdf",
            "size_bytes": 1234,
        }
        event = UnifyMessageReceived(
            contact={"contact_id": 2},
            content="See attached",
            attachments=[attachment],
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        msgs = mock_cm.contact_index.get_messages_for_contact(2)
        assert len(msgs) == 1
        assert msgs[0].attachments == [attachment]

    @pytest.mark.asyncio
    async def test_received_from_unknown_contact_uses_event_contact(self, mock_cm):
        """A sender missing from the contact catalogue is named from the event."""
        event = UnifyMessageReceived(
            contact={"contact_id": 42, "first_name": "Zed", "surname": "Nobody"},
            content="Hi",
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        msgs = mock_cm.contact_index.get_messages_for_contact(42)
        assert len(msgs) == 1
        assert (
            mock_cm.notifications_bar.notifications[0].content
            == "Unify message from Zed Nobody"
        )
        mock_cm.request_llm_run.assert_called_once_with(triggering_contact_id=42)

    @pytest.mark.asyncio
    async def test_sent_updates_contact_index_with_assistant_role(self, mock_cm):
        """UnifyMessageSent adds the message with the assistant role."""
        event = UnifyMessageSent(
            contact={"contact_id": 2},
            content="Reply to you",
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        msgs = mock_cm.contact_index.get_messages_for_contact(2)
        assert len(msgs) == 1
        assert msgs[0].role == "assistant"
        assert (
            mock_cm.notifications_bar.notifications[0].content
            == "Unify message sent to Alice Smith"
        )

    @pytest.mark.asyncio
    async def test_sent_does_not_record_inbound_reply_context(self, mock_cm):
        """Outbound messages never update the inbound reply routing."""
        event = UnifyMessageSent(
            contact={"contact_id": 2},
            content="Outgoing message",
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        mock_cm.record_last_inbound_reply.assert_not_called()


class TestOutboundSentWakePolicy:
    """UnifyMessageSent honors suppress_slow_brain_wake on the event."""

    @pytest.mark.asyncio
    async def test_unify_message_sent_with_suppress_flag_skips_llm_run(self, mock_cm):
        event = UnifyMessageSent(
            contact={"contact_id": 2},
            content="Quiet reply",
            suppress_slow_brain_wake=True,
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        mock_cm.request_llm_run.assert_not_called()
        assert len(mock_cm.notifications_bar.notifications) == 1

    @pytest.mark.asyncio
    async def test_unify_message_sent_without_suppress_flag_wakes_slow_brain(
        self,
        mock_cm,
    ):
        event = UnifyMessageSent(
            contact={"contact_id": 2},
            content="Actor follow-up",
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        mock_cm.request_llm_run.assert_called_once_with(triggering_contact_id=2)

    @pytest.mark.asyncio
    async def test_received_ignores_suppress_flag(self, mock_cm):
        """An inbound user message always wakes the slow brain."""
        event = UnifyMessageReceived(
            contact={"contact_id": 2},
            content="Still needs a reply",
            suppress_slow_brain_wake=True,
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        mock_cm.request_llm_run.assert_called_once_with(triggering_contact_id=2)


# =============================================================================
# 5. Error Event Handler Tests
# =============================================================================


class TestErrorHandler:
    """Tests for the Error event handler."""

    @pytest.mark.asyncio
    async def test_error_pushes_notification_and_wakes_brain(self, mock_cm):
        """Error surfaces on the notification bar and triggers an immediate turn."""
        event = Error(message="send_unify_message failed: connection reset")

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        assert len(mock_cm.notifications_bar.notifications) == 1
        notif = mock_cm.notifications_bar.notifications[0]
        assert notif.type == "Error"
        assert notif.content == "send_unify_message failed: connection reset"
        mock_cm.request_llm_run.assert_called_once_with(delay=0)


# =============================================================================
# 6. ActionStopRequested Handler Tests
# =============================================================================


class TestActionStopRequestedHandler:
    """Tests for the ActionStopRequested event handler."""

    @pytest.mark.asyncio
    async def test_stop_delegates_to_cm_with_reason(self, mock_cm):
        """The handler stops the in-flight action by calling_id."""
        event = ActionStopRequested(
            calling_id="act-123",
            reason="User changed their mind",
            source="chat",
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        mock_cm.stop_in_flight_action_by_calling_id.assert_awaited_once_with(
            "act-123",
            reason="User changed their mind",
        )

    @pytest.mark.asyncio
    async def test_stop_without_reason_uses_default(self, mock_cm):
        """An empty reason falls back to the default stop message."""
        event = ActionStopRequested(calling_id="act-123")

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        mock_cm.stop_in_flight_action_by_calling_id.assert_awaited_once_with(
            "act-123",
            reason="Stop requested by the user.",
        )

    @pytest.mark.asyncio
    async def test_stop_for_unknown_calling_id_does_not_raise(self, mock_cm):
        """An unknown calling_id is logged, not raised."""
        mock_cm.stop_in_flight_action_by_calling_id = AsyncMock(return_value=False)
        event = ActionStopRequested(calling_id="missing")

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        mock_cm.stop_in_flight_action_by_calling_id.assert_awaited_once()


# =============================================================================
# 7. BackupContactsEvent Handler Tests
# =============================================================================


class TestBackupContactsHandler:
    """Tests for the BackupContactsEvent handler."""

    @pytest.mark.asyncio
    async def test_caches_contacts_before_contact_manager_is_set(self, mock_cm):
        """Without a ContactManager the contacts are cached as fallbacks."""
        mock_cm.contact_index = ContactIndex()
        event = BackupContactsEvent(
            contacts=[{"contact_id": 9, "first_name": "Fallback", "surname": "One"}],
        )

        await EventHandler.handle_event(event, mock_cm)

        cached = mock_cm.contact_index.get_contact(9)
        assert cached is not None
        assert cached["first_name"] == "Fallback"

    @pytest.mark.asyncio
    async def test_ignored_once_contact_manager_is_set(self, mock_cm):
        """With a ContactManager attached the fallback cache is left alone."""
        event = BackupContactsEvent(
            contacts=[{"contact_id": 9, "first_name": "Fallback", "surname": "One"}],
        )

        await EventHandler.handle_event(event, mock_cm)

        assert 9 not in mock_cm.contact_index._fallback_contacts


# =============================================================================
# 8. State Update Handler Tests
# =============================================================================


class TestStateUpdateHandlers:
    """Tests for state update event handlers."""

    @pytest.mark.asyncio
    async def test_get_chat_history_prepends_to_history(self, mock_cm):
        """GetChatHistory prepends messages to existing history."""
        mock_cm.chat_history = [{"role": "user", "content": "existing"}]
        event = GetChatHistory(
            chat_history=[
                {"role": "user", "content": "older message"},
                {"role": "assistant", "content": "older response"},
            ],
        )

        await EventHandler.handle_event(event, mock_cm)

        # New history should be prepended
        assert len(mock_cm.chat_history) == 3
        assert mock_cm.chat_history[0]["content"] == "older message"
        assert mock_cm.chat_history[2]["content"] == "existing"


# =============================================================================
# 9. Actor Event Handler Tests
# =============================================================================


class TestActorEventHandlers:
    """Tests for Actor-related event handlers."""

    @pytest.mark.asyncio
    async def test_actor_handle_started_does_not_push_notification(self, mock_cm):
        """ActorHandleStarted does not push a notification (action state is shown in in_flight_actions)."""
        event = ActorHandleStarted(
            action_name="search_task",
            handle_id=1,
            query="Search for documents about Python",
        )

        await EventHandler.handle_event(event, mock_cm)

        assert len(mock_cm.notifications_bar.notifications) == 0

    @pytest.mark.asyncio
    async def test_actor_handle_started_does_not_trigger_slow_brain(self, mock_cm):
        """ActorHandleStarted is a no-op for the slow brain."""
        event = ActorHandleStarted(
            action_name="task",
            handle_id=1,
            query="Do something",
        )

        await EventHandler.handle_event(event, mock_cm)

        mock_cm.request_llm_run.assert_not_called()

    @pytest.mark.asyncio
    async def test_actor_result_moves_action_to_completed(self, mock_cm):
        """ActorResult moves action from in_flight_actions to completed_actions."""
        mock_cm.in_flight_actions = {
            1: {"query": "Test action", "action_type": "act", "handle_actions": []},
        }
        event = ActorResult(
            handle_id=1,
            success=True,
            result="Action completed successfully",
            action_type="act",
        )

        await EventHandler.handle_event(event, mock_cm)

        assert 1 not in mock_cm.in_flight_actions
        assert 1 in mock_cm.completed_actions
        assert mock_cm.completed_actions[1]["query"] == "Test action"
        # Result is recorded in handle_actions as act_completed event
        handle_actions = mock_cm.completed_actions[1]["handle_actions"]
        completion = next(
            a for a in handle_actions if a["action_name"] == "act_completed"
        )
        assert completion["success"] is True
        assert completion["action_type"] == "act"
        assert completion["result"] == "Action completed successfully"
        # No notification pushed (result is shown in completed_actions section)
        assert len(mock_cm.notifications_bar.notifications) == 0
        mock_cm.request_llm_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_actor_result_failure_records_error_context(self, mock_cm):
        """ActorResult failure stores error context before completion handoff."""
        mock_cm.in_flight_actions = {
            7: {
                "query": "Fix contact memberships",
                "action_type": "act",
                "handle_actions": [],
            },
        }
        event = ActorResult(
            handle_id=7,
            success=False,
            result={"error_kind": "permission_denied"},
            error="Admin role required",
            action_type="act",
        )

        await EventHandler.handle_event(event, mock_cm)

        assert 7 not in mock_cm.in_flight_actions
        completed = mock_cm.completed_actions[7]
        completion = next(
            a for a in completed["handle_actions"] if a["action_name"] == "act_failed"
        )
        assert completion["success"] is False
        assert completion["action_type"] == "act"
        assert completion["error"] == "Admin role required"
        assert completion["result"] == {"error_kind": "permission_denied"}

    @pytest.mark.asyncio
    async def test_actor_result_for_unknown_handle_still_wakes_brain(self, mock_cm):
        """A result for an untracked handle is tolerated and still wakes the brain."""
        event = ActorResult(handle_id=404, success=True, result="done")

        await EventHandler.handle_event(event, mock_cm)

        assert 404 not in mock_cm.completed_actions
        mock_cm.request_llm_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_actor_notification_records_progress_without_waking_brain(
        self,
        mock_cm,
    ):
        """Progress notifications accumulate in the action history silently."""
        mock_cm.in_flight_actions = {
            1: {"query": "Split Friday's dinner", "handle_actions": []},
        }
        event = ActorNotification(
            handle_id=1,
            response="Still working...",
            kind="progress",
        )

        await EventHandler.handle_event(event, mock_cm)

        handle_actions = mock_cm.in_flight_actions[1]["handle_actions"]
        assert handle_actions == [
            {
                "action_name": "progress",
                "query": "Still working...",
                "timestamp": handle_actions[0]["timestamp"],
            },
        ]
        mock_cm.request_llm_run.assert_not_called()

    @pytest.mark.asyncio
    async def test_actor_notification_records_late_notification_in_completed_actions(
        self,
        mock_cm,
    ):
        """A notification arriving after the handle moved to completed_actions
        (e.g. StorageCheck Phase 2 finishing after ActorResult already fired)
        must be recorded, not silently dropped."""
        mock_cm.in_flight_actions = {}
        mock_cm.completed_actions = {
            1: {"query": "Split Friday's dinner", "handle_actions": []},
        }
        event = ActorNotification(
            handle_id=1,
            response="Saved: bill-split rule, Sam fact, split_dinner_bill skill.",
            kind="storage_review_complete",
        )

        await EventHandler.handle_event(event, mock_cm)

        handle_actions = mock_cm.completed_actions[1]["handle_actions"]
        assert any(
            a["action_name"] == "progress" and "Saved" in a["query"]
            for a in handle_actions
        )

    @pytest.mark.asyncio
    async def test_actor_session_response_awaits_input_and_wakes_brain(
        self,
        mock_cm,
    ):
        """ActorSessionResponse records an awaiting_input turn and wakes the brain."""
        mock_cm.in_flight_actions = {
            1: {"query": "Persistent session", "handle_actions": []},
        }
        event = ActorSessionResponse(handle_id=1, content="Turn done. What next?")

        await EventHandler.handle_event(event, mock_cm)

        entry = mock_cm.in_flight_actions[1]["handle_actions"][0]
        assert entry["action_name"] == "response"
        assert entry["query"] == "Turn done. What next?"
        assert entry["status"] == "awaiting_input"
        mock_cm.request_llm_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_actor_handle_response_updates_matching_pending_action(
        self,
        mock_cm,
    ):
        """ActorHandleResponse should complete only the matching pending action."""
        response_text = "Action-specific response payload."

        mock_cm.in_flight_actions = {
            1: {
                "query": "Search transcripts for budget review",
                "handle_actions": [
                    {
                        "action_name": "interject_1",
                        "query": "add context",
                        "status": "pending",
                    },
                    {
                        "action_name": "ask_1",
                        "query": "what is the current status?",
                        "status": "pending",
                    },
                ],
            },
        }
        event = ActorHandleResponse(
            handle_id=1,
            action_name="ask",
            query="what is the current status?",
            response=response_text,
            call_id="",
        )

        await EventHandler.handle_event(event, mock_cm)

        interject_event = mock_cm.in_flight_actions[1]["handle_actions"][0]
        ask_event = mock_cm.in_flight_actions[1]["handle_actions"][1]

        assert interject_event["status"] == "pending"
        assert ask_event["status"] == "completed"
        assert ask_event.get("response") == response_text

    @pytest.mark.asyncio
    async def test_actor_handle_response_updates_completed_action(
        self,
        mock_cm,
    ):
        """ActorHandleResponse for a completed action should still update
        the pending ask and wake the brain.

        Regression: the handler only checked ``in_flight_actions``, so ask
        responses on actions that had already moved to ``completed_actions``
        were silently dropped.
        """
        response_text = "Detailed retrospective answer."

        mock_cm.completed_actions = {
            1: {
                "query": "Process accounts into standard format",
                "handle_actions": [
                    {
                        "action_name": "act_completed",
                        "query": "Done",
                        "status": "completed",
                    },
                    {
                        "action_name": "ask_1",
                        "query": "How did you break down the task?",
                        "status": "pending",
                    },
                ],
            },
        }
        event = ActorHandleResponse(
            handle_id=1,
            action_name="ask",
            query="How did you break down the task?",
            response=response_text,
            call_id="",
        )

        await EventHandler.handle_event(event, mock_cm)

        ask_event = mock_cm.completed_actions[1]["handle_actions"][1]
        assert ask_event["status"] == "completed"
        assert ask_event.get("response") == response_text
        mock_cm.request_llm_run.assert_called()

    @pytest.mark.asyncio
    async def test_actor_clarification_request_updates_handle_actions(self, mock_cm):
        """ActorClarificationRequest adds clarification to handle_actions."""
        mock_cm.in_flight_actions = {
            1: {"query": "Ambiguous action", "handle_actions": []},
        }
        event = ActorClarificationRequest(
            handle_id=1,
            query="What do you mean by 'documents'?",
            call_id="call_123",
        )

        await EventHandler.handle_event(event, mock_cm)

        assert len(mock_cm.in_flight_actions[1]["handle_actions"]) == 1
        clarification = mock_cm.in_flight_actions[1]["handle_actions"][0]
        assert clarification["action_name"] == "clarification_request"
        assert clarification["query"] == "What do you mean by 'documents'?"
        assert clarification["call_id"] == "call_123"
        mock_cm.request_llm_run.assert_called_once()


# =============================================================================
# 10. Notification Event Handler Tests
# =============================================================================


class TestNotificationEventHandlers:
    """Tests for notification injection/unpinning event handlers."""

    @pytest.mark.asyncio
    async def test_notification_injected_adds_to_bar(self, mock_cm):
        """NotificationInjectedEvent adds notification to bar."""
        event = NotificationInjectedEvent(
            content="Important update from the actor",
            source="Actor",
            target_conversation_id="conv_123",
        )

        await EventHandler.handle_event(event, mock_cm)

        assert len(mock_cm.notifications_bar.notifications) == 1
        notif = mock_cm.notifications_bar.notifications[0]
        assert notif.content == "Important update from the actor"
        assert notif.type == "Actor"
        assert notif.interjection_id == event.interjection_id

    @pytest.mark.asyncio
    async def test_notification_injected_preserves_pinned_flag(self, mock_cm):
        """A pinned interjection stays pinned on the bar."""
        event = NotificationInjectedEvent(
            content="Keep this visible",
            source="System",
            target_conversation_id="conv_123",
            pinned=True,
        )

        await EventHandler.handle_event(event, mock_cm)

        assert mock_cm.notifications_bar.notifications[0].pinned is True

    @pytest.mark.asyncio
    async def test_notification_injected_triggers_immediate_llm(self, mock_cm):
        """NotificationInjectedEvent triggers immediate LLM run."""
        event = NotificationInjectedEvent(
            content="React to this",
            source="Actor",
            target_conversation_id="conv_123",
        )

        await EventHandler.handle_event(event, mock_cm)

        mock_cm.request_llm_run.assert_called_once_with(delay=0)

    @pytest.mark.asyncio
    async def test_notification_unpinned_removes_from_bar(self, mock_cm, static_now):
        """NotificationUnpinnedEvent removes pinned notification."""
        # First add a pinned notification
        mock_cm.notifications_bar.push_notif(
            "Test",
            "Pinned content",
            static_now,
            pinned=True,
            id="notif_123",
        )
        assert len(mock_cm.notifications_bar.notifications) == 1

        event = NotificationUnpinnedEvent(
            interjection_id="notif_123",
            target_conversation_id="conv_123",
        )

        await EventHandler.handle_event(event, mock_cm)

        # Notification should be removed
        assert len(mock_cm.notifications_bar.notifications) == 0
        mock_cm.request_llm_run.assert_not_called()


# =============================================================================
# 11. InitializationComplete Handler Tests
# =============================================================================


class TestInitializationCompleteHandler:
    """Tests for the InitializationComplete handler.

    The handler always schedules a post-init brain turn (so deferred work
    from pre-init replies is not silently dropped), but the system
    notification it pins is deliberately worded to discourage gratuitous
    duplicate replies — see
    ``event_handlers.INITIALIZATION_COMPLETE_NOTIFICATION``. These tests
    lock in both the "always run the brain" contract and the key wording
    of that directive (regression coverage for the cold-start
    duplicate-reply bug observed for assistant 1820).
    """

    @pytest.fixture(autouse=True)
    def _init_handler_state(self, mock_cm):
        """Reset per-test CM attributes the handler reads."""
        # Default to the nothing-restored boot; tests that pin the
        # history-loaded wording set a positive count explicitly.
        mock_cm._hydrated_history_count = 0
        yield

    @pytest.mark.asyncio
    async def test_notification_pushed(self, mock_cm):
        """The handler must pin an 'Initialization complete' notification."""

        await EventHandler.handle_event(InitializationComplete(), mock_cm)

        assert any(
            "Initialization complete" in notif.content
            for notif in mock_cm.notifications_bar.notifications
        )

    @pytest.mark.asyncio
    async def test_notification_uses_anti_duplicate_directive(self, mock_cm):
        """The pinned notif must explicitly tell the brain to call wait
        and NOT send a duplicate/rephrased reply.

        Regression test for the Unify duplicate-reply bug: a looser
        wording ("Review any earlier responses … and follow up if needed
        — correct, elaborate, or confirm") was being read as permission
        to re-send a rephrased version of the pre-init reply. The
        wording must keep the legitimate follow-up paths (deferred work,
        wrong/incomplete due to missing context) but explicitly forbid
        rephrase/restate/confirm-style duplicates.
        """
        # Hydration restored history, so the notification may claim it.
        mock_cm._hydrated_history_count = 3
        await EventHandler.handle_event(InitializationComplete(), mock_cm)

        notif = next(
            n
            for n in mock_cm.notifications_bar.notifications
            if "Initialization complete" in n.content
        )
        assert notif.content == INITIALIZATION_COMPLETE_NOTIFICATION

        text = notif.content.lower()
        # Must keep the legitimate follow-up affordance.
        assert "deferred work" in text
        assert "incorrect or incomplete" in text
        # Must explicitly direct the brain to wait and forbid duplicates.
        assert "call wait" in text
        assert "do not send a message" in text
        assert "rephrases" in text or "restates" in text

    @pytest.mark.asyncio
    async def test_notification_is_truthful_when_nothing_was_restored(
        self,
        mock_cm,
    ):
        """A boot whose hydration restored nothing must not claim that
        history was loaded.

        Regression test for the cold-boot amnesia incident (2026-08-22):
        on a deployment without a persisted Comms stream, the rebooted CM
        announced "full conversation history has been loaded" over an
        empty thread render, and the brain went hunting elsewhere for
        context the notification told it already had.
        """
        await EventHandler.handle_event(InitializationComplete(), mock_cm)

        notif = next(
            n
            for n in mock_cm.notifications_bar.notifications
            if "Initialization complete" in n.content
        )
        assert notif.content == INITIALIZATION_COMPLETE_NO_HISTORY_NOTIFICATION

        text = notif.content.lower()
        assert "history has been loaded" not in text
        assert "no prior conversation history" in text
        # The anti-duplicate directive survives in both variants.
        assert "call wait" in text
        assert "do not send a message" in text
        assert "rephrases" in text or "restates" in text

    @pytest.mark.asyncio
    async def test_always_requests_post_init_brain_run(self, mock_cm):
        """The handler must always schedule a brain turn, even when the
        pre-init conversation already has a trailing assistant reply.

        The brain still needs the opportunity to follow up on deferred
        work (e.g. the user asked for something the brain couldn't do
        until managers were ready, replied with "I'll look into it",
        and now needs to actually do it). Suppressing the brain run
        here would silently drop those follow-ups, which is a worse
        failure mode than the duplicate reply we mitigate via the
        notification wording above.
        """

        mock_cm.contact_index.push_message(
            contact_id=1,
            sender_name="Boss",
            message_content="What meetings do I have today?",
            role="user",
        )
        mock_cm.contact_index.push_message(
            contact_id=1,
            sender_name="You",
            message_content="I'm still booting up — give me a moment.",
            role="assistant",
        )

        await EventHandler.handle_event(InitializationComplete(), mock_cm)

        mock_cm.request_llm_run.assert_called_once_with(delay=0)

    @pytest.mark.asyncio
    async def test_requests_brain_run_when_thread_is_empty(self, mock_cm):
        """No pre-init traffic — handler still schedules a brain turn so
        the brain can react to hydrated history."""

        await EventHandler.handle_event(InitializationComplete(), mock_cm)

        mock_cm.request_llm_run.assert_called_once_with(delay=0)


# =============================================================================
# 12. SyncContacts Handler Tests
# =============================================================================


class TestSyncContactsHandler:
    """Tests for SyncContacts event handler."""

    @pytest.mark.asyncio
    async def test_sync_contacts_logs_event(self, mock_cm):
        """SyncContacts logs the sync reason."""
        event = SyncContacts(reason="Manual refresh")

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        # Verify the handler logged the event
        mock_cm._session_logger.info.assert_any_call(
            "state_update",
            "SyncContacts: Manual refresh",
        )

    @pytest.mark.asyncio
    async def test_sync_contacts_queues_sync_operation(self, mock_cm):
        """SyncContacts queues the contact sync as a serialized operation."""
        event = SyncContacts(reason="Manual refresh")

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        queued = [
            call.args[0]
            for call in mock_utils.queue_operation.await_args_list
            if getattr(call.args[0], "__name__", "") == "_sync_contacts"
        ]
        assert len(queued) == 1


# =============================================================================
# 13. DirectMessageEvent Handler Tests
# =============================================================================


class TestDirectMessageEventHandler:
    """Tests for DirectMessageEvent handler."""

    @pytest.mark.asyncio
    async def test_direct_message_records_assistant_message_for_active_contact(
        self,
        mock_cm,
    ):
        """DirectMessageEvent records the message on the active contact's thread."""
        event = DirectMessageEvent(
            content="Direct message content",
            source="system",
        )

        await EventHandler.handle_event(event, mock_cm)

        msgs = mock_cm.contact_index.get_messages_for_contact(1)
        assert len(msgs) == 1
        assert msgs[0].content == "Direct message content"
        assert msgs[0].role == "assistant"
        mock_cm.request_llm_run.assert_not_called()

    @pytest.mark.asyncio
    async def test_direct_message_falls_back_to_boss_contact(self, mock_cm):
        """Without an active contact the message goes to the boss thread."""
        mock_cm.get_active_contact = MagicMock(return_value=None)
        event = DirectMessageEvent(content="Hello boss", source="handle")

        await EventHandler.handle_event(event, mock_cm)

        msgs = mock_cm.contact_index.get_messages_for_contact(
            SESSION_DETAILS.boss_contact_id,
        )
        assert len(msgs) == 1
        assert msgs[0].content == "Hello boss"


# =============================================================================
# 14. Edge Cases and Error Handling Tests
# =============================================================================


class TestEventHandlerEdgeCases:
    """Tests for edge cases and error handling in event handlers."""

    @pytest.mark.asyncio
    async def test_actor_clarification_for_nonexistent_action(self, mock_cm):
        """ActorClarificationRequest for non-existent action does nothing."""
        mock_cm.in_flight_actions = {}  # No actions
        event = ActorClarificationRequest(
            handle_id=999,  # Non-existent
            query="Question?",
            call_id="call_123",
        )

        # Should not raise
        await EventHandler.handle_event(event, mock_cm)

        # No notifications should be pushed
        assert len(mock_cm.notifications_bar.notifications) == 0
        mock_cm.request_llm_run.assert_not_called()

    @pytest.mark.asyncio
    async def test_actor_handle_response_for_nonexistent_action(self, mock_cm):
        """ActorHandleResponse for an untracked handle is a no-op."""
        event = ActorHandleResponse(
            handle_id=999,
            action_name="ask",
            query="status?",
            response="n/a",
            call_id="",
        )

        await EventHandler.handle_event(event, mock_cm)

        mock_cm.request_llm_run.assert_not_called()

    @pytest.mark.asyncio
    async def test_unify_message_without_contact_id_still_notifies(self, mock_cm):
        """A message whose contact carries no id is surfaced without a thread entry."""
        event = UnifyMessageReceived(
            contact={"first_name": "Anon"},
            content="Hello?",
        )

        with patch(
            "unify.conversation_manager.domains.event_handlers.managers_utils",
        ) as mock_utils:
            mock_utils.queue_operation = AsyncMock()
            await EventHandler.handle_event(event, mock_cm)

        assert len(mock_cm.contact_index.global_thread) == 0
        assert (
            mock_cm.notifications_bar.notifications[0].content
            == "Unify message from Anon"
        )
        mock_cm.request_llm_run.assert_called_once_with(triggering_contact_id=None)


# =============================================================================
# 15. OpenSlowBrainTurn Event Tests
# =============================================================================


class TestOpenSlowBrainTurnEvent:
    """Tests for the OpenSlowBrainTurn event."""

    def test_open_slow_brain_turn_is_registered(self):
        """OpenSlowBrainTurn should have a handler in the registry."""
        assert OpenSlowBrainTurn in EventHandler._registry

    @pytest.mark.asyncio
    async def test_open_slow_brain_turn_pushes_notification(self, mock_cm):
        """OpenSlowBrainTurn handler should push the follow-on notification."""
        event = OpenSlowBrainTurn(
            origin_run_id="llmrun-000001",
            previous_tools=["act"],
        )
        await EventHandler.handle_event(event, mock_cm)

        notifications = mock_cm.notifications_bar.notifications
        assert any(
            OPEN_SLOW_BRAIN_TURN_NOTIFICATION in n.content for n in notifications
        )

    @pytest.mark.asyncio
    async def test_open_slow_brain_turn_triggers_llm_run(self, mock_cm):
        """OpenSlowBrainTurn handler should trigger an LLM run."""
        event = OpenSlowBrainTurn(
            origin_run_id="llmrun-000001",
            previous_tools=["act"],
        )
        await EventHandler.handle_event(event, mock_cm)

        mock_cm.request_llm_run.assert_called_once_with(delay=0)
