"""
tests/conversation_manager/test_utils.py
=============================================

Unit tests for ConversationManager utility functions and helpers.

These are pure unit tests that don't require LLM calls or async infrastructure.
They test the building blocks used by the conversation manager.
"""

from datetime import timedelta

import pytest

from unify.conversation_manager.task_actions import (
    STEERING_OPERATIONS,
    derive_short_name,
    iter_steering_tools_for_action,
    iter_steering_tools_for_completed_action,
    safe_call_id_suffix,
)
from unify.conversation_manager.domains.notifications import (
    Notification,
    NotificationBar,
)
from unify.conversation_manager.domains.renderer import Renderer
from unify.conversation_manager.domains.contact_index import (
    ConversationState,
    ContactIndex,
    Message,
    EmailMessage,
)
from unify.conversation_manager.cm_types import Medium

# Alias for backward compatibility with tests
Contact = ConversationState


# =============================================================================
# task_actions.py tests
# =============================================================================


class TestDeriveShortName:
    """Tests for derive_short_name function."""

    def test_basic_query(self):
        """Simple query extracts first 4 words."""
        result = derive_short_name("List all contacts in the database")
        assert result == "list_all_contacts_in"

    def test_removes_punctuation(self):
        """Punctuation is stripped from the query."""
        result = derive_short_name("What's the weather?")
        assert result == "whats_the_weather"

    def test_lowercases_words(self):
        """All words are lowercased."""
        # Use a shorter query that won't hit the 25-char truncation limit
        result = derive_short_name("Find IMPORTANT Docs")
        assert result == "find_important_docs"

    def test_max_words_limit(self):
        """Respects max_words parameter."""
        result = derive_short_name("one two three four five six", max_words=2)
        assert result == "one_two"

    def test_empty_query(self):
        """Empty query returns 'task'."""
        result = derive_short_name("")
        assert result == "task"

    def test_only_punctuation(self):
        """Query with only punctuation returns 'task'."""
        result = derive_short_name("!@#$%^&*()")
        assert result == "task"

    def test_collapses_double_underscores(self):
        """Double underscores in result are collapsed to single."""
        # This could happen if a word boundary creates __ somehow
        # The function sanitizes this
        result = derive_short_name("test  query")  # double space
        assert "__" not in result

    def test_numbers_preserved(self):
        """Numbers in query are preserved."""
        result = derive_short_name("Send 100 emails to 50 people")
        assert result == "send_100_emails_to"

    def test_slashes_become_word_separators(self):
        """Slashes and other punctuation become word separators, not removed."""
        # Each slash-separated segment is a separate word: without this,
        # "docs/files/data" would collapse into one long unreadable word.
        # A short example keeps the result under the 25-char truncation limit.
        result = derive_short_name("Get docs/files/data")
        assert result == "get_docs_files_data"

    def test_character_limit_enforced(self):
        """Short name is truncated to stay under character limit."""
        # 25 chars max keeps pane labels compact
        long_query = "superlongwordthatexceedstwentyfivecharacters easily"
        result = derive_short_name(long_query)
        assert len(result) <= 25

    def test_character_limit_with_long_words(self):
        """Even queries with long words stay under the limit."""
        # Each word is long - tests truncation of the joined result
        query = "internationalization documentation implementation"
        result = derive_short_name(query)
        assert len(result) <= 25

    def test_truncation_removes_trailing_underscore(self):
        """Truncation doesn't leave trailing underscores."""
        # If truncation happens right after an underscore, it should be stripped
        query = "a b c d e f g h i j k l m n o p q r s t u v w x y z"
        result = derive_short_name(query)
        assert not result.endswith("_")
        assert len(result) <= 25


class TestSafeCallIdSuffix:
    """Tests for safe_call_id_suffix function."""

    def test_empty_call_id(self):
        """Empty call_id returns '0'."""
        assert safe_call_id_suffix("") == "0"

    def test_none_call_id(self):
        """None call_id returns '0'."""
        assert safe_call_id_suffix(None) == "0"

    def test_replaces_dashes(self):
        """Dashes are replaced with underscores."""
        result = safe_call_id_suffix("abc-def-ghi")
        assert "-" not in result
        assert "_" in result

    def test_takes_last_8_chars(self):
        """Only last 8 characters are returned."""
        result = safe_call_id_suffix("0123456789abcdef")
        assert result == "89abcdef"
        assert len(result) == 8

    def test_short_call_id(self):
        """Short call_id is returned as-is (with dash replacement)."""
        result = safe_call_id_suffix("abc")
        assert result == "abc"

    def test_collapses_double_underscores(self):
        """Double underscores are collapsed."""
        result = safe_call_id_suffix("test--id")
        assert "__" not in result


class TestIterSteeringToolsForAction:
    """Tests for iter_steering_tools_for_action function."""

    def test_basic_actions(self):
        """Generates standard steering tools for an action (backward compatible)."""
        # With is_paused=None (default), both pause and resume are included
        actions = iter_steering_tools_for_action(0, "List contacts")
        action_names = [a[0] for a in actions]

        assert any("ask_" in n for n in action_names)
        assert any("stop_" in n for n in action_names)
        assert any("interject_" in n for n in action_names)
        assert any("pause_" in n for n in action_names)
        assert any("resume_" in n for n in action_names)

    def test_is_paused_true_only_shows_resume(self):
        """When is_paused=True, only resume is shown (not pause)."""
        actions = iter_steering_tools_for_action(
            0,
            "List contacts",
            is_paused=True,
        )
        action_names = [a[0] for a in actions]

        assert any("resume_" in n for n in action_names)
        assert not any("pause_" in n for n in action_names)
        # Other steering tools should still be present
        assert any("ask_" in n for n in action_names)
        assert any("stop_" in n for n in action_names)

    def test_is_paused_false_only_shows_pause(self):
        """When is_paused=False, only pause is shown (not resume)."""
        actions = iter_steering_tools_for_action(
            0,
            "List contacts",
            is_paused=False,
        )
        action_names = [a[0] for a in actions]

        assert any("pause_" in n for n in action_names)
        assert not any("resume_" in n for n in action_names)
        # Other steering tools should still be present
        assert any("ask_" in n for n in action_names)
        assert any("stop_" in n for n in action_names)

    def test_no_answer_clarification_without_pending(self):
        """No answer_clarification without pending clarifications."""
        actions = iter_steering_tools_for_action(
            0,
            "Action",
            pending_clarifications=None,
        )
        action_names = [a[0] for a in actions]

        assert not any("answer_clarification" in n for n in action_names)

    def test_answer_clarification_with_pending(self):
        """Generates answer_clarification for pending clarifications."""
        pending = [{"call_id": "test_call_123"}]
        actions = iter_steering_tools_for_action(
            0,
            "Action",
            pending_clarifications=pending,
        )
        action_names = [a[0] for a in actions]

        assert any("answer_clarification" in n for n in action_names)

    def test_actions_have_descriptions(self):
        """All steering tools have non-empty descriptions."""
        actions = iter_steering_tools_for_action(0, "Test action")
        for name, description in actions:
            assert len(description) > 0, f"{name} should have a description"


class TestIterSteeringToolsForCompletedAction:
    """Tests for iter_steering_tools_for_completed_action function."""

    def test_includes_ask_only(self):
        """Completed action tools include ask but not close."""
        actions = iter_steering_tools_for_completed_action(0, "Find contacts")
        action_names = [a[0] for a in actions]

        assert any("ask_" in n for n in action_names)
        assert not any("close_" in n for n in action_names)

    def test_excludes_in_flight_only_tools(self):
        """Completed action tools exclude stop, pause, resume, interject."""
        actions = iter_steering_tools_for_completed_action(0, "Find contacts")
        action_names = [a[0] for a in actions]

        assert not any("stop_" in n for n in action_names)
        assert not any("pause_" in n for n in action_names)
        assert not any("resume_" in n for n in action_names)
        assert not any("interject_" in n for n in action_names)

    def test_all_have_descriptions(self):
        """All completed action tools have non-empty descriptions."""
        actions = iter_steering_tools_for_completed_action(0, "Test action")
        for name, description in actions:
            assert len(description) > 0, f"{name} should have a description"


class TestSteeringOperationDocstring:
    """Tests for SteeringOperation.get_docstring method."""

    def test_operations_have_docstrings(self):
        """All operations have docstrings from SteerableToolHandle."""
        for op in STEERING_OPERATIONS:
            # Some might have empty docstrings if method doesn't exist
            # but the method should not raise
            docstring = op.get_docstring()
            assert isinstance(docstring, str)


# =============================================================================
# domains/notifications.py tests
# =============================================================================


class TestNotification:
    """Tests for Notification dataclass."""

    def test_basic_notification(self, static_now):
        """Creates notification with required fields."""
        ts = static_now
        n = Notification(type="Test", content="Hello", timestamp=ts)
        assert n.type == "Test"
        assert n.content == "Hello"
        assert n.timestamp == ts
        assert n.pinned is False
        assert n.interjection_id is None

    def test_pinned_notification(self, static_now):
        """Creates pinned notification."""
        ts = static_now
        n = Notification(
            type="Alert",
            content="Important",
            timestamp=ts,
            pinned=True,
            interjection_id="inj_123",
        )
        assert n.pinned is True
        assert n.interjection_id == "inj_123"


class TestNotificationBar:
    """Tests for NotificationBar class."""

    def test_empty_bar(self):
        """New bar starts empty."""
        bar = NotificationBar()
        assert len(bar.notifications) == 0

    def test_push_notif(self, static_now):
        """Can push notifications."""
        bar = NotificationBar()
        ts = static_now
        bar.push_notif("Comms", "SMS received", ts)
        assert len(bar.notifications) == 1
        assert bar.notifications[0].type == "Comms"
        assert bar.notifications[0].content == "SMS received"

    def test_push_multiple(self, static_now):
        """Can push multiple notifications."""
        bar = NotificationBar()
        ts = static_now
        bar.push_notif("Type1", "Content1", ts)
        bar.push_notif("Type2", "Content2", ts)
        bar.push_notif("Type3", "Content3", ts)
        assert len(bar.notifications) == 3

    def test_push_pinned(self, static_now):
        """Can push pinned notifications."""
        bar = NotificationBar()
        ts = static_now
        bar.push_notif("Alert", "Urgent", ts, pinned=True, id="alert_1")
        assert bar.notifications[0].pinned is True
        assert bar.notifications[0].interjection_id == "alert_1"


# =============================================================================
# domains/renderer.py tests
# =============================================================================


class TestRenderer:
    """Tests for Renderer class."""

    @pytest.fixture
    def renderer(self):
        return Renderer()

    @pytest.fixture
    def sample_contact_info(self):
        """Contact info dict (from ContactManager)."""
        return {
            "contact_id": 1,
            "first_name": "John",
            "surname": "Doe",
            "phone_number": "+15551234567",
            "email_address": "john@example.com",
            "timezone": "America/New_York",
            "bio": "Test bio",
            "rolling_summary": "Test summary",
            "response_policy": "Be polite",
        }

    @pytest.fixture
    def sample_conv_state(self):
        """Conversation state (from ContactIndex)."""
        return ConversationState(contact_id=1, on_call=False)

    def test_render_in_flight_actions_empty(self, renderer):
        """Renders empty in-flight actions."""
        result = renderer.render_in_flight_actions({})
        assert "No actions currently executing" in result
        assert "<in_flight_actions>" in result
        assert "</in_flight_actions>" in result

    def test_render_in_flight_actions_with_action(self, renderer):
        """Renders in-flight actions with action data."""
        actions = {
            0: {
                "query": "List all contacts",
                "handle_actions": [],
            },
        }
        result = renderer.render_in_flight_actions(actions)
        assert "<action id='0'" in result
        assert "status='executing'" in result
        assert "List all contacts" in result
        assert "ask_" in result
        assert "stop_" in result

    def test_render_in_flight_actions_shows_paused_status(self, renderer):
        """Renders paused actions with status='paused'."""
        from unittest.mock import MagicMock

        # Create a paused handle
        mock_handle = MagicMock()
        mock_handle._pause_event = MagicMock()
        mock_handle._pause_event.is_set.return_value = False  # Paused

        actions = {
            0: {
                "query": "Paused action",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }
        result = renderer.render_in_flight_actions(actions)
        assert "status='paused'" in result
        assert "resume_" in result  # Should show resume tool
        assert "pause_" not in result  # Should NOT show pause tool

    def test_render_in_flight_actions_shows_executing_status(self, renderer):
        """Renders running actions with status='executing'."""
        from unittest.mock import MagicMock

        # Create a running handle
        mock_handle = MagicMock()
        mock_handle._pause_event = MagicMock()
        mock_handle._pause_event.is_set.return_value = True  # Running

        actions = {
            0: {
                "query": "Running action",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }
        result = renderer.render_in_flight_actions(actions)
        assert "status='executing'" in result
        assert "pause_" in result  # Should show pause tool
        assert "resume_" not in result  # Should NOT show resume tool

    def test_render_in_flight_actions_with_clarification(self, renderer):
        """Renders actions with pending clarifications."""
        actions = {
            0: {
                "query": "Do something",
                "handle_actions": [
                    {
                        "action_name": "clarification_request",
                        "query": "Need more info?",
                        "call_id": "call_123",
                    },
                ],
            },
        }
        result = renderer.render_in_flight_actions(actions)
        assert "answer_clarification" in result
        assert "clarification_request" in result
        assert "Need more info?" in result

    def test_render_notification_bar_empty(self, renderer, static_now):
        """Renders empty notification bar."""
        bar = NotificationBar()
        last_snapshot = static_now - timedelta(hours=1)
        result = renderer.render_notification_bar(bar, last_snapshot)
        assert "<notifications>" in result
        assert "</notifications>" in result

    def test_render_notification_bar_with_notifications(self, renderer, static_now):
        """Renders notification bar with notifications."""
        bar = NotificationBar()
        ts = static_now
        bar.push_notif("Comms", "SMS received from John", ts)
        last_snapshot = static_now - timedelta(seconds=10)
        result = renderer.render_notification_bar(bar, last_snapshot)
        assert "SMS received from John" in result

    def test_render_message(self, renderer, static_now):
        """Renders a message with NEW tag for new messages."""
        old_snapshot = static_now - timedelta(hours=1)
        msg = Message(
            name="John",
            content="Hello!",
            timestamp=static_now,
            role="user",
        )
        result = renderer.render_message(msg, old_snapshot)
        assert "**NEW**" in result
        assert "John" in result
        assert "Hello!" in result

    def test_render_message_not_new(self, renderer, static_now):
        """Old messages don't have NEW tag."""
        recent_snapshot = static_now + timedelta(seconds=10)
        msg = Message(
            name="John",
            content="Hello!",
            timestamp=static_now,
            role="user",
        )
        result = renderer.render_message(msg, recent_snapshot)
        assert "**NEW**" not in result

    def test_render_email_message(self, renderer, static_now):
        """Renders email message with subject and body."""
        old_snapshot = static_now - timedelta(hours=1)
        msg = EmailMessage(
            name="Jane",
            subject="Important Update",
            body="Please review the attached document.",
            email_id="email_456",
            thread_id="gmail-thread-456",
            timestamp=static_now,
            role="user",
        )
        result = renderer.render_message(msg, old_snapshot)
        assert "Subject: Important Update" in result
        assert "Email ID: email_456" in result
        assert "Thread ID: gmail-thread-456" in result
        assert "Please review" in result

    def test_render_contact(
        self,
        renderer,
        sample_contact_info,
        sample_conv_state,
        static_now,
    ):
        """Renders contact with all details."""
        last_snapshot = static_now
        result = renderer.render_contact(
            contact_info=sample_contact_info,
            conv_state=sample_conv_state,
            last_snapshot=last_snapshot,
        )
        assert 'contact_id="1"' in result
        assert 'first_name="John"' in result
        assert 'surname="Doe"' in result
        assert 'is_boss="True"' in result
        assert 'timezone="America/New_York"' in result
        assert "<bio>Test bio</bio>" in result

    def test_global_thread_rendered_before_per_medium_threads(
        self,
        renderer,
        static_now,
    ):
        """Global thread appears before per-medium threads in rendered output."""
        from unify.conversation_manager.domains.contact_index import GlobalThreadEntry

        contact_info = {
            "contact_id": 1,
            "first_name": "John",
            "surname": "Doe",
            "bio": "Test bio",
            "rolling_summary": "Test summary",
            "response_policy": "Be polite",
        }
        conv_state = ConversationState(contact_id=1)
        ts = static_now
        msg = Message(name="John", content="Hello!", timestamp=ts, role="user")
        entries = [
            GlobalThreadEntry(
                message=msg,
                medium=Medium.SMS_MESSAGE,
                contact_roles={1: None},
            ),
        ]

        last_snapshot = static_now - timedelta(hours=1)
        result = renderer.render_contact(
            contact_info=contact_info,
            conv_state=conv_state,
            entries=entries,
            last_snapshot=last_snapshot,
        )

        # Global thread should appear before sms thread
        global_pos = result.find("<global>")
        sms_pos = result.find("<sms_message>")
        assert global_pos != -1, "Global thread should be rendered"
        assert sms_pos != -1, "SMS_MESSAGE thread should be rendered"
        assert (
            global_pos < sms_pos
        ), "Global thread should appear before per-medium threads"

    def test_global_thread_shows_more_messages_than_per_medium(
        self,
        renderer,
        static_now,
    ):
        """Global thread renders all messages while per-medium caps at max_contact_medium_messages."""
        from unify.conversation_manager.domains.contact_index import GlobalThreadEntry

        contact_info = {
            "contact_id": 1,
            "first_name": "John",
            "surname": "Doe",
            "bio": "Test bio",
            "rolling_summary": "Test summary",
            "response_policy": "Be polite",
        }
        conv_state = ConversationState(contact_id=1)
        base_time = static_now
        entries = []
        for i in range(30):
            ts = base_time + timedelta(minutes=i)
            msg = Message(
                name="John",
                content=f"msg_idx_{i:03d}",
                timestamp=ts,
                role="user",
            )
            entries.append(
                GlobalThreadEntry(
                    message=msg,
                    medium=Medium.SMS_MESSAGE,
                    contact_roles={1: None},
                ),
            )

        last_snapshot = static_now - timedelta(hours=1)
        result = renderer.render_contact(
            contact_info=contact_info,
            conv_state=conv_state,
            entries=entries,
            max_contact_medium_messages=25,
            last_snapshot=last_snapshot,
        )

        # Global thread should have all 30 messages
        global_section = result[result.find("<global>") : result.find("</global>")]
        for i in range(30):
            assert (
                f"msg_idx_{i:03d}" in global_section
            ), f"msg_idx_{i:03d} should be in global"

        # Per-medium SMS capped at 25 — only messages 5-29 appear
        sms_section = result[
            result.find("<sms_message>") : result.find("</sms_message>")
        ]
        for i in range(5):
            assert (
                f"msg_idx_{i:03d}" not in sms_section
            ), f"msg_idx_{i:03d} should NOT be in sms_message (capped)"
        for i in range(5, 30):
            assert (
                f"msg_idx_{i:03d}" in sms_section
            ), f"msg_idx_{i:03d} should be in sms_message"

    def test_empty_entries_no_global_thread_rendered(
        self,
        renderer,
        sample_contact_info,
        sample_conv_state,
        static_now,
    ):
        """No entries means no global thread rendered."""
        last_snapshot = static_now
        result = renderer.render_contact(
            contact_info=sample_contact_info,
            conv_state=sample_conv_state,
            entries=[],
            last_snapshot=last_snapshot,
        )
        assert "<global>" not in result


# =============================================================================
# domains/contact_index.py tests
# =============================================================================


class TestContactIndex:
    """Tests for ContactIndex class and global thread functionality."""

    @pytest.fixture
    def contact_index(self):
        return ContactIndex()

    @pytest.fixture
    def sample_contact_dict(self):
        return {
            "contact_id": 1,
            "first_name": "John",
            "surname": "Doe",
            "phone_number": "+15551234567",
            "email_address": "john@example.com",
            "bio": "Test bio",
            "rolling_summary": "Test summary",
            "response_policy": "Be polite",
        }

    def test_push_message_adds_to_global_thread(
        self,
        contact_index,
        sample_contact_dict,
    ):
        """push_message adds message to the shared global thread."""
        contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.SMS_MESSAGE,
            message_content="Hello!",
        )

        assert len(contact_index.global_thread) == 1
        assert contact_index.global_thread[0].message.content == "Hello!"
        # Also accessible via helper
        msgs = contact_index.get_messages_for_contact(1, Medium.SMS_MESSAGE)
        assert len(msgs) == 1
        assert msgs[0].content == "Hello!"

    def test_global_thread_aggregates_all_mediums(
        self,
        contact_index,
        sample_contact_dict,
    ):
        """Global thread contains messages from all mediums."""
        contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.SMS_MESSAGE,
            message_content="SMS message",
        )
        contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.PHONE_CALL,
            message_content="Voice message",
        )
        contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.EMAIL,
            subject="Email subject",
            body="Email body",
        )

        # Per-medium views each have 1 message
        assert len(contact_index.get_messages_for_contact(1, Medium.SMS_MESSAGE)) == 1
        assert len(contact_index.get_messages_for_contact(1, Medium.PHONE_CALL)) == 1
        assert len(contact_index.get_messages_for_contact(1, Medium.EMAIL)) == 1

        # Global thread has all 3
        assert len(contact_index.global_thread) == 3

    def test_global_thread_maxlen_is_100(self, contact_index, sample_contact_dict):
        """Global thread has maxlen of 100."""
        # Push 120 messages
        for i in range(120):
            contact_index.push_message(
                contact_id=sample_contact_dict["contact_id"],
                sender_name="Test",
                thread_name=Medium.SMS_MESSAGE,
                message_content=f"Message {i}",
            )

        # Global thread capped at 100
        assert len(contact_index.global_thread) == 100
        # First 20 messages dropped
        assert contact_index.global_thread[0].message.content == "Message 20"

    def test_global_thread_preserves_chronological_order(
        self,
        contact_index,
        sample_contact_dict,
        static_now,
    ):
        """Messages in global thread maintain chronological order across mediums."""
        base_time = static_now

        # Interleave messages from different mediums
        contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.SMS_MESSAGE,
            message_content="SMS 1",
            timestamp=base_time,
        )
        contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.EMAIL,
            subject="Email 1",
            body="Body 1",
            timestamp=base_time + timedelta(minutes=1),
        )
        contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.PHONE_CALL,
            message_content="Voice 1",
            timestamp=base_time + timedelta(minutes=2),
        )
        contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.SMS_MESSAGE,
            message_content="SMS 2",
            timestamp=base_time + timedelta(minutes=3),
        )

        # Verify chronological order in global thread
        assert len(contact_index.global_thread) == 4
        assert contact_index.global_thread[0].message.content == "SMS 1"
        # Email messages store content differently
        assert contact_index.global_thread[1].message.subject == "Email 1"
        assert contact_index.global_thread[2].message.content == "Voice 1"
        assert contact_index.global_thread[3].message.content == "SMS 2"

    def test_push_message_returns_message_id(
        self,
        contact_index,
        sample_contact_dict,
    ):
        """push_message returns a monotonically increasing message_id for Message types."""
        id1 = contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.SMS_MESSAGE,
            message_content="First",
        )
        id2 = contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.PHONE_CALL,
            message_content="Second",
        )

        assert id1 == 1
        assert id2 == 2
        assert contact_index.global_thread[0].message.local_message_id == 1
        assert contact_index.global_thread[1].message.local_message_id == 2

    def test_push_message_returns_zero_for_non_message_types(
        self,
        contact_index,
        sample_contact_dict,
    ):
        """push_message returns 0 for EmailMessage and other non-Message types."""
        mid = contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.EMAIL,
            subject="Test email",
            body="Body",
        )
        assert mid == 0

    def test_message_screenshots_field_defaults_empty(
        self,
        contact_index,
        sample_contact_dict,
    ):
        """Message.screenshots defaults to an empty list."""
        contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.PHONE_CALL,
            message_content="Hello",
        )
        msg = contact_index.global_thread[0].message
        assert msg.screenshots == []

    def test_message_screenshots_mutable_after_creation(
        self,
        contact_index,
        sample_contact_dict,
    ):
        """Screenshot paths can be attached to a Message after it is created."""
        contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.PHONE_CALL,
            message_content="Click the button",
        )
        msg = contact_index.global_thread[0].message
        msg.screenshots.extend(
            [
                "Screenshots/User/2025-06-13T12-00-00.jpg",
                "Screenshots/Assistant/2025-06-13T12-00-01.jpg",
            ],
        )
        assert len(msg.screenshots) == 2
        assert "Screenshots/User/" in msg.screenshots[0]
        assert "Screenshots/Assistant/" in msg.screenshots[1]

    def test_message_image_ids_field_defaults_empty(
        self,
        contact_index,
        sample_contact_dict,
    ):
        """Message.image_ids defaults to an empty list."""
        contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.PHONE_CALL,
            message_content="Hello",
        )
        msg = contact_index.global_thread[0].message
        assert msg.image_ids == []

    def test_message_image_ids_mutable_after_creation(
        self,
        contact_index,
        sample_contact_dict,
    ):
        """Image IDs can be attached to a Message after it is created."""
        contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.PHONE_CALL,
            message_content="Click the button",
        )
        msg = contact_index.global_thread[0].message
        msg.image_ids.extend([101, 202])
        assert msg.image_ids == [101, 202]

    def test_local_to_global_message_id_mapping(
        self,
        contact_index,
        sample_contact_dict,
    ):
        """The _local_to_global_message_ids mapping can store and retrieve local->global links."""
        mapping: dict[int, int] = {}
        mid = contact_index.push_message(
            contact_id=sample_contact_dict["contact_id"],
            sender_name="Test",
            thread_name=Medium.PHONE_CALL,
            message_content="Hello",
        )
        mapping[mid] = 9999
        assert mapping[mid] == 9999
        assert mapping.get(mid + 1) is None


# =============================================================================
# Steering-invocation rendering tests
# =============================================================================


class TestSteeringInvocationRendering:
    """The panes render ready-to-use invocations of the fixed steering tools."""

    def test_invocations_carry_handle_id(self):
        """Every rendered invocation addresses the fixed tool by handle_id."""
        pending = [{"call_id": "clarification-uuid-12345"}]
        actions = iter_steering_tools_for_action(
            handle_id=3,
            query="Do something important",
            pending_clarifications=pending,
        )

        assert actions
        for action_name, _ in actions:
            assert "_action(handle_id=3" in action_name

    def test_clarification_invocation_carries_full_call_id(self):
        """The answer_clarification invocation embeds the clarification's
        call id, which no longer travels inside a tool name."""
        pending = [{"call_id": "clarification-uuid-12345"}]
        actions = iter_steering_tools_for_action(
            handle_id=3,
            query="Do something important",
            pending_clarifications=pending,
        )

        clarification_invocations = [
            name for name, _ in actions if name.startswith("answer_clarification")
        ]
        assert clarification_invocations == [
            "answer_clarification_action(handle_id=3, "
            "call_id='clarification-uuid-12345', ...)",
        ]

    def test_completed_invocation_is_ask_action(self):
        """A completed action renders one ask_action invocation."""
        actions = iter_steering_tools_for_completed_action(0, "Find contacts")
        assert [name for name, _ in actions] == ["ask_action(handle_id=0, ...)"]


# =============================================================================
# comms control-message code quality tests
# =============================================================================


class TestCommsPostCodeQuality:
    """Regression tests for the runtime's fire-and-forget calls into comms.

    These tests inspect the source code to prevent accidental regressions.
    """

    def test_uses_requests_post_not_http_post(self):
        """The comms POST helper must use requests.post directly, not http.post.

        The http module from unisdk.utils has retry logic baked in. For these
        fire-and-forget control messages we intentionally want NO retries - the
        timeout is expected and we should move on immediately. Using http.post
        would retry with backoff, which for agent dispatch means dispatching
        several agents into one room.
        """
        import inspect
        from unify.conversation_manager.utils import _post_to_comms

        source = inspect.getsource(_post_to_comms)

        assert "requests.post(" in source, (
            "_post_to_comms must use requests.post() directly, not http.post(). "
            "The http module has retry logic that would dispatch multiple "
            "agents due to the expected timeout - we want fire-and-forget."
        )
        assert "http.post(" not in source

    def test_public_helpers_route_through_the_shared_post(self):
        """Both comms calls must share one transport, so retry policy is one place."""
        import inspect
        from unify.conversation_manager.utils import (
            dispatch_livekit_agent,
            start_call_recording,
        )

        for func in (dispatch_livekit_agent, start_call_recording):
            source = inspect.getsource(func)
            assert "_post_to_comms(" in source, (
                f"{func.__name__} must POST via _post_to_comms so it inherits "
                "the no-retry, failure-swallowing transport."
            )
            assert "requests.post(" not in source

    def test_dispatch_does_not_request_recording(self):
        """Recording must not ride on agent dispatch.

        Starting egress at dispatch time races the participant join, and LiveKit
        aborts such a job without writing a file. Recording is requested from the
        call-started path instead.
        """
        import inspect
        from unify.conversation_manager.utils import dispatch_livekit_agent

        source = inspect.getsource(dispatch_livekit_agent)
        # No record flag in the request payload, and no egress endpoint reached.
        assert '"record"' not in source
        assert "record=" not in source
        assert "start-recording" not in source
        assert "/phone/dispatch-livekit-agent" in source

    def test_start_call_recording_targets_the_recording_endpoint(self):
        import inspect
        from unify.conversation_manager.utils import start_call_recording

        source = inspect.getsource(start_call_recording)
        assert "/phone/start-recording" in source
