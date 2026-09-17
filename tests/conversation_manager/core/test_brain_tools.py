"""
tests/conversation_manager/test_brain_tools.py
====================================================

Unit tests for ConversationManager brain tools.

Tests cover:
- ConversationManagerBrainTools (read-only inspection tools)
- ConversationManagerBrainActionTools (side-effecting action tools)

These tests verify the tool implementations directly, testing:
- Tool method signatures and return types
- Tool docstrings (important for LLM understanding)
- Fixed action steering tools addressed by handle_id
- Integration with ConversationManager state
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unify.contact_manager.simulated import SimulatedContactManager
from unify.conversation_manager.domains.brain_tools import (
    ConversationManagerBrainTools,
)
from unify.conversation_manager.domains.brain_action_tools import (
    ConversationManagerBrainActionTools,
    slow_brain_direct_outbound_active,
)
from unify.conversation_manager.events import UnifyMessageSent
from unify.file_manager.filesystem_adapters.local_adapter import (
    LocalFileSystemAdapter,
)
from unify.conversation_manager.domains.notifications import (
    NotificationBar,
)
from unify.conversation_manager.domains.contact_index import (
    ContactIndex,
)
from unify.session_details import SESSION_DETAILS

# =============================================================================
# Fixtures
# =============================================================================


def _setup_mock_contacts(
    contact_index,
    contacts: list[dict],
) -> SimulatedContactManager:
    """
    Set up a SimulatedContactManager with the given contacts on a ContactIndex.

    Returns the SimulatedContactManager for additional inspection if needed.
    """
    contact_manager = SimulatedContactManager()

    # Populate contacts - update system contacts (0, 1) and create others
    for contact_data in contacts:
        contact_id = contact_data["contact_id"]
        contact_manager.update_contact(
            contact_id=contact_id,
            first_name=contact_data.get("first_name"),
            surname=contact_data.get("surname"),
            email_address=contact_data.get("email_address"),
            phone_number=contact_data.get("phone_number"),
            should_respond=contact_data.get("should_respond", True),
        )

    contact_index.set_contact_manager(contact_manager)
    return contact_manager


def _published_sent_events(brain_action_tools) -> list[dict]:
    """Decode every UnifyMessageSent payload published through the broker."""
    return [
        json.loads(call.args[1])["payload"]
        for call in brain_action_tools._event_broker.publish.await_args_list
        if call.args[0] == UnifyMessageSent.topic
    ]


@pytest.fixture
def mock_cm():
    """Create a minimal mock ConversationManager for testing."""
    cm = MagicMock()
    cm.contact_index = ContactIndex()
    cm.in_flight_actions = {}
    cm.completed_actions = {}
    cm.notifications_bar = NotificationBar()
    cm.chat_history = []
    cm.initialized = True
    cm.event_broker.publish = AsyncMock()
    cm._pending_steering_tasks = set()
    cm._current_state_snapshot = None
    cm._current_snapshot_state = None
    # Set up SimulatedContactManager (starts with system contacts 0 and 1)
    cm.contact_manager = _setup_mock_contacts(cm.contact_index, [])
    return cm


@pytest.fixture
def brain_tools(mock_cm):
    """Create ConversationManagerBrainTools instance."""
    return ConversationManagerBrainTools(mock_cm)


@pytest.fixture
def brain_action_tools(mock_cm):
    """Create ConversationManagerBrainActionTools instance."""
    return ConversationManagerBrainActionTools(mock_cm)


@pytest.fixture
def sample_contacts():
    """Sample contacts for testing."""
    return [
        {
            "contact_id": 1,
            "first_name": "Alice",
            "surname": "Smith",
            "phone_number": "+15551111111",
            "email_address": "alice@example.com",
            "should_respond": True,
        },
        {
            "contact_id": 2,
            "first_name": "Bob",
            "surname": "Johnson",
            "phone_number": "+15552222222",
            "email_address": "bob@example.com",
            "should_respond": True,
        },
    ]


# =============================================================================
# ConversationManagerBrainTools Tests
# =============================================================================


class TestCmGetContact:
    """Tests for cm_get_contact tool."""

    def test_returns_contact_by_id(self, brain_tools, mock_cm, sample_contacts):
        """Returns contact when found by ID."""
        _setup_mock_contacts(mock_cm.contact_index, sample_contacts)
        result = brain_tools.cm_get_contact(1)
        assert result is not None
        assert result["contact_id"] == 1
        assert result["first_name"] == "Alice"

    def test_returns_none_for_unknown_id(self, brain_tools, mock_cm, sample_contacts):
        """Returns None when contact not found."""
        _setup_mock_contacts(mock_cm.contact_index, sample_contacts)
        result = brain_tools.cm_get_contact(999)
        assert result is None

    def test_excludes_threads_from_contact(self, brain_tools, mock_cm, sample_contacts):
        """Contact summary excludes thread data for efficiency."""
        _setup_mock_contacts(mock_cm.contact_index, sample_contacts)
        result = brain_tools.cm_get_contact(1)
        # get_contact uses model_dump(exclude={"threads", "global_thread"})
        assert "threads" not in result
        assert "global_thread" not in result


class TestCmListInFlightActions:
    """Tests for cm_list_in_flight_actions tool."""

    def test_returns_empty_list_when_no_actions(self, brain_tools, mock_cm):
        """Returns empty list when no in-flight actions."""
        mock_cm.in_flight_actions = {}
        result = brain_tools.cm_list_in_flight_actions()
        assert result == []

    def test_returns_action_summary(self, brain_tools, mock_cm):
        """Returns summary for each in-flight action."""
        mock_cm.in_flight_actions = {
            0: {"query": "Search for contacts", "handle_actions": []},
            1: {"query": "Summarise the thread", "handle_actions": [{"a": "test"}]},
        }
        result = brain_tools.cm_list_in_flight_actions()
        assert len(result) == 2
        assert result[0]["handle_id"] == 0
        assert result[0]["query"] == "Search for contacts"
        assert result[0]["num_handle_actions"] == 0
        assert result[1]["handle_id"] == 1
        assert result[1]["query"] == "Summarise the thread"
        assert result[1]["num_handle_actions"] == 1

    def test_handles_none_in_flight_actions(self, brain_tools, mock_cm):
        """Handles None in-flight actions gracefully."""
        mock_cm.in_flight_actions = None
        result = brain_tools.cm_list_in_flight_actions()
        assert result == []

    def test_handles_none_handle_actions(self, brain_tools, mock_cm):
        """Handles None handle_actions in action data."""
        mock_cm.in_flight_actions = {
            0: {"query": "Action", "handle_actions": None},
        }
        result = brain_tools.cm_list_in_flight_actions()
        assert result[0]["num_handle_actions"] == 0


class TestCmListNotifications:
    """Tests for cm_list_notifications tool."""

    def test_returns_empty_list_when_no_notifications(self, brain_tools, mock_cm):
        """Returns empty list when no notifications."""
        result = brain_tools.cm_list_notifications()
        assert result == []

    def test_returns_all_notifications(self, brain_tools, mock_cm, static_now):
        """Returns all notifications when pinned_only=False."""
        ts = static_now
        mock_cm.notifications_bar.push_notif("Type1", "Content1", ts)
        mock_cm.notifications_bar.push_notif("Type2", "Content2", ts, pinned=True)
        result = brain_tools.cm_list_notifications()
        assert len(result) == 2

    def test_filters_pinned_only(self, brain_tools, mock_cm, static_now):
        """Returns only pinned notifications when pinned_only=True."""
        ts = static_now
        mock_cm.notifications_bar.push_notif("Regular", "Not pinned", ts)
        mock_cm.notifications_bar.push_notif("Pinned", "Important", ts, pinned=True)
        result = brain_tools.cm_list_notifications(pinned_only=True)
        assert len(result) == 1
        assert result[0]["content"] == "Important"

    def test_converts_timestamp_to_isoformat(self, brain_tools, mock_cm):
        """Converts datetime timestamps to ISO format strings."""
        ts = datetime(2024, 1, 15, 10, 30, 0)
        mock_cm.notifications_bar.push_notif("Test", "Content", ts)
        result = brain_tools.cm_list_notifications()
        assert result[0]["timestamp"] == "2024-01-15T10:30:00"


class TestBrainToolsAsTools:
    """Tests for as_tools method."""

    def test_returns_dict_of_callables(self, brain_tools):
        """Returns dictionary mapping names to callable methods."""
        tools = brain_tools.as_tools()
        assert isinstance(tools, dict)
        assert all(callable(fn) for fn in tools.values())

    def test_contains_all_brain_tools(self, brain_tools):
        """Contains all expected brain tools."""
        tools = brain_tools.as_tools()
        expected = {
            "cm_get_contact",
            "cm_list_in_flight_actions",
            "cm_list_notifications",
        }
        assert set(tools.keys()) == expected

    def test_tools_are_bound_methods(self, brain_tools, mock_cm):
        """Tools are bound to the BrainTools instance."""
        mock_cm.in_flight_actions = {}
        tools = brain_tools.as_tools()
        # Calling through the dict should work
        assert tools["cm_list_in_flight_actions"]() == []


# =============================================================================
# ConversationManagerBrainActionTools Tests
# =============================================================================


class TestActionToolsAsTools:
    """Tests for action tools as_tools method."""

    def test_returns_dict_of_callables(self, brain_action_tools):
        """Returns dictionary mapping names to callable methods."""
        tools = brain_action_tools.as_tools()
        assert isinstance(tools, dict)
        assert all(callable(fn) for fn in tools.values())

    def test_contains_all_action_tools_when_initialized(self, brain_action_tools):
        """Every action tool is offered once the managers are initialized."""
        tools = brain_action_tools.as_tools()
        expected = {
            "send_unify_message",
            "act",
            "ask_about_contacts",
            "update_contacts",
            "query_past_transcripts",
            "wait",
        }
        assert set(tools.keys()) == expected

    def test_manager_backed_tools_wait_for_initialization(self, mock_cm):
        """Before the managers are up, only the chat and wait tools are offered."""
        mock_cm.initialized = False
        tools = ConversationManagerBrainActionTools(mock_cm).as_tools()
        assert set(tools.keys()) == {"send_unify_message", "wait"}


class TestSlowBrainDirectOutboundMarker:
    """The slow brain's own sends are marked so their sent events do not wake it."""

    @pytest.mark.asyncio
    async def test_send_unify_message_marks_outbound_origin(
        self,
        brain_action_tools,
        mock_cm,
        sample_contacts,
    ):
        _setup_mock_contacts(mock_cm.contact_index, sample_contacts)
        active_during_publish: list[bool] = []

        async def capture_publish(*args, **kwargs):
            active_during_publish.append(slow_brain_direct_outbound_active())

        mock_cm.event_broker.publish = AsyncMock(side_effect=capture_publish)

        result = await brain_action_tools.send_unify_message(
            content="Hello",
            contact_id=1,
        )

        assert result == {"status": "ok"}
        assert active_during_publish == [True]
        assert slow_brain_direct_outbound_active() is False

    @pytest.mark.asyncio
    async def test_sent_event_suppresses_slow_brain_wake(
        self,
        brain_action_tools,
        mock_cm,
        sample_contacts,
    ):
        _setup_mock_contacts(mock_cm.contact_index, sample_contacts)

        await brain_action_tools.send_unify_message(content="Hello", contact_id=1)

        (payload,) = _published_sent_events(brain_action_tools)
        assert payload["suppress_slow_brain_wake"] is True


class TestWaitTool:
    """Tests for wait tool."""

    @pytest.mark.asyncio
    async def test_returns_waiting_status(self, brain_action_tools):
        """Wait tool returns waiting status."""
        result = await brain_action_tools.wait()
        assert result == {"status": "waiting", "delay": None}

    def test_has_docstring(self, brain_action_tools):
        """Wait tool has descriptive docstring."""
        assert brain_action_tools.wait.__doc__ is not None
        assert "Wait" in brain_action_tools.wait.__doc__


class TestSendUnifyMessageTool:
    """Tests for send_unify_message tool."""

    def test_has_docstring(self, brain_action_tools):
        """Send Unify message tool has descriptive docstring."""
        doc = brain_action_tools.send_unify_message.__doc__
        assert doc is not None
        assert "chat message" in doc.lower()

    def test_docstring_mentions_attachment(self, brain_action_tools):
        """Send Unify message docstring mentions attachment parameter."""
        doc = brain_action_tools.send_unify_message.__doc__
        assert "attachment" in doc.lower()

    @pytest.mark.asyncio
    async def test_publishes_sent_event_for_contact(
        self,
        brain_action_tools,
        mock_cm,
        sample_contacts,
    ):
        """A plain send publishes UnifyMessageSent addressed to the contact."""
        _setup_mock_contacts(mock_cm.contact_index, sample_contacts)

        result = await brain_action_tools.send_unify_message(
            content="Hello Alice",
            contact_id=1,
        )

        assert result == {"status": "ok"}
        (payload,) = _published_sent_events(brain_action_tools)
        assert payload["contact"]["contact_id"] == 1
        assert payload["content"] == "Hello Alice"
        assert payload["attachments"] == []

    @pytest.mark.asyncio
    async def test_returns_error_for_unknown_contact(
        self,
        brain_action_tools,
        mock_cm,
        sample_contacts,
    ):
        """An unknown contact_id is reported without publishing anything."""
        _setup_mock_contacts(mock_cm.contact_index, sample_contacts)

        result = await brain_action_tools.send_unify_message(
            content="Hello",
            contact_id=999,
        )

        assert "999" in result["error"]
        assert _published_sent_events(brain_action_tools) == []

    @pytest.mark.asyncio
    async def test_returns_error_for_file_not_found(
        self,
        brain_action_tools,
        mock_cm,
        sample_contacts,
    ):
        """Returns error when attachment file not found."""
        _setup_mock_contacts(mock_cm.contact_index, sample_contacts)

        result = await brain_action_tools.send_unify_message(
            content="Here's the file",
            contact_id=1,
            attachment_filepath="/nonexistent/file.pdf",
        )

        assert "not found" in result["error"].lower()
        assert _published_sent_events(brain_action_tools) == []

    @pytest.mark.asyncio
    async def test_returns_error_for_file_too_large(
        self,
        brain_action_tools,
        mock_cm,
        sample_contacts,
        tmp_path,
    ):
        """Returns error when attachment exceeds size limit."""
        _setup_mock_contacts(mock_cm.contact_index, sample_contacts)

        # Create a file larger than 25MB
        large_file = tmp_path / "large_file.bin"
        large_file.write_bytes(b"x" * (26 * 1024 * 1024))

        # Patch the class in its home module so the deferred import inside
        # the attachment helper picks up the rooted adapter.
        rooted = type(
            "RootedAdapter",
            (LocalFileSystemAdapter,),
            {
                "__init__": lambda self: LocalFileSystemAdapter.__init__(
                    self,
                    root=str(tmp_path),
                ),
            },
        )
        with patch(
            "unify.file_manager.filesystem_adapters.local_adapter.LocalFileSystemAdapter",
            rooted,
        ):
            result = await brain_action_tools.send_unify_message(
                content="Here's the file",
                contact_id=1,
                attachment_filepath="large_file.bin",
            )

        assert "too large" in result["error"].lower()
        assert "25MB" in result["error"]
        assert _published_sent_events(brain_action_tools) == []

    @pytest.mark.asyncio
    async def test_send_with_attachment_success(
        self,
        brain_action_tools,
        mock_cm,
        sample_contacts,
        tmp_path,
    ):
        """An existing file is described as a local attachment on the sent event."""
        _setup_mock_contacts(mock_cm.contact_index, sample_contacts)

        test_file = tmp_path / "test_document.pdf"
        test_file.write_bytes(b"PDF content here")

        result = await brain_action_tools.send_unify_message(
            content="Here's the document",
            contact_id=1,
            attachment_filepath=str(test_file),
        )

        assert result == {"status": "ok"}
        (payload,) = _published_sent_events(brain_action_tools)
        assert payload["content"] == "Here's the document"
        (attachment,) = payload["attachments"]
        assert attachment == {
            "filename": "test_document.pdf",
            "filepath": str(test_file.resolve()),
            "content_type": "application/pdf",
            "size_bytes": len(b"PDF content here"),
        }


class TestSendUnifyMessageToBossTool:
    """Tests for send_unify_message_to_boss tool."""

    @pytest.mark.asyncio
    async def test_targets_boss_contact(self, brain_action_tools, mock_cm):
        """The boss-only tool addresses the session's boss contact."""
        result = await brain_action_tools.send_unify_message_to_boss(
            content="Done with the report.",
        )

        assert result == {"status": "ok"}
        (payload,) = _published_sent_events(brain_action_tools)
        assert payload["contact"]["contact_id"] == SESSION_DETAILS.boss_contact_id
        assert payload["content"] == "Done with the report."

    def test_docstring_restricts_recipient(self, brain_action_tools):
        """The docstring names the boss as the only recipient."""
        doc = brain_action_tools.send_unify_message_to_boss.__doc__
        assert doc is not None
        assert "boss" in doc.lower()


class TestActTool:
    """Tests for act tool."""

    def test_has_docstring(self, brain_action_tools):
        """Act tool has descriptive docstring."""
        assert brain_action_tools.act.__doc__ is not None
        assert len(brain_action_tools.act.__doc__) > 10


# =============================================================================
# Action Steering Tools Tests
# =============================================================================


class TestBuildActionSteeringTools:
    """Tests for build_action_steering_tools method.

    The steering surface is six fixed tools addressed by ``handle_id``. The
    set never varies with the in-flight action state — tool definitions
    precede messages in provider prompt-cache keys, so a changing schema
    would re-bill the static prompt on every action transition. Targets
    resolve at call time, with corrective errors for stale ids.
    """

    FIXED_TOOL_NAMES = {
        "interject_action",
        "stop_action",
        "pause_action",
        "resume_action",
        "ask_action",
        "answer_clarification_action",
    }

    def _running_handle(self):
        handle = MagicMock()
        handle._pause_event = MagicMock()
        handle._pause_event.is_set.return_value = True
        return handle

    def _paused_handle(self):
        handle = MagicMock()
        handle._pause_event = MagicMock()
        handle._pause_event.is_set.return_value = False
        return handle

    def test_fixed_tools_with_no_in_flight_actions(self, brain_action_tools, mock_cm):
        """The full fixed tool set is offered even with nothing in flight."""
        mock_cm.in_flight_actions = {}
        tools = brain_action_tools.build_action_steering_tools()
        assert set(tools.keys()) == self.FIXED_TOOL_NAMES

    def test_fixed_tools_with_none_in_flight_actions(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """None in_flight_actions still yields the full fixed tool set."""
        mock_cm.in_flight_actions = None
        tools = brain_action_tools.build_action_steering_tools()
        assert set(tools.keys()) == self.FIXED_TOOL_NAMES

    def test_tool_set_constant_across_action_transitions(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """The tool set is identical for running, paused, and multiple actions."""
        mock_cm.in_flight_actions = {}
        empty_names = set(brain_action_tools.build_action_steering_tools().keys())

        mock_cm.in_flight_actions = {
            0: {
                "query": "Action one",
                "handle": self._running_handle(),
                "handle_actions": [],
            },
            1: {
                "query": "Action two",
                "handle": self._paused_handle(),
                "handle_actions": [
                    {
                        "action_name": "clarification_request",
                        "query": "Need more info?",
                        "call_id": "call_123",
                    },
                ],
            },
        }
        busy_names = set(brain_action_tools.build_action_steering_tools().keys())

        assert empty_names == busy_names == self.FIXED_TOOL_NAMES

    def test_steering_tools_have_docstrings(self, brain_action_tools, mock_cm):
        """Every fixed steering tool documents its handle_id addressing."""
        tools = brain_action_tools.build_action_steering_tools()
        for name, fn in tools.items():
            assert fn.__doc__ is not None, f"{name} should have docstring"
            assert (
                "handle_id" in fn.__doc__
            ), f"{name} docstring should explain handle_id"

    @pytest.mark.asyncio
    async def test_stale_handle_id_returns_corrective_error(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """An unknown handle_id gets an error listing the live ids."""
        mock_cm.in_flight_actions = {
            0: {
                "query": "Only action",
                "handle": self._running_handle(),
                "handle_actions": [],
            },
        }
        tools = brain_action_tools.build_action_steering_tools()
        result = await tools["stop_action"](handle_id=7, reason="stale")
        assert result["status"] == "error"
        assert result["operation"] == "stop"
        assert "handle_id=7" in result["message"]
        assert "[0]" in result["message"]

    @pytest.mark.asyncio
    async def test_interject_delegates_to_handle(self, brain_action_tools, mock_cm):
        """interject_action resolves the handle at call time and interjects."""
        mock_handle = self._running_handle()
        mock_handle.interject = AsyncMock()
        mock_cm.in_flight_actions = {
            0: {
                "query": "Test",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }
        tools = brain_action_tools.build_action_steering_tools()
        result = await tools["interject_action"](handle_id=0, message="New info")
        mock_handle.interject.assert_called_once()
        assert result["status"] == "ok"
        assert result["operation"] == "interject"

    @pytest.mark.asyncio
    async def test_pause_running_action_delegates(self, brain_action_tools, mock_cm):
        """pause_action pauses a running handle."""
        mock_handle = self._running_handle()
        mock_handle.pause = AsyncMock()
        mock_cm.in_flight_actions = {
            0: {
                "query": "Running action",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }
        tools = brain_action_tools.build_action_steering_tools()
        result = await tools["pause_action"](handle_id=0)
        mock_handle.pause.assert_called_once()
        assert result["operation"] == "pause"

    @pytest.mark.asyncio
    async def test_pause_already_paused_short_circuits(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """pause_action on a paused handle reports so without delegating."""
        mock_handle = self._paused_handle()
        mock_handle.pause = AsyncMock()
        mock_cm.in_flight_actions = {
            0: {
                "query": "Paused action",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }
        tools = brain_action_tools.build_action_steering_tools()
        result = await tools["pause_action"](handle_id=0)
        mock_handle.pause.assert_not_called()
        assert result["status"] == "ok"
        assert "already paused" in result["message"]

    @pytest.mark.asyncio
    async def test_resume_paused_action_delegates(self, brain_action_tools, mock_cm):
        """resume_action resumes a paused handle."""
        mock_handle = self._paused_handle()
        mock_handle.resume = AsyncMock()
        mock_cm.in_flight_actions = {
            0: {
                "query": "Paused action",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }
        tools = brain_action_tools.build_action_steering_tools()
        result = await tools["resume_action"](handle_id=0)
        mock_handle.resume.assert_called_once()
        assert result["operation"] == "resume"

    @pytest.mark.asyncio
    async def test_resume_running_action_short_circuits(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """resume_action on a running handle reports so without delegating."""
        mock_handle = self._running_handle()
        mock_handle.resume = AsyncMock()
        mock_cm.in_flight_actions = {
            0: {
                "query": "Running action",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }
        tools = brain_action_tools.build_action_steering_tools()
        result = await tools["resume_action"](handle_id=0)
        mock_handle.resume.assert_not_called()
        assert result["status"] == "ok"
        assert "not paused" in result["message"]

    @pytest.mark.asyncio
    async def test_unknown_pause_state_treated_as_running(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """A handle without _pause_event (unknown state) pauses, not resumes."""
        mock_handle = MagicMock(spec=["pause", "resume"])
        mock_handle.pause = AsyncMock()
        mock_handle.resume = AsyncMock()
        mock_cm.in_flight_actions = {
            0: {
                "query": "Action with unknown state",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }
        tools = brain_action_tools.build_action_steering_tools()

        pause_result = await tools["pause_action"](handle_id=0)
        mock_handle.pause.assert_called_once()
        assert pause_result["operation"] == "pause"

        resume_result = await tools["resume_action"](handle_id=0)
        mock_handle.resume.assert_not_called()
        assert "not paused" in resume_result["message"]

    @pytest.mark.asyncio
    async def test_storage_check_handle_forwards_pause_state(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """_StorageCheckHandle forwards inner handle's pause state.

        Regression: _StorageCheckHandle didn't expose _pause_event, so
        get_handle_paused_state returned None (unknown) and a paused inner
        loop looked running. pause_action would then delegate a redundant
        pause instead of reporting the action already paused, producing
        serial "Pause" events visible on the frontend.
        """
        from unify.actor.code_act_actor import _StorageCheckHandle

        inner_handle = MagicMock()
        inner_handle._pause_event = asyncio.Event()
        inner_handle._pause_event.set()  # Start running
        inner_handle.pause = AsyncMock()

        # Block result() so the lifecycle stays in phase 1 ("task")
        _block = asyncio.Event()

        async def _blocking_result():
            await _block.wait()
            return "done"

        inner_handle.result = _blocking_result
        inner_handle.done = MagicMock(return_value=False)

        async def _block_forever():
            await asyncio.Event().wait()
            return {}

        inner_handle.next_notification = _block_forever
        inner_handle.next_clarification = _block_forever

        mock_actor = MagicMock()
        wrapped = _StorageCheckHandle(inner=inner_handle, actor=mock_actor)

        try:
            # Pause the inner handle
            inner_handle._pause_event.clear()

            mock_cm.in_flight_actions = {
                0: {
                    "query": "Access Dan's Gmail",
                    "handle": wrapped,
                    "handle_actions": [],
                },
            }

            tools = brain_action_tools.build_action_steering_tools()
            result = await tools["pause_action"](handle_id=0)

            inner_handle.pause.assert_not_called()
            assert result["status"] == "ok"
            assert "already paused" in result["message"]
        finally:
            _block.set()
            wrapped._lifecycle_task.cancel()
            try:
                await wrapped._lifecycle_task
            except (asyncio.CancelledError, Exception):
                pass

    @pytest.mark.asyncio
    async def test_ask_serves_completed_action(self, brain_action_tools, mock_cm):
        """ask_action reaches actions that have already completed."""
        mock_handle = MagicMock()
        mock_ask_handle = MagicMock()
        mock_ask_handle.result = AsyncMock(return_value="Answer")
        mock_handle.ask = AsyncMock(return_value=mock_ask_handle)

        mock_cm.in_flight_actions = {}
        mock_cm.completed_actions = {
            0: {
                "query": "Find contacts",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }

        tools = brain_action_tools.build_action_steering_tools()
        result = await tools["ask_action"](handle_id=0, question="What did you find?")

        for task in list(mock_cm._pending_steering_tasks):
            await task

        mock_handle.ask.assert_called_once()
        assert result["status"] == "ok"
        assert result["operation"] == "ask"

    @pytest.mark.asyncio
    async def test_ask_unknown_handle_errors(self, brain_action_tools, mock_cm):
        """ask_action on an id neither in flight nor completed errors."""
        mock_cm.in_flight_actions = {}
        mock_cm.completed_actions = {}
        tools = brain_action_tools.build_action_steering_tools()
        result = await tools["ask_action"](handle_id=5, question="Anything?")
        assert result["status"] == "error"
        assert result["operation"] == "ask"

    @pytest.mark.asyncio
    async def test_answer_clarification_without_pending_errors(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """answer_clarification_action errors when nothing is pending."""
        mock_cm.in_flight_actions = {
            0: {
                "query": "Do something",
                "handle": MagicMock(),
                "handle_actions": [],
            },
        }
        tools = brain_action_tools.build_action_steering_tools()
        result = await tools["answer_clarification_action"](
            handle_id=0,
            answer="Here you go",
        )
        assert result["status"] == "error"
        assert "no pending clarification" in result["message"]

    @pytest.mark.asyncio
    async def test_answer_clarification_ignores_answered_clarifications(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """Already answered clarifications do not count as pending."""
        mock_cm.in_flight_actions = {
            0: {
                "query": "Do something",
                "handle": MagicMock(),
                "handle_actions": [
                    {
                        "action_name": "clarification_request",
                        "query": "Need info?",
                        "call_id": "call_answered",
                        "response": "Here's the answer",
                    },
                ],
            },
        }
        tools = brain_action_tools.build_action_steering_tools()
        result = await tools["answer_clarification_action"](
            handle_id=0,
            answer="Again?",
        )
        assert result["status"] == "error"
        assert "no pending clarification" in result["message"]

    @pytest.mark.asyncio
    async def test_answer_clarification_single_pending_without_call_id(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """With exactly one pending clarification, call_id may be omitted."""
        mock_handle = MagicMock()
        mock_handle.answer_clarification = AsyncMock()
        mock_cm.in_flight_actions = {
            0: {
                "query": "Do something",
                "handle": mock_handle,
                "handle_actions": [
                    {
                        "action_name": "clarification_request",
                        "query": "Need more info?",
                        "call_id": "call_123",
                    },
                ],
            },
        }
        tools = brain_action_tools.build_action_steering_tools()
        result = await tools["answer_clarification_action"](
            handle_id=0,
            answer="Here is the answer",
        )
        mock_handle.answer_clarification.assert_called_once_with(
            "call_123",
            "Here is the answer",
        )
        assert result["status"] == "ok"
        assert result["operation"] == "answer_clarification"

    @pytest.mark.asyncio
    async def test_answer_clarification_targets_call_id(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """An explicit call_id picks one of several pending clarifications."""
        mock_handle = MagicMock()
        mock_handle.answer_clarification = AsyncMock()
        mock_cm.in_flight_actions = {
            0: {
                "query": "Do something",
                "handle": mock_handle,
                "handle_actions": [
                    {
                        "action_name": "clarification_request",
                        "query": "First question?",
                        "call_id": "call_first",
                    },
                    {
                        "action_name": "clarification_request",
                        "query": "Second question?",
                        "call_id": "call_second",
                    },
                ],
            },
        }
        tools = brain_action_tools.build_action_steering_tools()
        result = await tools["answer_clarification_action"](
            handle_id=0,
            answer="For the second one",
            call_id="call_second",
        )
        mock_handle.answer_clarification.assert_called_once_with(
            "call_second",
            "For the second one",
        )
        assert result["status"] == "ok"

    @pytest.mark.asyncio
    async def test_answer_clarification_ambiguous_without_call_id_errors(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """Several pending clarifications require an explicit call_id."""
        mock_cm.in_flight_actions = {
            0: {
                "query": "Do something",
                "handle": MagicMock(),
                "handle_actions": [
                    {
                        "action_name": "clarification_request",
                        "query": "First question?",
                        "call_id": "call_first",
                    },
                    {
                        "action_name": "clarification_request",
                        "query": "Second question?",
                        "call_id": "call_second",
                    },
                ],
            },
        }
        tools = brain_action_tools.build_action_steering_tools()
        result = await tools["answer_clarification_action"](
            handle_id=0,
            answer="Which one?",
        )
        assert result["status"] == "error"
        assert "call_id" in result["message"]
        assert "call_first" in result["message"]
        assert "call_second" in result["message"]


class TestMakeSteeringTool:
    """Tests for _make_steering_tool method."""

    @pytest.mark.asyncio
    async def test_ask_operation_calls_handle_ask(self, brain_action_tools, mock_cm):
        """Ask operation calls handle.ask with parameter."""
        mock_handle = MagicMock()
        mock_ask_handle = MagicMock()
        mock_ask_handle.result = AsyncMock(return_value="Answer")
        mock_handle.ask = AsyncMock(return_value=mock_ask_handle)

        mock_cm.in_flight_actions = {
            0: {
                "query": "Test",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }

        tool = brain_action_tools._make_steering_tool(
            handle_id=0,
            handle=mock_handle,
            operation="ask",
            param_name="query",
            docstring="Ask a question",
            query="Test",
        )
        result = await tool(query="What is the status?")

        # The ask operation spawns a background task — await it directly.
        for task in list(mock_cm._pending_steering_tasks):
            await task

        mock_handle.ask.assert_called_once()
        assert result["status"] == "ok"
        assert result["operation"] == "ask"

    @pytest.mark.asyncio
    async def test_stop_operation_calls_handle_stop(self, brain_action_tools, mock_cm):
        """Stop moves the action to completed_actions."""
        mock_handle = MagicMock()
        mock_handle.stop = AsyncMock()

        mock_cm.in_flight_actions = {
            0: {
                "query": "Test",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }

        tool = brain_action_tools._make_steering_tool(
            handle_id=0,
            handle=mock_handle,
            operation="stop",
            param_name="reason",
            docstring="Stop the action",
            query="Test",
        )
        result = await tool(reason="No longer needed")
        mock_handle.stop.assert_called_once_with(reason="No longer needed")
        assert result["operation"] == "stop"
        assert 0 not in mock_cm.in_flight_actions
        assert 0 in mock_cm.completed_actions

    @pytest.mark.asyncio
    async def test_interject_operation_calls_handle_interject(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """Interject operation calls handle.interject."""
        mock_handle = MagicMock()
        mock_handle.interject = AsyncMock()

        mock_cm.in_flight_actions = {
            0: {
                "query": "Test",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }

        tool = brain_action_tools._make_steering_tool(
            handle_id=0,
            handle=mock_handle,
            operation="interject",
            param_name="message",
            docstring="Interject a message",
            query="Test",
        )
        result = await tool(message="Important update")
        mock_handle.interject.assert_called_once()
        assert result["operation"] == "interject"

    @pytest.mark.asyncio
    async def test_pause_operation_calls_handle_pause(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """Pause operation calls handle.pause."""
        mock_handle = MagicMock()
        mock_handle.pause = AsyncMock()

        mock_cm.in_flight_actions = {
            0: {
                "query": "Test",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }

        tool = brain_action_tools._make_steering_tool(
            handle_id=0,
            handle=mock_handle,
            operation="pause",
            param_name="",
            docstring="Pause the action",
            query="Test",
        )
        result = await tool()
        mock_handle.pause.assert_called_once()
        assert result["operation"] == "pause"

    @pytest.mark.asyncio
    async def test_resume_operation_calls_handle_resume(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """Resume operation calls handle.resume."""
        mock_handle = MagicMock()
        mock_handle.resume = AsyncMock()

        mock_cm.in_flight_actions = {
            0: {
                "query": "Test",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }

        tool = brain_action_tools._make_steering_tool(
            handle_id=0,
            handle=mock_handle,
            operation="resume",
            param_name="",
            docstring="Resume the action",
            query="Test",
        )
        result = await tool()
        mock_handle.resume.assert_called_once()
        assert result["operation"] == "resume"

    @pytest.mark.asyncio
    async def test_answer_clarification_calls_handle_method(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """Answer clarification calls handle.answer_clarification."""
        mock_handle = MagicMock()
        mock_handle.answer_clarification = AsyncMock()

        mock_cm.in_flight_actions = {
            0: {
                "query": "Test",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }

        tool = brain_action_tools._make_steering_tool(
            handle_id=0,
            handle=mock_handle,
            operation="answer_clarification",
            param_name="answer",
            docstring="Answer clarification",
            query="Test",
            call_id="call_123",
        )
        result = await tool(answer="Here is the answer")
        mock_handle.answer_clarification.assert_called_once_with(
            "call_123",
            "Here is the answer",
        )
        assert result["operation"] == "answer_clarification"

    @pytest.mark.asyncio
    async def test_records_intervention_in_handle_actions(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """Steering operations record intervention in handle_actions."""
        mock_handle = MagicMock()
        mock_handle.pause = AsyncMock()

        mock_cm.in_flight_actions = {
            0: {
                "query": "Test",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }

        tool = brain_action_tools._make_steering_tool(
            handle_id=0,
            handle=mock_handle,
            operation="pause",
            param_name="",
            docstring="Pause",
            query="Test",
        )
        await tool()

        actions = mock_cm.in_flight_actions[0]["handle_actions"]
        assert len(actions) == 1
        assert actions[0]["action_name"] == "pause_0"

    @pytest.mark.asyncio
    async def test_handles_operation_errors(self, brain_action_tools, mock_cm):
        """Handles errors in steering operations gracefully."""
        mock_handle = MagicMock()
        mock_handle.pause = AsyncMock(side_effect=RuntimeError("Test error"))

        mock_cm.in_flight_actions = {
            0: {
                "query": "Test",
                "handle": mock_handle,
                "handle_actions": [],
            },
        }

        tool = brain_action_tools._make_steering_tool(
            handle_id=0,
            handle=mock_handle,
            operation="pause",
            param_name="",
            docstring="Pause",
            query="Test",
        )
        result = await tool()
        assert "Error" in result["result"]


# =============================================================================
# Tool Docstring Quality Tests
# =============================================================================


class TestToolDocstrings:
    """Tests verifying tool docstrings are informative for LLM usage."""

    def test_brain_tools_have_docstrings(self, brain_tools):
        """All brain tools have docstrings."""
        tools = brain_tools.as_tools()
        for name, fn in tools.items():
            assert fn.__doc__ is not None, f"{name} missing docstring"
            assert len(fn.__doc__) > 10, f"{name} docstring too short"

    def test_action_tools_have_docstrings(self, brain_action_tools):
        """All action tools have docstrings."""
        tools = brain_action_tools.as_tools()
        for name, fn in tools.items():
            assert fn.__doc__ is not None, f"{name} missing docstring"
            assert len(fn.__doc__) > 10, f"{name} docstring too short"

    def test_act_docstring_is_comprehensive(self, brain_action_tools):
        """act tool has comprehensive docstring explaining capabilities."""
        doc = brain_action_tools.act.__doc__
        assert len(doc) > 100, "act docstring should be comprehensive"


# =============================================================================
# Integration Tests
# =============================================================================


class TestCompletedActionTools:
    """Completed actions expose no per-action tools; ask_action serves them."""

    def test_no_completed_actions_yields_no_tools(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """Empty completed_actions yields no tools."""
        mock_cm.completed_actions = {}
        tools = brain_action_tools.build_completed_action_tools()
        assert tools == {}

    def test_completed_actions_yield_no_per_action_tools(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """Completed actions add nothing to the tool surface — the fixed
        ask_action steering tool serves them by handle_id."""
        mock_cm.completed_actions = {
            0: {
                "query": "Find contacts",
                "handle": MagicMock(),
                "handle_actions": [],
            },
            1: {
                "query": "Summarise the thread",
                "handle": MagicMock(),
                "handle_actions": [],
            },
        }

        tools = brain_action_tools.build_completed_action_tools()
        assert tools == {}


class TestBrainToolsIntegration:
    """Integration tests for brain tools working together."""

    def test_brain_and_action_tools_have_distinct_names(
        self,
        brain_tools,
        brain_action_tools,
    ):
        """Brain tools and action tools have non-overlapping names."""
        brain_names = set(brain_tools.as_tools().keys())
        action_names = set(brain_action_tools.as_tools().keys())
        overlap = brain_names & action_names
        assert len(overlap) == 0, f"Overlapping tool names: {overlap}"

    def test_steering_tools_distinct_from_static_tools(
        self,
        brain_action_tools,
        mock_cm,
    ):
        """Steering tools don't overlap with static action tools."""
        mock_cm.in_flight_actions = {
            0: {
                "query": "Test",
                "handle": MagicMock(),
                "handle_actions": [],
            },
        }
        static_names = set(brain_action_tools.as_tools().keys())
        steering_names = set(brain_action_tools.build_action_steering_tools().keys())
        overlap = static_names & steering_names
        assert len(overlap) == 0, f"Overlapping tool names: {overlap}"
