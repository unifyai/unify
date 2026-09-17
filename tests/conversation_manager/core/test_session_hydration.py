"""
tests/conversation_manager/core/test_session_hydration.py
=========================================================

Tests for global thread hydration from EventBus on wakeup.

Verifies that hydrate_global_thread() correctly reconstructs the shared
global deque from persisted Comms events, making session boundaries
invisible to the brain. All tests are symbolic — the EventBus search
is mocked to return synthetic events.
"""

from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unify.conversation_manager.domains.contact_index import (
    ContactIndex,
    UnifyMessage,
)
from unify.conversation_manager.domains.managers_utils import hydrate_global_thread
from unify.conversation_manager.events import (
    UnifyMessageReceived,
    UnifyMessageSent,
)

# =============================================================================
# Helpers
# =============================================================================

BASE_TIME = datetime(2025, 6, 13, 12, 0, 0, tzinfo=timezone.utc)
ALICE = {
    "contact_id": 2,
    "first_name": "Alice",
    "surname": "Smith",
    "phone_number": "+15555552222",
}
BOB = {
    "contact_id": 3,
    "first_name": "Bob",
    "surname": "Jones",
    "email_address": "bob@example.com",
}


def _make_bus_events(cm_events):
    """Convert CM events to bus events (as EventBus.search would return them).

    Returns events in descending timestamp order (newest first), matching
    the real EventBus.search() behavior.
    """
    bus_events = [ev.to_bus_event() for ev in cm_events]
    # Descending order (newest first)
    bus_events.reverse()
    return bus_events


def _make_mock_cm():
    """Create a minimal mock CM with a real ContactIndex."""
    cm = MagicMock()
    cm.contact_index = ContactIndex()
    return cm


# =============================================================================
# Unify Message Hydration
# =============================================================================


class TestUnifyMessageHydration:

    @pytest.mark.asyncio
    async def test_unify_messages_restored(self):
        """UnifyMessage events restore with content and attachments."""
        cm = _make_mock_cm()
        events = [
            UnifyMessageReceived(
                contact=ALICE,
                content="Check this file",
                attachments=[{"id": "a1", "filename": "report.pdf"}],
                timestamp=BASE_TIME,
            ),
            UnifyMessageSent(
                contact=ALICE,
                content="Got it, reviewing now",
                timestamp=BASE_TIME + timedelta(seconds=10),
            ),
        ]

        with patch(
            "unify.conversation_manager.domains.managers_utils.EVENT_BUS",
        ) as mock_bus:
            mock_bus.search = AsyncMock(return_value=_make_bus_events(events))
            await hydrate_global_thread(cm)

        msgs = cm.contact_index.get_messages_for_contact(2)
        assert len(msgs) == 2
        assert isinstance(msgs[0], UnifyMessage)
        assert msgs[0].role == "user"
        assert msgs[0].content == "Check this file"
        assert msgs[0].attachments == [{"id": "a1", "filename": "report.pdf"}]
        assert msgs[1].role == "assistant"
        assert msgs[1].name == "You"


# =============================================================================
# Cross-Cutting Concerns
# =============================================================================


class TestHydrationCrossCutting:

    @pytest.mark.asyncio
    async def test_chronological_order_preserved(self):
        """Messages from multiple contacts maintain chronological order."""
        cm = _make_mock_cm()
        events = [
            UnifyMessageReceived(contact=ALICE, content="msg_1", timestamp=BASE_TIME),
            UnifyMessageSent(
                contact=ALICE,
                content="msg_2",
                timestamp=BASE_TIME + timedelta(minutes=1),
            ),
            UnifyMessageReceived(
                contact=BOB,
                content="msg_3",
                timestamp=BASE_TIME + timedelta(minutes=2),
            ),
        ]

        with patch(
            "unify.conversation_manager.domains.managers_utils.EVENT_BUS",
        ) as mock_bus:
            mock_bus.search = AsyncMock(return_value=_make_bus_events(events))
            await hydrate_global_thread(cm)

        assert len(cm.contact_index.global_thread) == 3
        contents = [e.message.content for e in cm.contact_index.global_thread]
        assert contents == ["msg_1", "msg_2", "msg_3"]

    @pytest.mark.asyncio
    async def test_multi_contact(self):
        """Events across contacts hydrate into per-contact views."""
        cm = _make_mock_cm()
        events = [
            UnifyMessageReceived(
                contact=ALICE,
                content="from alice",
                timestamp=BASE_TIME,
            ),
            UnifyMessageReceived(
                contact=BOB,
                content="from bob",
                timestamp=BASE_TIME + timedelta(minutes=1),
            ),
            UnifyMessageSent(
                contact=ALICE,
                content="to alice",
                timestamp=BASE_TIME + timedelta(minutes=2),
            ),
        ]

        with patch(
            "unify.conversation_manager.domains.managers_utils.EVENT_BUS",
        ) as mock_bus:
            mock_bus.search = AsyncMock(return_value=_make_bus_events(events))
            await hydrate_global_thread(cm)

        assert len(cm.contact_index.get_messages_for_contact(2)) == 2
        assert len(cm.contact_index.get_messages_for_contact(3)) == 1

        # Active contacts derived from global thread
        active = cm.contact_index.get_active_contact_ids()
        assert active == {2, 3}

    @pytest.mark.asyncio
    async def test_non_message_events_skipped(self):
        """Events that don't produce messages (e.g. ActorResult) are skipped."""
        from unify.events.event_bus import Event as BusEvent

        cm = _make_mock_cm()
        # Simulate a mix: one real message + one non-message event
        message = UnifyMessageReceived(
            contact=ALICE,
            content="Hello",
            timestamp=BASE_TIME,
        )
        bus_events = _make_bus_events([message])

        # Insert a non-message bus event
        non_msg = BusEvent(
            type="Comms",
            payload_cls="ActorResult",
            payload={"handle_id": 1, "success": True, "result": "done"},
            timestamp=BASE_TIME.isoformat(),
        )
        bus_events.insert(0, non_msg)

        with patch(
            "unify.conversation_manager.domains.managers_utils.EVENT_BUS",
        ) as mock_bus:
            mock_bus.search = AsyncMock(return_value=bus_events)
            await hydrate_global_thread(cm)

        # Only the chat message should be in the deque
        assert len(cm.contact_index.global_thread) == 1

    @pytest.mark.asyncio
    async def test_empty_bus_is_noop(self):
        """No events in EventBus means empty deque (clean start)."""
        cm = _make_mock_cm()

        with patch(
            "unify.conversation_manager.domains.managers_utils.EVENT_BUS",
        ) as mock_bus:
            mock_bus.search = AsyncMock(return_value=[])
            await hydrate_global_thread(cm)

        assert len(cm.contact_index.global_thread) == 0

    @pytest.mark.asyncio
    async def test_missing_contact_id_skipped(self):
        """Events with no contact_id in the payload are skipped gracefully."""
        cm = _make_mock_cm()
        event = UnifyMessageReceived(contact={}, content="no id", timestamp=BASE_TIME)
        bus_events = _make_bus_events([event])

        with patch(
            "unify.conversation_manager.domains.managers_utils.EVENT_BUS",
        ) as mock_bus:
            mock_bus.search = AsyncMock(return_value=bus_events)
            await hydrate_global_thread(cm)

        assert len(cm.contact_index.global_thread) == 0
