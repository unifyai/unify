"""
tests/conversation_manager/core/test_managers_utils.py
======================================================

Tests for the managers_utils module: the initialization queue that holds
operations until the ConversationManager is fully initialized, and the
transcript attribution of chat messages logged via ``log_message``.
"""

from __future__ import annotations

import asyncio
import time as _time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unify.contact_manager.simulated import SimulatedContactManager
from unify.conversation_manager.domains import managers_utils
from unify.conversation_manager.domains.event_handlers import EventHandler
from unify.conversation_manager.events import (
    SyncContacts,
    UnifyMessageReceived,
    UnifyMessageSent,
)
from unify.transcript_manager.simulated import SimulatedTranscriptManager


async def _wait_for_condition(
    predicate,
    *,
    timeout: float = 5.0,
    poll: float = 0.02,
) -> bool:
    """Poll predicate() until True or timeout. Returns whether condition was met."""
    start = _time.perf_counter()
    while _time.perf_counter() - start < timeout:
        if predicate():
            return True
        await asyncio.sleep(poll)
    return False


@pytest.mark.asyncio
async def test_queue_operation_waits_for_initialization():
    """
    Verify that operations queued via queue_operation only execute
    after cm.initialized becomes True.

    This tests the real queueing mechanism in managers_utils.
    """
    # Create a mock CM that starts uninitialized
    mock_cm = MagicMock()
    mock_cm._session_logger = MagicMock()
    mock_cm.contact_manager = SimulatedContactManager()
    mock_cm.notifications_bar = MagicMock()
    mock_cm.initialized = False

    # The SyncContacts handler calls contact_manager._sync_required_contacts()
    # from the queued operation; wrap it to track the call.
    with patch.object(
        mock_cm.contact_manager,
        "_sync_required_contacts",
        wraps=mock_cm.contact_manager._sync_required_contacts,
    ) as mock_sync:
        # Fire the SyncContacts event (this queues the operation)
        event = SyncContacts(reason="test sync")
        await EventHandler.handle_event(event, mock_cm)

        # Give a moment for any immediate execution (there shouldn't be any)
        await asyncio.sleep(0.05)

        # Sync should NOT have been called yet - still waiting for initialization
        mock_sync.assert_not_called()

        # Start the operations listener in the background
        listener_task = asyncio.create_task(
            managers_utils.listen_to_operations(mock_cm),
        )

        # Still not called - listener is waiting for initialization
        await asyncio.sleep(0.05)
        mock_sync.assert_not_called()

        # Now mark as initialized - this unblocks the listener
        mock_cm.initialized = True

        # Wait for the queued operation to be processed (poll instead of fixed sleep)
        await _wait_for_condition(lambda: mock_sync.called, timeout=2.0)

        # NOW the sync should have been called
        mock_sync.assert_called_once()
        mock_cm.notifications_bar.push_notif.assert_called_once()

        # Cleanup
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# Transcript attribution: log_message stamps sender and receivers from the
# resolved self contact and the message's contact.
# ---------------------------------------------------------------------------


def _make_cm_for_log_message() -> MagicMock:
    """Build a minimal mock CM that satisfies log_message requirements."""
    cm = MagicMock()
    cm.contact_manager = SimulatedContactManager()
    cm.transcript_manager = SimulatedTranscriptManager()
    cm.contact_index = MagicMock()
    cm.contact_index.get_contact = MagicMock(
        return_value={"contact_id": 1, "first_name": "Test", "surname": "User"},
    )
    cm._conversation_exchange_ids = {}
    return cm


@pytest.mark.asyncio
async def test_log_message_uses_resolved_self_contact_for_assistant_messages(
    monkeypatch,
):
    """Assistant-authored transcript rows use the resolved self contact id."""
    monkeypatch.setattr(managers_utils.SESSION_DETAILS, "self_contact_id", 337)
    cm = _make_cm_for_log_message()
    cm.contact_index.get_contact = MagicMock(
        return_value={"contact_id": 441, "first_name": "Boss", "surname": "User"},
    )

    event = UnifyMessageSent(
        contact={"contact_id": 441, "first_name": "Boss", "surname": "User"},
        content="Here is the update.",
    )

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, event)

    logged = cm.transcript_manager._sim_messages[-1]
    assert logged.sender_id == 337
    assert logged.receiver_ids == [441]


@pytest.mark.asyncio
async def test_log_message_uses_resolved_self_contact_for_inbound_messages(
    monkeypatch,
):
    """Inbound transcript rows target the resolved assistant self contact id."""
    monkeypatch.setattr(managers_utils.SESSION_DETAILS, "self_contact_id", 337)
    cm = _make_cm_for_log_message()
    cm.contact_index.get_contact = MagicMock(
        return_value={"contact_id": 441, "first_name": "Boss", "surname": "User"},
    )

    event = UnifyMessageReceived(
        contact={"contact_id": 441, "first_name": "Boss", "surname": "User"},
        content="Can you check this?",
    )

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, event)

    logged = cm.transcript_manager._sim_messages[-1]
    assert logged.sender_id == 441
    assert logged.receiver_ids == [337]
