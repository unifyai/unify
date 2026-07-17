"""
tests/conversation_manager/test_managers_utils.py
======================================================

Tests for the managers_utils module, including the initialization queue
that holds operations until the ConversationManager is fully initialized,
and the exchange_id caching fix for call/meet utterances.
"""

from __future__ import annotations

import asyncio
import time as _time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unify.contact_manager.simulated import SimulatedContactManager
from unify.contact_manager.types.contact import UNASSIGNED
from unify.conversation_manager.domains import managers_utils
from unify.conversation_manager.domains.event_handlers import EventHandler
from unify.conversation_manager.events import (
    SyncContacts,
    EmailReceived,
    EmailSent,
    InboundUnifyMeetUtterance,
    OutboundUnifyMeetUtterance,
    InboundPhoneUtterance,
    OutboundPhoneUtterance,
    SlackChannelMessageReceived,
    SlackChannelMessageSent,
    SlackMessageReceived,
    SlackMessageSent,
    SMSReceived,
    SMSSent,
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

    # The SyncContacts event handler calls
    # contact_manager._sync_required_contacts(). An earlier rename
    # attempt to `_provision_system_overlays` was incorrect — that
    # method never landed in production. Patch the actual method
    # name so this test's call-tracking assertion lines up with the
    # production handler.
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
# Exchange-ID caching: log_message must set exchange_id on the call manager
# synchronously so that the next queued utterance reuses it.
# ---------------------------------------------------------------------------


def _make_cm_for_log_message() -> MagicMock:
    """Build a minimal mock CM that satisfies log_message requirements."""
    cm = MagicMock()
    cm.contact_manager = SimulatedContactManager()
    cm.transcript_manager = SimulatedTranscriptManager()

    # Real call_manager attributes (not MagicMock auto-attrs)
    cm.call_manager.call_exchange_id = UNASSIGNED
    cm.call_manager.unify_meet_exchange_id = UNASSIGNED
    cm.call_manager.google_meet_exchange_id = UNASSIGNED
    cm.call_manager.call_start_timestamp = None
    cm.call_manager.unify_meet_start_timestamp = None
    cm.call_manager.google_meet_start_timestamp = None

    cm.contact_index = MagicMock()
    cm.contact_index.get_contact = MagicMock(
        return_value={"contact_id": 1, "first_name": "Test", "surname": "User"},
    )
    cm.call_manager.teams_meet_exchange_id = UNASSIGNED
    cm.call_manager.teams_meet_start_timestamp = None
    cm._local_to_global_message_ids = {}
    cm._local_to_global_message_ids_by_destination = {}
    cm._local_message_destinations = {}
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


@pytest.mark.asyncio
async def test_log_message_unify_meet_expands_receiver_ids_from_roster(
    monkeypatch,
):
    """Unify Meet stamps 1 sender + N receivers when contact_ids are present."""
    monkeypatch.setattr(managers_utils.SESSION_DETAILS, "self_contact_id", 10)
    cm = _make_cm_for_log_message()
    cm.contact_index.get_contact = MagicMock(
        return_value={"contact_id": 2, "first_name": "Ada", "surname": "Owner"},
    )

    outbound = OutboundUnifyMeetUtterance(
        contact={"contact_id": 2, "first_name": "Ada", "surname": "Owner"},
        content="Hello everyone",
        participant_contact_ids=[2, 3, 4],
    )
    inbound = InboundUnifyMeetUtterance(
        contact={"contact_id": 2, "first_name": "Ada", "surname": "Owner"},
        content="Hi back",
        participant_contact_ids=[2, 3, 4],
    )

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, outbound)
        await managers_utils.log_message(cm, inbound)

    out_logged = cm.transcript_manager._sim_messages[-2]
    in_logged = cm.transcript_manager._sim_messages[-1]
    assert out_logged.sender_id == 10
    assert out_logged.receiver_ids == [2, 3, 4]
    assert in_logged.sender_id == 2
    assert in_logged.receiver_ids == [3, 4, 10]


@pytest.mark.asyncio
async def test_log_message_unify_meet_preserves_one_to_one_without_roster(
    monkeypatch,
):
    """Classic 1:1 Unify Meet keeps single-receiver attribution."""
    monkeypatch.setattr(managers_utils.SESSION_DETAILS, "self_contact_id", 10)
    cm = _make_cm_for_log_message()
    cm.contact_index.get_contact = MagicMock(
        return_value={"contact_id": 1, "first_name": "Boss", "surname": "User"},
    )

    event = OutboundUnifyMeetUtterance(
        contact={"contact_id": 1, "first_name": "Boss", "surname": "User"},
        content="Just us",
    )

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, event)

    logged = cm.transcript_manager._sim_messages[-1]
    assert logged.sender_id == 10
    assert logged.receiver_ids == [1]


@pytest.mark.asyncio
async def test_log_message_caches_unify_meet_exchange_id_synchronously():
    """
    Regression test for the exchange_id race condition.

    When two unify_meet utterances are queued back-to-back, the first
    log_message call must set cm.call_manager.unify_meet_exchange_id
    synchronously so that the second call reuses it — rather than waiting
    for the asynchronous LogMessageResponse handler, which may not have
    run yet.
    """
    cm = _make_cm_for_log_message()
    contact = {"contact_id": 1}

    assert cm.call_manager.unify_meet_exchange_id == UNASSIGNED

    utterance1 = OutboundUnifyMeetUtterance(contact=contact, content="Hello")
    utterance2 = InboundUnifyMeetUtterance(contact=contact, content="Hi there")

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, utterance1)

        # After the first utterance, the exchange_id must be cached
        first_exchange_id = cm.call_manager.unify_meet_exchange_id
        assert (
            first_exchange_id != UNASSIGNED
        ), "unify_meet_exchange_id should be set synchronously after first log_message"

        await managers_utils.log_message(cm, utterance2)

        # The second utterance must reuse the same exchange_id
        assert (
            cm.call_manager.unify_meet_exchange_id == first_exchange_id
        ), "Second utterance should reuse the cached unify_meet_exchange_id"


@pytest.mark.asyncio
async def test_log_message_caches_call_exchange_id_synchronously():
    """Same as above but for phone calls (call_exchange_id)."""
    cm = _make_cm_for_log_message()
    contact = {"contact_id": 1}

    assert cm.call_manager.call_exchange_id == UNASSIGNED

    utterance1 = OutboundPhoneUtterance(contact=contact, content="Hello")
    utterance2 = InboundPhoneUtterance(contact=contact, content="Hi there")

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, utterance1)

        first_exchange_id = cm.call_manager.call_exchange_id
        assert (
            first_exchange_id != UNASSIGNED
        ), "call_exchange_id should be set synchronously after first log_message"

        await managers_utils.log_message(cm, utterance2)

        assert (
            cm.call_manager.call_exchange_id == first_exchange_id
        ), "Second utterance should reuse the cached call_exchange_id"


@pytest.mark.asyncio
async def test_log_message_does_not_overwrite_existing_exchange_id():
    """
    If the exchange_id is already set (e.g. from a prior utterance),
    log_message must not overwrite it.
    """
    cm = _make_cm_for_log_message()
    contact = {"contact_id": 1}

    # Pre-set an exchange_id as if a previous utterance already cached it
    cm.call_manager.unify_meet_exchange_id = 42

    utterance = OutboundUnifyMeetUtterance(contact=contact, content="Hello")

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, utterance)

        assert (
            cm.call_manager.unify_meet_exchange_id == 42
        ), "log_message must not overwrite an already-set exchange_id"


# ---------------------------------------------------------------------------
# Email exchange grouping: messages sharing a provider thread_id land in the
# same exchange (inbound + outbound reply), distinct threads stay separate,
# and the exchange is recovered from Exchanges metadata after a restart clears
# the in-memory cache.
# ---------------------------------------------------------------------------


def _exchange_id_of_last_message(cm: MagicMock) -> int:
    return int(cm.transcript_manager._sim_messages[-1].exchange_id)


@pytest.mark.asyncio
async def test_log_message_groups_email_by_thread_id():
    """An inbound email and the assistant's reply on the same provider thread
    share one exchange; a message on a different thread opens a new one."""
    cm = _make_cm_for_log_message()
    contact = {"contact_id": 1, "first_name": "Test", "surname": "User"}

    inbound = EmailReceived(
        contact=contact,
        subject="Quote request",
        body="Can you send pricing?",
        thread_id="THREAD-A",
    )
    reply = EmailSent(
        contact=contact,
        subject="Re: Quote request",
        body="Here you go.",
        thread_id="THREAD-A",
    )
    other = EmailReceived(
        contact=contact,
        subject="Different topic",
        body="Unrelated.",
        thread_id="THREAD-B",
    )

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, inbound)
        first_exchange = _exchange_id_of_last_message(cm)
        assert first_exchange != UNASSIGNED

        await managers_utils.log_message(cm, reply)
        assert (
            _exchange_id_of_last_message(cm) == first_exchange
        ), "Reply on the same thread_id must reuse the inbound exchange"

        await managers_utils.log_message(cm, other)
        assert (
            _exchange_id_of_last_message(cm) != first_exchange
        ), "A different thread_id must open a new exchange"


@pytest.mark.asyncio
async def test_log_message_email_recovers_exchange_after_restart():
    """A cold in-memory cache (simulated restart) recovers the thread's
    exchange from Exchanges metadata rather than opening a duplicate."""
    cm = _make_cm_for_log_message()
    contact = {"contact_id": 1, "first_name": "Test", "surname": "User"}

    first = EmailReceived(
        contact=contact,
        subject="Ongoing thread",
        body="Message one.",
        thread_id="THREAD-DURABLE",
    )
    after_restart = EmailReceived(
        contact=contact,
        subject="Re: Ongoing thread",
        body="Message two.",
        thread_id="THREAD-DURABLE",
    )

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, first)
        first_exchange = _exchange_id_of_last_message(cm)
        assert first_exchange != UNASSIGNED

        # Simulate a CM restart: the in-memory cache is empty but the
        # Exchanges metadata persists.
        cm._conversation_exchange_ids = {}

        await managers_utils.log_message(cm, after_restart)
        assert (
            _exchange_id_of_last_message(cm) == first_exchange
        ), "Email thread must recover its exchange from metadata after restart"


@pytest.mark.asyncio
async def test_log_message_email_without_thread_id_creates_fresh_exchange():
    """Emails with no provider thread_id fall back to a fresh exchange per
    message rather than grouping under a blank key."""
    cm = _make_cm_for_log_message()
    contact = {"contact_id": 1, "first_name": "Test", "surname": "User"}

    first = EmailReceived(
        contact=contact,
        subject="No thread one",
        body="First.",
        thread_id=None,
    )
    second = EmailReceived(
        contact=contact,
        subject="No thread two",
        body="Second.",
        thread_id=None,
    )

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, first)
        first_exchange = _exchange_id_of_last_message(cm)

        await managers_utils.log_message(cm, second)
        assert (
            _exchange_id_of_last_message(cm) != first_exchange
        ), "Threadless emails must not group under a blank key"


# ---------------------------------------------------------------------------
# SMS exchange grouping: a 1:1 DM keeps a single exchange per contact from
# start to end (keyed on contact_id) with no inactivity window, and recovers
# that exchange from Exchanges metadata after a CM restart.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_log_message_groups_sms_by_contact():
    """An inbound SMS and the assistant reply share one exchange (keyed on
    contact_id)."""
    cm = _make_cm_for_log_message()
    contact = {"contact_id": 1, "first_name": "Test", "surname": "User"}

    inbound = SMSReceived(contact=contact, content="Hi there")
    reply = SMSSent(contact=contact, content="Hello back")

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, inbound)
        first_exchange = _exchange_id_of_last_message(cm)
        assert first_exchange != UNASSIGNED

        await managers_utils.log_message(cm, reply)
        assert (
            _exchange_id_of_last_message(cm) == first_exchange
        ), "SMS reply must reuse the inbound exchange"


@pytest.mark.asyncio
async def test_log_message_sms_reuses_exchange_regardless_of_time():
    """A 1:1 SMS reuses its exchange no matter how much time has passed — the
    conversation with a contact has one exchange id from start to end."""
    cm = _make_cm_for_log_message()
    contact = {"contact_id": 1, "first_name": "Test", "surname": "User"}

    first = SMSReceived(contact=contact, content="Morning")
    later = SMSReceived(contact=contact, content="Afternoon")

    base = datetime(2026, 1, 1, 12, 0, 0)
    clock = MagicMock(return_value=base)
    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        with patch.object(managers_utils, "prompt_now", clock):
            await managers_utils.log_message(cm, first)
            first_exchange = _exchange_id_of_last_message(cm)
            assert first_exchange != UNASSIGNED

            clock.return_value = base + timedelta(days=7)
            await managers_utils.log_message(cm, later)
            assert (
                _exchange_id_of_last_message(cm) == first_exchange
            ), "SMS must reuse the same exchange regardless of elapsed time"


@pytest.mark.asyncio
async def test_log_message_sms_recovers_exchange_after_restart():
    """A cold in-memory cache (simulated restart) recovers the contact's SMS
    exchange from Exchanges metadata rather than opening a duplicate."""
    cm = _make_cm_for_log_message()
    contact = {"contact_id": 1, "first_name": "Test", "surname": "User"}

    first = SMSReceived(contact=contact, content="Before restart")
    after_restart = SMSReceived(contact=contact, content="After restart")

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, first)
        first_exchange = _exchange_id_of_last_message(cm)
        assert first_exchange != UNASSIGNED

        # Simulate a CM restart: the in-memory cache is empty but the
        # Exchanges metadata persists.
        cm._conversation_exchange_ids = {}

        await managers_utils.log_message(cm, after_restart)
        assert (
            _exchange_id_of_last_message(cm) == first_exchange
        ), "SMS DM must recover its exchange from metadata after restart"


# ---------------------------------------------------------------------------
# Slack exchange grouping: DMs keep a single exchange per contact from start to
# end (keyed on contact_id); channels group by the native (team_id, channel_id,
# thread_ts) thread, where a top-level @mention keys on the message's own
# event_ts (the value the reply threads under).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_log_message_groups_slack_dm_by_contact():
    """A Slack DM and the assistant reply share one exchange (keyed on
    contact_id)."""
    cm = _make_cm_for_log_message()
    contact = {"contact_id": 1, "first_name": "Test", "surname": "User"}

    inbound = SlackMessageReceived(
        contact=contact,
        content="Hey",
        team_id="T1",
        channel_id="D1",
    )
    reply = SlackMessageSent(
        contact=contact,
        content="Hi",
        team_id="T1",
        channel_id="D1",
    )

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, inbound)
        first_exchange = _exchange_id_of_last_message(cm)
        assert first_exchange != UNASSIGNED

        await managers_utils.log_message(cm, reply)
        assert (
            _exchange_id_of_last_message(cm) == first_exchange
        ), "Slack DM reply must reuse the inbound exchange"


@pytest.mark.asyncio
async def test_log_message_groups_slack_channel_by_thread():
    """A top-level Slack @mention and its threaded reply share one exchange;
    a message in a different thread of the same channel opens a new one."""
    cm = _make_cm_for_log_message()
    contact = {"contact_id": 1, "first_name": "Test", "surname": "User"}

    # Top-level @mention: no thread_ts yet, so the thread is keyed on the
    # message's own event_ts.
    mention = SlackChannelMessageReceived(
        contact=contact,
        content="@bot help",
        team_id="T1",
        channel_id="C1",
        thread_ts="",
        event_ts="111.1",
    )
    # The reply threads under the mention (thread_ts == the mention's ts).
    reply = SlackChannelMessageSent(
        contact=contact,
        content="Sure, here you go",
        team_id="T1",
        channel_id="C1",
        thread_ts="111.1",
    )
    # A separate top-level mention in the same channel is a distinct thread.
    other_thread = SlackChannelMessageReceived(
        contact=contact,
        content="@bot another thing",
        team_id="T1",
        channel_id="C1",
        thread_ts="",
        event_ts="222.2",
    )

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, mention)
        first_exchange = _exchange_id_of_last_message(cm)
        assert first_exchange != UNASSIGNED

        await managers_utils.log_message(cm, reply)
        assert (
            _exchange_id_of_last_message(cm) == first_exchange
        ), "Threaded reply must reuse the mention's exchange"

        await managers_utils.log_message(cm, other_thread)
        assert (
            _exchange_id_of_last_message(cm) != first_exchange
        ), "A different thread in the same channel must open a new exchange"


@pytest.mark.asyncio
async def test_log_message_slack_channel_without_ids_creates_fresh_exchange():
    """Slack channel messages missing a channel_id/thread identifier fall back
    to a fresh exchange rather than grouping under a blank key."""
    cm = _make_cm_for_log_message()
    contact = {"contact_id": 1, "first_name": "Test", "surname": "User"}

    first = SlackChannelMessageReceived(
        contact=contact,
        content="One",
        team_id="T1",
        channel_id="",
        thread_ts="",
        event_ts="",
    )
    second = SlackChannelMessageReceived(
        contact=contact,
        content="Two",
        team_id="T1",
        channel_id="",
        thread_ts="",
        event_ts="",
    )

    with patch.object(
        managers_utils,
        "event_broker",
        new=MagicMock(publish=AsyncMock()),
    ):
        await managers_utils.log_message(cm, first)
        first_exchange = _exchange_id_of_last_message(cm)

        await managers_utils.log_message(cm, second)
        assert (
            _exchange_id_of_last_message(cm) != first_exchange
        ), "Slack channel messages without ids must not group under a blank key"
