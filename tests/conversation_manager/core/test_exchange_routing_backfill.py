"""Long-lived conversations repair their own routing identity.

Exchange metadata is written once, when the exchange is created, and a
Teams or Slack thread outlives that moment by months. A conversation that
began before routing identifiers were recorded would otherwise never
acquire them, leaving outbound replies unable to find it for as long as
the thread lives — no amount of new traffic would help. The backfill
closes that gap from the next message in either direction.
"""

from types import SimpleNamespace

import pytest

from unify.conversation_manager.cm_types import Medium
from unify.conversation_manager.domains import managers_utils


@pytest.fixture(autouse=True)
def _clear_backfill_cache():
    managers_utils._routing_backfilled_exchanges.clear()
    yield
    managers_utils._routing_backfilled_exchanges.clear()


def _event(**overrides):
    base = {
        "tenant_id": "tenant-a",
        "conversation_id": "conv-a",
        "channel_id": "",
        "team_id": "",
        "thread_id": "",
        "guild_id": "",
        "group_id": "",
        "thread_ts": "",
        "event_ts": "",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _cm(stored: dict):
    updates: list[tuple[int, dict]] = []
    transcript_manager = SimpleNamespace(
        get_exchange_metadata=lambda exchange_id: SimpleNamespace(metadata=stored),
        update_exchange_metadata=lambda exchange_id, metadata: updates.append(
            (exchange_id, metadata),
        ),
    )
    return SimpleNamespace(transcript_manager=transcript_manager), updates


def test_backfills_routing_onto_an_exchange_that_lacks_it():
    cm, updates = _cm({"conversation_key": "ms_teams_bot_message:dm:1"})

    managers_utils._backfill_exchange_routing(
        cm,
        exchange_id=77,
        event=_event(),
        medium=Medium.MS_TEAMS_BOT_MESSAGE,
        conversation_key="ms_teams_bot_message:dm:1",
    )

    assert updates
    exchange_id, written = updates[0]
    assert exchange_id == 77
    assert written["tenant_id"] == "tenant-a"
    assert written["conversation_id"] == "conv-a"


def test_leaves_an_already_complete_exchange_alone():
    cm, updates = _cm(
        {
            "conversation_key": "ms_teams_bot_message:dm:1",
            "tenant_id": "tenant-a",
            "conversation_id": "conv-a",
        },
    )

    managers_utils._backfill_exchange_routing(
        cm,
        exchange_id=77,
        event=_event(),
        medium=Medium.MS_TEAMS_BOT_MESSAGE,
        conversation_key="ms_teams_bot_message:dm:1",
    )

    assert updates == []


def test_repairs_once_per_exchange_per_process():
    """Every message in a busy thread would otherwise re-read the row."""
    cm, updates = _cm({})

    for _ in range(3):
        managers_utils._backfill_exchange_routing(
            cm,
            exchange_id=77,
            event=_event(),
            medium=Medium.MS_TEAMS_BOT_MESSAGE,
            conversation_key="ms_teams_bot_message:dm:1",
        )

    assert len(updates) == 1


def test_an_unreadable_exchange_does_not_break_logging():
    """A repair is opportunistic; losing it must not cost the message."""

    def _raise(exchange_id):
        raise RuntimeError("exchange row is gone")

    updates: list[tuple[int, dict]] = []
    cm = SimpleNamespace(
        transcript_manager=SimpleNamespace(
            get_exchange_metadata=_raise,
            update_exchange_metadata=lambda exchange_id, metadata: updates.append(
                (exchange_id, metadata),
            ),
        ),
    )

    managers_utils._backfill_exchange_routing(
        cm,
        exchange_id=77,
        event=_event(),
        medium=Medium.MS_TEAMS_BOT_MESSAGE,
        conversation_key="ms_teams_bot_message:dm:1",
    )

    assert updates == []
