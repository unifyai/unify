from __future__ import annotations

from datetime import datetime, UTC

from unify.transcript_manager.simulated import SimulatedTranscriptManager
from unify.conversation_manager.cm_types import Medium


def test_update_message_reactions_merges_and_removes():
    tm = SimulatedTranscriptManager()
    exchange_id, message_id = tm.log_first_message_in_new_exchange(
        {
            "medium": Medium.UNIFY_MESSAGE,
            "sender_id": 1,
            "receiver_ids": [2],
            "timestamp": datetime.now(UTC),
            "content": "Hello there",
        },
    )

    tm.update_message_reactions(
        message_id,
        [{"contact_id": 2, "emoji": "👍", "updated_at": "2026-07-04T12:00:00Z"}],
    )
    row = tm.get_message_by_id(message_id)
    assert row is not None
    assert row["metadata"]["reactions"][0]["emoji"] == "👍"

    tm.update_message_reactions(message_id, [])
    row = tm.get_message_by_id(message_id)
    assert row is not None
    assert row["metadata"]["reactions"] == []

    assert exchange_id is not None
