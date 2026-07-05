from __future__ import annotations

from datetime import UTC, datetime

from unify.conversation_manager.domains import managers_utils


def test_apply_reaction_delta_add_change_remove():
    ts = datetime.now(UTC)
    reactions, previous, action = managers_utils._apply_reaction_delta(
        [],
        contact_id=2,
        emoji="👍",
        timestamp=ts,
    )
    assert action == "added"
    assert previous is None
    assert reactions == [
        {"contact_id": 2, "emoji": "👍", "updated_at": ts.isoformat()},
    ]

    reactions, previous, action = managers_utils._apply_reaction_delta(
        reactions,
        contact_id=2,
        emoji="❤️",
        timestamp=ts,
    )
    assert action == "changed"
    assert previous == "👍"
    assert reactions[0]["emoji"] == "❤️"

    reactions, previous, action = managers_utils._apply_reaction_delta(
        reactions,
        contact_id=2,
        emoji="❤️",
        timestamp=ts,
    )
    assert action == "removed"
    assert previous == "❤️"
    assert reactions == []


def test_build_reaction_audit_content_variants():
    added = managers_utils._build_reaction_audit_content(
        reactor_name="Alex",
        action="added",
        target_message_id=42,
        target_content="I'll check that for you",
        emoji="👍",
        previous_emoji=None,
    )
    assert "reacted 👍" in added
    assert "message #42" in added

    removed = managers_utils._build_reaction_audit_content(
        reactor_name="Alex",
        action="removed",
        target_message_id=42,
        target_content="I'll check that for you",
        emoji=None,
        previous_emoji="👍",
    )
    assert "removed 👍" in removed
