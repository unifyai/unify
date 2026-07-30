"""Contract tests for Recall's realtime frame shapes.

The frames here are the shapes Recall documents, not shapes invented to match
the parser. That distinction is the point of the file: the previous fixtures
asserted the reading the consumer already performed, so both agreed with each
other and neither agreed with Recall -- a participant nested one level deeper
than expected read as "no events arrived", which is what a dead relay looks
like too.
"""

from unify.conversation_manager.domains.recall.events import (
    EVENT_CHAT_MESSAGE,
    EVENT_JOIN,
    EVENT_SPEECH_ON,
    ROSTER_EVENTS,
    SUBSCRIBED_EVENTS,
    parse_relayed_event,
    participant_from_payload,
)


def _frame(event: str, participant: dict, body: dict | None = None) -> dict:
    """One frame as the relay forwards it: the event name plus Recall's ``data``.

    Recall wraps the payload twice, and the artifacts alongside it are part of
    the real envelope -- kept here so a parser that reaches for the wrong
    ``data`` finds a plausible object rather than nothing.
    """
    return {
        "event": event,
        "data": {
            "data": {
                "participant": participant,
                "timestamp": {"absolute": "2026-07-30T10:00:00Z", "relative": 12.5},
                "data": body,
            },
            "realtime_endpoint": {"id": "endpoint-1", "metadata": {}},
            "participant_events": {"id": "pe-1", "metadata": {}},
            "recording": {"id": "rec-1", "metadata": {}},
            "bot": {"id": "bot-1", "metadata": {}},
        },
    }


def test_participant_is_read_from_the_doubly_nested_payload():
    """``data.data.participant`` -- one level short yields an empty speaker."""
    event = parse_relayed_event(
        _frame(EVENT_SPEECH_ON, {"id": 7, "name": "Ada", "is_host": True}),
    )
    assert event is not None
    assert event.name == EVENT_SPEECH_ON
    assert event.participant is not None
    assert event.participant.name == "Ada"
    assert event.participant.is_host is True


def test_chat_text_is_read_from_the_triply_nested_body():
    """``data.data.data.text``. Reading ``data.data.text`` drops every message."""
    event = parse_relayed_event(
        _frame(
            EVENT_CHAT_MESSAGE,
            {"id": 7, "name": "Ada"},
            {"text": "here is the link", "to": "everyone"},
        ),
    )
    assert event is not None
    assert event.chat_text == "here is the link"
    assert event.chat_to == "everyone"


def test_non_chat_events_carry_a_null_body():
    """Recall always sends the innermost ``data`` as null outside chat."""
    event = parse_relayed_event(_frame(EVENT_JOIN, {"id": 7, "name": "Ada"}, None))
    assert event is not None
    assert event.chat_text is None
    assert event.chat_to is None


def test_participant_id_is_normalised_to_a_string():
    """Realtime sends an int and REST a string; roster upserts match on it.

    Left unnormalised, 7 and "7" are two people in the same meeting and a leave
    never removes the person who joined.
    """
    realtime = parse_relayed_event(_frame(EVENT_JOIN, {"id": 7, "name": "Ada"}))
    rest = participant_from_payload({"id": "7", "name": "Ada"})
    assert realtime is not None and realtime.participant is not None
    assert rest is not None
    assert realtime.participant.id == rest.id == "7"


def test_email_is_taken_from_extra_data_when_absent_at_the_top_level():
    participant = participant_from_payload(
        {"id": 1, "name": "Ada", "extra_data": {"email": "ada@example.com"}},
    )
    assert participant is not None
    assert participant.email == "ada@example.com"


def test_a_missing_email_is_none_rather_than_empty():
    """Platforms disclose an address only sometimes; None is the normal case."""
    participant = participant_from_payload({"id": 1, "name": "Ada", "email": None})
    assert participant is not None
    assert participant.email is None


def test_a_participant_without_an_id_is_not_a_participant():
    assert participant_from_payload({"name": "Ada"}) is None
    assert participant_from_payload("not a mapping") is None


def test_frames_that_carry_nothing_are_rejected():
    """A frame read as empty must be distinguishable from one read wrongly."""
    assert parse_relayed_event({"event": EVENT_JOIN}) is None
    assert parse_relayed_event({"event": EVENT_JOIN, "data": {}}) is None
    assert parse_relayed_event({"data": {"data": {}}}) is None
    assert parse_relayed_event("not a mapping") is None


def test_a_frame_with_no_participant_parses_but_names_nobody():
    """Shape is intact, content is not -- the caller decides to skip it."""
    event = parse_relayed_event({"event": EVENT_JOIN, "data": {"data": {}}})
    assert event is not None
    assert event.participant is None


def test_roster_events_are_the_ones_that_change_who_is_present():
    assert EVENT_JOIN in ROSTER_EVENTS
    assert EVENT_SPEECH_ON not in ROSTER_EVENTS
    assert ROSTER_EVENTS <= set(SUBSCRIBED_EVENTS)
