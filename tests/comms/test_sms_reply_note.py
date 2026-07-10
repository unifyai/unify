"""Short-code reply-to note appended to the opening SMS of a thread.

In A2P-regulated destinations Twilio can override the outbound sender with a
short code / alphanumeric sender ID the recipient cannot reply to. To keep a
working reply path, ``CommsPrimitives.send_sms`` embeds the assistant's real
inbound number in the body of the first SMS to a contact. These tests pin that
behaviour on the helper that carries the logic.
"""

from unittest.mock import AsyncMock, MagicMock

from unify.comms.primitives import CommsPrimitives
from unify.conversation_manager.cm_types import Medium

NUMBER = "+16592575615"


def _make_primitives(*, assistant_number: str = NUMBER, prior_sms=None):
    cm = MagicMock()
    cm.assistant_number = assistant_number
    cm.contact_index.get_messages_for_contact.return_value = list(prior_sms or [])
    return CommsPrimitives(conversation_manager=cm, event_broker=AsyncMock()), cm


def test_reply_note_appended_on_first_sms():
    primitives, _ = _make_primitives()
    out = primitives._apply_sms_reply_note("Hi there", 5)
    assert out.startswith("Hi there")
    assert NUMBER in out
    assert "short code" in out.lower()


def test_reply_note_not_repeated_for_same_contact():
    primitives, _ = _make_primitives()
    first = primitives._apply_sms_reply_note("first", 5)
    assert NUMBER in first
    second = primitives._apply_sms_reply_note("second", 5)
    assert second == "second"


def test_reply_note_skipped_when_number_already_present():
    primitives, _ = _make_primitives()
    body = f"reach me at {NUMBER} any time"
    assert primitives._apply_sms_reply_note(body, 5) == body


def test_reply_note_skipped_without_assistant_number():
    primitives, _ = _make_primitives(assistant_number="")
    assert primitives._apply_sms_reply_note("hello", 5) == "hello"


def test_reply_note_skipped_when_prior_sms_exists():
    primitives, cm = _make_primitives(prior_sms=[MagicMock()])
    assert primitives._apply_sms_reply_note("hello", 5) == "hello"
    cm.contact_index.get_messages_for_contact.assert_called_once_with(
        5,
        medium=Medium.SMS_MESSAGE,
    )


def test_offline_path_without_cm_treats_send_as_first():
    primitives = CommsPrimitives(event_broker=AsyncMock())
    assert primitives._is_first_sms_to_contact(9) is True
