"""Unit tests for the comms-activity presence side channel.

``comms_activity_payload`` decides whether a transcript message should nudge the
Console call-window avatar into its "working on a laptop" pose, and in which
direction. ``unify_message`` / ``unify_meet`` already have dedicated Console
surfaces and must be excluded.
"""

import pytest

from unify.transcript_manager.activity_sync import (
    COMMS_ACTIVITY_EXCLUDED_MEDIA,
    comms_activity_payload,
)

SELF_CONTACT_ID = 7
OTHER_CONTACT_ID = 42


@pytest.mark.parametrize("medium", COMMS_ACTIVITY_EXCLUDED_MEDIA)
def test_excluded_media_never_trigger(medium):
    assert comms_activity_payload(medium, OTHER_CONTACT_ID, SELF_CONTACT_ID) is None
    assert comms_activity_payload(medium, SELF_CONTACT_ID, SELF_CONTACT_ID) is None


def test_blank_medium_is_ignored():
    assert comms_activity_payload("", OTHER_CONTACT_ID, SELF_CONTACT_ID) is None
    assert comms_activity_payload(None, OTHER_CONTACT_ID, SELF_CONTACT_ID) is None


def test_outbound_when_sender_is_self():
    assert comms_activity_payload("email", SELF_CONTACT_ID, SELF_CONTACT_ID) == {
        "medium": "email",
        "direction": "outbound",
    }


def test_inbound_when_sender_is_a_contact():
    assert comms_activity_payload(
        "whatsapp_message",
        OTHER_CONTACT_ID,
        SELF_CONTACT_ID,
    ) == {"medium": "whatsapp_message", "direction": "inbound"}


def test_inbound_when_self_contact_unknown():
    # Without a known self-contact we cannot prove it is outbound, so default to
    # inbound rather than mislabelling.
    assert comms_activity_payload("sms_message", OTHER_CONTACT_ID, None) == {
        "medium": "sms_message",
        "direction": "inbound",
    }
