"""
tests/conversation_manager/core/test_unify_attachments.py
=========================================================

Unit tests for chat message attachment handling.

These tests verify:
- Attachment metadata is properly structured in events
- The transcript Message model carries attachments

RUNNING THESE TESTS:
    These are isolated unit tests that don't require the store.
    Run with --confcutdir to skip the parent conftest.py session hooks:

    .venv/bin/python -m pytest tests/conversation_manager/core/test_unify_attachments.py \\
        --confcutdir=tests/conversation_manager/core -v
"""

from __future__ import annotations

from datetime import datetime

from unify.conversation_manager.events import UnifyMessageReceived, UnifyMessageSent

REPORT_ATTACHMENT = {
    "filename": "report.pdf",
    "filepath": "Attachments/att-uuid-1_report.pdf",
    "content_type": "application/pdf",
    "size_bytes": 1024,
}
DATA_ATTACHMENT = {
    "filename": "data.xlsx",
    "filepath": "Attachments/att-uuid-2_data.xlsx",
    "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "size_bytes": 2048,
}


# =============================================================================
# Event Attachment Metadata Tests
# =============================================================================


class TestUnifyMessageReceivedAttachments:
    """Tests for UnifyMessageReceived event attachment handling."""

    def test_event_includes_attachment_dicts(self):
        """Attachments field contains full metadata dicts."""
        event = UnifyMessageReceived(
            contact={"contact_id": 1, "first_name": "Boss"},
            content="Here's the document",
            attachments=[REPORT_ATTACHMENT, DATA_ATTACHMENT],
        )

        assert len(event.attachments) == 2
        assert event.attachments[0]["filename"] == "report.pdf"
        assert event.attachments[1]["filename"] == "data.xlsx"
        assert event.content == "Here's the document"

    def test_event_with_empty_attachments(self):
        """Event works with no attachments."""
        event = UnifyMessageReceived(
            contact={"contact_id": 1, "first_name": "Boss"},
            content="Just a message",
        )

        assert event.attachments == []

    def test_event_includes_full_attachment_metadata(self):
        """Events carry the local attachment shape: filename, filepath,
        content_type and size_bytes."""
        event = UnifyMessageReceived(
            contact={"contact_id": 1, "first_name": "Boss"},
            content="Here's the document",
            attachments=[REPORT_ATTACHMENT],
        )

        attachment = event.attachments[0]
        assert isinstance(attachment, dict)
        assert attachment["filepath"] == "Attachments/att-uuid-1_report.pdf"
        assert attachment["content_type"] == "application/pdf"
        assert attachment["size_bytes"] == 1024

    def test_attachments_survive_json_round_trip(self):
        """Attachment dicts are preserved through to_json / from_json."""
        from unify.conversation_manager.events import Event

        event = UnifyMessageReceived(
            contact={"contact_id": 1, "first_name": "Boss"},
            content="Here's the document",
            attachments=[REPORT_ATTACHMENT],
        )

        restored = Event.from_json(event.to_json())

        assert isinstance(restored, UnifyMessageReceived)
        assert restored.attachments == [REPORT_ATTACHMENT]


class TestUnifyMessageSentAttachments:
    """Tests for UnifyMessageSent event attachment handling."""

    def test_event_includes_attachment_dicts(self):
        """Attachments field contains full metadata dicts."""
        event = UnifyMessageSent(
            contact={"contact_id": 1, "first_name": "Boss"},
            content="Sending you this file",
            attachments=[DATA_ATTACHMENT],
        )

        assert len(event.attachments) == 1
        assert event.attachments[0]["filename"] == "data.xlsx"

    def test_sent_event_defaults_to_no_attachments(self):
        """A plain outbound message carries an empty attachment list."""
        event = UnifyMessageSent(
            contact={"contact_id": 1, "first_name": "Boss"},
            content="Here's the export",
        )

        assert event.attachments == []


# =============================================================================
# Message Model Attachments Tests
# =============================================================================


class TestMessageModelAttachments:
    """Tests for Message model attachment field."""

    def test_message_has_attachments_field(self):
        """Message model has an attachments field."""
        from unify.transcript_manager.types.message import Message
        from unify.conversation_manager.cm_types import Medium

        msg = Message(
            message_id=1,
            medium=Medium.UNIFY_MESSAGE,
            sender_id=1,
            receiver_ids=[2],
            timestamp=datetime.now(),
            content="Here's a file",
            exchange_id=1,
            attachments=[REPORT_ATTACHMENT],
        )

        assert hasattr(msg, "attachments")
        assert len(msg.attachments) == 1
        assert msg.attachments[0]["filename"] == "report.pdf"

    def test_message_attachments_shorthand(self):
        """Message SHORTHAND_MAP includes attachments -> atts."""
        from unify.transcript_manager.types.message import Message

        assert "attachments" in Message.SHORTHAND_MAP
        assert Message.SHORTHAND_MAP["attachments"] == "atts"

    def test_message_to_post_json_includes_attachments(self):
        """to_post_json includes attachments in the payload."""
        from unify.transcript_manager.types.message import Message
        from unify.conversation_manager.cm_types import Medium

        msg = Message(
            message_id=1,
            medium=Medium.UNIFY_MESSAGE,
            sender_id=1,
            receiver_ids=[2],
            timestamp=datetime.now(),
            content="File attached",
            exchange_id=1,
            attachments=[REPORT_ATTACHMENT],
        )

        payload = msg.to_post_json()
        assert "attachments" in payload
        assert len(payload["attachments"]) == 1
