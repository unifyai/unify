from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from unify.contact_manager.types.contact import UNASSIGNED
from unify.conversation_manager.cm_types import Medium
from unify.conversation_manager.domains import managers_utils
from unify.conversation_manager.domains.whatsapp_history import (
    whatsapp_sent_history_content,
)
from unify.conversation_manager.events import WhatsAppSent


def test_whatsapp_template_history_renders_delivered_template_and_original():
    event = WhatsAppSent(
        contact={"contact_id": 1, "first_name": "Daniel"},
        content="The clue is Blade Runner.",
        via_template=True,
        delivered_content=(
            "Hello Daniel, this is T-W1N from Unify. I have a message for you. "
            "Reply here and I'll share the details!"
        ),
    )

    rendered = whatsapp_sent_history_content(event)

    assert "Hello Daniel, this is T-W1N from Unify" in rendered
    assert "The clue is Blade Runner." in rendered
    assert "template fallback" in rendered
    assert "pending resend" in rendered


def test_whatsapp_freeform_history_is_unchanged():
    event = WhatsAppSent(
        contact={"contact_id": 1, "first_name": "Daniel"},
        content="The clue is Blade Runner.",
        via_template=False,
        delivered_content="The clue is Blade Runner.",
    )

    assert whatsapp_sent_history_content(event) == "The clue is Blade Runner."


@pytest.mark.anyio
async def test_log_message_uses_template_history_for_whatsapp_sent(monkeypatch):
    logged_messages: list[dict] = []

    transcript_manager = SimpleNamespace(
        log_first_message_in_new_exchange=lambda message, destination=None: (
            logged_messages.append(message) or (123, 456)
        ),
    )
    cm = SimpleNamespace(
        contact_index=SimpleNamespace(
            get_contact=lambda contact_id=None, **kwargs: (
                {
                    "contact_id": 1,
                    "first_name": "Daniel",
                }
                if contact_id == 1
                else None
            ),
        ),
        transcript_manager=transcript_manager,
        call_manager=SimpleNamespace(
            call_exchange_id=UNASSIGNED,
            unify_meet_exchange_id=UNASSIGNED,
            google_meet_exchange_id=UNASSIGNED,
            teams_meet_exchange_id=UNASSIGNED,
        ),
        _local_to_global_message_ids={},
        _local_to_global_message_ids_by_destination={},
        _local_message_destinations={},
    )
    monkeypatch.setattr(managers_utils, "ensure_runtime_context", lambda: "test/ctx")
    monkeypatch.setattr(
        managers_utils.ContextRegistry,
        "implicit_shared_destinations",
        lambda: [None],
    )
    monkeypatch.setattr(
        managers_utils,
        "event_broker",
        SimpleNamespace(publish=AsyncMock()),
    )

    event = WhatsAppSent(
        contact={"contact_id": 1, "first_name": "Daniel"},
        content="The clue is Blade Runner.",
        via_template=True,
        delivered_content=(
            "Hello Daniel, this is T-W1N from Unify. I have a message for you. "
            "Reply here and I'll share the details!"
        ),
    )

    await managers_utils.log_message(cm, event)

    assert logged_messages
    assert logged_messages[0]["medium"] == Medium.WHATSAPP_MESSAGE
    assert "Hello Daniel, this is T-W1N from Unify" in logged_messages[0]["content"]
    assert "The clue is Blade Runner." in logged_messages[0]["content"]
    assert "template fallback" in logged_messages[0]["content"]
