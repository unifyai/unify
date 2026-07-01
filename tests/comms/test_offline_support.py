from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from unify.comms import offline_support
from unify.comms import primitives as primitives_module
from unify.comms.primitives import CommsPrimitives
from unify.conversation_manager.cm_types import Medium
from unify.conversation_manager.domains import comms_utils
from unify.conversation_manager.events import Event, UnifyMessageSent, WhatsAppSent
from unify.task_scheduler.machine_state import (
    TaskOutboundOperationRecord,
    TaskOutboundOperationReference,
)


def _seed_offline_env(monkeypatch):
    monkeypatch.setenv(
        "UNITY_OFFLINE_TASK_RUN_KEY",
        "offline:scheduled:42:101:rev:once",
    )
    monkeypatch.setenv("UNITY_OFFLINE_TASK_ID", "101")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_SOURCE_TASK_LOG_ID", "555")
    monkeypatch.setattr(offline_support, "_OPERATION_COUNTER", 0)
    monkeypatch.setattr(offline_support.SESSION_DETAILS.assistant, "agent_id", 42)
    monkeypatch.setattr(offline_support.SESSION_DETAILS.assistant, "contact_id", 0)
    monkeypatch.setattr(
        offline_support.SESSION_DETAILS.assistant,
        "number",
        "+15555550000",
    )
    monkeypatch.setattr(
        offline_support.SESSION_DETAILS.assistant,
        "email",
        "assistant@test.com",
    )
    monkeypatch.setattr(
        offline_support.SESSION_DETAILS.assistant,
        "email_provider",
        "google_workspace",
    )
    monkeypatch.setattr(
        offline_support.SESSION_DETAILS.assistant,
        "whatsapp_number",
        "+15555550001",
    )
    monkeypatch.setattr(
        offline_support.SESSION_DETAILS.assistant,
        "first_name",
        "Offline",
        raising=False,
    )


def _stub_offline_tracking(monkeypatch, *, operation_key: str):
    updated_records: list[tuple[TaskOutboundOperationReference, dict]] = []
    transcript_calls: list[tuple[dict, dict]] = []

    monkeypatch.setattr(
        offline_support,
        "create_or_adopt_task_outbound_operation",
        lambda provenance, created_at=None: TaskOutboundOperationRecord(
            reference=TaskOutboundOperationReference(
                assistant_id="42",
                operation_key=operation_key,
            ),
            payload={"status": "pending"},
            created=True,
        ),
    )
    monkeypatch.setattr(
        offline_support,
        "update_task_outbound_operation_record",
        lambda reference, payload: updated_records.append((reference, payload)),
    )
    monkeypatch.setattr(
        offline_support,
        "ManagerRegistry",
        SimpleNamespace(
            get_transcript_manager=lambda: SimpleNamespace(
                log_first_message_in_new_exchange=lambda message, *, exchange_initial_metadata=None: (
                    transcript_calls.append((message, exchange_initial_metadata or {}))
                    or (77, 88)
                ),
            ),
        ),
    )
    return updated_records, transcript_calls


def test_missing_detail_error_is_coordinator_boss_aware(monkeypatch):
    comms = CommsPrimitives()
    monkeypatch.setattr(
        "unify.comms.primitives.SESSION_DETAILS.is_coordinator",
        True,
    )
    monkeypatch.setattr(
        "unify.comms.primitives.SESSION_DETAILS.boss_contact_id",
        1,
    )

    error, _ = comms._resolve_or_attach_detail(
        contact={"contact_id": 1, "first_name": "Boss", "surname": "User"},
        contact_id=1,
        field_name="whatsapp_number",
        inline_value=None,
        medium_label="WhatsApp",
    )

    assert error is not None
    assert "Update the boss contact first, then retry." in error
    assert "Provide `whatsapp_number`" not in error


def test_missing_detail_error_keeps_inline_guidance_for_regular_contacts(monkeypatch):
    comms = CommsPrimitives()
    monkeypatch.setattr(
        "unify.comms.primitives.SESSION_DETAILS.is_coordinator",
        False,
    )

    error, _ = comms._resolve_or_attach_detail(
        contact={"contact_id": 5, "first_name": "Alice", "surname": "Owner"},
        contact_id=5,
        field_name="whatsapp_number",
        inline_value=None,
        medium_label="WhatsApp",
    )

    assert error is not None
    assert "Provide `whatsapp_number` in this send" in error


def test_reserve_outbound_operation_dedupes_completed_row(monkeypatch):
    _seed_offline_env(monkeypatch)

    monkeypatch.setattr(
        offline_support,
        "create_or_adopt_task_outbound_operation",
        lambda provenance, created_at=None: TaskOutboundOperationRecord(
            reference=TaskOutboundOperationReference(
                assistant_id="42",
                operation_key="offline:scheduled:42:101:rev:once:op-1",
            ),
            payload={"status": "completed"},
            created=False,
        ),
    )

    decision = offline_support.reserve_outbound_operation(
        method_name="send_sms",
        medium=Medium.SMS_MESSAGE,
        target_kind="contact",
        target_metadata={"contact_id": 7, "phone_number": "+15555550123"},
        contact_id=7,
    )

    assert decision.reservation is not None
    assert decision.response == {"status": "ok", "deduped": True}


def test_finalize_outbound_operation_success_logs_history_and_updates_ledger(
    monkeypatch,
):
    _seed_offline_env(monkeypatch)

    fake_transcript_manager = SimpleNamespace()
    transcript_calls: list[tuple[dict, dict]] = []

    def _log_first_message_in_new_exchange(message, *, exchange_initial_metadata=None):
        transcript_calls.append((message, exchange_initial_metadata or {}))
        return (11, 22)

    fake_transcript_manager.log_first_message_in_new_exchange = (
        _log_first_message_in_new_exchange
    )
    monkeypatch.setattr(
        offline_support,
        "ManagerRegistry",
        SimpleNamespace(get_transcript_manager=lambda: fake_transcript_manager),
    )

    updates: list[tuple[TaskOutboundOperationReference, dict]] = []
    monkeypatch.setattr(
        offline_support,
        "update_task_outbound_operation_record",
        lambda reference, payload: updates.append((reference, payload)),
    )

    reservation = offline_support.OfflineOutboundReservation(
        reference=TaskOutboundOperationReference(
            assistant_id="42",
            operation_key="op-1",
        ),
        task_run_key="offline:scheduled:42:101:rev:once",
        operation_key="op-1",
        medium=Medium.EMAIL,
        target_kind="email",
        target_metadata={"to": ["alice@example.com"]},
    )

    offline_support.finalize_outbound_operation_success(
        reservation,
        attempted_content="Subject: Hello\n\nBody",
        receiver_ids=[7],
        target_metadata={"to": ["alice@example.com"]},
        metadata={"reply_to_email_id": "msg-1"},
        attachments=[{"id": "att-1", "filename": "brief.txt"}],
        provider_response={"id": "provider-1"},
    )

    assert transcript_calls
    message, exchange_metadata = transcript_calls[0]
    assert message["medium"] == Medium.EMAIL
    assert message["sender_id"] == 0
    assert message["receiver_ids"] == [7]
    assert message["content"] == "Subject: Hello\n\nBody"
    assert message["metadata"]["delivery_status"] == "completed"
    assert message["metadata"]["provider_message_id"] == "provider-1"
    assert message["metadata"]["task_run_key"] == "offline:scheduled:42:101:rev:once"
    assert exchange_metadata["offline_outbound"] is True
    assert exchange_metadata["operation_key"] == "op-1"

    assert updates
    reference, payload = updates[0]
    assert reference.operation_key == "op-1"
    assert payload["status"] == "completed"
    assert payload["provider_message_id"] == "provider-1"
    assert payload["history_exchange_id"] == 11
    assert payload["history_message_id"] == 22


def test_finalize_outbound_operation_failure_logs_history_and_updates_ledger(
    monkeypatch,
):
    _seed_offline_env(monkeypatch)

    fake_transcript_manager = SimpleNamespace()
    transcript_calls: list[tuple[dict, dict]] = []

    def _log_first_message_in_new_exchange(message, *, exchange_initial_metadata=None):
        transcript_calls.append((message, exchange_initial_metadata or {}))
        return (55, 66)

    fake_transcript_manager.log_first_message_in_new_exchange = (
        _log_first_message_in_new_exchange
    )
    monkeypatch.setattr(
        offline_support,
        "ManagerRegistry",
        SimpleNamespace(get_transcript_manager=lambda: fake_transcript_manager),
    )

    updates: list[tuple[TaskOutboundOperationReference, dict]] = []
    monkeypatch.setattr(
        offline_support,
        "update_task_outbound_operation_record",
        lambda reference, payload: updates.append((reference, payload)),
    )

    reservation = offline_support.OfflineOutboundReservation(
        reference=TaskOutboundOperationReference(
            assistant_id="42",
            operation_key="op-2",
        ),
        task_run_key="offline:scheduled:42:101:rev:once",
        operation_key="op-2",
        medium=Medium.SMS_MESSAGE,
        target_kind="contact",
        target_metadata={"contact_id": 7, "phone_number": "+15555550123"},
    )

    offline_support.finalize_outbound_operation_failure(
        reservation,
        error="Provider rejected the send.",
        attempted_content="Hello from offline",
        receiver_ids=[7],
        target_metadata={"contact_id": 7, "phone_number": "+15555550123"},
        metadata={"contact_display_name": "Alice Owner"},
    )

    assert transcript_calls
    message, exchange_metadata = transcript_calls[0]
    assert message["medium"] == Medium.SMS_MESSAGE
    assert message["receiver_ids"] == [7]
    assert message["content"].startswith("[Send Failed] Provider rejected the send.")
    assert message["metadata"]["delivery_status"] == "failed"
    assert message["metadata"]["error"] == "Provider rejected the send."
    assert exchange_metadata["operation_key"] == "op-2"

    assert updates
    reference, payload = updates[0]
    assert reference.operation_key == "op-2"
    assert payload["status"] == "failed"
    assert payload["error"] == "Provider rejected the send."
    assert payload["history_exchange_id"] == 55
    assert payload["history_message_id"] == 66


@pytest.mark.anyio
async def test_send_sms_offline_success_reserves_and_finalizes(monkeypatch):
    _seed_offline_env(monkeypatch)

    created_records: list[tuple[object, str | None]] = []
    updated_records: list[tuple[TaskOutboundOperationReference, dict]] = []
    transcript_calls: list[tuple[dict, dict]] = []

    monkeypatch.setattr(
        offline_support,
        "create_or_adopt_task_outbound_operation",
        lambda provenance, created_at=None: (
            created_records.append((provenance, created_at))
            or TaskOutboundOperationRecord(
                reference=TaskOutboundOperationReference(
                    assistant_id="42",
                    operation_key="op-1",
                ),
                payload={"status": "pending"},
                created=True,
            )
        ),
    )
    monkeypatch.setattr(
        offline_support,
        "update_task_outbound_operation_record",
        lambda reference, payload: updated_records.append((reference, payload)),
    )
    monkeypatch.setattr(
        offline_support,
        "ManagerRegistry",
        SimpleNamespace(
            get_transcript_manager=lambda: SimpleNamespace(
                log_first_message_in_new_exchange=lambda message, *, exchange_initial_metadata=None: (
                    transcript_calls.append((message, exchange_initial_metadata or {}))
                    or (33, 44)
                ),
            ),
        ),
    )

    comms = CommsPrimitives()
    comms._get_contact = lambda **kwargs: {
        "contact_id": 5,
        "first_name": "Alice",
        "surname": "Owner",
        "phone_number": "+15555550123",
        "should_respond": True,
    }
    comms._event_broker.publish = AsyncMock()

    async def _fake_send_sms_message_via_number(*, to_number: str, content: str):
        assert to_number == "+15555550123"
        assert content == "Hello from offline"
        return {"success": True, "sid": "sms-1"}

    monkeypatch.setattr(
        comms_utils,
        "send_sms_message_via_number",
        _fake_send_sms_message_via_number,
    )

    result = await comms.send_sms(contact_id=5, content="Hello from offline")

    assert result == {"status": "ok"}
    assert created_records
    assert transcript_calls
    assert updated_records
    assert updated_records[0][1]["status"] == "completed"
    assert updated_records[0][1]["provider_message_id"] == "sms-1"
    assert transcript_calls[0][0]["metadata"]["delivery_status"] == "completed"


@pytest.mark.anyio
async def test_send_sms_offline_reservation_uses_normalized_phone_number(monkeypatch):
    _seed_offline_env(monkeypatch)

    created_records: list[tuple[object, str | None]] = []
    updated_records: list[tuple[TaskOutboundOperationReference, dict]] = []
    transcript_calls: list[tuple[dict, dict]] = []
    contact_state = {
        "contact_id": 5,
        "first_name": "Alice",
        "surname": "Owner",
        "should_respond": True,
    }
    update_calls: list[tuple[int, dict]] = []

    monkeypatch.setattr(
        offline_support,
        "create_or_adopt_task_outbound_operation",
        lambda provenance, created_at=None: (
            created_records.append((provenance, created_at))
            or TaskOutboundOperationRecord(
                reference=TaskOutboundOperationReference(
                    assistant_id="42",
                    operation_key="op-normalized-sms",
                ),
                payload={"status": "pending"},
                created=True,
            )
        ),
    )
    monkeypatch.setattr(
        offline_support,
        "update_task_outbound_operation_record",
        lambda reference, payload: updated_records.append((reference, payload)),
    )
    monkeypatch.setattr(
        offline_support,
        "ManagerRegistry",
        SimpleNamespace(
            get_transcript_manager=lambda: SimpleNamespace(
                log_first_message_in_new_exchange=lambda message, *, exchange_initial_metadata=None: (
                    transcript_calls.append((message, exchange_initial_metadata or {}))
                    or (33, 44)
                ),
            ),
        ),
    )

    comms = CommsPrimitives()

    def _fake_get_contact(**kwargs):
        if kwargs.get("contact_id") == 5:
            return dict(contact_state)
        phone_number = kwargs.get("phone_number")
        if phone_number and contact_state.get("phone_number") == phone_number:
            return dict(contact_state)
        return None

    def _fake_update_contact(*, contact_id: int, **kwargs):
        update_calls.append((contact_id, dict(kwargs)))
        contact_state.update(kwargs)

    comms._get_contact = _fake_get_contact
    comms._contact_manager = lambda: SimpleNamespace(
        update_contact=_fake_update_contact,
    )
    comms._event_broker.publish = AsyncMock()

    async def _fake_send_sms_message_via_number(*, to_number: str, content: str):
        assert to_number == "+15555550123"
        assert content == "Hello from offline"
        return {"success": True, "sid": "sms-1"}

    monkeypatch.setattr(
        comms_utils,
        "send_sms_message_via_number",
        _fake_send_sms_message_via_number,
    )

    result = await comms.send_sms(
        contact_id=5,
        content="Hello from offline",
        phone_number="+15555550123",
    )

    assert result == {"status": "ok"}
    assert update_calls == [(5, {"phone_number": "+15555550123"})]
    assert created_records
    provenance, _created_at = created_records[0]
    assert provenance.target_metadata == {
        "contact_id": 5,
        "phone_number": "+15555550123",
    }
    assert updated_records
    assert transcript_calls


@pytest.mark.anyio
async def test_send_sms_offline_duplicate_skips_transport(monkeypatch):
    _seed_offline_env(monkeypatch)

    comms = CommsPrimitives()
    comms._get_contact = lambda **kwargs: {
        "contact_id": 5,
        "phone_number": "+15555550123",
        "should_respond": True,
    }
    comms._event_broker.publish = AsyncMock()

    monkeypatch.setattr(
        "unify.comms.primitives.reserve_outbound_operation",
        lambda **kwargs: offline_support.OfflineOutboundDecision(
            reservation=None,
            response={"status": "ok", "deduped": True},
        ),
    )

    send_mock = AsyncMock()
    monkeypatch.setattr(comms_utils, "send_sms_message_via_number", send_mock)

    result = await comms.send_sms(contact_id=5, content="Hello from offline")

    assert result == {"status": "ok", "deduped": True}
    send_mock.assert_not_awaited()


@pytest.mark.anyio
async def test_send_email_missing_assistant_email_does_not_reserve_or_attach(
    monkeypatch,
):
    comms = CommsPrimitives()
    comms._event_broker.publish = AsyncMock()
    comms._contact_manager = lambda: SimpleNamespace(
        update_contact=lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("contact details should not be attached"),
        ),
    )
    monkeypatch.setattr(comms, "_assistant_email", lambda: "")
    monkeypatch.setattr(
        "unify.comms.primitives.reserve_outbound_operation",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("offline reservation should not happen"),
        ),
    )
    send_mock = AsyncMock()
    monkeypatch.setattr(comms_utils, "send_email_via_address", send_mock)

    result = await comms.send_email(
        to=[{"contact_id": 5, "email_address": "alice@example.com"}],
        subject="Hello",
        body="Email body",
    )

    assert result["status"] == "error"
    assert "email address" in result["error"]
    send_mock.assert_not_awaited()


@pytest.mark.anyio
async def test_send_discord_channel_missing_bot_does_not_reserve_or_send(monkeypatch):
    comms = CommsPrimitives()
    comms._event_broker.publish = AsyncMock()
    monkeypatch.setattr(comms, "_assistant_discord_bot_id", lambda: "")
    monkeypatch.setattr(
        comms,
        "_assistant_anchor_contact",
        lambda: {
            "contact_id": 0,
            "first_name": "Offline",
            "surname": "Assistant",
        },
    )
    monkeypatch.setattr(
        "unify.comms.primitives.reserve_outbound_operation",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("offline reservation should not happen"),
        ),
    )
    send_mock = AsyncMock()
    monkeypatch.setattr(comms_utils, "send_discord_message", send_mock)

    result = await comms.send_discord_channel_message(
        channel_id="123456",
        content="Hello Discord",
    )

    assert result["status"] == "error"
    assert "Discord is not enabled" in result["error"]
    send_mock.assert_not_awaited()


@pytest.mark.anyio
async def test_send_whatsapp_template_offline_does_not_claim_pending_resend(
    monkeypatch,
):
    _seed_offline_env(monkeypatch)
    updated_records, transcript_calls = _stub_offline_tracking(
        monkeypatch,
        operation_key="op-wa-template",
    )

    comms = CommsPrimitives()
    comms._get_contact = lambda **kwargs: {
        "contact_id": 5,
        "first_name": "Alice",
        "surname": "Owner",
        "whatsapp_number": "+15555550123",
        "should_respond": True,
    }
    comms._event_broker.publish = AsyncMock()

    async def _fake_send_whatsapp_message(**kwargs):
        assert kwargs["to_number"] == "+15555550123"
        assert kwargs["content"] == "Hello from offline"
        return {"success": True, "method": "template", "sid": "wa-1"}

    monkeypatch.setattr(
        comms_utils,
        "send_whatsapp_message",
        _fake_send_whatsapp_message,
    )

    result = await comms.send_whatsapp(contact_id=5, content="Hello from offline")

    assert result["status"] == "ok"
    assert "pending_resend" not in result
    assert "not queued for automatic resend" in result["note"]
    assert transcript_calls
    assert updated_records
    assert updated_records[0][1]["status"] == "completed"


@pytest.mark.anyio
async def test_send_whatsapp_template_live_tracks_delivered_template_and_pending_resend(
    monkeypatch,
):
    pending_outbound = {
        "metadata": {
            "onboarding_trigger_step_id": "whatsapp-message-reference",
            "onboarding_reply_step_id": "whatsapp-message",
            "onboarding_request_id": "req-1",
            "onboarding_origin_event_id": "evt-1",
        },
    }

    def consume_pending_onboarding_outbound(medium):
        assert medium == "whatsapp_message"
        return pending_outbound.pop("metadata", None)

    cm = SimpleNamespace(
        _pending_whatsapp_resends={},
        _pending_whatsapp_resend_onboarding_metadata={},
        consume_pending_onboarding_outbound=consume_pending_onboarding_outbound,
    )
    comms = CommsPrimitives(conversation_manager=cm)
    comms._get_contact = lambda **kwargs: {
        "contact_id": 5,
        "first_name": "Alice",
        "surname": "Owner",
        "whatsapp_number": "+15555550123",
        "should_respond": True,
    }
    comms._event_broker.publish = AsyncMock()
    monkeypatch.setattr(comms, "_assistant_whatsapp_number", lambda: "+15555550001")
    monkeypatch.setattr(
        "unify.comms.primitives.SESSION_DETAILS.assistant.first_name",
        "T-W1N",
        raising=False,
    )
    monkeypatch.setattr(
        "unify.comms.primitives.SESSION_DETAILS.assistant.agent_id",
        42,
        raising=False,
    )

    responses = [
        {
            "success": True,
            "method": "template",
            "delivered_body": (
                "Hello Alice, this is T-W1N from Unify. I have a message for you. "
                "Reply here and I'll share the details!"
            ),
        },
        {
            "success": True,
            "method": "freeform",
            "delivered_body": "Original clue",
        },
    ]

    async def _fake_send_whatsapp_message(**kwargs):
        assert kwargs["content"] == "Original clue"
        return responses.pop(0)

    monkeypatch.setattr(
        comms_utils,
        "send_whatsapp_message",
        _fake_send_whatsapp_message,
    )

    result = await comms.send_whatsapp(contact_id=5, content="Original clue")

    assert result["status"] == "ok"
    assert result["pending_resend"] is True
    assert cm._pending_whatsapp_resends[5] == "Original clue"

    published = Event.from_json(comms._event_broker.publish.await_args.args[1])
    assert isinstance(published, WhatsAppSent)
    assert published.via_template is True
    assert published.content == "Original clue"
    assert published.delivered_content.startswith("Hello Alice, this is T-W1N")
    assert published.onboarding_trigger_step_id is None
    assert cm._pending_whatsapp_resend_onboarding_metadata[5] == {
        "onboarding_trigger_step_id": "whatsapp-message-reference",
        "onboarding_reply_step_id": "whatsapp-message",
        "onboarding_request_id": "req-1",
        "onboarding_origin_event_id": "evt-1",
    }

    cm._pending_whatsapp_resends.pop(5)
    result = await comms.send_whatsapp(contact_id=5, content="Original clue")

    assert result["status"] == "ok"
    published = Event.from_json(comms._event_broker.publish.await_args.args[1])
    assert isinstance(published, WhatsAppSent)
    assert published.via_template is False
    assert published.content == "Original clue"
    assert published.onboarding_trigger_step_id == "whatsapp-message-reference"
    assert published.onboarding_reply_step_id == "whatsapp-message"
    assert published.onboarding_request_id == "req-1"
    assert published.onboarding_origin_event_id == "evt-1"
    assert cm._pending_whatsapp_resend_onboarding_metadata == {}
    assert pending_outbound == {}


@pytest.mark.anyio
async def test_send_unify_message_live_stamps_workspace_demo_onboarding_metadata(
    monkeypatch,
):
    # A workspace-demo click arms a pending onboarding outbound on the channel
    # ``unify_message``; the next unify_message send must carry the onboarding
    # metadata so Orchestra can derive the demo step as complete.
    pending_outbound = {
        "metadata": {
            "onboarding_trigger_step_id": "workspace-mailbox",
            "onboarding_reply_step_id": "",
            "onboarding_request_id": "req-9",
            "onboarding_origin_event_id": "evt-9",
        },
    }

    def consume_pending_onboarding_outbound(medium):
        assert medium == "unify_message"
        return pending_outbound.pop("metadata", None)

    cm = SimpleNamespace(
        consume_pending_onboarding_outbound=consume_pending_onboarding_outbound,
    )
    comms = CommsPrimitives(conversation_manager=cm)
    comms._get_contact = lambda **kwargs: {
        "contact_id": 5,
        "first_name": "Alice",
        "surname": "Owner",
        "should_respond": True,
    }
    comms._event_broker.publish = AsyncMock()

    async def _fake_send_unify_message(**kwargs):
        assert kwargs["content"] == "Mailbox summary"
        assert kwargs["contact_id"] == 5
        return {"success": True}

    monkeypatch.setattr(
        comms_utils,
        "send_unify_message",
        _fake_send_unify_message,
    )

    result = await comms.send_unify_message(content="Mailbox summary", contact_id=5)

    assert result == {"status": "ok"}
    published = Event.from_json(comms._event_broker.publish.await_args.args[1])
    assert isinstance(published, UnifyMessageSent)
    assert published.content == "Mailbox summary"
    assert published.onboarding_trigger_step_id == "workspace-mailbox"
    assert published.onboarding_reply_step_id == ""
    assert published.onboarding_request_id == "req-9"
    assert published.onboarding_origin_event_id == "evt-9"
    # The pending outbound is consumed exactly once.
    assert pending_outbound == {}


@pytest.mark.anyio
async def test_make_whatsapp_call_invite_offline_does_not_claim_pending_callback(
    monkeypatch,
):
    _seed_offline_env(monkeypatch)
    updated_records, transcript_calls = _stub_offline_tracking(
        monkeypatch,
        operation_key="op-wa-call",
    )

    comms = CommsPrimitives()
    comms._get_contact = lambda **kwargs: {
        "contact_id": 5,
        "first_name": "Alice",
        "surname": "Owner",
        "whatsapp_number": "+15555550123",
        "should_respond": True,
    }
    comms._event_broker.publish = AsyncMock()

    monkeypatch.setattr(
        "unify.conversation_manager.domains.call_manager.make_room_name",
        lambda assistant_id, medium: f"{assistant_id}-{medium}",
    )

    async def _fake_start_whatsapp_call(**kwargs):
        assert kwargs["to_number"] == "+15555550123"
        return {"success": True, "method": "invite", "call_sid": "wa-call-1"}

    monkeypatch.setattr(
        comms_utils,
        "start_whatsapp_call",
        _fake_start_whatsapp_call,
    )

    result = await comms.make_whatsapp_call(
        contact_id=5,
        context="Ask whether Alice is free to chat now.",
    )

    assert result["status"] == "ok"
    assert "pending_callback" not in result
    assert "not queued with your briefing context" in result["note"]
    assert transcript_calls
    assert updated_records
    assert updated_records[0][1]["status"] == "completed"


@pytest.mark.anyio
async def test_make_whatsapp_call_live_selfhost_requests_permission_probe(monkeypatch):
    monkeypatch.setenv("SELF_HOST", "1")
    monkeypatch.setattr(comms_utils.SESSION_DETAILS.assistant, "agent_id", 42)
    monkeypatch.setattr(
        comms_utils.SESSION_DETAILS.assistant,
        "first_name",
        "T-W1N",
        raising=False,
    )
    seen_kwargs = {}
    cm = SimpleNamespace(
        call_manager=SimpleNamespace(
            has_active_call=False,
            has_active_google_meet=False,
            has_active_teams_meet=False,
            _whatsapp_call_joining=False,
        ),
        _pending_whatsapp_call_contexts={},
        assistant_whatsapp_number="+15555550001",
    )
    comms = CommsPrimitives(conversation_manager=cm)
    comms._get_contact = lambda **kwargs: {
        "contact_id": 5,
        "first_name": "Alice",
        "surname": "Owner",
        "whatsapp_number": "+15555550123",
        "should_respond": True,
    }
    comms._event_broker.publish = AsyncMock()

    async def _fake_start_whatsapp_call(**kwargs):
        seen_kwargs.update(kwargs)
        return {
            "success": True,
            "method": "invite_pending",
            "pool_number": "+15555550001",
        }

    monkeypatch.setattr(comms_utils, "start_whatsapp_call", _fake_start_whatsapp_call)
    monkeypatch.setattr(
        comms_utils,
        "store_pending_whatsapp_call_intent",
        AsyncMock(),
    )

    result = await comms.make_whatsapp_call(contact_id=5, context="Call Alice.")

    assert result["status"] == "ok"
    assert seen_kwargs["allow_permission_probe"] is True
    assert seen_kwargs["pending_call_context"] == "Call Alice."


@pytest.mark.anyio
async def test_make_whatsapp_call_live_hosted_does_not_probe(monkeypatch):
    for name in (
        "SELF_HOST",
        "NEXT_PUBLIC_SELF_HOST",
        "WHATSAPP_CALL_PERMISSION_PROBE_ENABLED",
        "ORCHESTRA_URL",
        "COMMUNICATION_URL",
        "UNITY_COMMS_URL",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(comms_utils.SESSION_DETAILS.assistant, "agent_id", 42)
    monkeypatch.setattr(
        comms_utils.SESSION_DETAILS.assistant,
        "first_name",
        "T-W1N",
        raising=False,
    )
    seen_kwargs = {}
    cm = SimpleNamespace(
        call_manager=SimpleNamespace(
            has_active_call=False,
            has_active_google_meet=False,
            has_active_teams_meet=False,
            _whatsapp_call_joining=False,
        ),
        _pending_whatsapp_call_contexts={},
        assistant_whatsapp_number="+15555550001",
    )
    comms = CommsPrimitives(conversation_manager=cm)
    comms._get_contact = lambda **kwargs: {
        "contact_id": 5,
        "first_name": "Alice",
        "surname": "Owner",
        "whatsapp_number": "+15555550123",
        "should_respond": True,
    }
    comms._event_broker.publish = AsyncMock()

    async def _fake_start_whatsapp_call(**kwargs):
        seen_kwargs.update(kwargs)
        return {
            "success": True,
            "method": "invite_pending",
            "pool_number": "+15555550001",
        }

    monkeypatch.setattr(comms_utils, "start_whatsapp_call", _fake_start_whatsapp_call)
    monkeypatch.setattr(
        comms_utils,
        "store_pending_whatsapp_call_intent",
        AsyncMock(),
    )

    result = await comms.make_whatsapp_call(contact_id=5, context="Call Alice.")

    assert result["status"] == "ok"
    assert seen_kwargs["allow_permission_probe"] is False
    assert seen_kwargs["pending_call_context"] == "Call Alice."


@pytest.mark.anyio
async def test_make_whatsapp_call_waits_for_voice_session_to_clear(monkeypatch):
    monkeypatch.setenv("SELF_HOST", "1")
    monkeypatch.setattr(comms_utils.SESSION_DETAILS.assistant, "agent_id", 42)
    monkeypatch.setattr(
        comms_utils.SESSION_DETAILS.assistant,
        "first_name",
        "T-W1N",
        raising=False,
    )
    monkeypatch.setattr(
        primitives_module,
        "_VOICE_SESSION_CLEAR_TIMEOUT_SECONDS",
        0.2,
    )
    monkeypatch.setattr(
        primitives_module,
        "_VOICE_SESSION_CLEAR_POLL_SECONDS",
        0.01,
    )

    class ClearingCallManager:
        has_active_google_meet = False
        has_active_teams_meet = False
        _whatsapp_call_joining = False

        def __init__(self) -> None:
            self.polls = 0

        @property
        def has_active_call(self) -> bool:
            self.polls += 1
            return self.polls < 3

        def _clear_stale_dispatch_state(self) -> bool:
            return False

    call_manager = ClearingCallManager()
    cm = SimpleNamespace(
        call_manager=call_manager,
        _pending_whatsapp_call_contexts={},
        assistant_whatsapp_number="+15555550001",
    )
    comms = CommsPrimitives(conversation_manager=cm)
    comms._get_contact = lambda **kwargs: {
        "contact_id": 5,
        "first_name": "Alice",
        "surname": "Owner",
        "whatsapp_number": "+15555550123",
        "should_respond": True,
    }
    comms._event_broker.publish = AsyncMock()
    seen_kwargs = {}

    async def _fake_start_whatsapp_call(**kwargs):
        seen_kwargs.update(kwargs)
        return {"success": True, "method": "direct", "pool_number": "+15555550001"}

    monkeypatch.setattr(comms_utils, "start_whatsapp_call", _fake_start_whatsapp_call)

    result = await comms.make_whatsapp_call(contact_id=5, context="Call Alice.")

    assert result["status"] == "ok"
    assert call_manager.polls >= 3
    assert seen_kwargs["pending_call_context"] == "Call Alice."


@pytest.mark.anyio
async def test_make_whatsapp_call_returns_retry_later_if_voice_session_stays_active(
    monkeypatch,
):
    monkeypatch.setattr(
        primitives_module,
        "_VOICE_SESSION_CLEAR_TIMEOUT_SECONDS",
        0.02,
    )
    monkeypatch.setattr(
        primitives_module,
        "_VOICE_SESSION_CLEAR_POLL_SECONDS",
        0.01,
    )

    call_manager = SimpleNamespace(
        has_active_call=True,
        has_active_google_meet=False,
        has_active_teams_meet=False,
        _whatsapp_call_joining=False,
        _clear_stale_dispatch_state=lambda: False,
    )
    cm = SimpleNamespace(
        call_manager=call_manager,
        _pending_whatsapp_call_contexts={},
        assistant_whatsapp_number="+15555550001",
    )
    comms = CommsPrimitives(conversation_manager=cm)
    called = False

    async def _fake_start_whatsapp_call(**kwargs):
        nonlocal called
        called = True
        return {"success": True}

    monkeypatch.setattr(comms_utils, "start_whatsapp_call", _fake_start_whatsapp_call)

    result = await comms.make_whatsapp_call(contact_id=5, context="Call Alice.")

    assert result["status"] == "retry_later_active_voice_session"
    assert called is False
