"""Round-trip tests for canonical gateway envelope schemas.

These tests pin the wire format defined in
``unify/gateway/envelopes.py`` against representative payloads taken from
the existing ingress paths
(``unify/conversation_manager/local_ingress.py``). Channel migrations in
Phase B will extend the catalogue but the shapes covered here must remain
backward-compatible.
"""

from __future__ import annotations

import pytest

from unify.gateway.envelopes import (
    BaseEnvelope,
    EmailEnvelope,
    EmailReceivedEvent,
    GenericEnvelope,
    KNOWN_THREADS,
    SMSEnvelope,
    SMSReceivedEvent,
    SystemEventEnvelope,
    UnifyMessageEnvelope,
    UnifyMessageReceivedEvent,
    UnitySystemEvent,
    ValidationError,
    parse_envelope,
)


def test_known_threads_includes_every_thread_used_in_local_ingress() -> None:
    """If a new ``thread`` value lands in ingress, the catalogue must learn it."""
    expected = {
        "msg",
        "whatsapp",
        "email",
        "unify_message",
        "api_message",
        "unify_meet",
        "unity_system_event",
        "log_pre_hire_chats",
        "call",
        "call_answered",
        "call_not_answered",
        "whatsapp_call",
        "whatsapp_call_answered",
        "whatsapp_call_not_answered",
        "whatsapp_call_sent",
        "discord",
        "teams_chat",
        "teams_channel",
    }
    assert expected.issubset(KNOWN_THREADS)


def test_sms_envelope_roundtrip_matches_local_ingress_shape() -> None:
    raw = {
        "thread": "msg",
        "publish_timestamp": 1700000000.0,
        "event": {
            "assistant_id": "42",
            "contacts": [],
            "to_number": "+15555550000",
            "from_number": "+15555550100",
            "body": "hello",
        },
    }
    env = SMSEnvelope.model_validate(raw)
    assert env.event.body == "hello"
    assert env.thread == "msg"
    assert env.model_dump(by_alias=True)["event"]["from_number"] == "+15555550100"


def test_email_envelope_preserves_from_alias() -> None:
    """The wire field is ``from``; the Python attribute is ``from_``."""
    raw = {
        "thread": "email",
        "publish_timestamp": 1700000000.0,
        "event": {
            "assistant_id": "42",
            "contacts": [],
            "from": "sender@example.com",
            "subject": "hi",
            "body": "hello there",
            "email_id": "msg-1",
            "thread_id": "gmail-thread-1",
            "to": ["assistant@example.com"],
            "cc": [],
            "bcc": [],
            "attachments": [
                {
                    "id": "att-1",
                    "filename": "report.pdf",
                    "content_type": "application/pdf",
                    "size_bytes": 1234,
                },
            ],
        },
    }
    env = EmailEnvelope.model_validate(raw)
    assert env.event.from_ == "sender@example.com"
    assert env.event.thread_id == "gmail-thread-1"
    assert env.event.attachments[0].filename == "report.pdf"

    dumped = env.model_dump(by_alias=True)
    assert dumped["event"]["from"] == "sender@example.com"
    assert "from_" not in dumped["event"]


def test_unify_message_envelope_requires_contact_id() -> None:
    raw_ok = {
        "thread": "unify_message",
        "publish_timestamp": 1700000000.0,
        "event": {
            "assistant_id": "42",
            "contacts": [],
            "contact_id": 7,
            "body": "hi",
            "attachments": [],
        },
    }
    UnifyMessageEnvelope.model_validate(raw_ok)

    raw_missing = {**raw_ok, "event": {**raw_ok["event"]}}
    raw_missing["event"].pop("contact_id")
    with pytest.raises(ValidationError):
        UnifyMessageEnvelope.model_validate(raw_missing)


def test_unify_message_team_fanout_allows_missing_contact_id() -> None:
    """Team group-chat fan-out is ordinary unify_message traffic.

    The sender may not be this assistant's owner, so ``contact_id`` may be
    absent when ``team_id`` is set — the runtime resolves the sender against
    its Contacts table by email. Without ``team_id``, ``contact_id`` stays
    mandatory.
    """
    raw = {
        "thread": "unify_message",
        "publish_timestamp": 1700000000.0,
        "event": {
            "assistant_id": "42",
            "contacts": [],
            "body": "Morning everyone",
            "team_id": 3,
            "team_name": "Growth",
            "sender_user_id": "user-1",
            "sender_email": "dana@example.com",
            "sender_name": "Dana",
            "group_message_id": 7,
        },
    }
    env = parse_envelope(raw)
    assert isinstance(env, UnifyMessageEnvelope)
    assert env.event.contact_id is None
    assert env.event.team_id == 3
    assert env.event.sender_email == "dana@example.com"

    raw_no_team = {**raw, "event": {**raw["event"]}}
    raw_no_team["event"].pop("team_id")
    with pytest.raises(ValidationError):
        UnifyMessageEnvelope.model_validate(raw_no_team)


def test_unify_message_group_fanout_allows_missing_contact_id() -> None:
    """Org chat-group fan-out is ordinary unify_message traffic.

    Same contact_id omission rules as team_id: when ``group_id`` is set the
    runtime resolves the sender by email.
    """
    raw = {
        "thread": "unify_message",
        "publish_timestamp": 1700000000.0,
        "event": {
            "assistant_id": "42",
            "contacts": [],
            "body": "Hello group",
            "group_id": 9,
            "sender_user_id": "user-1",
            "sender_email": "dana@example.com",
            "sender_name": "Dana",
            "group_message_id": 11,
        },
    }
    env = parse_envelope(raw)
    assert isinstance(env, UnifyMessageEnvelope)
    assert env.event.contact_id is None
    assert env.event.group_id == 9
    assert env.event.sender_email == "dana@example.com"

    raw_no_group = {**raw, "event": {**raw["event"]}}
    raw_no_group["event"].pop("group_id")
    with pytest.raises(ValidationError):
        UnifyMessageEnvelope.model_validate(raw_no_group)


def test_system_event_envelope_requires_event_type() -> None:
    raw = {
        "thread": "unity_system_event",
        "publish_timestamp": 1700000000.0,
        "event": {
            "assistant_id": "42",
            "contacts": [],
            "event_type": "task_due",
            "task_id": 17,
            "source_task_log_id": 3,
            "activation_revision": "rev-1",
            "scheduled_for": "2026-01-01T00:00:00Z",
            "execution_mode": "live",
            "source_type": "scheduled",
            "binding_id": "binding-1",
            "desktop_url": "",
            "vm_type": "",
        },
    }
    env = SystemEventEnvelope.model_validate(raw)
    assert env.event.event_type == "task_due"
    assert env.event.task_id == 17


def test_parse_envelope_dispatches_known_threads_to_concrete_models() -> None:
    raw_sms = {
        "thread": "msg",
        "publish_timestamp": 1.0,
        "event": {
            "assistant_id": "1",
            "contacts": [],
            "to_number": "+15555550000",
            "from_number": "+15555550100",
            "body": "x",
        },
    }
    parsed = parse_envelope(raw_sms)
    assert isinstance(parsed, SMSEnvelope)


def test_parse_envelope_falls_back_to_generic_for_unknown_thread() -> None:
    raw = {
        "thread": "whatsapp",
        "publish_timestamp": 1.0,
        "event": {
            "assistant_id": "1",
            "contacts": [],
            "to_number": "whatsapp:+15555550000",
            "from_number": "whatsapp:+15555550100",
            "body": "x",
        },
    }
    parsed = parse_envelope(raw)
    assert isinstance(parsed, GenericEnvelope)
    assert parsed.thread == "whatsapp"


def test_envelope_rejects_extra_top_level_fields() -> None:
    raw = {
        "thread": "msg",
        "publish_timestamp": 1.0,
        "event": {
            "assistant_id": "1",
            "contacts": [],
            "to_number": "+1",
            "from_number": "+2",
            "body": "x",
        },
        "extra_top": "nope",
    }
    with pytest.raises(ValidationError):
        SMSEnvelope.model_validate(raw)


def test_event_payload_allows_extra_fields_for_forward_compat() -> None:
    """Channel migrations must be able to add fields without breaking older consumers."""
    raw = {
        "thread": "msg",
        "publish_timestamp": 1.0,
        "event": {
            "assistant_id": "1",
            "contacts": [],
            "to_number": "+1",
            "from_number": "+2",
            "body": "x",
            "future_field_added_by_phase_b": "fine",
        },
    }
    env = SMSEnvelope.model_validate(raw)
    assert env.event.model_extra == {"future_field_added_by_phase_b": "fine"}


def test_base_envelope_is_a_base_for_concrete_envelopes() -> None:
    assert issubclass(SMSEnvelope, BaseEnvelope)
    assert issubclass(EmailEnvelope, BaseEnvelope)
    assert issubclass(GenericEnvelope, BaseEnvelope)


def test_event_models_inherit_assistant_id_and_contacts_defaults() -> None:
    assert SMSReceivedEvent().assistant_id == ""
    assert SMSReceivedEvent().contacts == []
    assert EmailReceivedEvent().to == []
    assert UnitySystemEvent(event_type="x").event_type == "x"
    assert UnifyMessageReceivedEvent(contact_id=1).body == ""
