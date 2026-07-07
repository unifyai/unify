"""Unit tests for team-chat sender contact resolution.

Team group-chat fan-out can deliver messages from senders the receiving
assistant has never talked to (another member's assistant, or an org member
without a provisioned contact row). The comms manager resolves them by email
and provisions a responsive system contact on first message.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from unify.conversation_manager.comms_manager import (
    _get_or_create_team_chat_sender_contact,
)


def _stub_contact_manager(existing_contacts: list[dict]) -> MagicMock:
    manager = MagicMock()
    manager.filter_contacts.return_value = {"contacts": existing_contacts}
    manager._create_contact.return_value = {"details": {"contact_id": 7}}
    manager.get_contact_info.return_value = {
        7: {
            "contact_id": 7,
            "first_name": "Pat",
            "surname": "Peer",
            "email_address": "pat@assistants.unify.ai",
            "is_system": True,
        },
    }
    return manager


def test_existing_contact_is_returned_without_creation(monkeypatch) -> None:
    existing = {"contact_id": 3, "email_address": "pat@assistants.unify.ai"}
    manager = _stub_contact_manager([existing])
    monkeypatch.setattr(
        "unify.manager_registry.ManagerRegistry.get_contact_manager",
        lambda: manager,
    )

    contact = _get_or_create_team_chat_sender_contact(
        "Pat Peer",
        "pat@assistants.unify.ai",
    )

    assert contact == existing
    manager._create_contact.assert_not_called()


def test_unknown_teammate_is_provisioned_as_responsive_system_contact(
    monkeypatch,
) -> None:
    manager = _stub_contact_manager([])
    monkeypatch.setattr(
        "unify.manager_registry.ManagerRegistry.get_contact_manager",
        lambda: manager,
    )

    contact = _get_or_create_team_chat_sender_contact(
        "Pat Peer",
        "pat@assistants.unify.ai",
    )

    assert contact is not None
    assert contact["contact_id"] == 7
    create_kwargs = manager._create_contact.call_args.kwargs
    assert create_kwargs["first_name"] == "Pat"
    assert create_kwargs["surname"] == "Peer"
    assert create_kwargs["email_address"] == "pat@assistants.unify.ai"
    assert create_kwargs["should_respond"] is True
    assert create_kwargs["is_system"] is True


def test_missing_email_resolves_to_none(monkeypatch) -> None:
    manager = _stub_contact_manager([])
    monkeypatch.setattr(
        "unify.manager_registry.ManagerRegistry.get_contact_manager",
        lambda: manager,
    )

    assert _get_or_create_team_chat_sender_contact("Pat Peer", "") is None
    manager.filter_contacts.assert_not_called()
