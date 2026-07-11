"""Unit tests for team-chat sender contact resolution and peer selection.

Team group-chat fan-out can deliver messages from senders the receiving
assistant has never talked to (another member's assistant, or an org member
without a provisioned contact row). Assistant senders resolve by their stable
``agent_id`` — provisioned into teammate contacts at startup — so resolution
works even for assistants with no provisioned email; email is the fallback
identity, and a responsive system contact is provisioned on first message
when the startup sync has not covered the sender.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from unify.contact_manager.system_contacts import select_team_assistant_peers
from unify.conversation_manager.comms_manager import (
    _get_or_create_team_chat_sender_contact,
)


def _stub_contact_manager(contacts_by_filter: dict[str, list[dict]]) -> MagicMock:
    manager = MagicMock()

    def filter_contacts(*, filter: str, limit: int):
        return {"contacts": contacts_by_filter.get(filter, [])}

    manager.filter_contacts.side_effect = filter_contacts
    manager._create_contact.return_value = {"details": {"contact_id": 7}}
    manager.get_contact_info.return_value = {
        7: {
            "contact_id": 7,
            "first_name": "Pat",
            "surname": "Peer",
            "is_system": True,
        },
    }
    return manager


def _install(monkeypatch, manager: MagicMock) -> None:
    monkeypatch.setattr(
        "unify.manager_registry.ManagerRegistry.get_contact_manager",
        lambda: manager,
    )


def test_assistant_sender_resolves_by_agent_id_without_email(monkeypatch) -> None:
    existing = {"contact_id": 3, "agent_id": "777"}
    manager = _stub_contact_manager({"agent_id == '777'": [existing]})
    _install(monkeypatch, manager)

    contact = _get_or_create_team_chat_sender_contact("Pat Peer", "", 777)

    assert contact == existing
    manager._create_contact.assert_not_called()


def test_email_is_fallback_identity(monkeypatch) -> None:
    existing = {"contact_id": 4, "email_address": "pat@assistants.unify.ai"}
    manager = _stub_contact_manager(
        {"email_address == 'pat@assistants.unify.ai'": [existing]},
    )
    _install(monkeypatch, manager)

    contact = _get_or_create_team_chat_sender_contact(
        "Pat Peer",
        "pat@assistants.unify.ai",
        777,
    )

    assert contact == existing
    manager._create_contact.assert_not_called()


def test_unknown_emailless_assistant_is_provisioned_with_agent_id(
    monkeypatch,
) -> None:
    manager = _stub_contact_manager({})
    _install(monkeypatch, manager)

    contact = _get_or_create_team_chat_sender_contact("Pat Peer", "", 777)

    assert contact is not None
    assert contact["contact_id"] == 7
    create_kwargs = manager._create_contact.call_args.kwargs
    assert create_kwargs.get("custom_fields", {}).get("agent_id") == "777"
    assert create_kwargs["email_address"] is None
    assert create_kwargs["first_name"] == "Pat"
    assert create_kwargs["surname"] == "Peer"
    assert create_kwargs["should_respond"] is True
    assert create_kwargs["is_system"] is True


def test_no_identity_resolves_to_none(monkeypatch) -> None:
    manager = _stub_contact_manager({})
    _install(monkeypatch, manager)

    assert _get_or_create_team_chat_sender_contact("Pat Peer", "", None) is None
    manager.filter_contacts.assert_not_called()


def test_select_team_assistant_peers_filters_correctly() -> None:
    assistants = [
        {"agent_id": 1, "team_ids": [10], "is_coordinator": False},  # self
        {"agent_id": 2, "team_ids": [10], "is_coordinator": False},  # peer
        {"agent_id": 3, "team_ids": [10], "is_coordinator": True},  # coordinator
        {"agent_id": 4, "team_ids": [99], "is_coordinator": False},  # other team
        {"agent_id": 5, "team_ids": [10, 99], "is_coordinator": False},  # peer
    ]

    peers = select_team_assistant_peers(assistants, [10], 1)

    assert [peer["agent_id"] for peer in peers] == [2, 5]
    assert select_team_assistant_peers(assistants, [], 1) == []
