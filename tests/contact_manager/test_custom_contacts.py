"""Tests for custom contact collection and synchronization."""

import json

import pytest
from pathlib import Path

from unify.contact_manager.contact_manager import ContactManager
from unify.contact_manager.custom_contacts import (
    CONTACTS_JSONL_FILENAME,
    contact_entry_key,
    collect_contacts_from_directories,
    collect_custom_contacts,
    compute_custom_contacts_hash,
)
from unify.common.context_registry import ContextRegistry
from tests.helpers import _handle_project

_EXAMPLE_CONTACT_LINES = [
    {
        "key": "alice|smith",
        "first_name": "Alice",
        "surname": "Smith",
        "email_address": "alice@x.com",
    },
    {
        "key": "team|contact",
        "first_name": "Team",
        "surname": "Contact",
        "email_address": "team@x.com",
        "destination": "team:42",
    },
    {
        "key": "draft|user",
        "first_name": "Draft",
        "surname": "User",
        "email_address": "draft@x.com",
        "auto_sync": False,
    },
]


@pytest.fixture
def custom_contacts_dir(tmp_path: Path) -> Path:
    contacts_dir = tmp_path / "contacts"
    contacts_dir.mkdir()
    lines = "\n".join(json.dumps(row) for row in _EXAMPLE_CONTACT_LINES)
    (contacts_dir / CONTACTS_JSONL_FILENAME).write_text(lines + "\n")
    return contacts_dir


@pytest.fixture
def contact_manager_factory():
    managers = []

    def _create():
        ContextRegistry.forget(ContactManager, "Contacts")
        ContextRegistry.forget(ContactManager, "Contacts/Meta")
        cm = ContactManager()
        managers.append(cm)
        return cm

    yield _create

    for cm in managers:
        try:
            cm.clear()
        except Exception:
            pass


def test_contact_entry_key():
    assert contact_entry_key(first_name="Alice", surname="Smith") == "alice|smith"


def test_collect_custom_contacts_finds_entries(custom_contacts_dir):
    contacts = collect_custom_contacts(path=custom_contacts_dir)
    assert "alice|smith" in contacts
    assert "team|contact" in contacts


def test_collect_custom_contacts_excludes_auto_sync_false(custom_contacts_dir):
    contacts = collect_custom_contacts(path=custom_contacts_dir)
    assert "draft|user" not in contacts


def test_collect_custom_contacts_has_required_fields(custom_contacts_dir):
    entry = collect_custom_contacts(path=custom_contacts_dir)["alice|smith"]
    assert entry["custom_key"] == "alice|smith"
    assert entry["first_name"] == "Alice"
    assert entry["email_address"] == "alice@x.com"
    assert len(entry["custom_hash"]) == 16


def test_compute_custom_contacts_hash_is_deterministic(custom_contacts_dir):
    contacts = collect_custom_contacts(path=custom_contacts_dir)
    assert compute_custom_contacts_hash(
        source_contacts=contacts,
    ) == compute_custom_contacts_hash(source_contacts=contacts)


def test_collect_contacts_from_directories_later_dir_overrides(tmp_path):
    dir_a = tmp_path / "a"
    dir_a.mkdir()
    (dir_a / CONTACTS_JSONL_FILENAME).write_text(
        json.dumps(
            {
                "key": "shared|person",
                "first_name": "Shared",
                "surname": "Person",
                "email_address": "a@x.com",
            },
        )
        + "\n",
    )

    dir_b = tmp_path / "b"
    dir_b.mkdir()
    (dir_b / CONTACTS_JSONL_FILENAME).write_text(
        json.dumps(
            {
                "key": "shared|person",
                "first_name": "Shared",
                "surname": "Person",
                "email_address": "b@x.com",
            },
        )
        + "\n",
    )

    merged = collect_contacts_from_directories([dir_a, dir_b])
    assert merged["shared|person"]["email_address"] == "b@x.com"


@_handle_project
@pytest.mark.asyncio
async def test_sync_custom_contacts_inserts_new_entries(
    contact_manager_factory,
    custom_contacts_dir,
):
    cm = contact_manager_factory()
    source = collect_custom_contacts(path=custom_contacts_dir)
    assert cm.sync_custom_contacts(source_contacts=source) is True

    result = cm.filter_contacts(limit=100)
    emails = {contact.email_address for contact in result["contacts"]}
    assert "alice@x.com" in emails
    assert "team@x.com" in emails


@_handle_project
@pytest.mark.asyncio
async def test_sync_custom_contacts_is_idempotent(
    contact_manager_factory,
    custom_contacts_dir,
):
    cm = contact_manager_factory()
    source = collect_custom_contacts(path=custom_contacts_dir)

    assert cm.sync_custom_contacts(source_contacts=source) is True
    cm._custom_contacts_synced = False
    assert cm.sync_custom_contacts(source_contacts=source) is False


@_handle_project
@pytest.mark.asyncio
async def test_user_contact_without_custom_hash_is_preserved(
    contact_manager_factory,
    custom_contacts_dir,
):
    cm = contact_manager_factory()
    cm._create_contact(
        first_name="User",
        surname="Contact",
        email_address="user@x.com",
    )

    source = collect_custom_contacts(path=custom_contacts_dir)
    cm.sync_custom_contacts(source_contacts=source)

    result = cm.filter_contacts(limit=100)
    emails = {contact.email_address for contact in result["contacts"]}
    assert "user@x.com" in emails
    assert "alice@x.com" in emails
