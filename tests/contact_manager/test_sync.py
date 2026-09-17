import pytest

from unify.contact_manager.contact_manager import ContactManager
from unify.session_details import SESSION_DETAILS
from tests.helpers import _handle_project

# ---------------------------------------------------------------------------
#  Test-local fixture – ensure deterministic assistant state
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_cached_assistant(monkeypatch):
    """Force *unity* to behave as if no real assistant were configured.

    We reset assistant details so that every time a ``ContactManager``
    instance synchronises the assistant self contact it sees no populated name
    and therefore falls back to the dummy placeholder record.

    The fixture is *autouse* and therefore applies to every test in this
    module without having to be listed explicitly.
    """
    from unify.session_details import AssistantDetails

    monkeypatch.setattr(SESSION_DETAILS, "assistant", AssistantDetails())


def _configure_real_assistant(
    monkeypatch,
    *,
    agent_id: int,
    first_name: str,
    surname: str,
    number: str,
    email: str,
    about: str,
    timezone: str,
) -> None:
    """Configure SESSION_DETAILS with a populated assistant profile for sync tests."""
    from unify.session_details import AssistantDetails

    monkeypatch.setattr(SESSION_DETAILS, "_initialized", True)
    monkeypatch.setattr(
        SESSION_DETAILS,
        "assistant",
        AssistantDetails(
            agent_id=agent_id,
            first_name=first_name,
            surname=surname,
            number=number,
            email=email,
            about=about,
            timezone=timezone,
        ),
    )


@_handle_project
def test_dummy_assistant(monkeypatch):
    """When the account has no assistants, default system contacts are created."""
    from unify.session_details import (
        PLACEHOLDER_ASSISTANT_EMAIL,
        PLACEHOLDER_ASSISTANT_FIRST_NAME,
        PLACEHOLDER_ASSISTANT_PHONE,
        PLACEHOLDER_ASSISTANT_SURNAME,
    )

    cm = ContactManager()

    assistants = cm.filter_contacts(
        filter=f"contact_id == {SESSION_DETAILS.self_contact_id}",
    )["contacts"]
    assert len(assistants) == 1, "Exactly one assistant self contact should exist"

    a = assistants[0]
    assert a.first_name == PLACEHOLDER_ASSISTANT_FIRST_NAME
    assert a.surname == PLACEHOLDER_ASSISTANT_SURNAME
    assert a.email_address == PLACEHOLDER_ASSISTANT_EMAIL
    assert a.phone_number == PLACEHOLDER_ASSISTANT_PHONE
    # System contact timezone should be hard-coded to UTC for now
    assert a.timezone == "UTC"

    # Default user should also have UTC for now
    users = cm.filter_contacts(
        filter=f"contact_id == {SESSION_DETAILS.boss_contact_id}",
    )["contacts"]
    assert users, "Default user should exist"
    assert users[0].timezone == "UTC"


@_handle_project
def test_real_assistant(monkeypatch):
    """If a real assistant is configured, its details should populate the self contact."""
    _configure_real_assistant(
        monkeypatch,
        agent_id=123,
        first_name="Alice",
        surname="Smith",
        number="+15551234567",
        email="alice.smith@example.com",
        about="Helpful assistant",
        timezone="America/New_York",
    )

    cm = ContactManager()

    assistants = cm.filter_contacts(
        filter=f"contact_id == {SESSION_DETAILS.self_contact_id}",
    )["contacts"]
    assert len(assistants) == 1

    a = assistants[0]
    # Core fields mapped directly
    assert a.first_name == "Alice"
    assert a.surname == "Smith"
    assert a.email_address == "alice.smith@example.com"
    assert a.phone_number == "+15551234567"
    # Timezone should be synced from the assistant record
    assert a.timezone == "America/New_York"

    users = cm.filter_contacts(
        filter=f"contact_id == {SESSION_DETAILS.boss_contact_id}",
    )["contacts"]
    assert users, "Default user should exist"


@_handle_project
def test_real_assistant_accepts_digits_in_name_parts(monkeypatch):
    """System contact sync accepts assistant names that contain digits."""
    _configure_real_assistant(
        monkeypatch,
        agent_id=2016,
        first_name="Central1",
        surname="South Patch 1 Supervisor",
        number="+15551234567",
        email="central.patch1@example.com",
        about="Patch-level operational support assistant",
        timezone="Asia/Karachi",
    )

    cm = ContactManager()

    assistants = cm.filter_contacts(
        filter=f"contact_id == {SESSION_DETAILS.self_contact_id}",
    )["contacts"]
    assert len(assistants) == 1
    assistant = assistants[0]
    assert assistant.first_name == "Central1"
    assert assistant.surname == "South Patch 1 Supervisor"


@_handle_project
def test_system_contacts_have_is_system_flag(monkeypatch):
    """Assistant and user contacts should have is_system=True."""
    cm = ContactManager()

    assistants = cm.filter_contacts(
        filter=f"contact_id == {SESSION_DETAILS.self_contact_id}",
    )["contacts"]
    assert len(assistants) == 1, "Exactly one assistant self contact should exist"
    assert (
        assistants[0].is_system is True
    ), "Assistant contact should have is_system=True"

    users = cm.filter_contacts(
        filter=f"contact_id == {SESSION_DETAILS.boss_contact_id}",
    )["contacts"]
    assert len(users) == 1, "Exactly one boss contact should exist"
    assert users[0].is_system is True, "User contact should have is_system=True"


@_handle_project
def test_fresh_sync_creates_only_system_contacts(monkeypatch):
    """A fresh sync provisions exactly the assistant and user contacts."""
    cm = ContactManager()

    all_contacts = cm.filter_contacts()["contacts"]
    contact_ids = {c.contact_id for c in all_contacts}

    assert SESSION_DETAILS.self_contact_id in contact_ids, "Assistant should exist"
    assert SESSION_DETAILS.boss_contact_id in contact_ids, "User should exist"
    assert len(contact_ids) == 2, "Only assistant and user should exist"
