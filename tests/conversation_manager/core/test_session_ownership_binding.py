"""set_details binds team ownership from the platform's assistant record.

Session-config payloads (StartupEvent / AssistantUpdateEvent) deliver
``owner_team_id`` as a hint only. The record is authoritative: an omitted
value self-heals and a disagreement stops the session, so a payload that
forgot the field cannot silently route a team-owned assistant's shared
tables to the personal root.
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

import unisdk
from unify.session_details import SESSION_DETAILS

TEST_API_KEY = "test_key"  # pragma: allowlist secret


def _record(owner_team_id: int | None) -> list[dict]:
    return [{"agent_id": "456", "owner_team_id": owner_team_id}]


def _startup_payload(**overrides) -> dict:
    payload = {
        "api_key": TEST_API_KEY,
        "assistant_id": 456,
        "user_id": "user_1",
        "assistant_first_name": "Live",
        "assistant_surname": "Assistant",
        "assistant_age": "30",
        "assistant_nationality": "British",
        "assistant_about": "A live assistant",
        "assistant_number": "+15555559999",
        "assistant_email": "live@test.com",
        "user_first_name": "Live",
        "user_surname": "User",
        "user_number": "+15555558888",
        "user_email": "live_user@test.com",
        "voice_provider": "cartesia",
        "voice_id": "voice_123",
        "self_contact_id": 42,
        "boss_contact_id": 43,
    }
    payload.update(overrides)
    return payload


@pytest_asyncio.fixture
async def bare_cm():
    """A ConversationManager constructed idle, with session state restored."""
    from unify.conversation_manager.conversation_manager import ConversationManager
    from unify.conversation_manager.in_memory_event_broker import (
        create_in_memory_event_broker,
        reset_in_memory_event_broker,
    )

    reset_in_memory_event_broker()
    broker = create_in_memory_event_broker()
    prior_agent_id = SESSION_DETAILS.assistant.agent_id
    prior_owner_team_id = SESSION_DETAILS.assistant.owner_team_id
    cm = ConversationManager(
        event_broker=broker,
        job_name="test-ownership-binding-job",
        user_id="user_1",
        assistant_id=None,
        user_first_name="Test",
        user_surname="User",
        assistant_first_name="Test",
        assistant_surname="Assistant",
        assistant_age="25",
        assistant_nationality="American",
        assistant_about="Test bio",
        assistant_number="+15555550000",
        assistant_email="assistant@test.com",
        user_number="+15555551111",
        user_email="user@test.com",
        stop=asyncio.Event(),
    )
    yield cm
    SESSION_DETAILS.assistant.agent_id = prior_agent_id
    SESSION_DETAILS.assistant.owner_team_id = prior_owner_team_id
    reset_in_memory_event_broker()


@pytest.mark.real_ownership_binding
@pytest.mark.asyncio
async def test_set_details_heals_an_omitted_owner_from_the_record(
    bare_cm,
    monkeypatch,
):
    monkeypatch.setattr(unisdk, "list_assistants", lambda agent_id: _record(11))

    bare_cm.set_details(_startup_payload())

    assert SESSION_DETAILS.owner_team_id == 11
    assert bare_cm.owner_team_id == 11


@pytest.mark.real_ownership_binding
@pytest.mark.asyncio
async def test_set_details_split_brain_stops_the_session(bare_cm, monkeypatch):
    monkeypatch.setattr(unisdk, "list_assistants", lambda agent_id: _record(7))

    with pytest.raises(RuntimeError, match="split-brain"):
        bare_cm.set_details(_startup_payload(owner_team_id=11))


@pytest.mark.real_ownership_binding
@pytest.mark.asyncio
async def test_set_details_keeps_a_user_owned_assistant_personal(
    bare_cm,
    monkeypatch,
):
    monkeypatch.setattr(unisdk, "list_assistants", lambda agent_id: _record(None))

    bare_cm.set_details(_startup_payload())

    assert SESSION_DETAILS.owner_team_id is None
    assert bare_cm.owner_team_id is None
