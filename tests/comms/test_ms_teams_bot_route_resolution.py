"""Teams bot replies resolve their own conversation when none is in context.

The Bot Framework channel routes on ``(tenant_id, conversation_id)``, which
the live renderer surfaces on an inbound message. A headless run has no
rendered thread, so without resolution it refuses every Teams send — the
failure these tests exist to prevent. Resolution is layered: live session
state, then the durable conversation exchange, then the server-side route
table, and a refusal only once all three come up empty.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from unify.comms import offline_support
from unify.comms.primitives import CommsPrimitives
from unify.conversation_manager.cm_types import Medium
from unify.conversation_manager.domains import comms_utils

TEST_SELF_CONTACT_ID = 337
TEST_CONTACT_ID = 1


def _make_comms(monkeypatch) -> CommsPrimitives:
    monkeypatch.setattr(
        "unify.comms.primitives.SESSION_DETAILS.self_contact_id",
        TEST_SELF_CONTACT_ID,
    )
    comms = CommsPrimitives()
    monkeypatch.setattr(
        "unify.comms.primitives.reserve_outbound_operation",
        lambda **kwargs: offline_support.OfflineOutboundDecision(
            reservation=None,
            response=None,
        ),
    )
    comms._get_contact = lambda **kwargs: {
        "contact_id": TEST_CONTACT_ID,
        "first_name": "Julia",
        "surname": "Goh",
        "email_address": "julia@example.com",
        "should_respond": True,
    }
    comms._event_broker.publish = AsyncMock()
    return comms


def _stub_transport(monkeypatch) -> list[dict]:
    """Capture Teams bot sends instead of performing them."""
    sends: list[dict] = []

    async def _fake_send(*, tenant_id, conversation_id, body):
        sends.append(
            {
                "tenant_id": tenant_id,
                "conversation_id": conversation_id,
                "body": body,
            },
        )
        return {"success": True}

    monkeypatch.setattr(comms_utils, "send_ms_teams_bot_message", _fake_send)
    return sends


def _stub_exchange(monkeypatch, metadata: dict | None) -> None:
    """Stand in for the durable Transcripts exchange lookup."""
    transcript_manager = SimpleNamespace(
        resolve_exchange_id_by_metadata=lambda key, value: (
            91 if metadata is not None else None
        ),
        get_exchange_metadata=lambda exchange_id: SimpleNamespace(
            metadata=metadata or {},
        ),
    )
    monkeypatch.setattr(
        "unify.comms.primitives.ManagerRegistry",
        SimpleNamespace(get_transcript_manager=lambda: transcript_manager),
    )


def _stub_route(monkeypatch, route: dict | None) -> None:
    async def _fake_lookup(**kwargs):
        return route

    monkeypatch.setattr(
        comms_utils,
        "find_ms_teams_bot_conversation_route",
        _fake_lookup,
    )


@pytest.mark.anyio
async def test_explicit_ids_are_used_verbatim(monkeypatch):
    """A reply to an inbound message must not consult any lookup."""
    comms = _make_comms(monkeypatch)
    sends = _stub_transport(monkeypatch)

    def _explode(*args, **kwargs):
        raise AssertionError("resolution ran despite explicit ids")

    monkeypatch.setattr(comms, "_stored_ms_teams_bot_route", _explode)

    result = await comms.send_ms_teams_bot_message(
        contact_id=TEST_CONTACT_ID,
        content="on my way",
        tenant_id="tenant-inbound",
        conversation_id="conv-inbound",
    )

    assert result == {"status": "ok"}
    assert sends == [
        {
            "tenant_id": "tenant-inbound",
            "conversation_id": "conv-inbound",
            "body": "on my way",
        },
    ]


@pytest.mark.anyio
async def test_resolves_from_durable_exchange_when_ids_omitted(monkeypatch):
    """The scheduled-task case: no ids in hand, conversation on record."""
    comms = _make_comms(monkeypatch)
    sends = _stub_transport(monkeypatch)
    _stub_exchange(
        monkeypatch,
        {"tenant_id": "tenant-stored", "conversation_id": "conv-stored"},
    )
    _stub_route(monkeypatch, None)

    result = await comms.send_ms_teams_bot_message(
        contact_id=TEST_CONTACT_ID,
        content="the report is ready",
    )

    assert result == {"status": "ok"}
    assert sends[0]["tenant_id"] == "tenant-stored"
    assert sends[0]["conversation_id"] == "conv-stored"


@pytest.mark.anyio
async def test_falls_back_to_route_table(monkeypatch):
    """No exchange (or one without routing) still reaches the route table."""
    comms = _make_comms(monkeypatch)
    sends = _stub_transport(monkeypatch)
    _stub_exchange(monkeypatch, None)
    _stub_route(
        monkeypatch,
        {"tenant_id": "tenant-routed", "conversation_id": "conv-routed"},
    )

    result = await comms.send_ms_teams_bot_message(
        contact_id=TEST_CONTACT_ID,
        content="standup moved",
    )

    assert result == {"status": "ok"}
    assert sends[0]["conversation_id"] == "conv-routed"


@pytest.mark.anyio
async def test_live_session_wins_over_stored(monkeypatch):
    """The conversation in progress beats whatever was last persisted."""
    comms = _make_comms(monkeypatch)
    sends = _stub_transport(monkeypatch)
    _stub_exchange(
        monkeypatch,
        {"tenant_id": "tenant-stored", "conversation_id": "conv-stored"},
    )
    _stub_route(monkeypatch, None)
    comms._cm = SimpleNamespace(
        _last_inbound_reply_context={
            "medium": Medium.MS_TEAMS_BOT_MESSAGE.value,
            "contact_id": TEST_CONTACT_ID,
            "tenant_id": "tenant-live",
            "conversation_id": "conv-live",
        },
        contact_index=SimpleNamespace(get_messages_for_contact=lambda *a, **k: []),
    )

    await comms.send_ms_teams_bot_message(
        contact_id=TEST_CONTACT_ID,
        content="right now",
    )

    assert sends[0]["conversation_id"] == "conv-live"


@pytest.mark.anyio
async def test_reply_context_for_another_contact_is_not_reused(monkeypatch):
    """Session state must not leak one person's conversation into another's."""
    comms = _make_comms(monkeypatch)
    sends = _stub_transport(monkeypatch)
    _stub_exchange(
        monkeypatch,
        {"tenant_id": "tenant-stored", "conversation_id": "conv-stored"},
    )
    _stub_route(monkeypatch, None)
    comms._cm = SimpleNamespace(
        _last_inbound_reply_context={
            "medium": Medium.MS_TEAMS_BOT_MESSAGE.value,
            "contact_id": TEST_CONTACT_ID + 99,
            "tenant_id": "tenant-someone-else",
            "conversation_id": "conv-someone-else",
        },
        contact_index=SimpleNamespace(get_messages_for_contact=lambda *a, **k: []),
    )

    await comms.send_ms_teams_bot_message(
        contact_id=TEST_CONTACT_ID,
        content="hello",
    )

    assert sends[0]["conversation_id"] == "conv-stored"


@pytest.mark.anyio
async def test_refuses_when_no_conversation_on_record(monkeypatch):
    """The bot cannot open a conversation, so this stays a hard refusal —
    and the error has to say why, not just that ids were missing."""
    comms = _make_comms(monkeypatch)
    sends = _stub_transport(monkeypatch)
    _stub_exchange(monkeypatch, None)
    _stub_route(monkeypatch, None)

    result = await comms.send_ms_teams_bot_message(
        contact_id=TEST_CONTACT_ID,
        content="are you around?",
    )

    assert result["status"] == "error"
    assert "Julia Goh" in result["error"]
    assert "cannot start a conversation" in result["error"]
    assert not sends


@pytest.mark.anyio
async def test_channel_send_still_requires_explicit_ids(monkeypatch):
    """A shared thread has no contact to resolve from — refuse, don't guess."""
    comms = _make_comms(monkeypatch)
    sends = _stub_transport(monkeypatch)
    _stub_exchange(
        monkeypatch,
        {"tenant_id": "tenant-stored", "conversation_id": "conv-stored"},
    )

    result = await comms.send_ms_teams_bot_channel_message(
        contact_id=TEST_CONTACT_ID,
        content="posting to the channel",
        tenant_id="",
        conversation_id="",
    )

    assert result["status"] == "error"
    assert not sends
