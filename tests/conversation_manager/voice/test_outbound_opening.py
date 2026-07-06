"""An agent-initiated call must always open with a verbatim opener.

The slow brain passes ``opener`` to make_call/make_whatsapp_call; it lands on
``call_manager.pending_opener`` BEFORE the dial. ``start_call`` must turn that
into a spoken ``opener`` opening_config — including on inbound-shaped legs of
agent-initiated calls (the WhatsApp permission-callback dial-back) — and must
refuse an outbound call with no opener queued.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from unify.conversation_manager.domains.call_manager import (
    CallConfig,
    LivekitCallManager,
)


def _manager_with_worker() -> tuple[LivekitCallManager, dict]:
    cfg = CallConfig(
        assistant_id="1",
        user_id="user-1",
        assistant_bio="bio",
        assistant_number="+15555550000",
        voice_provider="elevenlabs",
        voice_id="voice-1",
        job_name="job-1",
    )
    manager = LivekitCallManager(cfg)
    proc = MagicMock()
    proc.poll.return_value = None  # worker alive -> persistent-worker dispatch path
    manager._worker_proc = proc

    captured: dict = {}

    async def _fake_dispatch(
        room_name,
        channel,
        contact,
        boss,
        outbound,
        *,
        extra_metadata=None,
    ):
        captured["extra_metadata"] = extra_metadata
        captured["outbound"] = outbound
        return True

    manager._dispatch_job = _fake_dispatch  # type: ignore[assignment]
    manager._ensure_socket_server = AsyncMock(return_value=None)  # type: ignore[assignment]
    socket = MagicMock()
    socket.set_forward_channels = AsyncMock()
    socket.queue_for_clients = AsyncMock()
    manager._socket_server = socket
    broker = MagicMock()
    broker.publish = AsyncMock()
    manager._event_broker = broker
    return manager, captured


@pytest.mark.asyncio
async def test_outbound_call_opens_with_verbatim_opener():
    manager, captured = _manager_with_worker()
    manager.pending_opener = "Call Daniel to confirm tomorrow's 3pm demo."

    await manager.start_call(
        {"contact_id": 2},
        {"contact_id": 1},
        outbound=True,
        channel="whatsapp_call",
    )

    opening = captured["extra_metadata"]["opening_config"]
    assert opening["mode"] == "opener"
    assert opening["opener_text"] == "Call Daniel to confirm tomorrow's 3pm demo."
    assert captured["outbound"] is True
    assert manager.pending_opener == ""


@pytest.mark.asyncio
async def test_outbound_call_without_opener_is_refused():
    manager, captured = _manager_with_worker()
    manager.pending_opener = ""

    with pytest.raises(RuntimeError, match="no verbatim opener"):
        await manager.start_call({"contact_id": 2}, {"contact_id": 1}, outbound=True)

    assert "extra_metadata" not in captured  # never dispatched


@pytest.mark.asyncio
async def test_inbound_leg_with_queued_opener_still_speaks_it():
    """The WhatsApp permission-callback call arrives inbound-shaped, but the
    opener queued when we tried to place the call must still be spoken."""
    manager, captured = _manager_with_worker()
    manager.pending_opener = "Hi Dan — quick quiz to test the WhatsApp channel."

    await manager.start_call(
        {"contact_id": 2},
        {"contact_id": 1},
        outbound=False,
        channel="whatsapp_call",
    )

    opening = captured["extra_metadata"]["opening_config"]
    assert opening["mode"] == "opener"
    assert opening["opener_text"] == (
        "Hi Dan — quick quiz to test the WhatsApp channel."
    )
    assert captured["outbound"] is False
    assert manager.pending_opener == ""


@pytest.mark.asyncio
async def test_inbound_call_without_opener_has_no_opening_config():
    manager, captured = _manager_with_worker()

    await manager.start_call(
        {"contact_id": 2},
        {"contact_id": 1},
        outbound=False,
    )

    assert captured["extra_metadata"] is None


@pytest.mark.asyncio
async def test_outbound_unify_meet_uses_verbatim_opener():
    manager, captured = _manager_with_worker()
    manager.pending_opener = "Hi — continuing onboarding on the live call."

    await manager.start_unify_meet(
        {"contact_id": 1},
        {"contact_id": 1},
        "unity_1_meet",
    )

    opening = captured["extra_metadata"]["opening_config"]
    assert opening["mode"] == "opener"
    assert opening["opener_text"] == "Hi — continuing onboarding on the live call."
    assert manager.is_outbound is True
    assert manager.pending_opener == ""


@pytest.mark.asyncio
async def test_outbound_call_carries_unspoken_briefing():
    manager, captured = _manager_with_worker()
    manager.pending_opener = "Hi Dan — quick quiz to test this channel."
    manager.pending_briefing = "Expected answer: Dune. Confirm warmly and wrap up."

    await manager.start_call(
        {"contact_id": 2},
        {"contact_id": 1},
        outbound=True,
        channel="whatsapp_call",
    )

    opening = captured["extra_metadata"]["opening_config"]
    assert opening["mode"] == "opener"
    assert opening["briefing"] == ("Expected answer: Dune. Confirm warmly and wrap up.")
    assert manager.pending_briefing == ""
    assert manager.active_call_briefing == (
        "Expected answer: Dune. Confirm warmly and wrap up."
    )


@pytest.mark.asyncio
async def test_unify_meet_ring_answer_reattaches_queued_briefing():
    """The meet-ring opener round-trips through the Console without the
    briefing; start_unify_meet must reattach the CM-side queued briefing."""
    manager, captured = _manager_with_worker()
    manager.pending_briefing = "Continue onboarding from the Slack step."

    await manager.start_unify_meet(
        {"contact_id": 1},
        {"contact_id": 1},
        "unity_1_meet",
        opening_config={
            "mode": "opener",
            "opener_text": "Hi — picking up where we left off.",
            "source": "unify_meet_ring",
        },
    )

    opening = captured["extra_metadata"]["opening_config"]
    assert opening["briefing"] == "Continue onboarding from the Slack step."
    assert manager.pending_briefing == ""
    assert manager.active_call_briefing == ("Continue onboarding from the Slack step.")
