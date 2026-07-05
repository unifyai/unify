"""An agent-initiated (outbound) call must always open with a verbatim opener.

The slow brain passes ``opener`` to make_call/make_whatsapp_call; it lands on
``call_manager.pending_opener``. ``start_call`` must turn that into a
``simulated`` opening_config for every outbound call.
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
async def test_outbound_call_opens_with_simulated_opener():
    manager, captured = _manager_with_worker()
    manager.pending_opener = "Call Daniel to confirm tomorrow's 3pm demo."

    await manager.start_call(
        {"contact_id": 2},
        {"contact_id": 1},
        outbound=True,
        channel="whatsapp_call",
    )

    opening = captured["extra_metadata"]["opening_config"]
    assert opening["mode"] == "simulated"
    assert opening["simulated_utterance"] == (
        "Call Daniel to confirm tomorrow's 3pm demo."
    )
    assert captured["outbound"] is True
    assert manager.pending_opener == ""


@pytest.mark.asyncio
async def test_outbound_call_without_opener_still_uses_simulated_mode():
    manager, captured = _manager_with_worker()
    manager.pending_opener = ""

    await manager.start_call({"contact_id": 2}, {"contact_id": 1}, outbound=True)

    opening = captured["extra_metadata"]["opening_config"]
    assert opening["mode"] == "simulated"
    assert opening["simulated_utterance"] == ""


@pytest.mark.asyncio
async def test_inbound_call_does_not_force_an_opener():
    manager, captured = _manager_with_worker()
    manager.pending_opener = "Inbound context"

    await manager.start_call(
        {"contact_id": 2},
        {"contact_id": 1},
        outbound=False,
    )

    assert captured["extra_metadata"] is None


@pytest.mark.asyncio
async def test_outbound_unify_meet_uses_simulated_opener():
    manager, captured = _manager_with_worker()
    manager.pending_opener = "Hi — continuing onboarding on the live call."

    await manager.start_unify_meet(
        {"contact_id": 1},
        {"contact_id": 1},
        "unity_1_meet",
    )

    assert manager.is_outbound is True
    assert manager.pending_opener == ""


def test_opener_guardrails_handle_reply_to_hello():
    """The opener is held until the callee speaks, so the drafted line must read
    naturally both as a standalone opener AND as a reply to their "Hello?"."""
    from unify.conversation_manager.prompt_builders import (
        _BRIEFED_OPENING_GUARDRAIL,
        _OPENING_GREETING_GUARDRAIL,
    )

    for guardrail in (_OPENING_GREETING_GUARDRAIL, _BRIEFED_OPENING_GUARDRAIL):
        assert "Hello?" in guardrail
        flat = " ".join(guardrail.lower().split())
        assert "do not assume silence" in flat
