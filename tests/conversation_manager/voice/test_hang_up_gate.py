"""Hang-up gate: the slow brain sanctions ending a call; the fast brain closes.

Covers the plumbing around the gate (no LLM calls):
- ``LivekitCallManager.set_hang_up_gate`` mirrors gate state to the voice agent
  over the ``app:call:status`` IPC channel and stores it CM-side.
- The voice agent's ``llm_node`` passes the gate reason into turn selection and
  marks a hang_up-classified reply for post-playout finalization.
- The ``FastBrainHangUp`` event handler clears the gate and tears the session
  down via the standard hang-up teardown.
- Session-ended events always clear the gate (it never outlives the call).
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from unify.conversation_manager.domains.call_manager import (
    CallConfig,
    LivekitCallManager,
)
from unify.conversation_manager.events import (
    FAST_BRAIN_TURN_HANG_UP,
    FastBrainHangUp,
)


@pytest.fixture
def boss_contact():
    return {
        "contact_id": 1,
        "first_name": "Test",
        "surname": "Boss",
        "phone_number": "+15555555555",
        "email_address": "boss@test.com",
    }


def _build_call_manager(event_broker) -> LivekitCallManager:
    cfg = CallConfig(
        assistant_id="42",
        user_id="user-1",
        assistant_bio="bio",
        assistant_number="+15555550000",
        voice_provider="elevenlabs",
        voice_id="voice-1",
        assistant_name="Assistant",
        job_name="job-1",
    )
    return LivekitCallManager(cfg, event_broker=event_broker)


@pytest.mark.asyncio
async def test_set_hang_up_gate_mirrors_state_to_voice_agent():
    broker = MagicMock()
    broker.publish = AsyncMock()
    manager = _build_call_manager(broker)
    assert manager.hang_up_gate_reason is None

    await manager.set_hang_up_gate("channel test complete — wrap up warmly")

    assert manager.hang_up_gate_reason == "channel test complete — wrap up warmly"
    channel, payload = broker.publish.await_args.args
    assert channel == "app:call:status"
    assert json.loads(payload) == {
        "type": "hang_up_gate",
        "armed": True,
        "reason": "channel test complete — wrap up warmly",
    }

    await manager.set_hang_up_gate(None)

    assert manager.hang_up_gate_reason is None
    channel, payload = broker.publish.await_args.args
    assert json.loads(payload) == {
        "type": "hang_up_gate",
        "armed": False,
        "reason": "",
    }


class TestVoiceAgentGatedTurn:
    """The gate reason flows into turn selection; hang_up marks the reply."""

    def _assistant(self, boss_contact):
        from unify.conversation_manager.medium_scripts.call import Assistant

        a = Assistant(
            contact=boss_contact,
            boss=boss_contact,
            channel="phone_call",
            instructions="x",
            outbound=False,
        )
        a.call_received = True
        a._capture_screenshots_for_llm = AsyncMock()
        a._request_idle_smalltalk_state = AsyncMock(return_value=False)
        a._publish_fast_brain_turn_completed = AsyncMock()
        return a

    @pytest.mark.asyncio
    async def test_gate_reason_passed_to_selection(self, boss_contact, monkeypatch):
        from livekit.agents import llm

        from unify.conversation_manager.domains.fast_brain_turn import (
            ResolvedFastBrainTurn,
        )
        from unify.conversation_manager.events import FAST_BRAIN_TURN_DEFER
        from unify.conversation_manager.medium_scripts import call as call_mod

        a = self._assistant(boss_contact)
        a._hang_up_gate_reason = "wrap up warmly"
        captured: dict = {}

        async def _resolved(*args, **kwargs):
            captured.update(kwargs)
            return ResolvedFastBrainTurn(
                classification=FAST_BRAIN_TURN_DEFER,
                intended_speech="One moment.",
            )

        monkeypatch.setattr(call_mod, "select_fast_brain_turn", _resolved)

        ctx = llm.ChatContext()
        ctx.add_message(role="user", content=["okay bye"])
        [chunk async for chunk in a.llm_node(ctx, [], None)]

        assert captured.get("hang_up_gate_reason") == "wrap up warmly"

    @pytest.mark.asyncio
    async def test_hang_up_classification_marks_pending_cut(
        self,
        boss_contact,
        monkeypatch,
    ):
        from livekit.agents import llm

        from unify.conversation_manager.domains.fast_brain_turn import (
            ResolvedFastBrainTurn,
        )
        from unify.conversation_manager.medium_scripts import call as call_mod

        a = self._assistant(boss_contact)
        a._hang_up_gate_reason = "wrap up"

        async def _resolved(*args, **kwargs):
            return ResolvedFastBrainTurn(
                classification=FAST_BRAIN_TURN_HANG_UP,
                intended_speech="Bye Dan — talk soon!",
            )

        monkeypatch.setattr(call_mod, "select_fast_brain_turn", _resolved)

        ctx = llm.ChatContext()
        ctx.add_message(role="user", content=["bye!"])
        chunks = [chunk async for chunk in a.llm_node(ctx, [], None)]

        # The farewell is spoken like any reply, and the pending-cut marker is
        # set for the speech_created observer to finalize after playout.
        assert len(chunks) == 1
        assert chunks[0].delta.content == "Bye Dan — talk soon!"
        assert a._pending_gated_hang_up == "Bye Dan — talk soon!"

        # The slow brain is informed of the closing turn.
        a._publish_fast_brain_turn_completed.assert_awaited_once()
        kwargs = a._publish_fast_brain_turn_completed.await_args.kwargs
        assert kwargs["classification"] == FAST_BRAIN_TURN_HANG_UP


class TestFastBrainHangUpHandler:
    def _cm(self):
        cm = MagicMock()
        cm.call_manager.hang_up_gate_reason = "wrap up"
        cm.call_manager.has_active_google_meet = False
        cm.call_manager.has_active_teams_meet = False
        cm.call_manager.end_call = AsyncMock()
        cm.call_manager.await_ready_for_outbound_call = AsyncMock(return_value=True)
        cm.notifications_bar = MagicMock()
        return cm

    @pytest.mark.asyncio
    async def test_handler_clears_gate_and_tears_down(self, monkeypatch):
        from unittest.mock import patch

        from unify.conversation_manager.domains.event_handlers import EventHandler

        cm = self._cm()
        event = FastBrainHangUp(
            contact={"contact_id": 1, "first_name": "Dan", "surname": "Lenton"},
            farewell="Bye Dan — talk soon!",
            trigger="user_turn",
            gate_reason="channel test complete",
        )

        with patch(
            "unify.conversation_manager.domains.brain_action_tools.get_event_broker",
        ) as mock_broker:
            mock_broker.return_value = MagicMock()
            mock_broker.return_value.publish = AsyncMock()
            await EventHandler.handle_event(event, cm)

        assert cm.call_manager.hang_up_gate_reason is None
        cm.call_manager.end_call.assert_awaited_once()
        cm.notifications_bar.push_notif.assert_called_once()


@pytest.mark.asyncio
async def test_call_ended_clears_hang_up_gate(monkeypatch):
    """The gate is per-session permission — any *Ended event clears it."""
    from unify.conversation_manager.domains.event_handlers import EventHandler
    from unify.conversation_manager.events import PhoneCallEnded

    cm = MagicMock()
    cm.call_manager.hang_up_gate_reason = "wrap up"
    cm.call_manager.call_exchange_id = -1
    cm.call_manager.unify_meet_exchange_id = -1
    cm.call_manager.google_meet_exchange_id = -1
    cm.call_manager.teams_meet_exchange_id = -1
    cm.call_manager.cleanup_call_proc = AsyncMock()
    cm.call_manager.cleanup_google_meet = AsyncMock()
    cm.call_manager.cleanup_teams_meet = AsyncMock()
    cm.contact_index = MagicMock()
    cm.contact_index.get_contact.return_value = {
        "contact_id": 1,
        "first_name": "Dan",
        "surname": "Lenton",
    }
    cm.notifications_bar = MagicMock()

    event = PhoneCallEnded(
        contact={"contact_id": 1, "first_name": "Dan", "surname": "Lenton"},
    )
    try:
        await EventHandler.handle_event(event, cm)
    except Exception:
        # Downstream cleanup on a MagicMock CM may fail past the gate reset;
        # only the gate-clearing contract matters here.
        pass

    assert cm.call_manager.hang_up_gate_reason is None
