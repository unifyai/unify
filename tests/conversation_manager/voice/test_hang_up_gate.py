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


# ---------------------------------------------------------------------------
# Pre-armed gate at call placement: the slow brain sanctions the close when it
# places an expected-short call; the gate rides the dispatch metadata so the
# voice agent starts armed with no IPC round trip.
# ---------------------------------------------------------------------------


def _manager_with_dispatch_capture() -> tuple[LivekitCallManager, dict]:
    manager = _build_call_manager(MagicMock())
    proc = MagicMock()
    proc.poll.return_value = None
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
    return manager, captured


@pytest.mark.asyncio
async def test_pre_armed_gate_rides_call_dispatch_metadata():
    manager, captured = _manager_with_dispatch_capture()
    manager.pending_opener = "Hi Dan — quick message for you."
    manager.pending_hang_up_gate = "Deliver the message, then wrap up."

    await manager.start_call(
        {"contact_id": 2},
        {"contact_id": 1},
        outbound=True,
        channel="whatsapp_call",
    )

    meta = captured["extra_metadata"]
    assert meta["hang_up_gate_reason"] == "Deliver the message, then wrap up."
    # Consumed into the CM-side mirror so tools/prompt/proactive-speech see it.
    assert manager.pending_hang_up_gate == ""
    assert manager.hang_up_gate_reason == "Deliver the message, then wrap up."


@pytest.mark.asyncio
async def test_call_without_pre_armed_gate_has_no_metadata_key():
    manager, captured = _manager_with_dispatch_capture()
    manager.pending_opener = "Hi Dan."

    await manager.start_call(
        {"contact_id": 2},
        {"contact_id": 1},
        outbound=True,
        channel="phone_call",
    )

    assert "hang_up_gate_reason" not in (captured["extra_metadata"] or {})
    assert manager.hang_up_gate_reason is None


@pytest.mark.asyncio
async def test_pre_armed_gate_reattached_on_meet_ring_answer():
    """The Console answer flow round-trips only the opener; the queued gate is
    consumed CM-side when the meet session starts."""
    manager, captured = _manager_with_dispatch_capture()
    manager.pending_hang_up_gate = "Quick check-in, wrap up after."

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

    meta = captured["extra_metadata"]
    assert meta["hang_up_gate_reason"] == "Quick check-in, wrap up after."
    assert manager.pending_hang_up_gate == ""
    assert manager.hang_up_gate_reason == "Quick check-in, wrap up after."


@pytest.mark.asyncio
async def test_cleanup_clears_pre_armed_gate_state():
    manager, _ = _manager_with_dispatch_capture()
    manager.pending_hang_up_gate = "stale"
    manager.hang_up_gate_reason = "stale"
    manager._socket_server = None

    await manager.cleanup_call_proc()

    assert manager.pending_hang_up_gate == ""
    assert manager.hang_up_gate_reason is None


class TestPreArmedToolFlow:
    """make_call / make_whatsapp_call queue the gate before dialing."""

    def _comms(self, cm):
        from unify.comms.primitives import CommsPrimitives

        comms = CommsPrimitives(conversation_manager=cm)
        comms._get_contact = lambda **kwargs: {
            "contact_id": 5,
            "first_name": "Alice",
            "surname": "Owner",
            "phone_number": "+15555550123",
            "whatsapp_number": "+15555550123",
            "should_respond": True,
        }
        comms._event_broker.publish = AsyncMock()
        return comms

    def _cm(self):
        from types import SimpleNamespace

        from unify.conversation_manager.events import WhatsAppCallSent

        call_manager = SimpleNamespace(
            has_active_call=False,
            has_active_google_meet=False,
            has_active_teams_meet=False,
            _whatsapp_call_joining=False,
            pending_opener="",
            pending_briefing="",
            pending_hang_up_gate="",
        )
        return SimpleNamespace(
            call_manager=call_manager,
            _pending_whatsapp_call_openers={},
            assistant_whatsapp_number="+15555550001",
            active_pending_onboarding_outbound=lambda: None,
            build_whatsapp_call_sent_event=lambda contact: WhatsAppCallSent(
                contact=contact,
            ),
        )

    @pytest.mark.asyncio
    async def test_allow_hang_up_queued_before_whatsapp_dial(self, monkeypatch):
        from unify.conversation_manager.domains import comms_utils

        cm = self._cm()
        comms = self._comms(cm)
        monkeypatch.setattr(
            comms_utils.SESSION_DETAILS.assistant,
            "agent_id",
            42,
        )
        seen_at_dial: dict = {}

        async def _fake_start_whatsapp_call(**kwargs):
            seen_at_dial["gate"] = cm.call_manager.pending_hang_up_gate
            return {"success": True, "method": "direct"}

        monkeypatch.setattr(
            comms_utils,
            "start_whatsapp_call",
            _fake_start_whatsapp_call,
        )

        result = await comms.make_whatsapp_call(
            contact_id=5,
            opener="Hi Alice — quick question.",
            allow_hang_up="One question, then wrap up.",
        )

        assert result["status"] == "ok"
        assert seen_at_dial["gate"] == "One question, then wrap up."

    @pytest.mark.asyncio
    async def test_gate_cleared_on_whatsapp_dial_failure(self, monkeypatch):
        from unify.conversation_manager.domains import comms_utils

        cm = self._cm()
        comms = self._comms(cm)
        monkeypatch.setattr(
            comms_utils.SESSION_DETAILS.assistant,
            "agent_id",
            42,
        )

        async def _fake_start_whatsapp_call(**kwargs):
            return {"success": False}

        monkeypatch.setattr(
            comms_utils,
            "start_whatsapp_call",
            _fake_start_whatsapp_call,
        )
        comms._surface_comms_error = AsyncMock(
            return_value={"status": "error", "error": "failed"},
        )

        await comms.make_whatsapp_call(
            contact_id=5,
            opener="Hi Alice.",
            allow_hang_up="wrap up",
        )

        assert cm.call_manager.pending_hang_up_gate == ""

    @pytest.mark.asyncio
    async def test_permission_invite_stashes_gate_for_callback(self, monkeypatch):
        from unify.conversation_manager.domains import comms_utils

        cm = self._cm()
        comms = self._comms(cm)
        monkeypatch.setattr(
            comms_utils.SESSION_DETAILS.assistant,
            "agent_id",
            42,
        )

        async def _fake_start_whatsapp_call(**kwargs):
            return {
                "success": True,
                "method": "invite",
                "pool_number": "+15555550001",
            }

        monkeypatch.setattr(
            comms_utils,
            "start_whatsapp_call",
            _fake_start_whatsapp_call,
        )
        monkeypatch.setattr(
            comms_utils,
            "store_pending_whatsapp_call_intent",
            AsyncMock(),
        )

        await comms.make_whatsapp_call(
            contact_id=5,
            opener="Hi Alice.",
            allow_hang_up="One question, then wrap up.",
        )

        stashed = cm._pending_whatsapp_call_openers[5]
        assert stashed["hang_up_gate"] == "One question, then wrap up."
        # The live queue is cleared — no leg exists yet.
        assert cm.call_manager.pending_hang_up_gate == ""

    @pytest.mark.asyncio
    async def test_onboarding_quiz_call_force_arms_gate(self, monkeypatch):
        from unify.comms.primitives import _ONBOARDING_CALL_HANG_UP_REASON
        from unify.conversation_manager.domains import comms_utils

        cm = self._cm()
        cm.active_pending_onboarding_outbound = lambda: {
            "channel": "whatsapp_call",
            "onboarding_trigger_step_id": "whatsapp-call-reference",
        }
        comms = self._comms(cm)
        monkeypatch.setattr(
            comms_utils.SESSION_DETAILS.assistant,
            "agent_id",
            42,
        )
        seen_at_dial: dict = {}

        async def _fake_start_whatsapp_call(**kwargs):
            seen_at_dial["gate"] = cm.call_manager.pending_hang_up_gate
            return {"success": True, "method": "direct"}

        monkeypatch.setattr(
            comms_utils,
            "start_whatsapp_call",
            _fake_start_whatsapp_call,
        )

        # The slow brain passed NO allow_hang_up — onboarding forces it on.
        await comms.make_whatsapp_call(
            contact_id=5,
            opener="Hi Dan — quick quiz.",
        )

        assert seen_at_dial["gate"] == _ONBOARDING_CALL_HANG_UP_REASON

    @pytest.mark.asyncio
    async def test_onboarding_force_arm_keeps_llm_reason_when_given(self):
        from unify.comms.primitives import CommsPrimitives

        cm = self._cm()
        cm.active_pending_onboarding_outbound = lambda: {
            "channel": "phone_call",
            "onboarding_trigger_step_id": "phone-call-reference",
        }
        comms = CommsPrimitives(conversation_manager=cm)

        assert comms._pre_armed_hang_up_reason(
            "One clue then hang up",
            "phone_call",
        ) == ("One clue then hang up")

    @pytest.mark.asyncio
    async def test_no_onboarding_no_allow_hang_up_means_no_gate(self):
        from unify.comms.primitives import CommsPrimitives

        cm = self._cm()
        comms = CommsPrimitives(conversation_manager=cm)

        assert comms._pre_armed_hang_up_reason(None, "phone_call") == ""
        assert comms._pre_armed_hang_up_reason("  ", "whatsapp_call") == ""
