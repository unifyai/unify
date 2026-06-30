from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unify.conversation_manager.cm_types import Medium
from unify.conversation_manager.conversation_manager import (
    CREDIT_GATE_REPLY_THROTTLE_SECONDS,
    DEPLETED_CREDITS_EMAIL_SUBJECT,
    DEPLETED_CREDITS_SLOW_BRAIN_RESPONSE,
    ConversationManager,
)
from unify.spending_limits import CreditGateState


def _cm_for_queued_run(*, reply_context: dict | None):
    cm = object.__new__(ConversationManager)
    cm.ready_for_brain = True
    cm._pending_llm_requests = [(0, False, False)]
    cm._pending_llm_request_meta = [
        {
            "request_id": "llmreq-000001",
            "origin_event_id": "event-000001",
            "origin_event_name": "UnifyMessageReceived",
            "triggering_contact_id": 1,
            "is_user_origin": False,
        },
    ]
    if reply_context is not None:
        cm._pending_llm_request_meta[0]["credit_gate_reply_context"] = reply_context
    cm._llm_run_seq = 0
    cm._credit_gate_reply_sent_at = {}
    cm._session_logger = MagicMock()
    cm.debouncer = SimpleNamespace(was_queued=False)
    cm.mode = "text"
    cm.loop = SimpleNamespace(time=MagicMock(return_value=1000.0))
    cm.run_llm = AsyncMock()
    cm._send_credit_gate_reply = AsyncMock(return_value=True)
    return cm


@pytest.mark.asyncio
async def test_request_llm_run_stores_credit_gate_reply_context():
    cm = object.__new__(ConversationManager)
    cm._llm_request_seq = 0
    cm._current_event_trace = {
        "event_id": "event-000001",
        "event_name": "SMSReceived",
    }
    cm._pending_llm_requests = []
    cm._pending_llm_request_meta = []
    cm._session_logger = MagicMock()
    cm.ready_for_brain = True

    reply_context = {
        "medium": Medium.SMS_MESSAGE.value,
        "contact_id": 1,
    }

    await ConversationManager.request_llm_run(
        cm,
        triggering_contact_id=1,
        credit_gate_reply_context=reply_context,
    )

    assert cm._pending_llm_request_meta[0]["credit_gate_reply_context"] == reply_context


@pytest.mark.asyncio
async def test_flush_depleted_credits_sends_reply_and_skips_llm():
    reply_context = {
        "medium": Medium.UNIFY_MESSAGE.value,
        "contact_id": 1,
    }
    cm = _cm_for_queued_run(reply_context=reply_context)

    with patch(
        "unify.conversation_manager.conversation_manager.check_credit_gate_state",
        AsyncMock(
            return_value=CreditGateState(
                allowed=False,
                reason="Insufficient credits",
                credit_balance=0.0,
                billing_mode="CREDITS",
            ),
        ),
    ):
        await ConversationManager.flush_llm_requests(cm)

    cm._send_credit_gate_reply.assert_awaited_once_with(reply_context)
    cm.run_llm.assert_not_awaited()
    assert cm._pending_llm_requests == []
    assert cm._pending_llm_request_meta == []


@pytest.mark.asyncio
async def test_flush_allowed_credits_submits_llm():
    reply_context = {
        "medium": Medium.UNIFY_MESSAGE.value,
        "contact_id": 1,
    }
    cm = _cm_for_queued_run(reply_context=reply_context)

    with patch(
        "unify.conversation_manager.conversation_manager.check_credit_gate_state",
        AsyncMock(return_value=CreditGateState(allowed=True, credit_balance=10.0)),
    ):
        await ConversationManager.flush_llm_requests(cm)

    cm._send_credit_gate_reply.assert_not_awaited()
    cm.run_llm.assert_awaited_once()


def test_credit_gate_reply_throttles_repeated_human_replies():
    cm = object.__new__(ConversationManager)
    cm._credit_gate_reply_sent_at = {}
    cm.loop = SimpleNamespace(time=MagicMock(return_value=1000.0))
    reply_context = {
        "medium": Medium.UNIFY_MESSAGE.value,
        "contact_id": 1,
    }

    assert (
        ConversationManager._credit_gate_reply_is_throttled(cm, reply_context) is False
    )
    assert (
        ConversationManager._credit_gate_reply_is_throttled(cm, reply_context) is True
    )

    cm.loop.time.return_value = 1000.0 + CREDIT_GATE_REPLY_THROTTLE_SECONDS + 1
    assert (
        ConversationManager._credit_gate_reply_is_throttled(cm, reply_context) is False
    )


def test_credit_gate_reply_does_not_throttle_api_responses():
    cm = object.__new__(ConversationManager)
    cm._credit_gate_reply_sent_at = {}
    cm.loop = SimpleNamespace(time=MagicMock(return_value=1000.0))
    reply_context = {
        "medium": Medium.API_MESSAGE.value,
        "contact_id": 1,
        "api_message_id": "api-msg-1",
    }

    assert (
        ConversationManager._credit_gate_reply_is_throttled(cm, reply_context) is False
    )
    assert (
        ConversationManager._credit_gate_reply_is_throttled(cm, reply_context) is False
    )


@pytest.mark.asyncio
async def test_credit_gate_reply_routes_unify_message_and_restores_suppression():
    cm = object.__new__(ConversationManager)
    cm._outbound_suppress_gen = -1
    cm._llm_gen = 12
    tools = MagicMock()
    tools.send_unify_message = AsyncMock(return_value={"status": "ok"})

    with patch(
        "unify.conversation_manager.conversation_manager.ConversationManagerBrainActionTools",
        return_value=tools,
    ):
        sent = await ConversationManager._send_credit_gate_reply(
            cm,
            {
                "medium": Medium.UNIFY_MESSAGE.value,
                "contact_id": 1,
            },
        )

    assert sent is True
    tools.send_unify_message.assert_awaited_once_with(
        contact_id=1,
        content=DEPLETED_CREDITS_SLOW_BRAIN_RESPONSE,
    )
    assert cm._outbound_suppress_gen == -1


@pytest.mark.asyncio
async def test_credit_gate_reply_routes_email_replies():
    cm = object.__new__(ConversationManager)
    cm._outbound_suppress_gen = -1
    cm._llm_gen = 12
    tools = MagicMock()
    tools.send_email = AsyncMock(return_value={"status": "ok"})

    with patch(
        "unify.conversation_manager.conversation_manager.ConversationManagerBrainActionTools",
        return_value=tools,
    ):
        sent = await ConversationManager._send_credit_gate_reply(
            cm,
            {
                "medium": Medium.EMAIL.value,
                "contact_id": 1,
                "email_id": "message-id",
                "thread_id": "gmail-thread-id",
            },
        )

    assert sent is True
    tools.send_email.assert_awaited_once_with(
        subject=DEPLETED_CREDITS_EMAIL_SUBJECT,
        body=DEPLETED_CREDITS_SLOW_BRAIN_RESPONSE,
        reply_all=True,
        email_id_to_reply_to="message-id",
        thread_id="gmail-thread-id",
    )
