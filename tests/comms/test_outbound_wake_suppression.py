from unittest.mock import AsyncMock

import pytest

from unify.comms.outbound_origin import (
    mark_slow_brain_direct_outbound,
    reset_slow_brain_direct_outbound,
)
from unify.comms.primitives import CommsPrimitives
from unify.conversation_manager.events import SMSSent


@pytest.mark.asyncio
async def test_publish_stamps_flag_when_slow_brain_context_active():
    broker = AsyncMock()
    primitives = CommsPrimitives(event_broker=broker)
    event = SMSSent(contact={"contact_id": 2}, content="hello")

    token = mark_slow_brain_direct_outbound()
    try:
        await primitives._publish_comms_event("app:comms:sms_sent", event)
    finally:
        reset_slow_brain_direct_outbound(token)

    assert event.suppress_slow_brain_wake is True
    broker.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_publish_leaves_flag_false_without_slow_brain_context():
    broker = AsyncMock()
    primitives = CommsPrimitives(event_broker=broker)
    event = SMSSent(contact={"contact_id": 2}, content="hello")

    await primitives._publish_comms_event("app:comms:sms_sent", event)

    assert event.suppress_slow_brain_wake is False
    broker.publish.assert_awaited_once()
