"""Tests for ``CommsManager.ingress_transport_factory`` (Phase A.bis.5).

The factory parameter unlocks lazy transport construction, which the
hosted Pub/Sub path needs because the subscription ID depends on
``SESSION_DETAILS.assistant.agent_id`` -- a value that isn't known when
``CommsManager`` is constructed in an idle pod. The factory is invoked
inside ``_start_inbound_subscription`` (after ``_poll_for_assignment``
has assigned the pod) so the transport sees a fully-populated
``agent_id`` at construction time.
"""

from __future__ import annotations

import json
import asyncio
from typing import Any
from unittest.mock import patch

import pytest

from droid.conversation_manager.comms_manager import CommsManager
from droid.conversation_manager.in_memory_event_broker import (
    create_in_memory_event_broker,
)
from droid.gateway.ingress_inmemory import InMemoryIngressTransport

# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_constructor_accepts_factory() -> None:
    broker = create_in_memory_event_broker()

    def factory() -> InMemoryIngressTransport:
        return InMemoryIngressTransport()

    cm = CommsManager(event_broker=broker, ingress_transport_factory=factory)
    assert cm.ingress_transport_factory is factory
    assert cm.ingress_transport is None


def test_constructor_accepts_both_transport_and_factory() -> None:
    """Both may be supplied; factory wins (lazy construction takes precedence)."""
    broker = create_in_memory_event_broker()
    eager = InMemoryIngressTransport()

    def factory() -> InMemoryIngressTransport:
        return InMemoryIngressTransport()

    cm = CommsManager(
        event_broker=broker,
        ingress_transport=eager,
        ingress_transport_factory=factory,
    )
    assert cm.ingress_transport is eager  # held until factory is resolved
    assert cm.ingress_transport_factory is factory


# ---------------------------------------------------------------------------
# Resolution behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_factory_invoked_on_start_inbound_subscription() -> None:
    """The factory must be called inside _start, not at construction."""
    broker = create_in_memory_event_broker()
    calls: list[int] = []

    def factory() -> InMemoryIngressTransport:
        calls.append(1)
        return InMemoryIngressTransport()

    cm = CommsManager(event_broker=broker, ingress_transport_factory=factory)
    assert calls == []  # not yet called

    await cm._start_inbound_subscription()
    assert calls == [1]
    await cm._stop_inbound_subscription()


@pytest.mark.asyncio
async def test_factory_winner_over_eager_transport() -> None:
    """When both are supplied, the factory-materialized transport wins."""
    broker = create_in_memory_event_broker()
    eager = InMemoryIngressTransport()
    factory_transport = InMemoryIngressTransport()

    cm = CommsManager(
        event_broker=broker,
        ingress_transport=eager,
        ingress_transport_factory=lambda: factory_transport,
    )

    await cm._start_inbound_subscription()
    # The factory's transport replaced the eager one and was started.
    assert cm.ingress_transport is factory_transport
    # The eager one was NOT started.
    with pytest.raises(RuntimeError, match="not started"):
        await eager.deliver({"thread": "msg", "publish_timestamp": 1.0, "event": {}})
    await cm._stop_inbound_subscription()


@pytest.mark.asyncio
async def test_factory_returning_none_falls_through_to_legacy() -> None:
    """A factory may opt out by returning None; legacy path then runs."""
    broker = create_in_memory_event_broker()
    cm = CommsManager(
        event_broker=broker,
        ingress_transport_factory=lambda: None,
    )
    with patch.object(cm, "subscribe_to_topic") as mock_subscribe:
        await cm._start_inbound_subscription()
        mock_subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_factory_materialized_transport_dispatches_through_comms_manager() -> (
    None
):
    """End-to-end: factory -> transport -> deliver -> dispatch -> event broker."""
    broker = create_in_memory_event_broker()
    cm = CommsManager(
        event_broker=broker,
        ingress_transport_factory=InMemoryIngressTransport,
    )
    await cm._start_inbound_subscription()
    transport = cm.ingress_transport
    assert isinstance(transport, InMemoryIngressTransport)

    received: list[dict[str, Any]] = []

    async def consume() -> None:
        async with broker.pubsub() as pubsub:
            await pubsub.psubscribe("app:comms:*")
            msg = await pubsub.get_message(
                timeout=2.0,
                ignore_subscribe_messages=True,
            )
            if msg is not None:
                received.append(
                    {
                        "channel": msg["channel"],
                        "data": json.loads(msg["data"]),
                    },
                )

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    envelope = {
        "thread": "msg",
        "publish_timestamp": 1.0,
        "event": {
            "assistant_id": "1",
            "contacts": [],
            "to_number": "+1",
            "from_number": "+2",
            "body": "via factory",
        },
    }
    await transport.deliver(envelope)
    await consumer
    await cm._stop_inbound_subscription()

    assert len(received) == 1
    assert received[0]["channel"].startswith("app:comms:")


@pytest.mark.asyncio
async def test_factory_can_inspect_session_details_at_resolution_time() -> None:
    """Lazy resolution: factory can read SESSION_DETAILS.assistant.agent_id.

    This is the pattern hosted Droid will use: the factory closes over
    SESSION_DETAILS and constructs a PubSubIngressTransport whose
    subscription_id derives from agent_id, which by the time
    _start_inbound_subscription runs has been set by
    _poll_for_assignment.
    """
    from droid.session_details import SESSION_DETAILS

    broker = create_in_memory_event_broker()
    seen_agent_ids: list[Any] = []

    def factory() -> InMemoryIngressTransport:
        seen_agent_ids.append(SESSION_DETAILS.assistant.agent_id)
        return InMemoryIngressTransport()

    cm = CommsManager(event_broker=broker, ingress_transport_factory=factory)

    # Simulate _poll_for_assignment setting agent_id just before
    # _start_inbound_subscription runs.
    original = SESSION_DETAILS.assistant.agent_id
    try:
        SESSION_DETAILS.assistant.agent_id = 99
        await cm._start_inbound_subscription()
    finally:
        SESSION_DETAILS.assistant.agent_id = original

    assert seen_agent_ids == [99]
    await cm._stop_inbound_subscription()
