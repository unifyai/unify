"""Verify the in-memory broker satisfies the gateway ``EventBroker`` protocol.

This test pins the Phase A invariant that the seam in ``unify.gateway``
matches the existing implementation in
``unify.conversation_manager.in_memory_event_broker`` exactly. A future
``PubSubEventBroker`` will reuse the same test to assert structural
parity.
"""

from __future__ import annotations

import asyncio

import pytest

from unify.conversation_manager.in_memory_event_broker import (
    InMemoryEventBroker,
    create_in_memory_event_broker,
)
from unify.gateway.event_broker import (
    EventBroker,
    PubSubConnection,
)


def test_in_memory_event_broker_satisfies_event_broker_protocol() -> None:
    broker = create_in_memory_event_broker()
    assert isinstance(broker, EventBroker)


@pytest.mark.asyncio
async def test_in_memory_pubsub_connection_satisfies_protocol() -> None:
    broker: EventBroker = create_in_memory_event_broker()
    async with broker.pubsub() as pubsub:
        assert isinstance(pubsub, PubSubConnection)


@pytest.mark.asyncio
async def test_publish_subscribe_roundtrip_through_protocol_only() -> None:
    """Drive the broker exclusively through the protocol surface."""
    broker: EventBroker = create_in_memory_event_broker()
    received: list[str] = []

    async def consume() -> None:
        async with broker.pubsub() as pubsub:
            await pubsub.subscribe("test:channel")
            msg = await pubsub.get_message(
                timeout=2.0,
                ignore_subscribe_messages=True,
            )
            if msg is not None:
                received.append(msg["data"])

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    delivered = await broker.publish("test:channel", "hello")
    assert delivered == 1
    await consumer
    assert received == ["hello"]


@pytest.mark.asyncio
async def test_psubscribe_pattern_match_through_protocol_only() -> None:
    broker: EventBroker = create_in_memory_event_broker()
    received: list[tuple[str, str]] = []

    async def consume() -> None:
        async with broker.pubsub() as pubsub:
            await pubsub.psubscribe("app:comms:*")
            msg = await pubsub.get_message(
                timeout=2.0,
                ignore_subscribe_messages=True,
            )
            if msg is not None:
                received.append((msg["channel"], msg["data"]))

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    await broker.publish("app:comms:startup", "payload")
    await consumer
    assert received == [("app:comms:startup", "payload")]


@pytest.mark.asyncio
async def test_execute_command_pubsub_numpat() -> None:
    """The Redis-compat escape hatch is part of the protocol surface."""
    broker: EventBroker = create_in_memory_event_broker()
    async with broker.pubsub() as pubsub:
        await pubsub.psubscribe("a:*", "b:*")
        await pubsub.subscribe("c")
        numpat = await broker.execute_command("PUBSUB", "NUMPAT")
        assert numpat == 2


@pytest.mark.asyncio
async def test_get_message_returns_none_on_timeout() -> None:
    broker: EventBroker = create_in_memory_event_broker()
    async with broker.pubsub() as pubsub:
        await pubsub.subscribe("never:fires")
        msg = await pubsub.get_message(
            timeout=0.05,
            ignore_subscribe_messages=True,
        )
        assert msg is None


def test_concrete_class_remains_exported_for_in_process_callers() -> None:
    """Direct construction must still work for in-process tests/sandboxes."""
    broker = InMemoryEventBroker()
    assert isinstance(broker, EventBroker)
