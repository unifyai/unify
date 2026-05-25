"""End-to-end wire-in test for ``CommsManager`` + ``IngressTransport``.

Phase A.bis.4 lands the optional ``ingress_transport`` constructor
argument on ``CommsManager`` plus the ``_start_inbound_subscription`` /
``_stop_inbound_subscription`` helpers that gate on it. This test
exercises both arms:

1. With an injected ``InMemoryIngressTransport``, an envelope delivered
   through the transport reaches the event broker via
   ``CommsManager.dispatch_envelope_payload``.
2. Without an injected transport (the default), the legacy inline
   ``subscribe_to_topic`` path is still invoked. Mocked at the
   ``subscribe`` boundary to avoid requiring a real Pub/Sub project.

Together these guarantee the wire-in is purely additive: legacy
callers keep their behaviour, new callers opt in by passing a
transport.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from unity.conversation_manager.comms_manager import CommsManager
from unity.conversation_manager.in_memory_event_broker import (
    create_in_memory_event_broker,
)
from unity.gateway.ingress_inmemory import InMemoryIngressTransport

# ---------------------------------------------------------------------------
# Default construction (backward compat)
# ---------------------------------------------------------------------------


def test_constructor_accepts_no_transport_keeps_legacy_path() -> None:
    """Backward compat: existing callers pass only event_broker."""
    broker = create_in_memory_event_broker()
    cm = CommsManager(event_broker=broker)
    assert cm.ingress_transport is None


def test_constructor_accepts_explicit_none_for_transport() -> None:
    broker = create_in_memory_event_broker()
    cm = CommsManager(event_broker=broker, ingress_transport=None)
    assert cm.ingress_transport is None


# ---------------------------------------------------------------------------
# Injected transport path
# ---------------------------------------------------------------------------


def test_constructor_accepts_injected_transport() -> None:
    broker = create_in_memory_event_broker()
    transport = InMemoryIngressTransport()
    cm = CommsManager(event_broker=broker, ingress_transport=transport)
    assert cm.ingress_transport is transport


@pytest.mark.asyncio
async def test_start_inbound_subscription_routes_through_injected_transport() -> None:
    """When a transport is injected, _start_inbound_subscription uses it."""
    broker = create_in_memory_event_broker()
    transport = InMemoryIngressTransport()
    cm = CommsManager(event_broker=broker, ingress_transport=transport)

    await cm._start_inbound_subscription()
    # Transport now holds the dispatcher and is ready to accept deliveries.
    # Calling deliver should drive the dispatcher (CommsManager
    # .dispatch_envelope_payload) and ultimately publish to the event broker.

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
        "publish_timestamp": 1700000000.0,
        "event": {
            "assistant_id": "1",
            "contacts": [],
            "to_number": "+15555550000",
            "from_number": "+15555550100",
            "body": "hello via injected transport",
        },
    }
    await transport.deliver(envelope, source_topic="unity-1-test")
    await consumer
    await cm._stop_inbound_subscription()

    assert len(received) == 1
    assert received[0]["channel"].startswith("app:comms:")


@pytest.mark.asyncio
async def test_stop_inbound_subscription_stops_injected_transport() -> None:
    broker = create_in_memory_event_broker()
    transport = InMemoryIngressTransport()
    cm = CommsManager(event_broker=broker, ingress_transport=transport)
    await cm._start_inbound_subscription()
    await cm._stop_inbound_subscription()

    # After stop, the transport must reject further delivery.
    with pytest.raises(RuntimeError, match="not started"):
        await transport.deliver(
            {"thread": "msg", "publish_timestamp": 1.0, "event": {}},
        )


@pytest.mark.asyncio
async def test_stop_inbound_subscription_tolerates_transport_stop_failure() -> None:
    """If the injected transport's stop raises, we still cancel legacy futures."""
    broker = create_in_memory_event_broker()

    class _FlakyTransport(InMemoryIngressTransport):
        async def stop(self) -> None:
            raise RuntimeError("simulated stop failure")

    transport = _FlakyTransport()
    cm = CommsManager(event_broker=broker, ingress_transport=transport)

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        pass

    await transport.start(dispatcher)
    cm.subscribers["fake-sub"] = MagicMock()  # legacy future to cancel

    # Must not raise even though transport.stop raises
    await cm._stop_inbound_subscription()
    cm.subscribers["fake-sub"].cancel.assert_called_once()


# ---------------------------------------------------------------------------
# Legacy path (no transport) -- verify subscribe_to_topic is still invoked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_inbound_subscription_falls_back_to_subscribe_to_topic() -> None:
    """Without an injected transport, the legacy inline path is preserved."""
    broker = create_in_memory_event_broker()
    cm = CommsManager(event_broker=broker)  # no transport

    with patch.object(cm, "subscribe_to_topic") as mock_subscribe:
        await cm._start_inbound_subscription()
        mock_subscribe.assert_called_once()
        # Validate the call shape: positional subscription_id + keyword max_messages
        args, kwargs = mock_subscribe.call_args
        assert kwargs == {"max_messages": 10}
        assert isinstance(args[0], str)
        assert args[0].startswith("unity-")
        assert args[0].endswith("-sub")


@pytest.mark.asyncio
async def test_stop_inbound_subscription_cancels_legacy_futures_only() -> None:
    """Without a transport, _stop only cancels self.subscribers futures."""
    broker = create_in_memory_event_broker()
    cm = CommsManager(event_broker=broker)  # no transport

    future_a = MagicMock()
    future_b = MagicMock()
    cm.subscribers["sub-a"] = future_a
    cm.subscribers["sub-b"] = future_b

    await cm._stop_inbound_subscription()
    future_a.cancel.assert_called_once()
    future_b.cancel.assert_called_once()
