"""Behavioural tests for ``InMemoryIngressTransport``."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from unity.gateway.ingress import IngressTransport
from unity.gateway.ingress_inmemory import InMemoryIngressTransport


def test_satisfies_ingress_transport_protocol() -> None:
    assert isinstance(InMemoryIngressTransport(), IngressTransport)


@pytest.mark.asyncio
async def test_deliver_before_start_raises() -> None:
    transport = InMemoryIngressTransport()
    with pytest.raises(RuntimeError, match="not started"):
        await transport.deliver(
            {"thread": "msg", "publish_timestamp": 1.0, "event": {}},
        )


@pytest.mark.asyncio
async def test_deliver_after_stop_raises() -> None:
    received: list[dict] = []

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        received.append(payload)

    transport = InMemoryIngressTransport()
    await transport.start(dispatcher)
    await transport.stop()
    with pytest.raises(RuntimeError, match="not started"):
        await transport.deliver(
            {"thread": "msg", "publish_timestamp": 1.0, "event": {}},
        )
    assert received == []


@pytest.mark.asyncio
async def test_double_start_raises_without_silently_dropping_previous_dispatcher() -> (
    None
):
    """Re-entrance protection: start twice without stop must fail loud."""
    received: list[str] = []

    async def first(payload: dict, **_kwargs: Any) -> None:
        received.append("first")

    async def second(payload: dict, **_kwargs: Any) -> None:
        received.append("second")

    transport = InMemoryIngressTransport()
    await transport.start(first)
    with pytest.raises(RuntimeError, match="already started"):
        await transport.start(second)
    await transport.deliver({"thread": "msg", "publish_timestamp": 1.0, "event": {}})
    assert received == ["first"]
    await transport.stop()


@pytest.mark.asyncio
async def test_dispatcher_receives_payload_and_kwargs() -> None:
    received: list[dict[str, Any]] = []

    async def dispatcher(
        payload: dict,
        *,
        source_topic: str = "",
        ack: Any = None,
        nack: Any = None,
    ) -> None:
        received.append(
            {
                "payload": payload,
                "source_topic": source_topic,
                "ack": ack,
                "nack": nack,
            },
        )

    transport = InMemoryIngressTransport()
    await transport.start(dispatcher)

    env = {"thread": "msg", "publish_timestamp": 1.0, "event": {"to_number": "+1"}}
    ack_called: list[None] = []
    await transport.deliver(
        env,
        source_topic="unity-42-staging",
        ack=lambda: ack_called.append(None),
        nack=None,
    )
    assert received == [
        {
            "payload": env,
            "source_topic": "unity-42-staging",
            "ack": received[0]["ack"],
            "nack": None,
        },
    ]
    received[0]["ack"]()
    assert ack_called == [None]
    await transport.stop()


@pytest.mark.asyncio
async def test_delivered_count_increments_only_on_success() -> None:
    raise_next = False

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        if raise_next:
            raise RuntimeError("simulated dispatcher failure")

    transport = InMemoryIngressTransport()
    await transport.start(dispatcher)
    assert transport.delivered_count == 0

    await transport.deliver({"thread": "msg", "publish_timestamp": 1.0, "event": {}})
    assert transport.delivered_count == 1

    raise_next = True
    with pytest.raises(RuntimeError, match="simulated dispatcher failure"):
        await transport.deliver(
            {"thread": "msg", "publish_timestamp": 2.0, "event": {}},
        )
    assert transport.delivered_count == 1

    raise_next = False
    await transport.deliver({"thread": "msg", "publish_timestamp": 3.0, "event": {}})
    assert transport.delivered_count == 2
    await transport.stop()


@pytest.mark.asyncio
async def test_delivered_count_resets_on_restart() -> None:
    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        pass

    transport = InMemoryIngressTransport()
    await transport.start(dispatcher)
    await transport.deliver({"thread": "msg", "publish_timestamp": 1.0, "event": {}})
    await transport.deliver({"thread": "msg", "publish_timestamp": 2.0, "event": {}})
    assert transport.delivered_count == 2

    await transport.stop()
    await transport.start(dispatcher)
    assert transport.delivered_count == 0


@pytest.mark.asyncio
async def test_concurrent_deliveries_all_dispatch() -> None:
    """Many concurrent deliveries all reach the dispatcher."""
    received: list[int] = []
    lock = asyncio.Lock()

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        async with lock:
            received.append(payload["seq"])

    transport = InMemoryIngressTransport()
    await transport.start(dispatcher)

    await asyncio.gather(
        *(
            transport.deliver(
                {"thread": "msg", "publish_timestamp": float(i), "event": {}, "seq": i},
            )
            for i in range(20)
        ),
    )
    assert sorted(received) == list(range(20))
    assert transport.delivered_count == 20
    await transport.stop()


@pytest.mark.asyncio
async def test_stop_is_idempotent() -> None:
    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        pass

    transport = InMemoryIngressTransport()
    await transport.start(dispatcher)
    await transport.stop()
    await transport.stop()  # must not raise


@pytest.mark.asyncio
async def test_stop_without_start_is_a_noop() -> None:
    """Lifecycle: stopping a never-started transport must not raise."""
    transport = InMemoryIngressTransport()
    await transport.stop()
