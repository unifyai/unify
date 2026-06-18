"""Contract tests for the ``IngressTransport`` Protocol.

Phase A.bis.1 lands only the protocol itself; concrete implementations
arrive in A.bis.2 (in-memory) and A.bis.3 (Pub/Sub). The tests here
exercise the contract against a tiny inline implementation to pin the
runtime-checkable behaviour and the dispatch-call shape.
"""

from __future__ import annotations

from typing import Any

import pytest

from droid.gateway.ingress import (
    AckCallable,
    EnvelopeDispatcher,
    IngressTransport,
)


class _CapturingTransport:
    """Minimal IngressTransport implementation used to validate the seam.

    Holds an in-memory inbox; calls to ``deliver`` route through the
    registered dispatcher. Lets tests assert on dispatch shape without
    depending on any external broker.
    """

    def __init__(self) -> None:
        self._dispatcher: EnvelopeDispatcher | None = None
        self._stopped = False

    async def start(self, dispatcher: EnvelopeDispatcher) -> None:
        self._dispatcher = dispatcher
        self._stopped = False

    async def stop(self) -> None:
        self._dispatcher = None
        self._stopped = True

    async def deliver(
        self,
        payload: dict,
        *,
        source_topic: str = "",
        ack: AckCallable | None = None,
        nack: AckCallable | None = None,
    ) -> None:
        if self._dispatcher is None:
            raise RuntimeError("deliver called before start (or after stop)")
        await self._dispatcher(
            payload,
            source_topic=source_topic,
            ack=ack,
            nack=nack,
        )


def test_capturing_transport_satisfies_ingress_transport_protocol() -> None:
    transport = _CapturingTransport()
    assert isinstance(transport, IngressTransport)


@pytest.mark.asyncio
async def test_dispatcher_receives_payload_with_keyword_args() -> None:
    received: list[dict[str, Any]] = []

    async def dispatcher(
        payload: dict,
        *,
        source_topic: str = "",
        ack: AckCallable | None = None,
        nack: AckCallable | None = None,
    ) -> None:
        received.append(
            {
                "payload": payload,
                "source_topic": source_topic,
                "ack": ack,
                "nack": nack,
            },
        )

    transport = _CapturingTransport()
    await transport.start(dispatcher)
    envelope = {
        "thread": "msg",
        "publish_timestamp": 1.0,
        "event": {
            "assistant_id": "1",
            "contacts": [],
            "to_number": "+1",
            "from_number": "+2",
            "body": "hi",
        },
    }
    await transport.deliver(envelope, source_topic="droid-1-staging")
    assert received == [
        {
            "payload": envelope,
            "source_topic": "droid-1-staging",
            "ack": None,
            "nack": None,
        },
    ]
    await transport.stop()


@pytest.mark.asyncio
async def test_dispatcher_receives_ack_and_nack_callables_when_provided() -> None:
    invoked: list[str] = []

    async def dispatcher(
        payload: dict,
        *,
        source_topic: str = "",
        ack: AckCallable | None = None,
        nack: AckCallable | None = None,
    ) -> None:
        if ack is not None:
            ack()
            invoked.append("ack")
        if nack is not None:
            invoked.append("nack-available")

    transport = _CapturingTransport()
    await transport.start(dispatcher)

    ack_calls: list[str] = []
    nack_calls: list[str] = []
    await transport.deliver(
        {"thread": "msg", "publish_timestamp": 1.0, "event": {}},
        ack=lambda: ack_calls.append("acked"),
        nack=lambda: nack_calls.append("nacked"),
    )
    assert ack_calls == ["acked"]
    assert nack_calls == []
    assert invoked == ["ack", "nack-available"]
    await transport.stop()


@pytest.mark.asyncio
async def test_deliver_after_stop_raises() -> None:
    """Contract: dispatcher must not be invoked after stop()."""

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        pass

    transport = _CapturingTransport()
    await transport.start(dispatcher)
    await transport.stop()

    with pytest.raises(RuntimeError):
        await transport.deliver(
            {"thread": "msg", "publish_timestamp": 1.0, "event": {}},
        )


@pytest.mark.asyncio
async def test_stop_is_idempotent() -> None:
    transport = _CapturingTransport()

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        pass

    await transport.start(dispatcher)
    await transport.stop()
    await transport.stop()


@pytest.mark.asyncio
async def test_envelope_dispatcher_type_alias_matches_real_dispatcher_shape() -> None:
    """The EnvelopeDispatcher signature matches CommsManager.dispatch_envelope_payload.

    Pinned here because Phase A.bis.4 wires CommsManager directly; if its
    public signature drifts, the seam silently breaks.
    """
    from droid.conversation_manager.comms_manager import CommsManager
    import inspect

    sig = inspect.signature(CommsManager.dispatch_envelope_payload)
    params = sig.parameters
    assert "payload" in params
    assert params["source_topic"].kind == inspect.Parameter.KEYWORD_ONLY
    assert params["ack"].kind == inspect.Parameter.KEYWORD_ONLY
    assert params["nack"].kind == inspect.Parameter.KEYWORD_ONLY


@pytest.mark.asyncio
async def test_multiple_concurrent_deliveries_dispatch_in_order_from_a_single_caller() -> (
    None
):
    """Single-caller invocation order is preserved (no implicit reordering)."""
    order: list[int] = []

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        order.append(payload["seq"])

    transport = _CapturingTransport()
    await transport.start(dispatcher)

    for i in range(5):
        await transport.deliver(
            {"thread": "msg", "publish_timestamp": float(i), "event": {}, "seq": i},
        )
    assert order == [0, 1, 2, 3, 4]
    await transport.stop()


def test_module_exports_match_documented_surface() -> None:
    """Public exports match what the docstring promises."""
    import droid.gateway.ingress as ingress_mod

    assert hasattr(ingress_mod, "IngressTransport")
    assert hasattr(ingress_mod, "EnvelopeDispatcher")
    assert hasattr(ingress_mod, "AckCallable")
