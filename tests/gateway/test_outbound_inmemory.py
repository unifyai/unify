"""Behavioural tests for ``InMemoryOutboundTransport``."""

from __future__ import annotations

import threading

import pytest

from unify.gateway.outbound import OutboundTransport
from unify.gateway.outbound_inmemory import (
    InMemoryOutboundTransport,
    PublishedEnvelope,
)


def test_satisfies_outbound_transport_protocol() -> None:
    assert isinstance(InMemoryOutboundTransport(), OutboundTransport)


def test_publish_records_topic_message_and_thread() -> None:
    transport = InMemoryOutboundTransport()
    msg_id = transport.publish(
        "unity-42-staging",
        b'{"thread":"msg","event":{}}',
        thread="msg",
    )
    assert msg_id == "inmemory-0"
    assert transport.published == [
        PublishedEnvelope(
            topic="unity-42-staging",
            message=b'{"thread":"msg","event":{}}',
            thread="msg",
            message_id="inmemory-0",
        ),
    ]


def test_publish_assigns_sequential_ids() -> None:
    transport = InMemoryOutboundTransport()
    ids = [transport.publish("t", b"x", thread="msg") for _ in range(5)]
    assert ids == [f"inmemory-{i}" for i in range(5)]
    assert transport.published_count == 5


def test_publish_ignores_timeout_argument() -> None:
    """In-memory transport never blocks; timeout has no effect."""
    transport = InMemoryOutboundTransport()
    msg_id = transport.publish("t", b"x", thread="msg", timeout=0.001)
    assert msg_id == "inmemory-0"


@pytest.mark.asyncio
async def test_publish_after_aclose_raises() -> None:
    transport = InMemoryOutboundTransport()
    await transport.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        transport.publish("t", b"x")


@pytest.mark.asyncio
async def test_aclose_preserves_recorded_published_envelopes() -> None:
    """aclose flips the gate but does not erase the post-mortem record."""
    transport = InMemoryOutboundTransport()
    transport.publish("t", b"a", thread="msg")
    transport.publish("t", b"b", thread="msg")
    await transport.aclose()
    # The published record is still introspectable after close.
    assert transport.published_count == 2
    assert [e.message for e in transport.published] == [b"a", b"b"]


@pytest.mark.asyncio
async def test_aclose_is_idempotent() -> None:
    transport = InMemoryOutboundTransport()
    await transport.aclose()
    await transport.aclose()


def test_published_returns_a_copy_snapshot() -> None:
    """Callers may mutate the returned list without disturbing internal state."""
    transport = InMemoryOutboundTransport()
    transport.publish("t", b"x", thread="msg")
    snapshot = transport.published
    snapshot.clear()
    assert transport.published_count == 1
    assert len(transport.published) == 1


def test_publish_is_thread_safe_under_concurrency() -> None:
    """Many concurrent publishers all record without losing any envelopes."""
    transport = InMemoryOutboundTransport()
    threads = [
        threading.Thread(
            target=lambda i=i: transport.publish(
                "t",
                f"msg-{i}".encode(),
                thread="msg",
            ),
        )
        for i in range(50)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert transport.published_count == 50
    messages = sorted(e.message for e in transport.published)
    assert messages == sorted(f"msg-{i}".encode() for i in range(50))


def test_published_envelope_is_a_frozen_dataclass() -> None:
    env = PublishedEnvelope(topic="t", message=b"x", thread="msg", message_id="m-0")
    with pytest.raises((TypeError, AttributeError)):  # frozen
        env.topic = "other"  # type: ignore[misc]
