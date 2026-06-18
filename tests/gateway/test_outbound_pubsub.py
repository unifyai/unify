"""Tests for ``PubSubOutboundTransport``.

Exercises the transport against a mocked ``google.cloud.pubsub_v1``
surface. Real Pub/Sub integration is exercised in deployment soak,
not in the unit suite.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from droid.gateway.outbound import OutboundTransport
from droid.gateway.outbound_pubsub import PubSubOutboundTransport

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _fake_publisher_client(message_id: str = "broker-msg-1") -> MagicMock:
    client = MagicMock(name="PublisherClient")
    client.topic_path.side_effect = (
        lambda project, topic: f"projects/{project}/topics/{topic}"
    )
    future = MagicMock(name="PublishFuture")
    future.result.return_value = message_id
    client.publish.return_value = future
    return client


def _install_pubsub_stub(
    monkeypatch: pytest.MonkeyPatch,
    *,
    message_id: str = "broker-msg-1",
) -> MagicMock:
    pubsub_stub = MagicMock(name="pubsub_v1")
    pubsub_stub.PublisherClient.return_value = _fake_publisher_client(message_id)
    monkeypatch.setattr(
        "droid.gateway.outbound_pubsub.pubsub_v1",
        pubsub_stub,
    )
    return pubsub_stub


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_rejects_empty_project_id() -> None:
    with pytest.raises(ValueError, match="project_id"):
        PubSubOutboundTransport(project_id="")


def test_satisfies_outbound_transport_protocol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_pubsub_stub(monkeypatch)
    transport = PubSubOutboundTransport(project_id="responsive-city-458413-a2")
    assert isinstance(transport, OutboundTransport)


def test_publisher_construction_is_lazy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Avoid GCP auth failures at import time / construction time."""
    pubsub_stub = _install_pubsub_stub(monkeypatch)
    transport = PubSubOutboundTransport(project_id="p")
    # Constructor does not touch the SDK.
    pubsub_stub.PublisherClient.assert_not_called()
    # First publish triggers client construction.
    transport.publish("topic", b"data", thread="msg")
    pubsub_stub.PublisherClient.assert_called_once_with()


def test_publisher_is_constructed_with_credentials_when_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pubsub_stub = _install_pubsub_stub(monkeypatch)
    creds = MagicMock(name="Credentials")
    transport = PubSubOutboundTransport(project_id="p", credentials=creds)
    transport.publish("topic", b"data", thread="msg")
    pubsub_stub.PublisherClient.assert_called_once_with(credentials=creds)


def test_publisher_is_constructed_only_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pubsub_stub = _install_pubsub_stub(monkeypatch)
    transport = PubSubOutboundTransport(project_id="p")
    transport.publish("topic", b"a", thread="msg")
    transport.publish("topic", b"b", thread="msg")
    transport.publish("topic", b"c", thread="msg")
    assert pubsub_stub.PublisherClient.call_count == 1


# ---------------------------------------------------------------------------
# Publish behaviour
# ---------------------------------------------------------------------------


def test_publish_builds_full_topic_path_via_publisher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pubsub_stub = _install_pubsub_stub(monkeypatch)
    transport = PubSubOutboundTransport(project_id="responsive-city-458413-a2")
    transport.publish("droid-42-staging", b'{"thread":"msg"}', thread="msg")

    client = pubsub_stub.PublisherClient.return_value
    client.topic_path.assert_called_once_with(
        "responsive-city-458413-a2",
        "droid-42-staging",
    )
    client.publish.assert_called_once_with(
        "projects/responsive-city-458413-a2/topics/droid-42-staging",
        b'{"thread":"msg"}',
        thread="msg",
    )


def test_publish_omits_thread_attribute_when_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pubsub_stub = _install_pubsub_stub(monkeypatch)
    transport = PubSubOutboundTransport(project_id="p")
    transport.publish("topic", b"data")

    client = pubsub_stub.PublisherClient.return_value
    client.publish.assert_called_once_with(
        "projects/p/topics/topic",
        b"data",
    )


def test_publish_returns_broker_message_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_pubsub_stub(monkeypatch, message_id="broker-msg-xyz-789")
    transport = PubSubOutboundTransport(project_id="p")
    msg_id = transport.publish("topic", b"data", thread="msg")
    assert msg_id == "broker-msg-xyz-789"


def test_publish_forwards_timeout_to_future_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pubsub_stub = _install_pubsub_stub(monkeypatch)
    transport = PubSubOutboundTransport(project_id="p")
    transport.publish("topic", b"data", thread="msg", timeout=5.0)

    future = pubsub_stub.PublisherClient.return_value.publish.return_value
    future.result.assert_called_once_with(timeout=5.0)


def test_publish_without_timeout_calls_future_result_without_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pubsub_stub = _install_pubsub_stub(monkeypatch)
    transport = PubSubOutboundTransport(project_id="p")
    transport.publish("topic", b"data", thread="msg")

    future = pubsub_stub.PublisherClient.return_value.publish.return_value
    future.result.assert_called_once_with()


def test_publish_propagates_future_result_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pubsub_stub = _install_pubsub_stub(monkeypatch)
    future = pubsub_stub.PublisherClient.return_value.publish.return_value
    future.result.side_effect = RuntimeError("publish failed")
    transport = PubSubOutboundTransport(project_id="p")
    with pytest.raises(RuntimeError, match="publish failed"):
        transport.publish("topic", b"data", thread="msg")


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_after_aclose_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_pubsub_stub(monkeypatch)
    transport = PubSubOutboundTransport(project_id="p")
    transport.publish("topic", b"data", thread="msg")
    await transport.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        transport.publish("topic", b"data", thread="msg")


@pytest.mark.asyncio
async def test_aclose_closes_underlying_publisher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pubsub_stub = _install_pubsub_stub(monkeypatch)
    transport = PubSubOutboundTransport(project_id="p")
    transport.publish("topic", b"data", thread="msg")
    client = pubsub_stub.PublisherClient.return_value
    await transport.aclose()
    client.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_aclose_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_pubsub_stub(monkeypatch)
    transport = PubSubOutboundTransport(project_id="p")
    await transport.aclose()
    await transport.aclose()  # must not raise


@pytest.mark.asyncio
async def test_aclose_before_publish_is_a_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_pubsub_stub(monkeypatch)
    transport = PubSubOutboundTransport(project_id="p")
    await transport.aclose()  # no publisher constructed; must not raise


def test_publish_without_pubsub_v1_raises_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("droid.gateway.outbound_pubsub.pubsub_v1", None)
    transport = PubSubOutboundTransport(project_id="p")
    with pytest.raises(RuntimeError, match="google-cloud-pubsub"):
        transport.publish("topic", b"data", thread="msg")
