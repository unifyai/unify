"""Tests for ``PubSubIngressTransport``.

Exercises the transport against a mocked ``google.cloud.pubsub_v1``
surface. Real Pub/Sub integration tests live in the deployment-soak
checks, not the unit test suite.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from droid.gateway.ingress import IngressTransport
from droid.gateway.ingress_pubsub import PubSubIngressTransport

# ---------------------------------------------------------------------------
# Test helpers: minimal stand-ins for Pub/Sub primitives
# ---------------------------------------------------------------------------


class _FakeMessage:
    def __init__(self, payload: dict) -> None:
        self.data = json.dumps(payload).encode("utf-8")
        self.ack = MagicMock()
        self.nack = MagicMock()


def _fake_subscriber_client() -> MagicMock:
    """Builds a SubscriberClient stand-in matching the surface we use."""
    client = MagicMock(name="SubscriberClient")
    client.subscription_path.side_effect = (
        lambda project, sub: f"projects/{project}/subscriptions/{sub}"
    )
    streaming_pull_future = MagicMock(name="StreamingPullFuture")
    client.subscribe.return_value = streaming_pull_future
    return client


def _install_pubsub_stub(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace the module-level pubsub_v1 with a controllable mock.

    Returns the stub so tests can assert on calls and trigger callbacks.
    """
    pubsub_stub = MagicMock(name="pubsub_v1")
    pubsub_stub.SubscriberClient.return_value = _fake_subscriber_client()
    pubsub_stub.types.FlowControl.side_effect = lambda **kwargs: MagicMock(
        name="FlowControl",
        spec=set(kwargs.keys()),
        **kwargs,
    )
    monkeypatch.setattr(
        "droid.gateway.ingress_pubsub.pubsub_v1",
        pubsub_stub,
    )
    return pubsub_stub


# ---------------------------------------------------------------------------
# Construction guards
# ---------------------------------------------------------------------------


def test_rejects_empty_subscription_id() -> None:
    with pytest.raises(ValueError, match="subscription_id"):
        PubSubIngressTransport(subscription_id="", project_id="p")


def test_rejects_empty_project_id() -> None:
    with pytest.raises(ValueError, match="project_id"):
        PubSubIngressTransport(subscription_id="s", project_id="")


def test_satisfies_ingress_transport_protocol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_pubsub_stub(monkeypatch)
    transport = PubSubIngressTransport(
        subscription_id="droid-42-sub",
        project_id="responsive-city-458413-a2",
    )
    assert isinstance(transport, IngressTransport)


def test_source_topic_derives_from_subscription_id() -> None:
    transport = PubSubIngressTransport(
        subscription_id="droid-42-staging-sub",
        project_id="p",
    )
    assert transport.source_topic == "droid-42-staging"


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_creates_subscriber_and_subscribes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pubsub_stub = _install_pubsub_stub(monkeypatch)
    transport = PubSubIngressTransport(
        subscription_id="droid-42-sub",
        project_id="responsive-city-458413-a2",
        max_messages=10,
    )

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        pass

    await transport.start(dispatcher)
    pubsub_stub.SubscriberClient.assert_called_once_with()
    client_instance = pubsub_stub.SubscriberClient.return_value
    client_instance.subscription_path.assert_called_once_with(
        "responsive-city-458413-a2",
        "droid-42-sub",
    )
    pubsub_stub.types.FlowControl.assert_called_once_with(max_messages=10)
    client_instance.subscribe.assert_called_once()
    await transport.stop()


@pytest.mark.asyncio
async def test_start_passes_credentials_when_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pubsub_stub = _install_pubsub_stub(monkeypatch)
    creds = MagicMock(name="Credentials")
    transport = PubSubIngressTransport(
        subscription_id="droid-42-sub",
        project_id="p",
        credentials=creds,
    )

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        pass

    await transport.start(dispatcher)
    pubsub_stub.SubscriberClient.assert_called_once_with(credentials=creds)
    await transport.stop()


@pytest.mark.asyncio
async def test_double_start_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_pubsub_stub(monkeypatch)
    transport = PubSubIngressTransport(subscription_id="s", project_id="p")

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        pass

    await transport.start(dispatcher)
    with pytest.raises(RuntimeError, match="already started"):
        await transport.start(dispatcher)
    await transport.stop()


@pytest.mark.asyncio
async def test_stop_cancels_future_and_closes_subscriber(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pubsub_stub = _install_pubsub_stub(monkeypatch)
    transport = PubSubIngressTransport(subscription_id="s", project_id="p")

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        pass

    await transport.start(dispatcher)
    streaming_pull_future = (
        pubsub_stub.SubscriberClient.return_value.subscribe.return_value
    )
    client_instance = pubsub_stub.SubscriberClient.return_value
    await transport.stop()
    streaming_pull_future.cancel.assert_called_once()
    client_instance.close.assert_called_once()


@pytest.mark.asyncio
async def test_stop_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_pubsub_stub(monkeypatch)
    transport = PubSubIngressTransport(subscription_id="s", project_id="p")

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        pass

    await transport.start(dispatcher)
    await transport.stop()
    await transport.stop()  # must not raise


@pytest.mark.asyncio
async def test_start_without_pubsub_v1_raises_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("droid.gateway.ingress_pubsub.pubsub_v1", None)
    transport = PubSubIngressTransport(subscription_id="s", project_id="p")

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        pass

    with pytest.raises(RuntimeError, match="google-cloud-pubsub"):
        await transport.start(dispatcher)


# ---------------------------------------------------------------------------
# Callback bridging: Pub/Sub thread -> dispatcher on asyncio loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_message_decodes_payload_and_dispatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pubsub_stub = _install_pubsub_stub(monkeypatch)
    received: list[dict[str, Any]] = []
    dispatched = asyncio.Event()

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
        dispatched.set()

    transport = PubSubIngressTransport(
        subscription_id="droid-42-staging-sub",
        project_id="p",
    )
    await transport.start(dispatcher)

    envelope = {
        "thread": "msg",
        "publish_timestamp": 1.0,
        "event": {
            "assistant_id": "42",
            "contacts": [],
            "to_number": "+1",
            "from_number": "+2",
            "body": "hi",
        },
    }
    message = _FakeMessage(envelope)
    # Simulate Pub/Sub invoking the registered callback on its background pool.
    # We invoke it inline because the dispatcher's run_coroutine_threadsafe
    # would deadlock on the running loop otherwise; we instead schedule
    # the bridging on the same loop via a thread.
    await asyncio.to_thread(transport._handle_message, message)
    await asyncio.wait_for(dispatched.wait(), timeout=2.0)

    assert len(received) == 1
    assert received[0]["payload"] == envelope
    assert received[0]["source_topic"] == "droid-42-staging"
    assert received[0]["ack"] is message.ack
    assert received[0]["nack"] is message.nack
    await transport.stop()


@pytest.mark.asyncio
async def test_blocking_dispatch_for_call_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Threads matching `blocking_dispatch_threads` must block the callback."""
    _install_pubsub_stub(monkeypatch)
    order: list[str] = []
    proceed = asyncio.Event()

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        order.append("dispatch_start")
        await proceed.wait()
        order.append("dispatch_end")

    transport = PubSubIngressTransport(
        subscription_id="droid-42-sub",
        project_id="p",
        blocking_dispatch_threads=("call", "meet"),
    )
    await transport.start(dispatcher)

    envelope = {"thread": "call_answered", "publish_timestamp": 1.0, "event": {}}
    message = _FakeMessage(envelope)
    callback_task = asyncio.create_task(
        asyncio.to_thread(transport._handle_message, message),
    )
    await asyncio.sleep(0.05)
    assert order == ["dispatch_start"]
    assert not callback_task.done()

    proceed.set()
    await asyncio.wait_for(callback_task, timeout=2.0)
    assert order == ["dispatch_start", "dispatch_end"]
    await transport.stop()


@pytest.mark.asyncio
async def test_nonblocking_dispatch_for_regular_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-call/meet threads must NOT block the Pub/Sub callback."""
    _install_pubsub_stub(monkeypatch)
    dispatched = asyncio.Event()
    proceed = asyncio.Event()

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        dispatched.set()
        await proceed.wait()

    transport = PubSubIngressTransport(
        subscription_id="droid-42-sub",
        project_id="p",
    )
    await transport.start(dispatcher)

    envelope = {"thread": "msg", "publish_timestamp": 1.0, "event": {}}
    message = _FakeMessage(envelope)
    callback_task = asyncio.create_task(
        asyncio.to_thread(transport._handle_message, message),
    )
    await asyncio.wait_for(callback_task, timeout=2.0)
    await asyncio.wait_for(dispatched.wait(), timeout=2.0)
    proceed.set()
    await transport.stop()


@pytest.mark.asyncio
async def test_transport_error_callback_invoked_on_json_decode_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_pubsub_stub(monkeypatch)
    errors: list[Exception] = []

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        pytest.fail("dispatcher must not be invoked on transport error")

    transport = PubSubIngressTransport(
        subscription_id="s",
        project_id="p",
        on_transport_error=errors.append,
    )
    await transport.start(dispatcher)

    bad_message = MagicMock()
    bad_message.data = b"not valid json"
    bad_message.ack = MagicMock()
    await asyncio.to_thread(transport._handle_message, bad_message)

    assert len(errors) == 1
    assert isinstance(errors[0], json.JSONDecodeError)
    bad_message.ack.assert_called_once()
    await transport.stop()


@pytest.mark.asyncio
async def test_transport_error_acks_even_without_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_pubsub_stub(monkeypatch)

    async def dispatcher(payload: dict, **_kwargs: Any) -> None:
        pass

    transport = PubSubIngressTransport(subscription_id="s", project_id="p")
    await transport.start(dispatcher)

    bad_message = MagicMock()
    bad_message.data = b"\xff invalid"
    bad_message.ack = MagicMock()
    await asyncio.to_thread(transport._handle_message, bad_message)
    bad_message.ack.assert_called_once()
    await transport.stop()
