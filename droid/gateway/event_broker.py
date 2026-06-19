"""Transport-agnostic broker protocol for inbound gateway events.

This module defines the contract that every concrete broker in
``droid.gateway`` must satisfy. Today there is one concrete implementation
(``droid.conversation_manager.in_memory_event_broker.InMemoryEventBroker``)
which already satisfies this protocol; subsequent phases add a
``PubSubEventBroker`` that wraps Google Cloud Pub/Sub behind the same
surface.

The shape is intentionally a strict subset of ``redis.asyncio.Redis``
pub/sub. That subset is what the existing in-memory broker already
exposes (including ``execute_command("PUBSUB", "NUMPAT")``), and matching
Redis keeps the door open for a Redis Streams backend without further
churn -- a natural fit for the "$5 VPS" self-hosting story described in
the nearby reference designs.
"""

from __future__ import annotations

from typing import (
    Any,
    AsyncContextManager,
    Protocol,
    TypedDict,
    runtime_checkable,
)


class PubSubMessage(TypedDict, total=False):
    """Single message returned by ``PubSubConnection.get_message``.

    ``type`` is one of ``message``, ``pmessage``, ``subscribe``,
    ``psubscribe``, ``unsubscribe``, ``punsubscribe``. For ``pmessage``
    the ``pattern`` key is also set.
    """

    type: str
    channel: str
    data: str
    pattern: str


@runtime_checkable
class PubSubConnection(Protocol):
    """A live subscription to one or more channels on an ``EventBroker``.

    Acquired via ``async with broker.pubsub() as pubsub:``. Implementations
    must guarantee that the context manager cleans up all subscriptions
    on exit, even if the body raises.
    """

    async def subscribe(self, *channels: str) -> None:
        """Subscribe to one or more exact channel names."""

    async def psubscribe(self, *patterns: str) -> None:
        """Subscribe to one or more glob-style channel patterns."""

    async def unsubscribe(self, *channels: str) -> None:
        """Unsubscribe from named channels (all, if no names given)."""

    async def punsubscribe(self, *patterns: str) -> None:
        """Unsubscribe from named patterns (all, if no patterns given)."""

    async def get_message(
        self,
        *,
        timeout: float | None = None,
        ignore_subscribe_messages: bool = False,
    ) -> PubSubMessage | None:
        """Wait for the next message on any subscribed channel.

        Returns ``None`` if ``timeout`` elapses before a message arrives.
        When ``ignore_subscribe_messages`` is true, confirmation messages
        from ``subscribe``/``psubscribe``/``unsubscribe``/``punsubscribe``
        are silently discarded and the next real message is returned.
        """

    async def aclose(self) -> None:
        """Tear down the connection and release all subscriptions."""


@runtime_checkable
class EventBroker(Protocol):
    """Async publish/subscribe broker bridging gateway transports to Droid.

    Concrete backends so far:

    * ``InMemoryEventBroker`` -- default; lives in
      ``droid.conversation_manager.in_memory_event_broker``. Used in
      tests, single-process self-hosted Droid, and offline runs.
    * ``PubSubEventBroker`` -- planned; wraps Google Cloud Pub/Sub for
      hosted multi-tenant deployments. Will land behind a
      ``DROID_EVENT_BROKER=pubsub`` selector in a dedicated PR so its
      threading model can be exercised in isolation.

    Backends may add a Redis Streams variant later; the ``execute_command``
    escape hatch is sized to that future without breaking this protocol.
    """

    async def publish(self, channel: str, message: str) -> int:
        """Publish ``message`` to ``channel``.

        Returns the number of subscribers that received the message.
        ``message`` is treated as opaque bytes-as-string by the broker;
        envelope shape is the caller's concern (see
        ``droid.gateway.envelopes``).
        """

    def pubsub(self) -> AsyncContextManager[PubSubConnection]:
        """Open a new subscription context.

        Each call returns a fresh ``PubSubConnection`` with its own
        independent subscription set. Implementations must clean up all
        of that connection's subscriptions when the context exits.
        """

    async def aclose(self) -> None:
        """Close the broker and drop all subscriptions."""

    async def execute_command(self, *args: Any) -> Any:
        """Backend-specific command escape hatch.

        The in-memory backend implements only ``PUBSUB NUMPAT`` for test
        parity with Redis. Other backends may implement more; callers
        should avoid this unless mirroring Redis semantics explicitly.
        """


__all__ = ["EventBroker", "PubSubConnection", "PubSubMessage"]
