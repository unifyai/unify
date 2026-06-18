"""Transport-agnostic protocol for external inbound envelope ingress.

An ``IngressTransport`` is the seam between an external messaging
infrastructure (Google Cloud Pub/Sub, an in-process aiohttp ingress
server, a Redis Streams consumer, etc.) and the assistant runtime. It
delivers normalized envelope payloads (the
``{thread, publish_timestamp, event}`` shape defined in
``droid.gateway.envelopes``) to a caller-supplied dispatch coroutine.

Why this is separate from ``droid.gateway.event_broker.EventBroker``
------------------------------------------------------------------

``EventBroker`` is the *internal* Redis-like asyncio pub/sub that
``ConversationManager.wait_for_events()`` consumes on ``app:comms:*``
channels. It owns no external transport semantics: no acknowledgements,
no thread-pool callbacks, no JSON decoding from on-wire bytes.

The Pub/Sub plumbing currently inline in
``droid.conversation_manager.comms_manager.CommsManager`` is a different
animal: it speaks an external protocol, ack/nack-s messages, and
marshals callbacks from a background thread pool into the asyncio loop
via ``asyncio.run_coroutine_threadsafe``. That belongs behind its own
protocol -- this one -- not behind ``EventBroker``.

Once envelopes are delivered through the dispatch callback, the
internal ``EventBroker`` is still the bus they end up on (via
``CommsManager.dispatch_envelope_payload`` -> ``dispatch_inbound_envelope``
-> ``event_broker.publish("app:comms:...")``). The two protocols
collaborate; they do not duplicate each other.

Concrete implementations
========================

Planned (Phase A.bis subdivision in ``PHASES.md``):

* ``InMemoryIngressTransport`` -- default; callers (tests,
  ``LocalCommsIngress``, single-process self-hosted Droid) inject
  envelopes via an ``await transport.deliver(payload)`` call. No
  external broker.
* ``PubSubIngressTransport`` -- hosted; wraps
  ``google.cloud.pubsub_v1.SubscriberClient.subscribe()``. Bridges the
  Pub/Sub thread-pool callback model to the asyncio dispatch coroutine
  via ``asyncio.run_coroutine_threadsafe``.

Future implementations can include Redis Streams consumers, NATS
subscribers, or any other external delivery mechanism -- as long as
they satisfy this protocol, ``CommsManager`` can consume them without
modification.
"""

from __future__ import annotations

from typing import Awaitable, Callable, Protocol, runtime_checkable

AckCallable = Callable[[], None]
"""Acknowledgement callable handed to the dispatcher.

Transports that support at-least-once delivery (notably Pub/Sub)
provide ``ack``/``nack`` callables on each delivery. The dispatcher
calls ``ack()`` after successfully processing the envelope and
``nack()`` to request redelivery. Transports without ack/nack
semantics (e.g. in-process HTTP ingress) pass ``None`` for both.
"""


EnvelopeDispatcher = Callable[..., Awaitable[None]]
"""Coroutine the transport invokes for each delivered envelope.

The transport calls the dispatcher with the keyword signature::

    await dispatcher(
        payload,
        source_topic="...",
        ack=<callable or None>,
        nack=<callable or None>,
    )

This matches ``CommsManager.dispatch_envelope_payload`` exactly so the
seam introduces no shape change in the consumer.

``payload`` is the decoded envelope dict (``{thread, event,
publish_timestamp}``). ``source_topic`` is an observability hint
identifying which external source delivered the message (for Pub/Sub,
the topic name; for in-process, an empty string or a tag the caller
chooses). ``ack`` and ``nack`` are described above.
"""


@runtime_checkable
class IngressTransport(Protocol):
    """Pluggable external inbound transport for envelope delivery.

    Lifecycle:

    1. The owner constructs the transport.
    2. The owner calls ``await transport.start(dispatcher)``. The
       transport registers the dispatcher and begins delivering
       envelopes. ``start`` returns as soon as the transport is
       ready, not when delivery has stopped.
    3. Envelopes are delivered by calling ``await dispatcher(payload,
       source_topic=..., ack=..., nack=...)`` for each message. The
       transport is responsible for marshalling cross-thread callbacks
       into the asyncio loop if its underlying delivery mechanism uses
       a thread pool.
    4. The owner calls ``await transport.stop()`` to tear down. After
       ``stop`` returns, no further dispatch calls will occur.

    Implementations must be re-entrant-safe between ``start`` and
    ``stop``; calling ``start`` twice without an intervening ``stop``
    is undefined behaviour.
    """

    async def start(self, dispatcher: EnvelopeDispatcher) -> None:
        """Begin delivering envelopes to ``dispatcher``.

        Must return promptly once delivery is ready, not when delivery
        has stopped. The dispatcher is invoked for the lifetime of the
        transport until ``stop`` is called.
        """

    async def stop(self) -> None:
        """Tear down delivery and release any underlying resources.

        After ``stop`` returns, the registered dispatcher must not be
        invoked again. Idempotent: calling ``stop`` on a transport
        that has not been started, or has already been stopped, is a
        no-op.
        """


__all__ = ["AckCallable", "EnvelopeDispatcher", "IngressTransport"]
