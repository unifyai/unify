"""Transport-agnostic protocol for outbound envelope publishing.

Counterpart to ``unify.gateway.ingress.IngressTransport``. Where
``IngressTransport`` handles the *receive* side (external broker
delivers envelopes to the runtime), ``OutboundTransport`` handles the
*send* side (the runtime publishes envelopes to an external broker).

Today the publish call sites live inline in
``unify/conversation_manager/domains/comms_utils.py`` -- three of them
(``send_unify_message``, ``publish_system_error``,
``publish_assistant_desktop_ready``) all build a
``f"unity-{agent_id}{env_suffix}"`` topic and call
``pubsub_v1.PublisherClient().publish(topic_path, json.dumps(envelope)
.encode("utf-8"), thread=...).result()``. The local-comms path bypasses
Pub/Sub entirely via ``_publish_local_outbox_sync/async`` helpers.

This protocol replaces both with one seam. Phase A.bis.7.4 wires
``comms_utils.py`` to consume an injected transport via the lazy
factory pattern that A.bis.5/6 established for the ingress side.

API shape
=========

The single ``publish`` method is **synchronous**, returning the
broker-assigned message id (or empty string for backends that have no
such concept). This matches the underlying
``pubsub_v1.PublisherClient.publish().result()`` pattern that all
three call sites already use today.

Async callers that don't want to block the event loop should wrap with
``await asyncio.to_thread(transport.publish, ...)``. The current
async call sites in ``comms_utils.py`` actually do block the event
loop with ``future.result()`` -- preserving that behaviour bit-for-bit
in the migration would just inherit the existing block-in-async bug.
The transport contract leaves that decision to the caller, which is
the right place for it.

Concrete implementations
========================

Planned (Phase A.bis.7 subdivision in ``PHASES.md``):

* ``InMemoryOutboundTransport`` -- default; appends published envelopes
  to an in-memory list for inspection by tests and single-process
  self-hosted Unity. No external broker.
* ``PubSubOutboundTransport`` -- hosted; wraps
  ``pubsub_v1.PublisherClient.publish().result()`` with the same
  ``thread`` attribute and topic-path conventions today's call sites
  use.

Future implementations can include Redis Streams, NATS publishers,
or any other external delivery mechanism -- as long as they satisfy
this protocol, ``comms_utils.py`` consumers see no difference.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class OutboundTransport(Protocol):
    """Pluggable outbound envelope publisher.

    Lifecycle:

    1. The owner constructs the transport (concrete impls take any
       backend-specific config in their constructors -- project_id,
       credentials, etc.).
    2. Callers invoke ``transport.publish(topic, message, ...)`` for
       each envelope to send. Multiple calls may be made concurrently
       from different threads or coroutines; implementations must be
       safe for that.
    3. The owner calls ``await transport.aclose()`` at shutdown to
       release backend resources (sockets, thread pools, etc.).

    Implementations must be re-entrant-safe across many ``publish``
    calls; calling ``aclose`` while a publish is in flight is allowed
    and should drain or abort cleanly without leaking.
    """

    def publish(
        self,
        topic: str,
        message: bytes,
        *,
        thread: str = "",
        timeout: float | None = None,
    ) -> str:
        """Publish ``message`` bytes to ``topic`` and return the message id.

        Blocks until the publish has been accepted by the backend (or
        ``timeout`` elapses, if given and supported). On success
        returns the broker-assigned message id, or empty string if the
        backend has no such concept (in-memory, file-backed test
        transports).

        ``thread`` is the envelope's logical thread name (``"msg"``,
        ``"system_error"``, etc.) and is forwarded as a backend
        attribute when the backend supports attributes (Pub/Sub does;
        the in-memory transport just records it alongside the message).

        Raises on transport-level failure: the caller decides whether
        to retry, log, or propagate.
        """

    async def aclose(self) -> None:
        """Release backend resources.

        Idempotent: calling ``aclose`` on a transport that has not been
        used, or that has already been closed, is a no-op.
        """


__all__ = ["OutboundTransport"]
