"""In-process default implementation of ``IngressTransport``.

Used by single-process self-hosted Unity, by tests, and (in Phase
A.bis.4) by ``LocalCommsIngress`` as the delivery target underneath
its HTTP webhook routes. The transport is a thin async router: it
holds no external broker, opens no sockets, manages no threads.
Callers invoke ``await transport.deliver(payload, ...)`` to inject
envelopes; the registered dispatcher coroutine is then invoked
directly on the caller's event loop.

For test scenarios that need a configurable transport instance, use
``InMemoryIngressTransport()`` directly. For the production
self-hosted default (a process-wide singleton wired through
``unity.conversation_manager``), Phase A.bis.4 will add a factory in
the broker/transport selection layer.
"""

from __future__ import annotations

from unity.gateway.ingress import (
    AckCallable,
    EnvelopeDispatcher,
    IngressTransport,
)


class InMemoryIngressTransport(IngressTransport):
    """Synchronous in-process envelope router.

    Lifecycle matches the ``IngressTransport`` contract. The transport
    must be started before ``deliver`` is called; calling ``start``
    twice without an intervening ``stop`` raises ``RuntimeError``
    rather than silently dropping the previous dispatcher.

    Threading. The transport does not manage threads; ``deliver``
    invokes the dispatcher on the caller's event loop. Callers
    bridging from a thread pool (e.g. an aiohttp handler offloading
    work) are responsible for using ``asyncio.run_coroutine_threadsafe``
    or equivalent before invoking ``deliver``.
    """

    def __init__(self) -> None:
        self._dispatcher: EnvelopeDispatcher | None = None
        self._delivered = 0

    @property
    def delivered_count(self) -> int:
        """Number of envelopes successfully dispatched since last start.

        Reset on each ``start``. Intended for tests and lightweight
        runtime observability; not a substitute for proper metrics.
        """
        return self._delivered

    async def start(self, dispatcher: EnvelopeDispatcher) -> None:
        if self._dispatcher is not None:
            raise RuntimeError(
                "InMemoryIngressTransport.start: transport already started; "
                "call stop() first",
            )
        self._dispatcher = dispatcher
        self._delivered = 0

    async def stop(self) -> None:
        self._dispatcher = None

    async def deliver(
        self,
        payload: dict,
        *,
        source_topic: str = "",
        ack: AckCallable | None = None,
        nack: AckCallable | None = None,
    ) -> None:
        """Inject an envelope for dispatch.

        Raises ``RuntimeError`` if the transport has not been started
        (or has already been stopped). Exceptions raised by the
        dispatcher propagate to the caller; the delivery counter is
        not incremented in that case.
        """
        if self._dispatcher is None:
            raise RuntimeError(
                "InMemoryIngressTransport.deliver: transport not started",
            )
        await self._dispatcher(
            payload,
            source_topic=source_topic,
            ack=ack,
            nack=nack,
        )
        self._delivered += 1


__all__ = ["InMemoryIngressTransport"]
