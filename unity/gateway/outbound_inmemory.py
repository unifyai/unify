"""In-process default implementation of ``OutboundTransport``.

Counterpart to ``InMemoryIngressTransport``. Records every published
envelope into an in-memory list so callers (tests, single-process
self-hosted Unity, the future ``LocalCommsIngress``-style outbox
plumbing) can inspect what would have been sent to an external
broker without needing one. No network, no threads, no external
state.

The transport assigns a synthetic incrementing message id of the
form ``"inmemory-<n>"`` so call sites that branch on a non-empty
return value still get one. Tests can also inspect ``published`` to
assert the exact envelope payload, topic, and thread name.
"""

from __future__ import annotations

from dataclasses import dataclass
import threading

from unity.gateway.outbound import OutboundTransport


@dataclass(frozen=True)
class PublishedEnvelope:
    """One published envelope captured by ``InMemoryOutboundTransport``."""

    topic: str
    message: bytes
    thread: str = ""
    message_id: str = ""


class InMemoryOutboundTransport(OutboundTransport):
    """Synchronous in-process outbound publisher.

    Thread-safe across concurrent ``publish`` calls thanks to a
    threading lock; multiple producers can append safely. Stored
    envelopes are returned by the ``published`` property as an
    immutable snapshot.

    Lifecycle: no setup is required. ``aclose`` flips a flag that
    causes subsequent ``publish`` calls to raise ``RuntimeError``,
    matching the ingress transport's "no delivery after stop"
    invariant.
    """

    def __init__(self) -> None:
        self._published: list[PublishedEnvelope] = []
        self._lock = threading.Lock()
        self._closed = False

    @property
    def published(self) -> list[PublishedEnvelope]:
        """Snapshot of envelopes published since construction.

        Returned as a fresh list copy so callers can mutate freely
        without disturbing the transport's internal record. The
        record is **not** cleared on ``aclose``; that would defeat
        post-mortem inspection in tests.
        """
        with self._lock:
            return list(self._published)

    @property
    def published_count(self) -> int:
        with self._lock:
            return len(self._published)

    def publish(
        self,
        topic: str,
        message: bytes,
        *,
        thread: str = "",
        timeout: float | None = None,
    ) -> str:
        del timeout  # in-memory transport never blocks; nothing to time out
        if self._closed:
            raise RuntimeError(
                "InMemoryOutboundTransport.publish: transport has been closed",
            )
        with self._lock:
            message_id = f"inmemory-{len(self._published)}"
            self._published.append(
                PublishedEnvelope(
                    topic=topic,
                    message=message,
                    thread=thread,
                    message_id=message_id,
                ),
            )
        return message_id

    async def aclose(self) -> None:
        self._closed = True


__all__ = ["InMemoryOutboundTransport", "PublishedEnvelope"]
