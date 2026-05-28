"""In-memory dedup + Pub/Sub publisher factory shared by inbound channels.

Promoted to the common layer when both ``discord/`` and ``slack/``
needed the same dedup window (Discord ``MESSAGE_CREATE`` redelivery on
reconnect; Slack Events API redelivery on missed-ack) plus the same
process-wide ``pubsub_v1.PublisherClient`` singleton.

Each gateway calls ``already_published`` to short-circuit duplicate
events and ``get_pubsub_client`` to publish onto the per-assistant
Pub/Sub topic ``unity-<assistant_id>{ENV_SUFFIX}``.
"""

from __future__ import annotations

import time

from google.cloud import pubsub_v1

_DEDUP_TTL = 300.0
_seen_ids: dict[str, dict[str, float]] = {}
_pubsub_client: pubsub_v1.PublisherClient | None = None


def already_published(namespace: str, message_id: str) -> bool:
    """Return True if ``(namespace, message_id)`` was seen recently.

    ``namespace`` lets us keep per-channel dedup windows isolated even
    when message IDs collide across providers (e.g. Slack ``event_id``
    vs Discord snowflake).
    """
    bucket = _seen_ids.setdefault(namespace, {})
    now = time.time()
    cutoff = now - _DEDUP_TTL
    expired = [k for k, t in bucket.items() if t < cutoff]
    for k in expired:
        del bucket[k]
    if message_id in bucket:
        return True
    bucket[message_id] = now
    return False


def get_pubsub_client() -> pubsub_v1.PublisherClient:
    """Return a process-wide PublisherClient singleton."""
    global _pubsub_client
    if _pubsub_client is None:
        _pubsub_client = pubsub_v1.PublisherClient()
    return _pubsub_client


__all__ = ["already_published", "get_pubsub_client"]
