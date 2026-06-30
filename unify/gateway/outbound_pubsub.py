"""Google Cloud Pub/Sub implementation of ``OutboundTransport``.

Extracted from the inline ``_get_publisher()`` + ``publisher.publish()``
+ ``future.result()`` pattern used by three call sites in
``unify/conversation_manager/domains/comms_utils.py``
(``send_unify_message``, ``publish_system_error``,
``publish_assistant_desktop_ready``). This is Phase A.bis.7.3: the
new transport lands as standalone code while ``comms_utils.py`` keeps
its inline copy operational. A.bis.7.4 will wire ``comms_utils.py``
to consume this transport.

Behaviour faithfully preserved
==============================

* Constructs a ``pubsub_v1.PublisherClient`` lazily (matching the
  original lazy-init pattern that avoids import-time GCP auth
  failures in tests).
* Builds the topic path as ``publisher.topic_path(project_id, topic)``
  so the same naming convention applies
  (``unity-{agent_id}{env_suffix}`` in hosted Unity).
* Forwards the ``thread`` kwarg as a Pub/Sub message attribute,
  matching how today's call sites tag every publish.
* Blocks on ``future.result(timeout)`` to return the broker-assigned
  message id, matching the existing call sites' behaviour.

Error handling
==============

Pub/Sub-level exceptions propagate from ``publish``. The current call
sites in ``comms_utils.py`` wrap each publish in their own try/except
and log+swallow errors; that policy stays with the caller, not the
transport. The transport's responsibility is to surface errors loud
and fast.
"""

from __future__ import annotations

import logging
from typing import Any

from unify.gateway.outbound import OutboundTransport

try:
    from google.cloud import pubsub_v1
except ImportError:  # pragma: no cover - exercised in pubsub-less installs
    pubsub_v1 = None

_log = logging.getLogger("unify.gateway.pubsub")


class PubSubOutboundTransport(OutboundTransport):
    """Publish envelopes to a Google Cloud Pub/Sub topic."""

    def __init__(
        self,
        *,
        project_id: str,
        credentials: Any = None,
    ) -> None:
        if not project_id:
            raise ValueError("project_id must be non-empty")
        self._project_id = project_id
        self._credentials = credentials
        self._publisher: Any = None
        self._closed = False

    @property
    def project_id(self) -> str:
        return self._project_id

    def _get_publisher(self) -> Any:
        if self._publisher is None:
            if pubsub_v1 is None:
                raise RuntimeError(
                    "PubSubOutboundTransport requires google-cloud-pubsub; "
                    "install the package and retry, or use a different "
                    "transport (e.g. InMemoryOutboundTransport)",
                )
            if self._credentials:
                self._publisher = pubsub_v1.PublisherClient(
                    credentials=self._credentials,
                )
            else:
                self._publisher = pubsub_v1.PublisherClient()
        return self._publisher

    def publish(
        self,
        topic: str,
        message: bytes,
        *,
        thread: str = "",
        timeout: float | None = None,
    ) -> str:
        if self._closed:
            raise RuntimeError(
                "PubSubOutboundTransport.publish: transport has been closed",
            )
        publisher = self._get_publisher()
        topic_path = publisher.topic_path(self._project_id, topic)
        attributes: dict[str, str] = {}
        if thread:
            attributes["thread"] = thread
        future = publisher.publish(topic_path, message, **attributes)
        if timeout is not None:
            return future.result(timeout=timeout)
        return future.result()

    async def aclose(self) -> None:
        self._closed = True
        if self._publisher is not None:
            close = getattr(self._publisher, "close", None)
            if close is not None:
                try:
                    close()
                except Exception as exc:
                    _log.warning(
                        "PubSubOutboundTransport.aclose: publisher.close raised: %s",
                        exc,
                    )
            self._publisher = None


__all__ = ["PubSubOutboundTransport"]
