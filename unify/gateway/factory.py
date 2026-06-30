"""Factory for selecting the inbound transport based on configuration.

Centralises the "which IngressTransport should this Unity process
use?" decision in one place so that ``unify/conversation_manager/main.py``,
the future ``unity gateway serve`` CLI, and tests all reach the same
answer for the same configuration.

The factory deliberately returns ``None`` for the default ("legacy" /
"" / unset) case. ``CommsManager`` treats a ``None`` factory as
"factory not provided" and falls through to its inline
``subscribe_to_topic`` path -- so production behaviour is bit-for-bit
identical to today's inline Pub/Sub subscriber unless someone
explicitly opts in via ``UNITY_CONVERSATION_INGRESS_TRANSPORT``.
"""

from __future__ import annotations

import logging
from typing import Callable

from unify.gateway.ingress import IngressTransport
from unify.gateway.ingress_inmemory import InMemoryIngressTransport
from unify.gateway.ingress_pubsub import PubSubIngressTransport
from unify.gateway.outbound import OutboundTransport
from unify.gateway.outbound_inmemory import InMemoryOutboundTransport
from unify.gateway.outbound_pubsub import PubSubOutboundTransport

_log = logging.getLogger("unify.gateway.factory")


# Supported values for the UNITY_CONVERSATION_INGRESS_TRANSPORT env var.
# Anything else (including empty / unset) selects the legacy inline path.
TRANSPORT_KIND_LEGACY = "legacy"
TRANSPORT_KIND_INMEMORY = "inmemory"
TRANSPORT_KIND_PUBSUB = "pubsub"

KNOWN_TRANSPORT_KINDS: frozenset[str] = frozenset(
    {TRANSPORT_KIND_LEGACY, TRANSPORT_KIND_INMEMORY, TRANSPORT_KIND_PUBSUB},
)


def create_ingress_transport_factory(
    *,
    kind: str,
    subscription_id_resolver: Callable[[], str] | None = None,
    project_id: str = "",
    max_messages: int | None = 10,
) -> Callable[[], IngressTransport | None] | None:
    """Build a factory that materialises an IngressTransport at start time.

    Returns ``None`` -- meaning "don't supply a factory; use the legacy
    inline path" -- for ``kind`` values of ``""``, ``"legacy"``, or
    anything else not in ``KNOWN_TRANSPORT_KINDS``. Logs a warning for
    the unknown-kind case so misspellings surface in deploy logs.

    Returns a callable that, when invoked, constructs:

    - ``InMemoryIngressTransport`` for ``kind="inmemory"`` -- intended
      for tests and the future local-process self-hosted path.
    - ``PubSubIngressTransport`` for ``kind="pubsub"`` -- intended for
      the hosted Cloud Run + per-assistant Kubernetes Job topology.
      The factory reads ``subscription_id_resolver()`` at invocation
      time so the resolved value reflects the
      ``SESSION_DETAILS.assistant.agent_id`` that
      ``_poll_for_assignment`` set just before the factory ran.

    ``project_id`` and ``max_messages`` only matter for the pubsub
    case; they are ignored otherwise.
    """
    if not kind or kind == TRANSPORT_KIND_LEGACY:
        return None
    if kind not in KNOWN_TRANSPORT_KINDS:
        _log.warning(
            "create_ingress_transport_factory: unknown kind %r; "
            "falling back to legacy. Expected one of %s.",
            kind,
            sorted(KNOWN_TRANSPORT_KINDS),
        )
        return None

    if kind == TRANSPORT_KIND_INMEMORY:

        def _build_inmemory() -> IngressTransport:
            return InMemoryIngressTransport()

        return _build_inmemory

    if kind == TRANSPORT_KIND_PUBSUB:
        if subscription_id_resolver is None:
            raise ValueError(
                "create_ingress_transport_factory(kind='pubsub') requires "
                "subscription_id_resolver to be supplied",
            )
        if not project_id:
            raise ValueError(
                "create_ingress_transport_factory(kind='pubsub') requires "
                "project_id to be non-empty",
            )

        def _build_pubsub() -> IngressTransport | None:
            subscription_id = subscription_id_resolver()
            if not subscription_id:
                _log.warning(
                    "create_ingress_transport_factory: pubsub factory was "
                    "invoked but subscription_id_resolver returned empty "
                    "string; opting out of transport (CommsManager will use "
                    "legacy path).",
                )
                return None
            return PubSubIngressTransport(
                subscription_id=subscription_id,
                project_id=project_id,
                max_messages=max_messages,
            )

        return _build_pubsub

    # Unreachable; defensive return so the type checker sees a path.
    return None


def create_outbound_transport(
    *,
    kind: str,
    project_id: str = "",
) -> OutboundTransport | None:
    """Build an OutboundTransport at process startup.

    Returns ``None`` -- meaning "use the legacy inline path in
    ``comms_utils.py``" -- for ``kind`` values of ``""``, ``"legacy"``,
    or anything else not in ``KNOWN_TRANSPORT_KINDS``. Logs a warning
    for the unknown-kind case so misspellings surface in deploy logs.

    Returns:

    - ``InMemoryOutboundTransport`` for ``kind="inmemory"`` -- intended
      for tests and the future local-process self-hosted path.
    - ``PubSubOutboundTransport`` for ``kind="pubsub"`` -- intended
      for the hosted Cloud Run + per-assistant Kubernetes Job
      topology. ``project_id`` is required.

    Unlike the ingress factory, this returns a transport instance
    directly (not a callable factory). Outbound transports don't
    depend on ``SESSION_DETAILS.assistant.agent_id``, so they can be
    constructed eagerly at process startup.
    """
    if not kind or kind == TRANSPORT_KIND_LEGACY:
        return None
    if kind not in KNOWN_TRANSPORT_KINDS:
        _log.warning(
            "create_outbound_transport: unknown kind %r; "
            "falling back to legacy. Expected one of %s.",
            kind,
            sorted(KNOWN_TRANSPORT_KINDS),
        )
        return None

    if kind == TRANSPORT_KIND_INMEMORY:
        return InMemoryOutboundTransport()

    if kind == TRANSPORT_KIND_PUBSUB:
        if not project_id:
            raise ValueError(
                "create_outbound_transport(kind='pubsub') requires "
                "project_id to be non-empty",
            )
        return PubSubOutboundTransport(project_id=project_id)

    # Unreachable; defensive return so the type checker sees a path.
    return None


__all__ = [
    "KNOWN_TRANSPORT_KINDS",
    "TRANSPORT_KIND_INMEMORY",
    "TRANSPORT_KIND_LEGACY",
    "TRANSPORT_KIND_PUBSUB",
    "create_ingress_transport_factory",
    "create_outbound_transport",
]
