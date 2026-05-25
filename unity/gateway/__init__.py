"""External communication gateway for Unity.

The ``unity.gateway`` package is the transport layer that connects the
assistant runtime to the outside world. It owns the abstractions through
which Unity speaks to phone networks, email providers, chat platforms,
and inbound webhook surfaces, and it owns the in-process broker and
schemas that those transports use.

Boundary
========

This package is distinct from ``unity.comms``:

* ``unity.comms`` is the *assistant-behavioural* outbound layer
  (``CommsPrimitives``). It is what the assistant *does* when it decides to
  reach out: contact resolution, capability gating, transcript publication.

* ``unity.gateway`` is the *transport* layer. It is *how* those calls reach
  the outside world: which broker carries inbound events, which storage
  backs attachments, which secret store holds API keys, which webhook
  envelope schema is on the wire.

Naming
======

The package is named ``gateway`` to match the convention used by the two
nearest open-source reference designs, ``openclaw`` and ``hermes-agent``,
which both organise their external channel surface under a ``gateway/``
directory. Adopting the same term makes Unity legible to readers coming
from either project and reinforces that Unity's external-comms layer can
be deployed locally in the same shape as those projects.

Status
======

Phase A (this package landing) provides the seam only: protocols,
in-process default implementations, and Pydantic envelope schemas. The
channel routers themselves (Twilio, Microsoft Graph, Discord, etc.) still
live in the private ``communication`` repository and are migrated in
later phases. See ``unity/gateway/PHASES.md`` for the rollout plan.
"""

from unity.gateway.event_broker import (
    EventBroker,
    PubSubConnection,
    PubSubMessage,
)
from unity.gateway.ingress import (
    AckCallable,
    EnvelopeDispatcher,
    IngressTransport,
)
from unity.gateway.ingress_inmemory import InMemoryIngressTransport
from unity.gateway.ingress_pubsub import PubSubIngressTransport
from unity.gateway.envelopes import (
    BaseEnvelope,
    BaseInboundEvent,
    EmailEnvelope,
    EmailReceivedEvent,
    Envelope,
    GenericEnvelope,
    SMSEnvelope,
    SMSReceivedEvent,
    SystemEventEnvelope,
    UnifyMessageEnvelope,
    UnifyMessageReceivedEvent,
    UnitySystemEvent,
    parse_envelope,
)
from unity.gateway.secrets import EnvSecretManager, SecretManager
from unity.gateway.storage import LocalDiskStorage, Storage

__all__ = [
    "AckCallable",
    "BaseEnvelope",
    "BaseInboundEvent",
    "EmailEnvelope",
    "EmailReceivedEvent",
    "Envelope",
    "EnvSecretManager",
    "EnvelopeDispatcher",
    "EventBroker",
    "GenericEnvelope",
    "InMemoryIngressTransport",
    "IngressTransport",
    "LocalDiskStorage",
    "PubSubConnection",
    "PubSubIngressTransport",
    "PubSubMessage",
    "SMSEnvelope",
    "SMSReceivedEvent",
    "SecretManager",
    "Storage",
    "SystemEventEnvelope",
    "UnifyMessageEnvelope",
    "UnifyMessageReceivedEvent",
    "UnitySystemEvent",
    "parse_envelope",
]
