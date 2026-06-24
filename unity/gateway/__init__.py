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

This package provides the seam — protocols, in-process default
implementations, and Pydantic envelope schemas — plus the channel routers
(Twilio, Microsoft Graph, Discord, etc.) under ``unity.gateway.channels``.
The hosted infrastructure that wraps these routers (VM pools, tunnels,
schedulers, Kubernetes activation) lives in the ``unity-deploy`` repository.
See ``unity/gateway/PHASES.md`` for the migration history.
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
from unity.gateway.outbound import OutboundTransport
from unity.gateway.outbound_inmemory import (
    InMemoryOutboundTransport,
    PublishedEnvelope,
)
from unity.gateway.outbound_pubsub import PubSubOutboundTransport
from unity.gateway.factory import (
    KNOWN_TRANSPORT_KINDS,
    TRANSPORT_KIND_INMEMORY,
    TRANSPORT_KIND_LEGACY,
    TRANSPORT_KIND_PUBSUB,
    create_ingress_transport_factory,
    create_outbound_transport,
)
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
from unity.gateway.credentials import (
    CredentialNotFoundError,
    CredentialStore,
    EnvCredentialStore,
)
from unity.gateway.context import (
    GatewayContext,
    create_default_gateway_context,
    get_gateway_context,
)
from unity.gateway.envelope_sink import (
    DirectEnvelopeSink,
    EnvelopeSink,
    HttpEnvelopeSink,
    MissingEnvelopeSink,
    OutboundTransportEnvelopeSink,
)
from unity.gateway.public_url import (
    PublicUrlProvider,
    StaticPublicUrlProvider,
)
from unity.gateway.runtime import (
    HostedRuntimeActivator,
    LocalRuntimeActivator,
    RuntimeActivation,
    RuntimeActivator,
)
from unity.gateway.scheduler import (
    LocalScheduler,
    MissingScheduler,
    ScheduledHandle,
    Scheduler,
)
from unity.gateway.storage import LocalDiskStorage, Storage

# The aggregator (app.py) and per-channel routers live deeper in the tree
# and are not re-exported here -- importing the package should not pay
# the cost of loading every channel's third-party SDKs (msgraph, twilio,
# discord helpers, ...). Use ``from unity.gateway.app import app`` or
# ``python -m unity.gateway`` instead.

__all__ = [
    "AckCallable",
    "KNOWN_TRANSPORT_KINDS",
    "TRANSPORT_KIND_INMEMORY",
    "TRANSPORT_KIND_LEGACY",
    "TRANSPORT_KIND_PUBSUB",
    "create_ingress_transport_factory",
    "create_outbound_transport",
    "BaseEnvelope",
    "BaseInboundEvent",
    "EmailEnvelope",
    "CredentialNotFoundError",
    "CredentialStore",
    "EmailReceivedEvent",
    "Envelope",
    "EnvCredentialStore",
    "EnvelopeDispatcher",
    "EventBroker",
    "DirectEnvelopeSink",
    "EnvelopeSink",
    "GatewayContext",
    "HttpEnvelopeSink",
    "GenericEnvelope",
    "HostedRuntimeActivator",
    "InMemoryIngressTransport",
    "InMemoryOutboundTransport",
    "IngressTransport",
    "LocalRuntimeActivator",
    "LocalScheduler",
    "LocalDiskStorage",
    "MissingEnvelopeSink",
    "MissingScheduler",
    "OutboundTransport",
    "OutboundTransportEnvelopeSink",
    "PublishedEnvelope",
    "PubSubConnection",
    "PubSubIngressTransport",
    "PubSubMessage",
    "PubSubOutboundTransport",
    "PublicUrlProvider",
    "RuntimeActivation",
    "RuntimeActivator",
    "SMSEnvelope",
    "SMSReceivedEvent",
    "ScheduledHandle",
    "Scheduler",
    "StaticPublicUrlProvider",
    "Storage",
    "SystemEventEnvelope",
    "UnifyMessageEnvelope",
    "UnifyMessageReceivedEvent",
    "UnitySystemEvent",
    "create_default_gateway_context",
    "get_gateway_context",
    "parse_envelope",
]
