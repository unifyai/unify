"""External channel routers for ``unify.gateway``.

Each subpackage of ``unify.gateway.channels.*`` mirrors one of the
channel modules currently living in the private ``communication``
repository, ported to consume the Phase A abstractions
(``EventBroker``, ``IngressTransport``, ``OutboundTransport``,
``Storage``, ``CredentialStore``, ``envelopes``) instead of raw GCP and
configuration globals.

See ``unify/gateway/channels/README.md`` for the migration pattern
and ``unify/gateway/PHASES.md`` (Phase B) for the per-channel
rollout schedule.
"""
