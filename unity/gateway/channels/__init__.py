"""External channel routers for ``unity.gateway``.

Each subpackage of ``unity.gateway.channels.*`` mirrors one of the
channel modules currently living in the private ``communication``
repository, ported to consume the Phase A abstractions
(``EventBroker``, ``IngressTransport``, ``OutboundTransport``,
``Storage``, ``CredentialStore``, ``envelopes``) instead of raw GCP and
configuration globals.

See ``unity/gateway/channels/README.md`` for the migration pattern
and ``unity/gateway/PHASES.md`` (Phase B) for the per-channel
rollout schedule.
"""
