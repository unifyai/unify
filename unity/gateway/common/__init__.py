"""Shared helpers for ``unity.gateway.channels.*``.

Promotes per-channel helpers to a common location once a second
channel needs the same surface. The bar for promotion is "two
channels independently reach for the same helper"; below that, keep
helpers module-local in the channel that needs them to avoid
premature abstraction.

Current members:

* ``twilio`` -- Twilio REST Client factory shared between
  ``social/`` (the Phase B.1 POC) and ``phone/`` (Phase B.2).
* ``livekit`` -- LiveKit SDK helpers (SIP URI construction, dispatch
  rule management, agent dispatch, API client factory) needed by
  ``phone/`` and -- in upcoming channel migrations -- ``whatsapp/``
  and ``teams/`` for call/meet bridging.

Both modules consume credentials from ``unity.gateway.credentials``
rather than reading process env directly, so the credential
resolution path stays consistent with the abstractions Phase A
established.
"""
