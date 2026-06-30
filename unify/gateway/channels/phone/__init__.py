"""Phone channel: SMS / voice via Twilio + LiveKit SIP bridging.

Mirrors ``communication/phone/views.py``. The phone channel exposes
both an ``auth_router`` (admin endpoints called from inside Unity:
``/send-text``, ``/send-call``, ``/create``, ``/delete``, ``/hang-up``,
``/end-conference``, ``/dispatch-livekit-agent``,
``/available-countries``) and an ``unauth_router`` (Twilio-side
webhooks: ``/twiml``, ``/conference-status``). The Phase B
aggregator mounts both at ``/phone`` with the authenticated routes
gated by ``admin_auth``.

This is the Phase B.2 migration: the first channel to exercise BOTH
the Phase A.bis ingress + A.bis.7 outbound transports indirectly
(through ``ensure_phone_dispatch_rule`` LiveKit calls, Twilio call
creation, etc.) end-to-end against the Phase A abstractions.
"""

from unify.gateway.channels.phone.views import auth_router, unauth_router

__all__ = ["auth_router", "unauth_router"]
