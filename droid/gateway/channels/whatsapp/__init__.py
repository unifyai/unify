"""WhatsApp channel: messaging + voice calls via Twilio WhatsApp Business.

Mirrors ``communication/whatsapp/views.py``. Two routers (admin
auth_router for tenant-side endpoints and unauth_router for the
Twilio status callback). Seven endpoints total.

This is the Phase B.6 migration: the second Twilio channel (after
phone) and the largest single channel migrated so far (568 LOC).
Exercises every gateway/common helper landed so far -- twilio
factories, livekit SIP dispatch, plus the Orchestra HTTP lookups
for routing / call permission / notification status.
"""

from droid.gateway.channels.whatsapp.views import auth_router, unauth_router

__all__ = ["auth_router", "unauth_router"]
