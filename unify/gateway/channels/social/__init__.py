"""Social-verification channel: SMS / WhatsApp verification codes.

Mirrors ``communication/social/views.py``. Two endpoints:

* ``GET  /available-platforms`` -- enumerates supported verification
  channels and their per-account cost.
* ``POST /verify`` -- sends a 6-digit verification code to the
  supplied account via the chosen platform's Twilio service.

This is the Phase B proof-of-concept channel: the smallest
representative channel in the migration set, used to establish the
``unify.gateway.channels`` directory layout, the test layout, and the
translation rules documented in
``unify/gateway/channels/README.md``.
"""

from unify.gateway.channels.social.views import router

__all__ = ["router"]
