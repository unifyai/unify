"""Email channel: provider-agnostic dispatcher routing to gmail or outlook.

Mirrors ``communication/email/views.py``. Single ``router`` (both
routes are admin-authed). Two endpoints that look up the assistant's
configured email provider in Orchestra and delegate to the matching
provider-specific handler:

* ``POST /send``        -- send an email
* ``GET  /attachment``  -- download an attachment

This is the natural fifth migration: completes the email family
(gmail in B.3 + outlook in B.4 + email dispatcher in B.5) and
demonstrates inter-channel composition under the channels/ tree.
"""

from droid.gateway.channels.email.views import router

__all__ = ["router"]
