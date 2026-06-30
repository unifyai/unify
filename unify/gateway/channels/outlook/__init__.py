"""Outlook channel: send / watch / delete via Microsoft Graph.

Mirrors ``communication/outlook/views.py``. Single ``router`` (all 5
routes are admin-authed). Counterpart to gmail for Microsoft 365 /
Outlook users; uses MS Graph SDK with per-user BYOD OAuth tokens
where available and falls back to tenant-level admin credentials
otherwise.

Endpoints:

* ``DELETE /delete``       -- delete an MS365 user
* ``POST   /send``         -- send an email (with optional attachment + threading)
* ``POST   /watch``        -- create Graph webhook subscription for inbox events
* ``DELETE /watch``        -- delete the subscription
* ``GET    /attachment``   -- download an inbound attachment
"""

from unify.gateway.channels.outlook.views import router

__all__ = ["router"]
