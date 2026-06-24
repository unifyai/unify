"""Gmail channel: send / watch / delete via Google Workspace.

Mirrors ``communication/gmail/views.py``. Single ``router`` (no
auth/unauth split -- every endpoint here is admin-authed). Five
endpoints:

* ``POST /send``           -- send an email (BYOD OAuth or SA delegation)
* ``POST /watch``          -- start Gmail push notifications
* ``DELETE /watch``        -- stop Gmail push notifications
* ``DELETE /delete``       -- delete a Workspace user
* ``GET  /attachment``     -- download an inbound attachment

This is the Phase B.3 migration: first channel that exercises the
Google API client construction pattern (service-account delegation
with optional BYOD OAuth fallback) and the Orchestra lookup helper.
Both stay channel-local for now; promote to ``unity/gateway/common/``
when the second channel needs the same surface (likely ``outlook/``
for the Orchestra lookup pattern and ``email/`` for the SA helpers).
"""

from unity.gateway.channels.gmail.views import router

__all__ = ["router"]
