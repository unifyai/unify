"""Slack channel: tenant-side admin endpoints.

This package only exposes the admin-authed endpoints (``install``,
``send``, ``status``). The high-volume inbound webhook (Slack Events
API → ``/slack/events``) is owned by ``communication/adapters`` so
it shares the same deploy-surface and rate-limit middleware as the
Twilio/Microsoft/Google adapter webhooks, while the
admin-authed pieces continue to live in the gateway alongside the
other channel routers.

Per-workspace bot tokens live in Orchestra's ``slack_installs`` table
(populated by ``/install``) and are resolved server-side by
``/send`` before forwarding to ``chat.postMessage``.
"""

from unity.gateway.channels.slack.views import auth_router

__all__ = ["auth_router"]
