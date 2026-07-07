"""MS Teams bot channel: tenant-side admin endpoints.

The Bot-Framework analogue of ``unify.gateway.channels.slack``. Exposes
the admin-authed endpoints (``install``, ``pending-install``, ``bind``,
``send``, ``status``). The high-volume inbound webhook (Bot Connector →
``/ms-teams-bot/messages``) is owned by the hosted adapters (and mirrored
in ``unify.gateway.adapters.ms_teams_bot`` for local parity), so it shares
the same deploy surface as the Twilio / Microsoft / Google / Slack
webhooks.

Per-tenant install state (``service_url``, ``bot_app_id``, ownership) lives
in Orchestra's ``ms_teams_bot_installs`` table; the shared bot app id +
secret used to mint Bot Connector tokens are deployment secrets
(``MS_TEAMS_BOT_APP_ID`` / ``MS_TEAMS_BOT_APP_SECRET``).
"""

from unify.gateway.channels.ms_teams_bot.views import auth_router

__all__ = ["auth_router"]
