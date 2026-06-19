"""Discord channel: bot pool + Gateway WebSocket + admin API.

Mirrors ``communication/discord/{views,bot_manager,gateway}.py``.
Most complex channel because it owns long-lived WebSocket
connections to Discord's Gateway API -- one per pool bot. Inbound
DMs and guild channel @mentions are resolved via Orchestra and
published to the destination assistant's Pub/Sub topic so the
Droid job for that assistant can pick them up.

Module layout (mirrors source):

- views.py: 5 admin endpoints (send / create / sync / delete / status)
- bot_manager.py: pool registry + connect/disconnect/sync helpers
- gateway.py: per-bot GatewayConnection + inbound Pub/Sub publish

Tenth (final) channel migration in Phase B.
"""

from droid.gateway.channels.discord.views import router

__all__ = ["router"]
