"""Manages the pool of Discord bot Gateway connections.

Ports ``communication/discord/bot_manager.py`` into ``unify.gateway``.
On startup the canonical pool state is fetched from Orchestra via
``sync_from_orchestra()``. Orchestra also calls ``POST /discord/sync``
after pool mutations (token rotation, deactivation, new bots). A
lightweight health-check loop handles transient disconnects between
syncs.

Translation applied:

* ``from common.settings import SETTINGS`` -> ``from unify.settings
  import SETTINGS``.
* ``SETTINGS.orchestra_url`` -> ``SETTINGS.ORCHESTRA_URL``.
* ``SETTINGS.orchestra_admin_key`` ->
  ``SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()``.
* ``from communication.discord.gateway import GatewayConnection``
  -> ``from unify.gateway.channels.discord.gateway import
  GatewayConnection``.
"""

from __future__ import annotations

import asyncio
import logging

import httpx

from unify.gateway.channels.discord.gateway import GatewayConnection
from unify.settings import SETTINGS

logger = logging.getLogger("unify.gateway.channels.discord.bot_manager")

# bot_id -> (token, GatewayConnection)
_bots: dict[str, tuple[str, GatewayConnection]] = {}


async def connect_bot(bot_id: str, bot_token: str) -> None:
    """Add a bot to the pool and connect it to the Discord Gateway."""
    if bot_id in _bots:
        existing_conn = _bots[bot_id][1]
        if existing_conn.connected:
            logger.info("bot %s already connected", bot_id)
            return
        await existing_conn.stop()

    conn = GatewayConnection(bot_id, bot_token)
    _bots[bot_id] = (bot_token, conn)
    await conn.start()
    logger.info("bot %s connected to Discord Gateway", bot_id)


async def disconnect_bot(bot_id: str) -> None:
    """Remove a bot from the pool and close its Gateway connection."""
    entry = _bots.pop(bot_id, None)
    if entry is None:
        return
    _, conn = entry
    await conn.stop()
    logger.info("bot %s disconnected from Discord Gateway", bot_id)


def get_bot_token(bot_id: str) -> str | None:
    """Return the token for a connected bot, or None."""
    entry = _bots.get(bot_id)
    return entry[0] if entry else None


def get_all_bot_ids() -> list[str]:
    """Return all bot IDs currently in the local registry."""
    return list(_bots.keys())


def get_all_status() -> dict[str, dict]:
    """Return connection status for every bot in the pool."""
    return {
        bot_id: {
            "connected": conn.connected,
            "fatal_close_code": conn._fatal_close_code,
        }
        for bot_id, (_, conn) in _bots.items()
    }


async def sync_from_orchestra() -> int:
    """Fetch the canonical bot pool from Orchestra and reconcile local state.

    Connects new bots, rotates tokens, disconnects deactivated /
    removed bots. Returns the number of bots in the Orchestra pool.
    """
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{SETTINGS.ORCHESTRA_URL}/admin/discord/pool",
                params={"include_auth": "true"},
                headers={
                    "Authorization": (
                        f"Bearer {SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()}"
                    ),
                },
                timeout=15.0,
            )
        if resp.status_code >= 400:
            logger.warning("pool sync failed: %s", resp.status_code)
            return 0
    except Exception:
        logger.exception("pool sync request failed")
        return 0

    orchestra_bots = resp.json()
    orchestra_bot_ids: set[str] = set()

    for bot_data in orchestra_bots:
        bid = bot_data["bot_id"]
        token = bot_data.get("auth_token")
        status = bot_data.get("status", "active")
        orchestra_bot_ids.add(bid)

        if status != "active" or not token:
            await disconnect_bot(bid)
            continue

        existing_token = get_bot_token(bid)
        existing_entry = _bots.get(bid)
        has_fatal = existing_entry and existing_entry[1]._fatal_close_code is not None
        if existing_token != token or has_fatal:
            await disconnect_bot(bid)
            await connect_bot(bid, token)

    for bid in list(_bots.keys()):
        if bid not in orchestra_bot_ids:
            await disconnect_bot(bid)

    logger.info("synced %d Discord pool bots", len(orchestra_bot_ids))
    return len(orchestra_bot_ids)


async def health_check() -> None:
    """Reconnect any bots whose Gateway connection has dropped.

    Skips bots that hit a fatal Discord close code (e.g. 4004
    invalid token) -- those require operator intervention or a
    pool sync to fix.
    """
    for bot_id, (token, conn) in list(_bots.items()):
        if conn._fatal_close_code:
            logger.error(
                "bot %s has fatal close code %s, skipping reconnect "
                "(needs pool sync or operator fix)",
                bot_id,
                conn._fatal_close_code,
            )
            continue
        if not conn.connected:
            logger.warning("bot %s disconnected, reconnecting", bot_id)
            await conn.stop()
            try:
                new_conn = GatewayConnection(bot_id, token)
                _bots[bot_id] = (token, new_conn)
                await new_conn.start()
            except Exception:
                logger.exception("bot %s: health-check reconnect failed", bot_id)


async def start_health_check_loop(interval: float = 30.0) -> None:
    """Periodically verify all bots are connected.

    Pool-level sync (token rotation, deactivation, new bots) is
    handled by Orchestra calling ``POST /discord/sync`` after
    mutations -- no polling needed here.
    """
    while True:
        await asyncio.sleep(interval)
        await health_check()


__all__ = [
    "connect_bot",
    "disconnect_bot",
    "get_all_bot_ids",
    "get_all_status",
    "get_bot_token",
    "health_check",
    "start_health_check_loop",
    "sync_from_orchestra",
]
