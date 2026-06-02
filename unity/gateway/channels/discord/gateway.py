"""Persistent WebSocket connection to the Discord Gateway API.

Ports ``communication/discord/gateway.py`` into ``unity.gateway``.
Each pool bot maintains one connection. Inbound messages -- DMs
and guild channel @mentions -- are resolved via Orchestra and
published to the assistant's Pub/Sub topic.

Translation applied:

* ``from common.settings import SETTINGS`` -> ``from unity.settings
  import SETTINGS``.
* ``SETTINGS.{orchestra_url, comms_url, gcp_project_id}`` ->
  ``SETTINGS.ORCHESTRA_URL`` / ``SETTINGS.conversation.COMMS_URL``
  / ``SETTINGS.GCP_PROJECT_ID``.
* ``SETTINGS.orchestra_admin_key`` ->
  ``SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()``.
* ``SETTINGS.assistant_topic(assistant_id)`` -> inlined as
  ``f"unity-{assistant_id}{SETTINGS.ENV_SUFFIX}"`` (the helper is a
  one-liner; mirrors Orchestra's topic creation pattern).
* Pub/Sub publish stays as direct ``pubsub_v1.PublisherClient``.
  Could be reworked to use the Phase A.bis.7 ``OutboundTransport``
  abstraction, but that would require registering the transport in
  the adapters service context too. Holding off on that until the
  Phase C cutover when the adapters service gets a proper bootstrap
  story.
"""

from __future__ import annotations

import asyncio
import json
import logging
import platform
import random
import re
import time

import aiohttp
import httpx

from unity.gateway.common.pubsub import already_published, get_pubsub_client
from unity.settings import SETTINGS

logger = logging.getLogger("unity.gateway.channels.discord.gateway")

DISCORD_GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
DISCORD_API_BASE = "https://discord.com/api/v10"

INTENTS_DIRECT_MESSAGES = 1 << 12
INTENTS_GUILD_MESSAGES = 1 << 9
INTENTS_MESSAGE_CONTENT = 1 << 15
BOT_INTENTS = INTENTS_DIRECT_MESSAGES | INTENTS_GUILD_MESSAGES | INTENTS_MESSAGE_CONTENT

FATAL_CLOSE_CODES = {4004, 4010, 4011, 4013, 4014}
FRESH_IDENTIFY_CODES = {4003, 4007, 4009}


def _admin_token() -> str:
    """Resolve the Orchestra admin bearer token (SecretStr-safe)."""
    return SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()


def _assistant_topic(assistant_id: str) -> str:
    """Pub/Sub topic name for a specific assistant.

    Mirrors Orchestra's _env_suffix(deploy_env) pattern so topic
    names line up across services.
    """
    return f"unity-{assistant_id}{SETTINGS.ENV_SUFFIX}"


async def _resolve_discord_route(bot_id: str, sender: str) -> dict | None:
    """Resolve an inbound Discord DM to an assistant via Orchestra."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/admin/discord/resolve",
            params={"bot_id": bot_id, "sender": sender},
            headers={"Authorization": f"Bearer {_admin_token()}"},
            timeout=10.0,
        )
    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        logger.error("Discord resolve failed: %s %s", resp.status_code, resp.text)
        return None
    return resp.json()


async def _fetch_assistant(assistant_id: str) -> dict | None:
    """Fetch assistant data from Orchestra."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/admin/assistant",
            params={"agent_id": assistant_id},
            headers={"Authorization": f"Bearer {_admin_token()}"},
            timeout=10.0,
        )
    if resp.status_code >= 400:
        logger.error(
            "failed to fetch assistant %s: %s",
            assistant_id,
            resp.status_code,
        )
        return None
    data = resp.json()
    assistants = data.get("info", [])
    if not assistants:
        return None
    return assistants[0]


def _default_contacts(assistant_data: dict) -> list[dict]:
    """Synthesise the (assistant, user) contact pair from assistant fields.

    Fallback used when the assistant has no Contacts log yet (fresh
    provisioning) or when we can't reach the logs endpoint.
    """
    return [
        {
            "contact_id": 0,
            "first_name": assistant_data.get("first_name") or "",
            "surname": assistant_data.get("surname") or "",
            "email_address": assistant_data.get("email") or "",
            "phone_number": assistant_data.get("phone") or "",
            "whatsapp_number": assistant_data.get("assistant_whatsapp_number") or "",
            "discord_id": assistant_data.get("assistant_discord_bot_id") or "",
            "slack_user_id": assistant_data.get("assistant_slack_bot_user_id") or "",
            "bio": "",
            "rolling_summary": "",
            "should_respond": False,
            "response_policy": "",
        },
        {
            "contact_id": 1,
            "first_name": assistant_data.get("user_first_name") or "",
            "surname": assistant_data.get("user_last_name") or "",
            "email_address": assistant_data.get("user_email") or "",
            "phone_number": assistant_data.get("user_phone") or "",
            "whatsapp_number": assistant_data.get("user_whatsapp_number") or "",
            "discord_id": assistant_data.get("user_discord_id") or "",
            "slack_user_id": assistant_data.get("user_slack_user_id") or "",
            "bio": "",
            "rolling_summary": "",
            "should_respond": True,
            "response_policy": "",
        },
    ]


async def _fetch_contacts(assistant_data: dict) -> list[dict]:
    """Fetch the assistant's contact list from Orchestra logs."""
    user_id = assistant_data.get("user_id") or ""
    assistant_id = assistant_data.get("agent_id") or ""
    api_key = assistant_data.get("api_key") or ""
    if not api_key:
        return _default_contacts(assistant_data)

    context = f"{user_id}/{assistant_id}/Contacts"
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/logs",
            params={"project_name": "Assistants", "context": context},
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10.0,
        )
    if resp.status_code != 200:
        return _default_contacts(assistant_data)
    logs = resp.json().get("logs", [])
    if len(logs) < 2:
        return _default_contacts(assistant_data)
    return [entry["entries"] for entry in logs]


async def _ensure_job_running(
    assistant_data: dict,
    medium: str = "discord",
) -> None:
    """Fire-and-forget request to start a Unity container for this assistant."""
    assistant_id = assistant_data.get("agent_id") or ""
    api_key = assistant_data.get("api_key") or ""
    if not api_key:
        return

    def _s(key: str, default: str = "") -> str:
        v = assistant_data.get(key)
        return str(v) if v is not None else default

    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{SETTINGS.conversation.COMMS_URL}/infra/job/start",
                headers={"Authorization": f"Bearer {_admin_token()}"},
                data={
                    "api_key": api_key,
                    "medium": medium,
                    "assistant_id": assistant_id,
                    "user_id": _s("user_id"),
                    "user_first_name": _s("user_first_name"),
                    "user_surname": _s("user_last_name"),
                    "user_email": _s("user_email"),
                    "assistant_first_name": _s("first_name"),
                    "assistant_surname": _s("surname"),
                    "assistant_age": _s("age"),
                    "assistant_nationality": _s("nationality"),
                    "assistant_about": _s("about"),
                    "assistant_job_title": _s("job_title"),
                    "assistant_timezone": _s("timezone", "UTC"),
                    "user_number": _s("user_phone"),
                    "assistant_number": _s("phone"),
                    "assistant_email": _s("email"),
                    "user_whatsapp_number": _s("user_whatsapp_number"),
                    "assistant_whatsapp_number": _s("assistant_whatsapp_number"),
                    "assistant_discord_bot_id": _s("assistant_discord_bot_id"),
                    "voice_provider": _s("voice_provider"),
                    "voice_id": _s("voice_id"),
                    "desktop_mode": _s("desktop_mode", "ubuntu"),
                    "user_desktop_mode": _s("user_desktop_mode"),
                    "user_desktop_filesys_sync": (
                        "true"
                        if assistant_data.get("user_desktop_filesys_sync")
                        else "false"
                    ),
                    "user_desktop_url": _s("user_desktop_url"),
                    "demo_id": _s("demo_id"),
                    "team_ids": json.dumps(assistant_data.get("team_ids") or []),
                    "org_id": _s("organization_id"),
                    "deploy_env": _s("deploy_env"),
                },
                timeout=5.0,
            )
    except Exception:
        logger.exception("failed to start job for assistant %s", assistant_id)


async def _send_dm(bot_token: str, user_id: str, content: str) -> None:
    """Send a DM to a Discord user (for auto-reply / reject messages)."""
    headers = {
        "Authorization": f"Bot {bot_token}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient() as client:
        ch_resp = await client.post(
            f"{DISCORD_API_BASE}/users/@me/channels",
            json={"recipient_id": user_id},
            headers=headers,
            timeout=10.0,
        )
        if ch_resp.status_code >= 400:
            logger.error(
                "failed to open DM channel with %s: %s",
                user_id,
                ch_resp.text,
            )
            return
        channel_id = ch_resp.json()["id"]

        await client.post(
            f"{DISCORD_API_BASE}/channels/{channel_id}/messages",
            json={"content": content},
            headers=headers,
            timeout=10.0,
        )


def _publish_to_pubsub(
    assistant_id: str,
    message_id: str,
    bot_id: str,
    sender_discord_id: str,
    channel_id: str,
    content: str,
    role: str,
    is_channel: bool = False,
    guild_id: str | None = None,
    attachments: list[dict] | None = None,
    contacts: list[dict] | None = None,
) -> None:
    """Publish an inbound Discord message to the assistant's Pub/Sub topic."""
    client = get_pubsub_client()
    topic_path = client.topic_path(
        SETTINGS.GCP_PROJECT_ID,
        _assistant_topic(assistant_id),
    )
    payload = {
        "thread": "discord",
        "publish_timestamp": time.time(),
        "event": {
            "message_id": message_id,
            "contacts": contacts or [],
            "bot_id": bot_id,
            "sender_discord_id": sender_discord_id,
            "channel_id": channel_id,
            "body": content,
            "role": role,
            "is_channel": is_channel,
            "guild_id": guild_id,
            "attachments": attachments or [],
        },
    }
    client.publish(
        topic_path,
        json.dumps(payload).encode("utf-8"),
        thread="inbound",
    )
    kind = "channel message" if is_channel else "DM"
    logger.info(
        "published Discord %s from %s to assistant %s",
        kind,
        sender_discord_id,
        assistant_id,
    )


class GatewayConnection:
    """Manages a single bot's WebSocket to the Discord Gateway.

    Handles IDENTIFY, heartbeat, resume, reconnect, and dispatches
    inbound DM and guild channel events.
    """

    def __init__(self, bot_id: str, bot_token: str) -> None:
        self.bot_id = bot_id
        self.bot_token = bot_token
        self._session_id: str | None = None
        self._seq: int | None = None
        self._resume_url: str | None = None
        self._heartbeat_interval: float = 41.25
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._http_session: aiohttp.ClientSession | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._receive_task: asyncio.Task | None = None
        self._bot_user_id: str | None = None
        self._running = False
        self._heartbeat_acked = True
        self._reconnecting = False
        self._fatal_close_code: int | None = None

    @property
    def connected(self) -> bool:
        return self._ws is not None and not self._ws.closed and self._running

    async def start(self) -> None:
        """Connect to the Gateway and begin processing events."""
        self._running = True
        await self._connect(resume=False)

    async def stop(self) -> None:
        """Gracefully disconnect."""
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._receive_task:
            self._receive_task.cancel()
        if self._ws and not self._ws.closed:
            await self._ws.close(code=1000)
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

    async def _connect(self, resume: bool = False) -> None:
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession()

        url = self._resume_url or DISCORD_GATEWAY_URL
        self._ws = await asyncio.wait_for(
            self._http_session.ws_connect(url),
            timeout=30.0,
        )
        self._heartbeat_acked = True

        hello = await asyncio.wait_for(self._ws.receive_json(), timeout=30.0)
        if hello.get("op") != 10:
            logger.error("bot %s: expected HELLO (op 10), got %s", self.bot_id, hello)
            await self._ws.close(code=1000)
            raise ConnectionError(
                f"Bot {self.bot_id}: did not receive HELLO, "
                f"got op={hello.get('op')}",
            )
        self._heartbeat_interval = hello["d"]["heartbeat_interval"] / 1000.0

        if resume and self._session_id:
            await self._send_resume()
        else:
            await self._send_identify()

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._receive_task = asyncio.create_task(self._receive_loop())

    async def _send_identify(self) -> None:
        await self._ws.send_json(
            {
                "op": 2,
                "d": {
                    "token": self.bot_token,
                    "intents": BOT_INTENTS,
                    "properties": {
                        "os": platform.system(),
                        "browser": "unify-comms",
                        "device": "unify-comms",
                    },
                },
            },
        )

    async def _send_resume(self) -> None:
        await self._ws.send_json(
            {
                "op": 6,
                "d": {
                    "token": self.bot_token,
                    "session_id": self._session_id,
                    "seq": self._seq,
                },
            },
        )

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats; reconnect on zombie detection."""
        await asyncio.sleep(self._heartbeat_interval * random.random())
        if not self._running or not self._ws or self._ws.closed:
            return
        await self._ws.send_json({"op": 1, "d": self._seq})

        while self._running:
            await asyncio.sleep(self._heartbeat_interval)
            if not self._heartbeat_acked:
                logger.warning(
                    "bot %s: heartbeat not ACKed, reconnecting",
                    self.bot_id,
                )
                await self._reconnect()
                return
            self._heartbeat_acked = False
            if self._ws and not self._ws.closed:
                await self._ws.send_json({"op": 1, "d": self._seq})

    async def _receive_loop(self) -> None:
        """Read Gateway events and dispatch them."""
        while self._running and self._ws and not self._ws.closed:
            msg = await self._ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self._handle_event(json.loads(msg.data))
            elif msg.type in (
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.ERROR,
                aiohttp.WSMsgType.CLOSING,
            ):
                close_code = self._ws.close_code
                logger.warning(
                    "bot %s: WebSocket closed (code=%s)",
                    self.bot_id,
                    close_code,
                )
                if close_code in FATAL_CLOSE_CODES:
                    logger.error(
                        "bot %s: fatal close code %s, not reconnecting",
                        self.bot_id,
                        close_code,
                    )
                    self._running = False
                    self._fatal_close_code = close_code
                    return
                if not self._running:
                    return
                if close_code in FRESH_IDENTIFY_CODES:
                    self._session_id = None
                    self._seq = None
                    await self._reconnect(resume=False)
                else:
                    await self._reconnect(resume=True)
                return

    async def _handle_event(self, data: dict) -> None:
        op = data.get("op")
        t = data.get("t")
        d = data.get("d")
        s = data.get("s")

        if s is not None:
            self._seq = s

        if op == 11:
            self._heartbeat_acked = True
            return

        if op == 1:
            if self._ws and not self._ws.closed:
                await self._ws.send_json({"op": 1, "d": self._seq})
            return

        if op == 7:
            logger.info("bot %s: received RECONNECT", self.bot_id)
            await self._reconnect()
            return

        if op == 9:
            resumable = bool(d)
            logger.info(
                "bot %s: INVALID_SESSION (resumable=%s)",
                self.bot_id,
                resumable,
            )
            await asyncio.sleep(2)
            if not resumable:
                self._session_id = None
                self._seq = None
            await self._reconnect(resume=resumable)
            return

        if op == 0:
            if t == "READY":
                self._session_id = d["session_id"]
                self._resume_url = d.get("resume_gateway_url")
                self._bot_user_id = d["user"]["id"]
                logger.info(
                    "bot %s: READY (session=%s)",
                    self.bot_id,
                    self._session_id,
                )
            elif t == "RESUMED":
                logger.info("bot %s: RESUMED", self.bot_id)
            elif t == "MESSAGE_CREATE":
                asyncio.create_task(self._handle_message(d))

    async def _handle_message(self, data: dict) -> None:
        """Process an inbound MESSAGE_CREATE (DM or guild channel @mention)."""
        author = data.get("author", {})
        if author.get("bot"):
            return

        guild_id = data.get("guild_id")
        is_channel = guild_id is not None

        if is_channel:
            mentions = data.get("mentions", [])
            if not any(m["id"] == self._bot_user_id for m in mentions):
                return

        sender_id = author["id"]
        message_id = data["id"]
        content = data.get("content", "")
        channel_id = data["channel_id"]

        if is_channel and self._bot_user_id:
            content = re.sub(
                rf"<@!?{re.escape(self._bot_user_id)}>",
                "",
                content,
            ).strip()

        attachments = [
            {
                "id": a["id"],
                "filename": a["filename"],
                "url": a["url"],
                "content_type": a.get("content_type"),
                "size": a.get("size"),
            }
            for a in data.get("attachments", [])
        ]

        route = await _resolve_discord_route(self.bot_id, sender_id)
        if route is None:
            return

        action = route.get("action")
        if action == "auto_reply":
            if not is_channel:
                await _send_dm(
                    self.bot_token,
                    sender_id,
                    "This bot is no longer active. Please visit console.unify.ai "
                    "to view your assistant details.",
                )
            return
        if action == "reject_cold":
            if not is_channel:
                await _send_dm(
                    self.bot_token,
                    sender_id,
                    "This bot is not accepting new messages.",
                )
            return

        assistant_id = str(route["assistant_id"])
        role = route.get("role", "contact")

        assistant_data = await _fetch_assistant(assistant_id)
        contacts: list[dict] = []
        if assistant_data:
            asyncio.create_task(_ensure_job_running(assistant_data))
            contacts = await _fetch_contacts(assistant_data)

        if already_published("discord", message_id):
            logger.debug(
                "bot %s: skipping duplicate MESSAGE_CREATE %s",
                self.bot_id,
                message_id,
            )
            return

        _publish_to_pubsub(
            assistant_id=assistant_id,
            message_id=message_id,
            bot_id=self.bot_id,
            sender_discord_id=sender_id,
            channel_id=channel_id,
            content=content,
            role=role,
            is_channel=is_channel,
            guild_id=guild_id,
            attachments=attachments,
            contacts=contacts,
        )

    async def _reconnect(self, resume: bool = True) -> None:
        """Tear down and re-establish the Gateway connection."""
        if self._reconnecting:
            return
        self._reconnecting = True
        try:
            current = asyncio.current_task()
            if self._heartbeat_task and self._heartbeat_task is not current:
                self._heartbeat_task.cancel()
            if self._receive_task and self._receive_task is not current:
                self._receive_task.cancel()
            if self._ws and not self._ws.closed:
                await self._ws.close(code=4000)

            backoff = 1.0
            while self._running:
                try:
                    await self._connect(resume=resume)
                    logger.info(
                        "bot %s: reconnected (resume=%s)",
                        self.bot_id,
                        resume,
                    )
                    return
                except Exception:
                    logger.exception(
                        "bot %s: reconnect failed, retrying in %ss",
                        self.bot_id,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60.0)
        finally:
            self._reconnecting = False


__all__ = [
    "BOT_INTENTS",
    "DISCORD_API_BASE",
    "DISCORD_GATEWAY_URL",
    "FATAL_CLOSE_CODES",
    "FRESH_IDENTIFY_CODES",
    "GatewayConnection",
]
