"""FastAPI routes for the Teams channel.

Ports ``communication/teams/views.py`` (1139 LOC) into
``unify.gateway``. Largest channel migration. Translation applied:

* ``from common.settings import SETTINGS`` -> ``from unify.settings
  import SETTINGS``; ``SETTINGS.adapters_url`` ->
  ``SETTINGS.conversation.ADAPTERS_URL``.
* ``from communication.helpers import _lookup_assistant,
  get_admin_graph_client, get_graph_client,
  graph_client_from_assistant`` -> imports from
  ``unify.gateway.common.{orchestra, graph}``.
* ``os.getenv("TEAMS_WEBHOOK_SECRET", ...)`` ->
  ``credentials.get_optional(...)`` via ``EnvCredentialStore``.
* ``from communication.teams.create_meeting import ...`` ->
  ``from unify.gateway.channels.teams.create_meeting import ...``
  (the meeting helpers ported verbatim alongside this module).

Wire behaviour preserved bit-for-bit so the gateway aggregator can
mount this router at ``/teams`` and external callers see no change.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Request
from msgraph.generated.models.aad_user_conversation_member import (
    AadUserConversationMember,
)
from msgraph.generated.models.body_type import BodyType
from msgraph.generated.models.channel import Channel
from msgraph.generated.models.channel_membership_type import ChannelMembershipType
from msgraph.generated.models.chat import Chat
from msgraph.generated.models.chat_message import ChatMessage
from msgraph.generated.models.chat_message_attachment import ChatMessageAttachment
from msgraph.generated.models.chat_type import ChatType
from msgraph.generated.models.item_body import ItemBody
from msgraph.generated.models.subscription import Subscription

from unify.gateway.common.graph import (
    get_admin_graph_client,
    get_graph_client,
    graph_client_from_assistant,
)
from unify.gateway.common.orchestra import lookup_assistant
from unify.gateway.credentials import CredentialStore, EnvCredentialStore
from unify.settings import SETTINGS

logger = logging.getLogger("unify.gateway.channels.teams")

router = APIRouter()


# Retry once on validation timeouts; Graph occasionally fails to reach
# the webhook on the first attempt while warming up the notification
# pipeline.
MAX_RETRIES = 1

# Cap concurrent subscription POSTs to Graph so we don't stampede a
# user with hundreds of channels. Graph rate-limits aggressively on
# /subscriptions; 20 in-flight is comfortable and still parallel.
_SUB_CONCURRENCY = 20


def _teams_webhook_secret(credentials: CredentialStore) -> str:
    """Resolve TEAMS_WEBHOOK_SECRET with the legacy default preserved."""
    return credentials.get_optional("TEAMS_WEBHOOK_SECRET", "unify-teams-webhook")


# ---------------------------------------------------------------------------
# Subscription resource helpers
# ---------------------------------------------------------------------------


def _chats_resource(user_id: str) -> str:
    """Canonical per-user 'all chats' subscription resource."""
    return f"/users/{user_id}/chats/getAllMessages"


def _channel_resource(team_id: str, channel_id: str) -> str:
    """Per-channel 'all messages in this channel' subscription resource."""
    return f"/teams/{team_id}/channels/{channel_id}/messages"


def _sub_owned_by(sub, webhook_secret: str, user_email: str) -> bool:
    """Return True if *sub* was created by this service for *user_email*.

    Uses ``clientState`` (``{secret}::{email}``) as the ownership signal
    so we can safely dedupe stale subs without touching subs that belong
    to other apps or users.
    """
    cs = (sub.client_state or "") if hasattr(sub, "client_state") else ""
    if "::" not in cs:
        return False
    prefix, _, email = cs.partition("::")
    return prefix == webhook_secret and email.lower() == user_email.lower()


def _owned_teams_sub(sub, webhook_secret: str, user_email: str) -> bool:
    """Return True if *sub* is one of ours for *user_email*."""
    if not _sub_owned_by(sub, webhook_secret, user_email):
        return False
    resource = (sub.resource or "").lower()
    if resource.endswith("/chats/getallmessages"):
        return True
    if (
        "/teams/" in resource
        and "/channels/" in resource
        and resource.endswith("/messages")
    ):
        return True
    return False


# ---------------------------------------------------------------------------
# Attachment + chat-message construction
# ---------------------------------------------------------------------------


async def _upload_and_build_attachments(
    graph,
    raw_attachments: list[dict],
) -> list[ChatMessageAttachment]:
    """Upload files to OneDrive and return Graph ChatMessageAttachment objects."""
    result: list[ChatMessageAttachment] = []
    if not raw_attachments:
        return result

    # ``graph.me.drive`` (DriveRequestBuilder) only supports fetching the drive;
    # item operations go through ``graph.drives.by_drive_id(...)``. Resolve the
    # personal drive id once, then address uploads by path via ``with_url``
    # (``drive.root.item_with_path`` no longer exists in the SDK).
    me_drive = await graph.me.drive.get()
    drive = graph.drives.by_drive_id(me_drive.id)
    base_url = graph.request_adapter.base_url.rstrip("/")

    for att in raw_attachments:
        filename = att.get("filename", "attachment")
        content_b64 = att.get("content_base64", "")
        if not content_b64:
            continue
        file_bytes = base64.b64decode(content_b64)

        safe_name = f"{uuid.uuid4().hex[:8]}_{filename}"
        encoded = quote(f"Teams Attachments/{safe_name}", safe="/")
        content_url = f"{base_url}/drives/{me_drive.id}/root:/{encoded}:/content"
        drive_item = (
            await drive.items.by_drive_item_id("root")
            .content.with_url(content_url)
            .put(file_bytes)
        )

        att_id = uuid.uuid4().hex
        result.append(
            ChatMessageAttachment(
                id=att_id,
                content_type="reference",
                content_url=drive_item.web_url,
                name=filename,
            ),
        )
    return result


def _build_chat_message(
    body: str,
    content_type: str,
    attachments: list[ChatMessageAttachment],
) -> ChatMessage:
    """Build a ChatMessage, embedding ``<attachment>`` tags when needed."""
    if attachments:
        att_tags = "".join(
            f'<attachment id="{a.id}"></attachment>' for a in attachments
        )
        html_body = f"{body} {att_tags}" if body else att_tags
        return ChatMessage(
            body=ItemBody(content=html_body, content_type=BodyType.Html),
            attachments=attachments,
        )
    return ChatMessage(
        body=ItemBody(
            content=body,
            content_type=BodyType.Html if content_type == "html" else BodyType.Text,
        ),
    )


def _build_owner_member(upn: str) -> AadUserConversationMember:
    """Build a chat/channel member bound to a user UPN with owner role."""
    member = AadUserConversationMember(
        odata_type="#microsoft.graph.aadUserConversationMember",
        roles=["owner"],
    )
    member.additional_data = {
        "user@odata.bind": f"https://graph.microsoft.com/v1.0/users/{upn}",
    }
    return member


# ---------------------------------------------------------------------------
# Watch helpers (channel enumeration + subscription POST + rebuild)
# ---------------------------------------------------------------------------


async def _enumerate_user_channels(graph) -> list[tuple[str, str, str]]:
    """Return ``[(team_id, channel_id, display_name), ...]`` for the user."""
    joined = await graph.me.joined_teams.get()
    teams = list(joined.value or [])

    sem = asyncio.Semaphore(_SUB_CONCURRENCY)

    async def _channels_for(team_id: str) -> list[tuple[str, str, str]]:
        async with sem:
            resp = await graph.teams.by_team_id(team_id).channels.get()
        return [
            (team_id, ch.id, ch.display_name or "")
            for ch in (resp.value or [])
            if ch.id
        ]

    per_team = await asyncio.gather(
        *[_channels_for(t.id) for t in teams if t.id],
        return_exceptions=True,
    )
    out: list[tuple[str, str, str]] = []
    for result in per_team:
        if isinstance(result, Exception):
            logger.warning("failed to list channels for a team: %s", result)
            continue
        out.extend(result)
    return out


async def _create_one_subscription(
    graph,
    *,
    resource: str,
    webhook_url: str,
    client_state: str,
    user_email: str,
) -> dict:
    """POST a single subscription with our retry behaviour."""
    sub_kwargs = dict(
        change_type="created",
        notification_url=webhook_url,
        resource=resource,
        expiration_date_time=datetime.now(timezone.utc) + timedelta(minutes=60),
        client_state=client_state,
    )
    last_err: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            result = await graph.subscriptions.post(Subscription(**sub_kwargs))
            logger.info(
                "Teams watch created on %s for %s: %s",
                resource,
                user_email,
                result.id,
            )
            return {
                "resource": resource,
                "subscription_id": result.id,
                "expiration": result.expiration_date_time.isoformat(),
            }
        except Exception as exc:
            last_err = exc
            error_str = str(exc).lower()
            is_validation_timeout = "validation" in error_str and "timeout" in error_str
            if is_validation_timeout and attempt < MAX_RETRIES:
                logger.warning(
                    "Teams watch validation timeout on %s for %s, retrying...",
                    resource,
                    user_email,
                )
                continue
            break
    logger.warning(
        "Teams watch on %s for %s failed: %s",
        resource,
        user_email,
        last_err,
    )
    return {"resource": resource, "error": str(last_err)}


async def _rebuild_teams_watches(
    graph,
    *,
    user_email: str,
    user_id: str,
    webhook_url: str,
    credentials: CredentialStore,
) -> dict:
    """Tear down owned subs for this user and recreate the full set."""
    webhook_secret = _teams_webhook_secret(credentials)
    client_state = f"{webhook_secret}::{user_email}"

    subs = await graph.subscriptions.get()
    for sub in subs.value or []:
        if not _owned_teams_sub(sub, webhook_secret, user_email):
            continue
        try:
            await graph.subscriptions.by_subscription_id(sub.id).delete()
        except Exception:
            pass

    channels = await _enumerate_user_channels(graph)
    channel_resources = [_channel_resource(tid, cid) for tid, cid, _ in channels]

    sem = asyncio.Semaphore(_SUB_CONCURRENCY)

    async def _guarded(resource: str) -> dict:
        async with sem:
            return await _create_one_subscription(
                graph,
                resource=resource,
                webhook_url=webhook_url,
                client_state=client_state,
                user_email=user_email,
            )

    chats_task = asyncio.create_task(_guarded(_chats_resource(user_id)))
    channel_tasks = [asyncio.create_task(_guarded(r)) for r in channel_resources]
    chats_result = await chats_task
    channel_results = await asyncio.gather(*channel_tasks) if channel_tasks else []

    channel_failures = sum(1 for r in channel_results if "error" in r)

    return {
        "success": "subscription_id" in chats_result,
        "chats": chats_result,
        "channels": channel_results,
        "channel_count": len(channel_results),
        "channel_failures": channel_failures,
    }


# ---------------------------------------------------------------------------
# Chat endpoints
# ---------------------------------------------------------------------------


@router.post("/send")
async def send_teams_chat(request: Request):
    """Send a message to a Teams chat."""
    data = await request.json()
    sender = data.get("from")
    chat_id = data.get("chat_id")
    body = data.get("body")
    content_type = data.get("content_type", "text")
    raw_attachments = data.get("attachments") or []

    if not sender or not chat_id or body is None:
        raise HTTPException(
            status_code=400,
            detail="Missing required fields: from, chat_id, body",
        )

    try:
        graph = await get_graph_client(sender)
        attachments = await _upload_and_build_attachments(graph, raw_attachments)
        message = _build_chat_message(body, content_type, attachments)
        result = await graph.me.chats.by_chat_id(chat_id).messages.post(message)
        logger.info("Teams chat message sent from %s to chat %s", sender, chat_id)
        return {"success": True, "message_id": result.id}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to send Teams chat message: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/chats")
async def create_teams_chat(request: Request):
    """Create (or return the existing) Teams chat."""
    data = await request.json()
    sender = data.get("from")
    chat_type_raw = (data.get("chat_type") or "").strip()
    members_in = data.get("members")
    topic = data.get("topic")

    if not sender or not chat_type_raw or not isinstance(members_in, list):
        raise HTTPException(
            status_code=400,
            detail="Missing required fields: from, chat_type, members",
        )
    if chat_type_raw not in ("oneOnOne", "group"):
        raise HTTPException(
            status_code=400,
            detail="chat_type must be 'oneOnOne' or 'group'",
        )
    if chat_type_raw == "oneOnOne" and len(members_in) != 1:
        raise HTTPException(
            status_code=400,
            detail="oneOnOne requires exactly one member (besides sender)",
        )
    if chat_type_raw == "group" and len(members_in) < 2:
        raise HTTPException(
            status_code=400,
            detail="group requires at least two members (besides sender)",
        )
    if chat_type_raw == "oneOnOne" and topic:
        raise HTTPException(
            status_code=400,
            detail="oneOnOne chats cannot have a topic",
        )

    try:
        graph = await get_graph_client(sender)
        me = await graph.me.get()
        sender_upn = me.user_principal_name or sender

        upns: list[str] = []
        seen: set[str] = set()
        for upn in [sender_upn, *members_in]:
            key = (upn or "").lower()
            if not key or key in seen:
                continue
            seen.add(key)
            upns.append(upn)

        chat = Chat(
            chat_type=(
                ChatType.OneOnOne if chat_type_raw == "oneOnOne" else ChatType.Group
            ),
            members=[_build_owner_member(upn) for upn in upns],
        )
        if chat_type_raw == "group" and topic:
            chat.topic = topic

        result = await graph.chats.post(chat)
        logger.info(
            "Teams chat created/returned for %s (%s, %d members): %s",
            sender,
            chat_type_raw,
            len(upns),
            result.id,
        )
        return {
            "success": True,
            "chat_id": result.id,
            "chat_type": chat_type_raw,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to create Teams chat: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/chats")
async def list_teams_chats(user_email: str):
    """List all chats for a user."""
    try:
        graph = await get_graph_client(user_email)
        chats = await graph.me.chats.get()
        chat_list = []
        for chat in chats.value or []:
            chat_list.append(
                {
                    "id": chat.id,
                    "topic": chat.topic,
                    "chat_type": str(chat.chat_type) if chat.chat_type else None,
                    "created_datetime": (
                        chat.created_date_time.isoformat()
                        if chat.created_date_time
                        else None
                    ),
                },
            )
        return {"success": True, "chats": chat_list}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to list Teams chats: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Watch endpoints
# ---------------------------------------------------------------------------


@router.post("/watch")
async def watch_teams(request: Request):
    """Create the unified Teams change-notification subscriptions for a user.

    BYOD-only: requires a delegated MICROSOFT_ACCESS_TOKEN. Returns
    409 when missing rather than silently falling back to app-only
    (which would require ?model= billing param and defeat the unified
    watcher's purpose).
    """
    credentials = EnvCredentialStore()
    data = await request.json()
    user_email = data.get("primary_email")
    webhook_url = (
        data.get("webhook_url")
        or f"{SETTINGS.conversation.ADAPTERS_URL}/microsoft/router"
    )

    if not user_email:
        raise HTTPException(status_code=400, detail="Missing primary_email")

    try:
        try:
            assistant = await lookup_assistant(user_email, credentials)
        except HTTPException:
            assistant = None

        has_user_token = bool(
            (assistant or {}).get("secrets", {}).get("MICROSOFT_ACCESS_TOKEN"),
        )
        if not has_user_token:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"No delegated MICROSOFT_ACCESS_TOKEN for {user_email}. "
                    "Run the BYOD OAuth flow (microsoft/auth/callback) "
                    "to mint delegated user tokens for this assistant."
                ),
            )

        graph = graph_client_from_assistant(assistant, user_email, credentials)

        me = await graph.me.get()
        user_id = me.id
        if not user_id:
            raise HTTPException(
                status_code=500,
                detail=f"Graph /me returned no id for {user_email}",
            )

        return await _rebuild_teams_watches(
            graph,
            user_email=user_email,
            user_id=user_id,
            webhook_url=webhook_url,
            credentials=credentials,
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to create Teams watch: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.delete("/watch")
async def delete_teams_watch(request: Request):
    """Delete all unified Teams subscriptions for a user."""
    credentials = EnvCredentialStore()
    data = await request.json()
    primary_email = data.get("primary_email")

    if not primary_email:
        raise HTTPException(status_code=400, detail="Missing primary_email")

    try:
        try:
            assistant = await lookup_assistant(primary_email, credentials)
            graph = graph_client_from_assistant(assistant, primary_email, credentials)
        except HTTPException:
            graph = get_admin_graph_client(credentials)

        webhook_secret = _teams_webhook_secret(credentials)

        subs = await graph.subscriptions.get()
        deleted = 0
        for sub in subs.value or []:
            if not _owned_teams_sub(sub, webhook_secret, primary_email):
                continue
            await graph.subscriptions.by_subscription_id(sub.id).delete()
            deleted += 1

        if deleted:
            logger.info(
                "Teams watch deleted for %s (%d subscription(s))",
                primary_email,
                deleted,
            )
            return {
                "success": True,
                "primary_email": primary_email,
                "deleted": deleted,
            }

        raise HTTPException(
            status_code=404,
            detail=f"No subscription found for {primary_email}",
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to delete Teams watch: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Chat messages
# ---------------------------------------------------------------------------


@router.get("/messages/{chat_id}")
async def get_teams_chat_messages(
    chat_id: str,
    user_email: str,
    top: int = 50,
):
    """Get messages from a specific Teams chat."""
    try:
        graph = await get_graph_client(user_email)
        messages = await graph.me.chats.by_chat_id(chat_id).messages.get()

        message_list = []
        for msg in (messages.value or [])[:top]:
            sender_info = msg.from_
            sender_name = "Unknown"
            sender_id = None
            if sender_info and sender_info.user:
                sender_name = sender_info.user.display_name or "Unknown"
                sender_id = sender_info.user.id
            message_list.append(
                {
                    "id": msg.id,
                    "sender": sender_name,
                    "sender_id": sender_id,
                    "content": msg.body.content if msg.body else None,
                    "content_type": (
                        str(msg.body.content_type)
                        if msg.body and msg.body.content_type
                        else None
                    ),
                    "created_datetime": (
                        msg.created_date_time.isoformat()
                        if msg.created_date_time
                        else None
                    ),
                },
            )

        return {"success": True, "messages": message_list}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to get Teams chat messages: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Team + channel endpoints
# ---------------------------------------------------------------------------


@router.get("/teams")
async def list_joined_teams(user_email: str):
    """List all teams the user is a member of."""
    try:
        graph = await get_graph_client(user_email)
        teams = await graph.me.joined_teams.get()
        team_list = []
        for team in teams.value or []:
            team_list.append(
                {
                    "id": team.id,
                    "display_name": team.display_name,
                    "description": team.description,
                },
            )
        return {"success": True, "teams": team_list}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to list joined teams: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/teams/{team_id}/channels")
async def list_team_channels(team_id: str, user_email: str):
    """List all channels in a team."""
    try:
        graph = await get_graph_client(user_email)
        channels = await graph.teams.by_team_id(team_id).channels.get()
        channel_list = []
        for channel in channels.value or []:
            channel_list.append(
                {
                    "id": channel.id,
                    "display_name": channel.display_name,
                    "description": channel.description,
                    "membership_type": (
                        str(channel.membership_type)
                        if channel.membership_type
                        else None
                    ),
                },
            )
        return {"success": True, "channels": channel_list}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to list team channels: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


_CHANNEL_MEMBERSHIP_TYPES = {
    "standard": ChannelMembershipType.Standard,
    "private": ChannelMembershipType.Private,
    "shared": ChannelMembershipType.Shared,
}


@router.post("/channels")
async def create_teams_channel(request: Request):
    """Create a channel inside an existing team."""
    credentials = EnvCredentialStore()
    data = await request.json()
    sender = data.get("from")
    team_id = data.get("team_id")
    display_name = data.get("display_name")
    description = data.get("description")
    membership_type_raw = (data.get("membership_type") or "standard").strip()
    owners = data.get("owners") or []

    if not sender or not team_id or not display_name:
        raise HTTPException(
            status_code=400,
            detail="Missing required fields: from, team_id, display_name",
        )
    if membership_type_raw not in _CHANNEL_MEMBERSHIP_TYPES:
        raise HTTPException(
            status_code=400,
            detail="membership_type must be 'standard', 'private', or 'shared'",
        )
    if not isinstance(owners, list):
        raise HTTPException(
            status_code=400,
            detail="owners must be a list of UPNs when provided",
        )
    if membership_type_raw != "standard" and not owners:
        raise HTTPException(
            status_code=400,
            detail=f"{membership_type_raw} channels require at least one owner",
        )

    try:
        graph = await get_graph_client(sender)

        channel = Channel(
            display_name=display_name,
            description=description,
            membership_type=_CHANNEL_MEMBERSHIP_TYPES[membership_type_raw],
        )
        if membership_type_raw != "standard":
            channel.members = [_build_owner_member(upn) for upn in owners]

        result = await graph.teams.by_team_id(team_id).channels.post(channel)
        logger.info(
            "Teams channel created by %s in team %s (%s): %s",
            sender,
            team_id,
            membership_type_raw,
            result.id,
        )

        webhook_url = f"{SETTINGS.conversation.ADAPTERS_URL}/microsoft/router"
        rebuild: dict | None = None
        try:
            me = await graph.me.get()
            if me.id:
                rebuild = await _rebuild_teams_watches(
                    graph,
                    user_email=sender,
                    user_id=me.id,
                    webhook_url=webhook_url,
                    credentials=credentials,
                )
        except Exception as sub_err:
            logger.warning(
                "Teams watch rebuild after channel create failed: %s",
                sub_err,
            )

        return {
            "success": True,
            "channel_id": result.id,
            "team_id": team_id,
            "membership_type": membership_type_raw,
            "watch_rebuild": rebuild,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to create Teams channel: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/channel/{team_id}/{channel_id}/send")
async def send_teams_channel_message(
    team_id: str,
    channel_id: str,
    request: Request,
):
    """Send a message to a Teams channel."""
    data = await request.json()
    sender = data.get("from")
    body = data.get("body")
    content_type = data.get("content_type", "text")
    raw_attachments = data.get("attachments") or []

    if not sender or body is None:
        raise HTTPException(
            status_code=400,
            detail="Missing required fields: from, body",
        )

    try:
        graph = await get_graph_client(sender)
        attachments = await _upload_and_build_attachments(graph, raw_attachments)
        message = _build_chat_message(body, content_type, attachments)

        result = (
            await graph.teams.by_team_id(team_id)
            .channels.by_channel_id(channel_id)
            .messages.post(message)
        )

        logger.info(
            "Teams channel message sent from %s to %s/%s",
            sender,
            team_id,
            channel_id,
        )
        return {"success": True, "message_id": result.id}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to send Teams channel message: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/channel/{team_id}/{channel_id}/messages")
async def get_teams_channel_messages(
    team_id: str,
    channel_id: str,
    user_email: str,
    top: int = 50,
):
    """Get messages from a Teams channel."""
    try:
        graph = await get_graph_client(user_email)
        messages = (
            await graph.teams.by_team_id(team_id)
            .channels.by_channel_id(channel_id)
            .messages.get()
        )

        message_list = []
        for msg in (messages.value or [])[:top]:
            sender_info = msg.from_
            sender_name = "Unknown"
            sender_id = None
            if sender_info and sender_info.user:
                sender_name = sender_info.user.display_name or "Unknown"
                sender_id = sender_info.user.id
            message_list.append(
                {
                    "id": msg.id,
                    "sender": sender_name,
                    "sender_id": sender_id,
                    "content": msg.body.content if msg.body else None,
                    "content_type": (
                        str(msg.body.content_type)
                        if msg.body and msg.body.content_type
                        else None
                    ),
                    "created_datetime": (
                        msg.created_date_time.isoformat()
                        if msg.created_date_time
                        else None
                    ),
                },
            )

        return {"success": True, "messages": message_list}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to get Teams channel messages: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Meeting creation
# ---------------------------------------------------------------------------


@router.post("/create_meeting")
async def create_teams_meeting(request: Request):
    """Create a Teams online meeting via Microsoft Graph (instant or scheduled)."""
    from unify.gateway.channels.teams.create_meeting import (
        create_instant_onlinemeeting,
        create_scheduled_meeting_event,
    )

    credentials = EnvCredentialStore()
    data = await request.json()
    assistant_email = data.get("assistant_email")
    mode = (data.get("mode") or "instant").strip().lower()

    if not assistant_email:
        raise HTTPException(status_code=400, detail="Missing assistant_email")
    if mode not in ("instant", "scheduled"):
        raise HTTPException(
            status_code=400,
            detail="mode must be 'instant' or 'scheduled'",
        )

    assistant = await lookup_assistant(assistant_email, credentials)
    access_token = (assistant.get("secrets") or {}).get("MICROSOFT_ACCESS_TOKEN") or ""
    if not access_token:
        raise HTTPException(
            status_code=409,
            detail=(
                f"No delegated MICROSOFT_ACCESS_TOKEN for {assistant_email}. "
                "Run the BYOD OAuth flow with OnlineMeetings.ReadWrite "
                "(and Calendars.ReadWrite for scheduled mode)."
            ),
        )

    try:
        if mode == "instant":
            created = await create_instant_onlinemeeting(
                access_token,
                subject=data.get("subject"),
                start_datetime=data.get("start"),
                end_datetime=data.get("end"),
            )
        else:
            subject = data.get("subject")
            start = data.get("start")
            end = data.get("end")
            if not (subject and start and end):
                raise HTTPException(
                    status_code=400,
                    detail="scheduled mode requires subject, start, end",
                )
            created = await create_scheduled_meeting_event(
                access_token,
                subject=subject,
                start_datetime=start,
                end_datetime=end,
                timezone=data.get("timezone") or "UTC",
                attendees=data.get("attendees"),
                body_html=data.get("body"),
                location=data.get("location"),
            )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except RuntimeError as exc:
        logger.error("Graph meeting creation failed: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc))

    return {
        "success": True,
        "join_web_url": created.join_web_url,
        "meeting_id": created.meeting_id,
        "event_id": created.event_id,
        "subject": created.subject,
        "start": created.start_datetime,
        "end": created.end_datetime,
        "web_link": created.web_link,
    }


__all__ = ["router"]
