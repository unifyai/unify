"""FastAPI routes for the Outlook channel.

Ports ``communication/outlook/views.py`` into ``unify.gateway``.
This is the first channel to consume the MS Graph + Orchestra
helpers promoted to ``unify.gateway.common`` in Phase B.4.prep.

Translation applied (per channels/README.md):

* ``from common.settings import SETTINGS`` -> ``from unify.settings
  import SETTINGS`` with ``adapters_url`` -> ``SETTINGS.conversation.
  ADAPTERS_URL``.
* ``os.getenv("OUTLOOK_WEBHOOK_SECRET", ...)`` -> ``credentials.
  get_optional("OUTLOOK_WEBHOOK_SECRET", ...)`` via
  ``EnvCredentialStore``.
* All four helpers (``_lookup_assistant``, ``get_admin_graph_client``,
  ``graph_client_from_assistant``, ``get_graph_client``) imported
  from ``unify.gateway.common.{orchestra, graph}`` instead of
  ``communication.helpers``. The ``_get_user_node`` dispatch (``/me``
  vs ``/users/{email}``) lives in ``unify.gateway.common.graph`` too.
* Existing structured logger calls preserved (already using
  ``logging.getLogger`` upstream).

Wire behaviour (route paths, request/response shapes, status codes,
MS Graph payload shapes, threading semantics, attachment encoding)
is preserved bit-for-bit so the gateway aggregator can mount this
router at ``/outlook`` and external callers see no change.
"""

from __future__ import annotations

import base64
import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Request, Response
from msgraph.generated.models.body_type import BodyType
from msgraph.generated.models.email_address import EmailAddress
from msgraph.generated.models.file_attachment import FileAttachment
from msgraph.generated.models.item_body import ItemBody
from msgraph.generated.models.message import Message
from msgraph.generated.models.recipient import Recipient
from msgraph.generated.models.subscription import Subscription
from msgraph.generated.users.item.messages.item.create_reply.create_reply_post_request_body import (  # noqa: E501
    CreateReplyPostRequestBody,
)
from msgraph.generated.users.item.messages.item.reply.reply_post_request_body import (
    ReplyPostRequestBody,
)
from msgraph.generated.users.item.send_mail.send_mail_post_request_body import (
    SendMailPostRequestBody,
)

from unify.gateway.common.graph import (
    _get_user_node,
    get_admin_graph_client,
    get_graph_client,
    graph_client_from_assistant,
)
from unify.gateway.common.orchestra import lookup_assistant
from unify.gateway.credentials import EnvCredentialStore
from unify.settings import SETTINGS

logger = logging.getLogger("unify.gateway.channels.outlook")

router = APIRouter()

# Microsoft Graph subscription creation can race the Microsoft validation
# webhook back to our adapter; one quick retry handles the transient
# "validation timeout" case without escalating to a 500 to the caller.
MAX_RETRIES = 1


# ---------------------------------------------------------------------------
# DELETE /delete -- MS365 user delete (Orchestra teardown)
# ---------------------------------------------------------------------------


@router.delete("/delete")
async def delete_outlook_user(request: Request):
    """Delete an MS365 user. Treats already-absent users as success."""
    data = await request.json()
    primary_email = data.get("primary_email")
    if not primary_email:
        raise HTTPException(status_code=400, detail="Missing primary_email")

    graph = get_admin_graph_client()
    try:
        await graph.users.by_user_id(primary_email).delete()
        logger.info("deleted MS365 user %s", primary_email)
        return {
            "success": True,
            "deleted": True,
            "already_absent": False,
            "message": f"User {primary_email} deleted.",
        }
    except Exception as exc:
        err = str(exc).lower()
        if "does not exist" in err or "not found" in err:
            logger.info("MS365 user %s already absent during delete", primary_email)
            return {
                "success": True,
                "deleted": False,
                "already_absent": True,
                "message": f"User {primary_email} already absent.",
            }
        logger.error("failed to delete MS365 user %s: %s", primary_email, exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# POST /send
# ---------------------------------------------------------------------------


async def send_outlook_email(request: Request, *, assistant: dict | None = None):
    """Send an email via Microsoft Graph.

    Required body fields: ``from``, ``to`` (str or list), ``body``.
    Optional: ``subject``, ``cc``, ``bcc``, ``in_reply_to``,
    ``attachment={filename, content_base64}``.

    Replies with an attachment use the ``createReply`` + ``send``
    two-step flow because Graph's ``reply`` endpoint doesn't accept
    attachments inline; everything else goes through ``send_mail``
    directly.

    ``assistant`` is an internal override for the ``/email`` dispatcher
    (already-resolved Orchestra record). It must not be a FastAPI route
    parameter: a ``dict`` annotation would bind the JSON body and skip
    Orchestra lookup.
    """
    credentials = EnvCredentialStore()
    data = await request.json()
    sender = data.get("from")
    to = data.get("to")
    subject = data.get("subject", "")
    body = data.get("body")
    in_reply_to = data.get("in_reply_to")
    attachment = data.get("attachment")

    if not sender or not to or body is None:
        raise HTTPException(
            status_code=400,
            detail="Missing required fields: from, to, body",
        )

    to_list = [to] if isinstance(to, str) else to
    cc = data.get("cc")
    bcc = data.get("bcc")
    cc_list = [cc] if isinstance(cc, str) and cc else (cc or [])
    bcc_list = [bcc] if isinstance(bcc, str) and bcc else (bcc or [])

    try:
        if assistant is None:
            assistant = await lookup_assistant(sender, credentials)
        graph = graph_client_from_assistant(assistant, sender, credentials)
        user = _get_user_node(graph, sender, assistant)

        message = Message(
            subject=subject,
            body=ItemBody(content=body, content_type=BodyType.Text),
            to_recipients=[
                Recipient(email_address=EmailAddress(address=addr)) for addr in to_list
            ],
        )
        if cc_list:
            message.cc_recipients = [
                Recipient(email_address=EmailAddress(address=addr)) for addr in cc_list
            ]
        if bcc_list:
            message.bcc_recipients = [
                Recipient(email_address=EmailAddress(address=addr)) for addr in bcc_list
            ]

        file_attachment = None
        if attachment:
            file_attachment = FileAttachment(
                odata_type="#microsoft.graph.fileAttachment",
                name=attachment.get("filename", "attachment"),
                content_type="application/octet-stream",
                content_bytes=base64.b64decode(
                    attachment.get("content_base64", ""),
                ),
            )

        if in_reply_to:
            if file_attachment:
                draft = await user.messages.by_message_id(
                    in_reply_to,
                ).create_reply.post(
                    CreateReplyPostRequestBody(message=message),
                )
                await user.messages.by_message_id(
                    draft.id,
                ).attachments.post(file_attachment)
                await user.messages.by_message_id(draft.id).send.post()
            else:
                await user.messages.by_message_id(in_reply_to).reply.post(
                    ReplyPostRequestBody(message=message),
                )
        else:
            if file_attachment:
                message.attachments = [file_attachment]
            await user.send_mail.post(
                SendMailPostRequestBody(
                    message=message,
                    save_to_sent_items=True,
                ),
            )

        logger.info("outlook email sent from %s to %s", sender, to)
        return {"success": True}

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to send outlook email: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/send")
async def outlook_send(request: Request):
    """HTTP entrypoint for ``POST /outlook/send``."""
    return await send_outlook_email(request)


# ---------------------------------------------------------------------------
# POST /watch + DELETE /watch
# ---------------------------------------------------------------------------


@router.post("/watch")
async def watch_outlook_email(request: Request):
    """Create a Graph webhook subscription for new emails in inbox.

    MS Graph subscriptions expire after 3 days max -- the scheduled
    renewal job in the adapters service keeps them alive while the
    assistant is active. A single retry handles the transient
    validation-timeout case that occurs when MS calls the validation
    webhook back faster than the adapters service can register it.
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
        graph = await get_graph_client(user_email, credentials)
        target_resource = f"users/{user_email}/mailFolders/inbox/messages"

        subs = await graph.subscriptions.get()
        for sub in subs.value or []:
            if sub.resource and sub.resource.lower() == target_resource.lower():
                try:
                    await graph.subscriptions.by_subscription_id(sub.id).delete()
                except Exception:
                    pass

        webhook_secret = credentials.get_optional(
            "OUTLOOK_WEBHOOK_SECRET",
            "unify-outlook-webhook",
        )
        client_state = f"{webhook_secret}::{user_email}"

        for attempt in range(MAX_RETRIES + 1):
            try:
                result = await graph.subscriptions.post(
                    Subscription(
                        change_type="created",
                        notification_url=webhook_url,
                        resource=target_resource,
                        expiration_date_time=datetime.now(timezone.utc)
                        + timedelta(days=3),
                        client_state=client_state,
                    ),
                )
                logger.info(
                    "outlook watch created for %s: %s",
                    user_email,
                    result.id,
                )
                return {
                    "success": True,
                    "subscription_id": result.id,
                    "expiration": result.expiration_date_time.isoformat(),
                }
            except Exception as exc:
                error_str = str(exc).lower()
                is_validation_timeout = (
                    "validation" in error_str and "timeout" in error_str
                )
                if is_validation_timeout and attempt < MAX_RETRIES:
                    logger.warning(
                        "outlook watch validation timeout for %s, retrying...",
                        user_email,
                    )
                    continue
                raise

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to create outlook watch: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.delete("/watch")
async def delete_outlook_watch(request: Request):
    """Delete the Graph webhook subscription for ``primary_email``."""
    credentials = EnvCredentialStore()
    data = await request.json()
    primary_email = data.get("primary_email")

    if not primary_email:
        raise HTTPException(status_code=400, detail="Missing primary_email")

    try:
        graph = await get_graph_client(primary_email, credentials)
        target_resource = f"users/{primary_email}/mailFolders/inbox/messages"

        subs = await graph.subscriptions.get()
        for sub in subs.value or []:
            if sub.resource and sub.resource.lower() == target_resource.lower():
                await graph.subscriptions.by_subscription_id(sub.id).delete()
                logger.info("outlook watch deleted for %s", primary_email)
                return {"success": True, "primary_email": primary_email}

        raise HTTPException(
            status_code=404,
            detail=f"No subscription found for {primary_email}",
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to delete outlook watch: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# GET /attachment
# ---------------------------------------------------------------------------


@router.get("/attachment")
async def get_outlook_attachment(
    user_email: str,
    message_id: str,
    attachment_id: str,
    filename: str | None = None,
):
    """Download an attachment from an Outlook message."""
    credentials = EnvCredentialStore()
    try:
        assistant = await lookup_assistant(user_email, credentials)
        graph = graph_client_from_assistant(assistant, user_email, credentials)
        user = _get_user_node(graph, user_email, assistant)

        attachment = (
            await user.messages.by_message_id(message_id)
            .attachments.by_attachment_id(attachment_id)
            .get()
        )

        if not attachment:
            raise HTTPException(status_code=404, detail="Attachment not found")

        content = getattr(attachment, "content_bytes", None)
        if not content:
            raise HTTPException(
                status_code=404,
                detail="Attachment content not found",
            )

        return Response(
            content=content,
            media_type=attachment.content_type or "application/octet-stream",
            headers={
                "Content-Disposition": (
                    f"attachment; "
                    f"filename={filename or attachment.name or 'attachment'}"
                ),
            },
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to get outlook attachment: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


__all__ = ["router"]
