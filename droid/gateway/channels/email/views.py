"""FastAPI routes for the provider-agnostic email dispatcher.

Ports ``communication/email/views.py`` into ``droid.gateway``. The
dispatcher inspects the assistant's stored ``email_provider`` (with a
legacy fallback that token-sniffs for ``MICROSOFT_ACCESS_TOKEN``) and
forwards the request to either the gmail or outlook channel handler
unchanged.

Translation applied:

* ``from communication.helpers import _lookup_assistant`` ->
  ``from droid.gateway.common.orchestra import lookup_assistant``.
* ``from communication.outlook.views import send_outlook_email,
  get_outlook_attachment`` ->
  ``from droid.gateway.channels.outlook.views import ...``. Same for
  gmail.
* ``_clone_request`` is preserved verbatim -- it touches ASGI
  internals (``request.scope`` + an inline ``receive`` callable) and
  is the only way to forward a Starlette ``Request`` once its body
  has been consumed.

Wire behaviour is preserved bit-for-bit: same route paths, same
forwarding rules, same provider sniffing fallback.
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Request
from starlette.requests import Request as StarletteRequest

from droid.gateway.channels.gmail.views import (
    get_attachment as gmail_get_attachment,
)
from droid.gateway.channels.gmail.views import (
    send_email as gmail_send_email,
)
from droid.gateway.channels.outlook.views import (
    get_outlook_attachment,
    send_outlook_email,
)
from droid.gateway.common.orchestra import lookup_assistant
from droid.gateway.credentials import EnvCredentialStore
from droid.settings import SETTINGS

logger = logging.getLogger("droid.gateway.channels.email")

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_outlook_assistant(assistant: dict) -> bool:
    """True when the assistant uses Microsoft 365 for email.

    Checks the canonical ``email_provider`` field first, falling back
    to token-sniffing (``MICROSOFT_ACCESS_TOKEN`` presence) for
    assistants that predate the field.
    """
    provider = assistant.get("email_provider")
    if provider:
        return provider == "microsoft_365"
    return bool(assistant.get("secrets", {}).get("MICROSOFT_ACCESS_TOKEN"))


def _is_shared_coordinator_email(sender: str) -> bool:
    return (
        sender.strip().lower()
        == SETTINGS.DROID_COORDINATOR_EMAIL_ADDRESS.strip().lower()
    )


async def _clone_request(request: Request, body: bytes) -> StarletteRequest:
    """Rebuild an ASGI Request with the same headers but a fresh body stream.

    Once a Starlette ``Request``'s body has been consumed (via
    ``await request.body()`` or ``.json()``), the underlying ``receive``
    callable is exhausted -- so forwarding the request to another
    handler that wants to read it again fails. This helper clones the
    scope and constructs a new ``receive`` that yields ``body`` once.
    """
    scope = request.scope.copy()

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    return StarletteRequest(scope, receive)


# ---------------------------------------------------------------------------
# POST /send -- dispatch to gmail or outlook
# ---------------------------------------------------------------------------


@router.post("/send")
async def send_email(request: Request):
    """Send an email, routing to the correct provider.

    Request body must include ``from`` (the assistant's email
    address). The remaining fields (``to``, ``subject``, ``body``,
    ``cc``, ``bcc``, ``in_reply_to``, ``attachment``) are forwarded
    unchanged to the provider-specific handler.
    """
    body_bytes = await request.body()
    data = json.loads(body_bytes)
    sender = data.get("from")
    if not sender:
        raise HTTPException(status_code=400, detail="Missing 'from' field")

    forwarded = await _clone_request(request, body_bytes)
    if _is_shared_coordinator_email(sender):
        return await gmail_send_email(forwarded)

    credentials = EnvCredentialStore()
    assistant = await lookup_assistant(sender, credentials)

    if _is_outlook_assistant(assistant):
        return await send_outlook_email(forwarded)

    return await gmail_send_email(forwarded)


# ---------------------------------------------------------------------------
# GET /attachment -- dispatch to gmail or outlook
# ---------------------------------------------------------------------------


@router.get("/attachment")
async def get_attachment(
    receiver_email: str,
    message_id: str,
    attachment_id: str,
    filename: str | None = None,
):
    """Download an attachment, routing to the correct provider.

    Uses ``receiver_email`` to look up the assistant and decide which
    provider-specific endpoint to call.
    """
    if _is_shared_coordinator_email(receiver_email):
        return await gmail_get_attachment(
            receiver_email=receiver_email,
            gmail_message_id=message_id,
            attachment_id=attachment_id,
            filename=filename,
        )

    credentials = EnvCredentialStore()
    assistant = await lookup_assistant(receiver_email, credentials)
    if _is_outlook_assistant(assistant):
        return await get_outlook_attachment(
            user_email=receiver_email,
            message_id=message_id,
            attachment_id=attachment_id,
            filename=filename,
        )

    return await gmail_get_attachment(
        receiver_email=receiver_email,
        gmail_message_id=message_id,
        attachment_id=attachment_id,
        filename=filename,
    )


__all__ = ["router"]
