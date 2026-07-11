"""Twilio-facing callback URL resolution for gateway channels.

In hosted deployments the comms-app and adapters run at public Cloud Run
URLs, so Twilio can fetch TwiML and POST status callbacks directly at
``COMMS_URL`` / ``ADAPTERS_URL``. In the self-host source stack those
settings resolve to the local gateway (``http://127.0.0.1:8001``), which
Twilio cannot reach; there the publicly reachable surface is the
cloudflared tunnel resolved from the runtime URL file or
``LOCAL_COMMS_PUBLIC_URL`` fronting the
ConversationManager local ingress (the ``/local/twilio/*`` routes).

These helpers let a single gateway code path emit callbacks that Twilio
can always reach: hosted callbacks keep their existing base and path,
while self-host callbacks are rewritten onto the tunnelled local ingress.
"""

from __future__ import annotations

from unify.conversation_manager.settings import (
    local_comms_listener_url,
    local_comms_public_url,
)
from unify.settings import SETTINGS

# Publicly reachable Twilio-hosted hold audio. Used as the conference
# ``wait_url`` so it never depends on a self-host-local route being
# reachable by Twilio (matches the inbound adapters/local-provider paths).
CONFERENCE_WAIT_URL = "https://auburn-eagle-6359.twil.io/assets/ring-tone-68676.mp3"


def _local_callback_base() -> str | None:
    """Resolve the local callback base when the gateway uses local comms.

    A configured cloudflared tunnel is the authoritative self-host signal that
    Twilio-facing callbacks must target the public tunnel rather than
    ``COMMS_URL``. The gateway process itself can run in "hosted" mode, so the
    local-mode flags alone are not reliable there.
    """
    conversation = SETTINGS.conversation
    public_url = local_comms_public_url(conversation)
    if public_url:
        return public_url
    if (
        conversation.LOCAL_COMMS_ENABLED is True
        or conversation.LOCAL_COMMS_MODE == "local"
    ):
        return local_comms_listener_url(conversation)
    return None


def use_local_comms() -> bool:
    """True when comms run through the self-host local ingress edge."""
    return _local_callback_base() is not None


def twilio_callback_url(*, local_path: str, hosted_base: str, hosted_path: str) -> str:
    """Resolve a Twilio-reachable callback URL for the current environment.

    Self-host: the current tunnel URL plus ``local_path``. Hosted:
    ``{hosted_base}{hosted_path}`` (public Cloud Run comms-app / adapters).
    """
    local_base = _local_callback_base()
    if local_base is not None:
        return f"{local_base}{local_path}"
    return f"{hosted_base.rstrip('/')}{hosted_path}"
