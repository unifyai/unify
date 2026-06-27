"""Twilio-facing callback URL resolution for gateway channels.

In hosted deployments the comms-app and adapters run at public Cloud Run
URLs, so Twilio can fetch TwiML and POST status callbacks directly at
``COMMS_URL`` / ``ADAPTERS_URL``. In the self-host source stack those
settings resolve to the local gateway (``http://127.0.0.1:8001``), which
Twilio cannot reach; there the publicly reachable surface is the
cloudflared tunnel in ``LOCAL_COMMS_PUBLIC_URL`` fronting the
ConversationManager local ingress (the ``/local/twilio/*`` routes).

These helpers let a single gateway code path emit callbacks that Twilio
can always reach: hosted callbacks keep their existing base and path,
while self-host callbacks are rewritten onto the tunnelled local ingress.
"""

from __future__ import annotations

from unity.settings import SETTINGS

# Publicly reachable Twilio-hosted hold audio. Used as the conference
# ``wait_url`` so it never depends on a self-host-local route being
# reachable by Twilio (matches the inbound adapters/local-provider paths).
CONFERENCE_WAIT_URL = "https://auburn-eagle-6359.twil.io/assets/ring-tone-68676.mp3"


def use_local_comms() -> bool:
    """True when comms run through the self-host local ingress edge.

    A configured cloudflared tunnel (``LOCAL_COMMS_PUBLIC_URL``) is the
    authoritative self-host signal that Twilio-facing callbacks must target the
    public tunnel rather than ``COMMS_URL``. The gateway process itself runs in
    "hosted" mode (``COMMS_URL`` set -> ``LOCAL_COMMS_MODE`` resolves to
    ``hosted`` / ``LOCAL_COMMS_ENABLED`` false), so those flags are not reliable
    here; the tunnel URL only ever exists in the self-host stack.
    """
    conversation = SETTINGS.conversation
    if conversation.LOCAL_COMMS_PUBLIC_URL.strip():
        return True
    if conversation.LOCAL_COMMS_ENABLED is True:
        return True
    return conversation.LOCAL_COMMS_MODE == "local"


def local_comms_public_base() -> str:
    """Externally reachable base URL for the local comms ingress."""
    conversation = SETTINGS.conversation
    public_url = conversation.LOCAL_COMMS_PUBLIC_URL.strip()
    if public_url:
        return public_url.rstrip("/")
    return f"http://{conversation.LOCAL_COMMS_HOST}:{conversation.LOCAL_COMMS_PORT}"


def twilio_callback_url(*, local_path: str, hosted_base: str, hosted_path: str) -> str:
    """Resolve a Twilio-reachable callback URL for the current environment.

    Self-host: ``{LOCAL_COMMS_PUBLIC_URL}{local_path}`` (cloudflared tunnel
    to the local ingress). Hosted: ``{hosted_base}{hosted_path}`` (public
    Cloud Run comms-app / adapters).
    """
    if use_local_comms():
        return f"{local_comms_public_base()}{local_path}"
    return f"{hosted_base.rstrip('/')}{hosted_path}"
