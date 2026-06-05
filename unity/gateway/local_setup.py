"""Local operator metadata for configuring gateway channels."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlparse

from unity.gateway.public_url import PublicUrlProvider, StaticPublicUrlProvider

GatewaySetupKind = Literal["channel", "capability", "internal"]
GatewaySetupSurface = Literal["comms", "adapters"]


@dataclass(frozen=True)
class CredentialSpec:
    """One environment variable used by a local gateway channel."""

    name: str
    description: str
    required: bool = True


@dataclass(frozen=True)
class CallbackSpec:
    """One provider-facing URL a local operator may need to configure."""

    name: str
    path: str
    surface: GatewaySetupSurface = "adapters"
    description: str = ""


@dataclass(frozen=True)
class ChannelSetup:
    """Declarative setup requirements for one local gateway channel."""

    name: str
    title: str
    summary: str
    kind: GatewaySetupKind = "channel"
    credentials: tuple[CredentialSpec, ...] = ()
    callbacks: tuple[CallbackSpec, ...] = ()
    public_https_required: bool = False
    signup_url: str = ""
    dashboard_url: str = ""
    setup_steps: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    @property
    def required_credentials(self) -> tuple[CredentialSpec, ...]:
        return tuple(spec for spec in self.credentials if spec.required)

    @property
    def optional_credentials(self) -> tuple[CredentialSpec, ...]:
        return tuple(spec for spec in self.credentials if not spec.required)


CHANNEL_SETUPS: tuple[ChannelSetup, ...] = (
    ChannelSetup(
        name="local-stack",
        title="Local Gateway Stack",
        summary="Shared local services, URLs, and auth used by Console and Unity gateway.",
        kind="capability",
        credentials=(
            CredentialSpec(
                "ORCHESTRA_ADMIN_KEY",
                "Admin key for local gateway admin routes",
            ),
            CredentialSpec(
                "UNITY_GATEWAY_PUBLIC_URL",
                "Public HTTPS callback base URL",
                required=False,
            ),
            CredentialSpec(
                "UNITY_GATEWAY_LOCAL_INGRESS_URL",
                "ConversationManager local ingress URL",
                required=False,
            ),
            CredentialSpec("UNITY_COMMS_URL", "Gateway comms base URL", required=False),
            CredentialSpec(
                "UNITY_ADAPTERS_URL",
                "Gateway adapters base URL",
                required=False,
            ),
            CredentialSpec(
                "UNITY_GATEWAY_STORAGE_DIR",
                "Local attachment storage directory",
                required=False,
            ),
        ),
        setup_steps=(
            "Start local Orchestra before using admin routes.",
            "Use a tunnel such as Cloudflare Tunnel, ngrok, or Tailscale Funnel for provider callbacks.",
            "Run `python -m unity.gateway smoke --base-url http://127.0.0.1:8001` after starting the gateway.",
        ),
        notes=(
            "Echo-mode local chat does not require third-party channel credentials.",
            "Provider callbacks require a public HTTPS URL; localhost is only for internal Console-to-gateway dispatch.",
        ),
    ),
    ChannelSetup(
        name="twilio",
        title="Twilio SMS and Phone",
        summary="Inbound SMS, inbound calls, outbound SMS, phone provisioning, and call control.",
        credentials=(
            CredentialSpec("TWILIO_ACCOUNT_SID", "Twilio account SID"),
            CredentialSpec("TWILIO_AUTH_TOKEN", "Twilio auth token"),
            CredentialSpec(
                "LIVEKIT_URL",
                "LiveKit URL for SIP dispatch",
                required=False,
            ),
            CredentialSpec("LIVEKIT_API_KEY", "LiveKit API key", required=False),
            CredentialSpec("LIVEKIT_API_SECRET", "LiveKit API secret", required=False),
            CredentialSpec("LIVEKIT_SIP_URI", "LiveKit SIP URI", required=False),
            CredentialSpec(
                "ASSISTANT_NUMBER",
                "Assistant phone number",
                required=False,
            ),
            CredentialSpec("USER_NUMBER", "Default user phone number", required=False),
        ),
        callbacks=(
            CallbackSpec("Inbound SMS webhook", "/twilio/sms"),
            CallbackSpec("Inbound call webhook", "/twilio/call"),
            CallbackSpec("Outbound call status", "/twilio/call-status"),
            CallbackSpec("Call TwiML callback", "/phone/twiml", surface="comms"),
            CallbackSpec(
                "Call conference status",
                "/phone/conference-status",
                surface="comms",
            ),
        ),
        public_https_required=True,
        signup_url="https://www.twilio.com/try-twilio",
        dashboard_url="https://console.twilio.com/",
        setup_steps=(
            "Create or select a Twilio phone number.",
            "Set the number's messaging webhook to the generated inbound SMS URL.",
            "Set the number's voice webhook to the generated inbound call URL.",
            "Configure LiveKit credentials only when testing full SIP call dispatch.",
        ),
        notes=(
            "Twilio signature validation uses the auth token configured for the local gateway.",
            "Local inbound calls and SMS require the generated HTTPS URLs to be reachable from Twilio.",
        ),
    ),
    ChannelSetup(
        name="whatsapp",
        title="Twilio WhatsApp",
        summary="Twilio-backed WhatsApp sender setup, status callbacks, and outbound messages.",
        credentials=(
            CredentialSpec("TWILIO_WA_ACCOUNT_SID", "Twilio WhatsApp account SID"),
            CredentialSpec("TWILIO_WA_AUTH_TOKEN", "Twilio WhatsApp auth token"),
            CredentialSpec("TWILIO_WA_FROM", "Default WhatsApp sender", required=False),
        ),
        callbacks=(
            CallbackSpec("WhatsApp inbound webhook", "/twilio/whatsapp"),
            CallbackSpec("WhatsApp call permission", "/twilio/whatsapp-call"),
            CallbackSpec("WhatsApp call status", "/twilio/whatsapp-call-status"),
            CallbackSpec("WhatsApp sender status", "/whatsapp/status", surface="comms"),
        ),
        public_https_required=True,
        signup_url="https://www.twilio.com/whatsapp",
        dashboard_url="https://console.twilio.com/us1/develop/sms/whatsapp/senders",
        setup_steps=(
            "Configure a Twilio WhatsApp sender.",
            "Point inbound and status callbacks at the generated URLs.",
            "Use the `/whatsapp/create` route when provisioning a sender through Console.",
        ),
        notes=(
            "Unity currently uses Twilio's WhatsApp transport rather than a QR-login Web session.",
        ),
    ),
    ChannelSetup(
        name="social",
        title="Social Verification",
        summary="Twilio-backed verification-code delivery for phone and social setup flows.",
        credentials=(
            CredentialSpec("TWILIO_ACCOUNT_SID", "Twilio account SID"),
            CredentialSpec("TWILIO_AUTH_TOKEN", "Twilio auth token"),
            CredentialSpec("ORCHESTRA_ADMIN_KEY", "Admin key for verification routes"),
        ),
        setup_steps=(
            "Configure Twilio first; this setup group reuses the same account credentials.",
            "Use `/social/available-platforms` and `/social/verify` through Console or admin clients.",
        ),
        notes=(
            "Social verification has no provider callback URL; it is an outbound/admin API over Twilio.",
        ),
    ),
    ChannelSetup(
        name="slack",
        title="Slack",
        summary="Slack Events API ingress plus workspace install, send, and user lookup routes.",
        credentials=(
            CredentialSpec("SLACK_SIGNING_SECRET", "Slack Events API signing secret"),
            CredentialSpec("ORCHESTRA_ADMIN_KEY", "Admin key for install/send routes"),
        ),
        callbacks=(CallbackSpec("Slack Events Request URL", "/slack/events"),),
        public_https_required=True,
        signup_url="https://api.slack.com/apps",
        dashboard_url="https://api.slack.com/apps",
        setup_steps=(
            "Create a Slack app and install it into the workspace.",
            "Subscribe to bot events and set the generated request URL.",
            "Save the Slack signing secret locally.",
        ),
        notes=("Slack Socket Mode is not implemented in Unity gateway yet.",),
    ),
    ChannelSetup(
        name="google",
        title="Google OAuth and Gmail",
        summary="Google OAuth callback, Gmail notifications, and Gmail send/watch routes.",
        credentials=(
            CredentialSpec("GOOGLE_OAUTH_CLIENT_ID", "Google OAuth client ID"),
            CredentialSpec("GOOGLE_OAUTH_CLIENT_SECRET", "Google OAuth client secret"),
            CredentialSpec("ORCHESTRA_ADMIN_KEY", "Admin key for Gmail routes"),
            CredentialSpec(
                "GCP_SA_KEY",
                "Google service account key for attachment access",
                required=False,
            ),
        ),
        callbacks=(
            CallbackSpec("Google OAuth redirect URI", "/google/auth/callback"),
            CallbackSpec("Gmail notification endpoint", "/email/gmail"),
            CallbackSpec("Google revoke endpoint", "/google/revoke"),
        ),
        public_https_required=True,
        signup_url="https://console.cloud.google.com/",
        dashboard_url="https://console.cloud.google.com/apis/credentials",
        setup_steps=(
            "Create a Google OAuth client and add the generated redirect URI.",
            "Enable Gmail API scopes for the local assistant account.",
            "Configure Gmail push notifications only when testing inbox watching.",
        ),
        notes=(
            "Gmail push notifications still require the provider-side Pub/Sub/watch setup.",
        ),
    ),
    ChannelSetup(
        name="microsoft",
        title="Microsoft OAuth, Outlook, Teams, and SharePoint",
        summary="Microsoft OAuth callback, Outlook notifications, Teams notifications, SharePoint, and Graph-backed channel APIs.",
        credentials=(
            CredentialSpec("MS365_BYOD_CLIENT_ID", "Microsoft OAuth client ID"),
            CredentialSpec("MS365_BYOD_CLIENT_SECRET", "Microsoft OAuth client secret"),
            CredentialSpec(
                "OUTLOOK_WEBHOOK_SECRET",
                "Outlook webhook client state",
                required=False,
            ),
            CredentialSpec(
                "TEAMS_WEBHOOK_SECRET",
                "Teams webhook client state",
                required=False,
            ),
            CredentialSpec(
                "MS365_ADMIN_TENANT_ID",
                "Microsoft admin tenant ID",
                required=False,
            ),
            CredentialSpec(
                "MS365_ADMIN_CLIENT_ID",
                "Microsoft admin app client ID",
                required=False,
            ),
            CredentialSpec(
                "MS365_ADMIN_CLIENT_SECRET",
                "Microsoft admin app client secret",
                required=False,
            ),
            CredentialSpec(
                "ORCHESTRA_ADMIN_KEY",
                "Admin key for Outlook, Teams, and SharePoint routes",
            ),
        ),
        callbacks=(
            CallbackSpec("Microsoft OAuth redirect URI", "/microsoft/auth/callback"),
            CallbackSpec("Outlook notification endpoint", "/email/outlook"),
            CallbackSpec("Teams notification endpoint", "/chat/teams"),
            CallbackSpec("Microsoft notification router", "/microsoft/router"),
        ),
        public_https_required=True,
        signup_url="https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade",
        dashboard_url="https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade",
        setup_steps=(
            "Create or select an Azure app registration.",
            "Add the generated OAuth redirect URI.",
            "Configure Graph notification subscriptions for Outlook or Teams only when needed.",
            "Configure admin app credentials only for tenant-level Graph provisioning or SharePoint flows.",
        ),
        notes=(
            "Graph notification subscriptions require Microsoft to reach the public HTTPS URL.",
            "Teams and SharePoint are Microsoft capability surfaces; they share this setup group.",
        ),
    ),
    ChannelSetup(
        name="discord",
        title="Discord",
        summary="Discord bot registration, pool sync, status, and outbound send routes.",
        credentials=(
            CredentialSpec("ORCHESTRA_ADMIN_KEY", "Admin key for Discord routes"),
            CredentialSpec(
                "DISCORD_PUBLIC_KEY",
                "Discord interaction public key",
                required=False,
            ),
        ),
        callbacks=(),
        public_https_required=False,
        signup_url="https://discord.com/developers/applications",
        dashboard_url="https://discord.com/developers/applications",
        setup_steps=(
            "Create a Discord application and bot in the developer portal.",
            "Use the admin routes to register and sync bot metadata with Orchestra.",
        ),
        notes=(
            "Discord outbound/admin routes work locally with admin auth; interaction webhooks are not part of the current Unity gateway surface.",
        ),
    ),
    ChannelSetup(
        name="email",
        title="Generic Email",
        summary="Generic email send and attachment routes.",
        credentials=(
            CredentialSpec("ORCHESTRA_ADMIN_KEY", "Admin key for email routes"),
        ),
        callbacks=(),
        public_https_required=False,
        setup_steps=(
            "Configure Gmail and/or Outlook for provider-specific inbox watching.",
            "Use generic email routes for provider-agnostic sends and attachment fetches.",
        ),
        notes=(
            "Provider-specific inbox watching is handled by the Gmail and Outlook setup paths.",
        ),
    ),
    ChannelSetup(
        name="unillm",
        title="UniLLM API Proxy",
        summary="OpenAI-compatible chat completion route authenticated by user API key.",
        kind="capability",
        credentials=(
            CredentialSpec(
                "OPENAI_API_KEY",
                "OpenAI API key for local model calls",
                required=False,
            ),
            CredentialSpec(
                "ANTHROPIC_API_KEY",
                "Anthropic API key for local model calls",
                required=False,
            ),
            CredentialSpec("UNIFY_KEY", "Local Orchestra user API key", required=False),
        ),
        setup_steps=(
            "Configure at least one LLM provider key for full ConversationManager mode.",
            "Use echo mode when testing gateway plumbing without LLM providers.",
        ),
        notes=(
            "The `/unillm/chat/completions` route authenticates with the caller's user API key, not the gateway admin key.",
        ),
    ),
    ChannelSetup(
        name="voice",
        title="Realtime Voice Providers",
        summary="Optional STT/TTS/realtime providers used by local voice and call experiments.",
        kind="capability",
        credentials=(
            CredentialSpec("DEEPGRAM_API_KEY", "Deepgram API key", required=False),
            CredentialSpec("ELEVEN_API_KEY", "ElevenLabs API key", required=False),
            CredentialSpec("CARTESIA_API_KEY", "Cartesia API key", required=False),
            CredentialSpec("OPENAI_API_KEY", "OpenAI API key", required=False),
            CredentialSpec("ANTHROPIC_API_KEY", "Anthropic API key", required=False),
        ),
        setup_steps=(
            "Configure these only for live voice, realtime transcription, or voice-call experiments.",
            "Keep phone/SMS setup separate from voice model provider setup.",
        ),
        notes=(
            "These are capability providers, not provider-facing callback channels.",
        ),
    ),
    ChannelSetup(
        name="internal",
        title="Console and Runtime Adapter Endpoints",
        summary="Internal Console/Orchestra dispatch endpoints that the local stack validates with smoke tests.",
        kind="internal",
        credentials=(
            CredentialSpec(
                "ORCHESTRA_ADMIN_KEY",
                "Admin key for internal adapter endpoints",
            ),
        ),
        setup_steps=(
            "Do not configure these in provider dashboards.",
            "Validate them through Console compatibility and gateway smoke tests.",
        ),
        notes=(
            "`/unify/message`, `/unify/attachment`, `/unify/meet`, `/unity/system-event`, and `/assistant/*` are local runtime surfaces.",
        ),
    ),
)


def all_channel_setups() -> tuple[ChannelSetup, ...]:
    return CHANNEL_SETUPS


def channel_names() -> tuple[str, ...]:
    return tuple(setup.name for setup in CHANNEL_SETUPS)


def select_channel_setups(
    names: list[str] | tuple[str, ...] | None,
) -> tuple[ChannelSetup, ...]:
    if not names:
        return CHANNEL_SETUPS
    names = tuple(
        part.strip() for name in names for part in name.split(",") if part.strip()
    )
    if "all" in names:
        return CHANNEL_SETUPS
    by_name = {setup.name: setup for setup in CHANNEL_SETUPS}
    unknown = sorted(set(names) - set(by_name))
    if unknown:
        raise ValueError(
            "unknown channel(s): "
            + ", ".join(unknown)
            + ". Known channels: "
            + ", ".join(channel_names()),
        )
    return tuple(by_name[name] for name in names)


def public_url_provider_from_base(
    public_url: str,
    *,
    single_url: bool = True,
) -> StaticPublicUrlProvider:
    base = public_url.rstrip("/")
    if single_url:
        return StaticPublicUrlProvider(comms_base_url=base, adapters_base_url=base)
    return StaticPublicUrlProvider(
        comms_base_url=os.environ.get("UNITY_COMMS_URL", base).rstrip("/"),
        adapters_base_url=os.environ.get("UNITY_ADAPTERS_URL", base).rstrip("/"),
    )


def callback_urls(
    setup: ChannelSetup,
    provider: PublicUrlProvider,
) -> tuple[tuple[CallbackSpec, str], ...]:
    return tuple(
        (callback, provider.url_for(callback.path, surface=callback.surface))
        for callback in setup.callbacks
    )


def credential_status(setup: ChannelSetup) -> dict[str, bool]:
    return {
        spec.name: bool(os.environ.get(spec.name, "").strip())
        for spec in setup.credentials
    }


def missing_required_credentials(setup: ChannelSetup) -> tuple[str, ...]:
    status = credential_status(setup)
    return tuple(
        spec.name
        for spec in setup.required_credentials
        if not status.get(spec.name, False)
    )


def validate_public_url(public_url: str) -> tuple[bool, str]:
    if not public_url.strip():
        return False, "public-url is not set"
    parsed = urlparse(public_url.strip())
    if parsed.scheme != "https":
        return False, "public-url must use https for real provider callbacks"
    if not parsed.netloc:
        return False, "public-url is missing a host"
    return True, f"public-url ok ({public_url.rstrip('/')})"


def env_placeholder_lines(setups: tuple[ChannelSetup, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    lines: list[str] = []
    for setup in setups:
        lines.append(f"# {setup.title}")
        for spec in setup.credentials:
            if spec.name in seen:
                continue
            seen.add(spec.name)
            lines.append(f"{spec.name}=")
        lines.append("")
    return tuple(lines)


__all__ = [
    "CallbackSpec",
    "ChannelSetup",
    "CredentialSpec",
    "all_channel_setups",
    "callback_urls",
    "channel_names",
    "credential_status",
    "env_placeholder_lines",
    "missing_required_credentials",
    "public_url_provider_from_base",
    "select_channel_setups",
    "validate_public_url",
]
