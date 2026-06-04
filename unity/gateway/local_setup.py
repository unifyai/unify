"""Local operator metadata for configuring gateway channels."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlparse

from unity.gateway.public_url import PublicUrlProvider, StaticPublicUrlProvider

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
    credentials: tuple[CredentialSpec, ...] = ()
    callbacks: tuple[CallbackSpec, ...] = ()
    public_https_required: bool = False
    notes: tuple[str, ...] = ()

    @property
    def required_credentials(self) -> tuple[CredentialSpec, ...]:
        return tuple(spec for spec in self.credentials if spec.required)

    @property
    def optional_credentials(self) -> tuple[CredentialSpec, ...]:
        return tuple(spec for spec in self.credentials if not spec.required)


CHANNEL_SETUPS: tuple[ChannelSetup, ...] = (
    ChannelSetup(
        name="twilio",
        title="Twilio SMS and Phone",
        summary="Inbound SMS, inbound calls, outbound SMS, and call control.",
        credentials=(
            CredentialSpec("TWILIO_ACCOUNT_SID", "Twilio account SID"),
            CredentialSpec("TWILIO_AUTH_TOKEN", "Twilio auth token"),
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
            CallbackSpec("Call TwiML callback", "/phone/twiml", surface="comms"),
            CallbackSpec(
                "Call conference status",
                "/phone/conference-status",
                surface="comms",
            ),
        ),
        public_https_required=True,
        notes=(
            "Configure the Twilio phone number webhooks to point at the generated HTTPS URLs.",
            "Twilio signature validation uses the auth token configured for the local gateway.",
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
            CallbackSpec("WhatsApp inbound webhook", "/twilio/sms"),
            CallbackSpec("WhatsApp sender status", "/whatsapp/status", surface="comms"),
        ),
        public_https_required=True,
        notes=(
            "Unity currently uses Twilio's WhatsApp transport rather than a QR-login Web session.",
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
        notes=(
            "Create a Slack app, subscribe to bot events, and use the generated request URL.",
            "Slack Socket Mode is not implemented in Unity gateway yet.",
        ),
    ),
    ChannelSetup(
        name="google",
        title="Google OAuth and Gmail",
        summary="Google OAuth callback, Gmail notifications, and Gmail send/watch routes.",
        credentials=(
            CredentialSpec("GOOGLE_OAUTH_CLIENT_ID", "Google OAuth client ID"),
            CredentialSpec("GOOGLE_OAUTH_CLIENT_SECRET", "Google OAuth client secret"),
            CredentialSpec("ORCHESTRA_ADMIN_KEY", "Admin key for Gmail routes"),
        ),
        callbacks=(
            CallbackSpec("Google OAuth redirect URI", "/google/auth/callback"),
            CallbackSpec("Gmail notification endpoint", "/email/gmail"),
        ),
        public_https_required=True,
        notes=(
            "Register the OAuth redirect URI in the Google Cloud console.",
            "Gmail push notifications still require the provider-side Pub/Sub/watch setup.",
        ),
    ),
    ChannelSetup(
        name="microsoft",
        title="Microsoft OAuth, Outlook, and Teams",
        summary="Microsoft OAuth callback, Outlook notifications, Teams notifications, and Graph-backed channel APIs.",
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
                "ORCHESTRA_ADMIN_KEY",
                "Admin key for Outlook and Teams routes",
            ),
        ),
        callbacks=(
            CallbackSpec("Microsoft OAuth redirect URI", "/microsoft/auth/callback"),
            CallbackSpec("Outlook notification endpoint", "/email/outlook"),
            CallbackSpec("Teams notification endpoint", "/chat/teams"),
            CallbackSpec("Microsoft notification router", "/microsoft/router"),
        ),
        public_https_required=True,
        notes=(
            "Register the OAuth redirect URI in the Azure app registration.",
            "Graph notification subscriptions require Microsoft to reach the public HTTPS URL.",
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
        notes=(
            "Provider-specific inbox watching is handled by the Gmail and Outlook setup paths.",
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
