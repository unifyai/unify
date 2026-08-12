"""Configuration for the pod-local LLM broker.

Read from the environment of the sidecar container, which is the only place
in the pod the provider credentials are mounted.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Mapping, Optional

from unify.llm_broker.proxy import CredentialProxy
from unify.llm_broker.voice import VoiceProvider

#: How long a positive authorisation may be reused for the same key and model.
#:
#: Zero means every call asks. A few seconds removes the round trip from the
#: middle of a rapid exchange -- voice turns arrive faster than an account's
#: standing changes -- at the cost of a window in which a newly-depleted
#: account can still spend. The window is bounded by this value and by usage
#: being reported after every call, so the overspend is one short burst rather
#: than an open tab. Negative verdicts are never reused.
_DEFAULT_AUTH_TTL_S = 5.0

_DEFAULT_PORT = 8787


@dataclass(frozen=True)
class BrokerSettings:
    """Resolved sidecar configuration."""

    host: str
    port: int
    orchestra_url: str
    openrouter_api_key: Optional[str]
    openrouter_api_base: str
    anthropic_api_key: Optional[str]
    anthropic_api_base: str
    auth_ttl_s: float
    #: Voice STT/TTS providers keyed by the path segment the plugin is pointed
    #: at (``/voice/<name>/...``). Empty when no voice keys are mounted, which
    #: leaves the routes present but refusing -- the same shape as an LLM leg
    #: whose key is absent.
    voice_providers: Mapping[str, VoiceProvider] = field(default_factory=dict)
    #: Non-LLM REST providers (Tavily, Recall) reached through the header-swap
    #: proxy. Empty leaves the routes present but refusing.
    credential_proxies: Mapping[str, CredentialProxy] = field(default_factory=dict)

    @property
    def authorize_url(self) -> str:
        return f"{self.orchestra_url.rstrip('/')}/llm/authorize"

    @property
    def settle_url(self) -> str:
        return f"{self.orchestra_url.rstrip('/')}/llm/settle"


def _float_env(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value >= 0 else default


#: Static shape of each voice provider: where its bytes go and where its
#: credential sits. The key comes from the sidecar env; everything else is a
#: property of the provider's protocol, confirmed against the installed plugin.
_VOICE_REGISTRY = (
    # Deepgram STT authenticates its listen socket with an Authorization header.
    (
        "deepgram",
        "https://api.deepgram.com",
        "DEEPGRAM_API_KEY",
        "header",
        "Authorization",
        "Token {key}",
    ),
    # Cartesia TTS carries the key as an ``api_key`` query parameter, not a header.
    (
        "cartesia",
        "https://api.cartesia.ai",
        "CARTESIA_API_KEY",
        "query",
        "api_key",
        "{key}",
    ),
    # ElevenLabs TTS uses an ``xi-api-key`` header holding the bare key.
    (
        "elevenlabs",
        "https://api.elevenlabs.io",
        "ELEVEN_API_KEY",
        "header",
        "xi-api-key",
        "{key}",
    ),
)


def _load_voice_providers() -> dict[str, VoiceProvider]:
    providers: dict[str, VoiceProvider] = {}
    for name, base, env_key, cred_in, cred_name, cred_template in _VOICE_REGISTRY:
        providers[name] = VoiceProvider(
            name=name,
            upstream_base=os.environ.get(f"{name.upper()}_UPSTREAM_BASE", base),
            api_key=os.environ.get(env_key) or None,
            cred_in=cred_in,
            cred_name=cred_name,
            cred_template=cred_template,
        )
    return providers


def _load_credential_proxies() -> dict[str, CredentialProxy]:
    proxies = {
        "tavily": CredentialProxy(
            name="tavily",
            upstream_base=os.environ.get(
                "TAVILY_UPSTREAM_BASE",
                "https://api.tavily.com",
            ),
            auth_scheme="Bearer",
            api_key=os.environ.get("TAVILY_API_KEY") or None,
        ),
    }
    # Recall's host is region-scoped; without a region there is no upstream to
    # forward to, so the provider stays unconfigured even if a key is present.
    region = os.environ.get("RECALL_REGION", "").strip()
    proxies["recall"] = CredentialProxy(
        name="recall",
        upstream_base=f"https://{region}.recall.ai" if region else "",
        auth_scheme="Token",
        api_key=(os.environ.get("RECALL_API_KEY") or None) if region else None,
    )
    return proxies


def load_settings() -> BrokerSettings:
    """Read broker configuration from the sidecar's environment.

    Binds loopback by default. The pod's containers share a network
    namespace, so loopback is reachable by the runtime beside it and by
    nothing outside the pod -- the broker is not a service anyone else can
    route to, and should not become one by having a bind address configured
    to something wider.
    """
    return BrokerSettings(
        host=os.environ.get("UNIFY_LLM_BROKER_HOST", "127.0.0.1"),
        port=int(os.environ.get("UNIFY_LLM_BROKER_PORT", _DEFAULT_PORT)),
        orchestra_url=os.environ.get("ORCHESTRA_URL", "").strip(),
        openrouter_api_key=os.environ.get("OPENROUTER_API_KEY") or None,
        openrouter_api_base=os.environ.get(
            "OPENROUTER_API_BASE",
            "https://openrouter.ai/api/v1",
        ),
        anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY") or None,
        anthropic_api_base=os.environ.get(
            "ANTHROPIC_API_BASE",
            "https://api.anthropic.com",
        ),
        auth_ttl_s=_float_env("UNIFY_LLM_BROKER_AUTH_TTL_S", _DEFAULT_AUTH_TTL_S),
        voice_providers=_load_voice_providers(),
        credential_proxies=_load_credential_proxies(),
    )
