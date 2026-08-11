"""Configuration for the pod-local LLM broker.

Read from the environment of the sidecar container, which is the only place
in the pod the provider credentials are mounted.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

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
    )
