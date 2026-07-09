"""Process-wide state for the localhost provider proxy.

Holds the bound host/port and the per-process capability nonce, and derives the
base URLs handed to the ``execute_code`` sandbox. This module is intentionally
dependency-light so it can be imported from both the trusted runtime and the
credential helpers without creating import cycles.

The sandbox never receives a real provider OAuth token. Instead it authorizes
against this local proxy with ``nonce`` and points its base URL at the proxy;
the trusted proxy swaps the nonce for the real upstream token and enforces the
file-access allowlist before anything leaves the process.
"""

from __future__ import annotations

import contextlib
import os
import threading
from dataclasses import dataclass
from typing import Iterator

# Sensitive provider credential env vars that must never reach the sandbox
# (venv/shell subprocesses or in-process actor code). These are the raw tokens
# whose presence would let actor code call Drive/Graph directly and bypass the
# allowlist. Kept in lockstep with the built-in providers in ``runtime_oauth``.
_PROVIDER_TOKEN_ENV_KEYS: frozenset[str] = frozenset(
    {
        "MICROSOFT_ACCESS_TOKEN",
        "MICROSOFT_REFRESH_TOKEN",
        "GOOGLE_ACCESS_TOKEN",
        "GOOGLE_REFRESH_TOKEN",
    },
)

# Platform / cross-tenant secrets that user-driven ``execute_code`` must never
# see. ``ORCHESTRA_ADMIN_KEY`` is a fleet-wide superuser bearer and
# ``SHARED_UNIFY_KEY`` is a shared data key; neither is needed by user code, and
# their presence in the sandbox would let a prompt-injected or malicious user
# read them (``echo $ORCHESTRA_ADMIN_KEY``) and pivot to other tenants. Trusted
# runtime code reads these from ``SETTINGS`` (captured at settings construction),
# not from ``os.environ`` at call time, so scrubbing them from the process
# environment for the duration of user code does not affect the parent runtime.
_PLATFORM_SECRET_ENV_KEYS: frozenset[str] = frozenset(
    {
        "ORCHESTRA_ADMIN_KEY",
        "SHARED_UNIFY_KEY",
    },
)


def provider_token_env_keys() -> frozenset[str]:
    """Return the credential env var names that must be scrubbed from sandboxes."""
    return _PROVIDER_TOKEN_ENV_KEYS


def platform_secret_env_keys() -> frozenset[str]:
    """Return the platform/cross-tenant secrets barred from user code."""
    return _PLATFORM_SECRET_ENV_KEYS


@contextlib.contextmanager
def scrub_platform_secrets_from_environ() -> Iterator[None]:
    """Temporarily remove platform secrets from ``os.environ``.

    Used to wrap in-process ``execute_code`` so user Python cannot read
    ``os.environ["ORCHESTRA_ADMIN_KEY"]`` directly. Subprocess sandboxes get
    the same guarantee for free via :func:`build_sandbox_env` (which never seeds
    these keys into the child env). Values are restored on exit so the parent
    process is unaffected.
    """
    saved: dict[str, str] = {}
    for key in _PLATFORM_SECRET_ENV_KEYS:
        if key in os.environ:
            saved[key] = os.environ.pop(key)
    try:
        yield
    finally:
        for key, value in saved.items():
            os.environ[key] = value


@dataclass(frozen=True)
class ProxySession:
    """A live localhost proxy binding plus its capability nonce."""

    host: str
    port: int
    nonce: str

    @property
    def root_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def provider_root(self, provider: str) -> str:
        return f"{self.root_url}/{provider}"

    @property
    def microsoft_graph_base(self) -> str:
        # Drop-in replacement for https://graph.microsoft.com/v1.0
        return f"{self.provider_root('microsoft')}/v1.0"

    @property
    def google_drive_base(self) -> str:
        # Drop-in replacement for https://www.googleapis.com/drive/v3
        return f"{self.provider_root('google')}/drive/v3"

    @property
    def google_api_base(self) -> str:
        # Drop-in replacement for https://www.googleapis.com
        return self.provider_root("google")

    def sandbox_env(self) -> dict[str, str]:
        """Env vars handed to the sandbox so SDKs/HTTP target the proxy."""
        return {
            "WORKSPACE_PROXY_URL": self.root_url,
            "WORKSPACE_PROXY_TOKEN": self.nonce,
            "MICROSOFT_GRAPH_BASE": self.microsoft_graph_base,
            "GOOGLE_DRIVE_BASE": self.google_drive_base,
            "GOOGLE_API_BASE": self.google_api_base,
        }


_LOCK = threading.Lock()
_SESSION: ProxySession | None = None


def set_session(session: ProxySession) -> None:
    global _SESSION
    with _LOCK:
        _SESSION = session


def current_session() -> ProxySession | None:
    with _LOCK:
        return _SESSION


def build_sandbox_env(base: dict[str, str] | None = None) -> dict[str, str]:
    """Return a subprocess env with provider tokens removed and proxy vars added.

    Copies the current process environment (or *base*), strips the raw provider
    credential vars and platform/cross-tenant secrets, and overlays the proxy
    base URLs + nonce. Used at every sandbox spawn site so no subprocess ever
    inherits a usable file token or platform superuser key.
    """
    env = dict(os.environ if base is None else base)
    for key in _PROVIDER_TOKEN_ENV_KEYS:
        env.pop(key, None)
    for key in _PLATFORM_SECRET_ENV_KEYS:
        env.pop(key, None)
    session = current_session()
    if session is not None:
        env.update(session.sandbox_env())
    return env
