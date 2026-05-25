"""Pluggable secret resolution for gateway transports.

Channel routers (Twilio, Microsoft Graph, Discord, ...) need API keys,
OAuth tokens, webhook signing secrets, and similar credentials. The
hosted code path resolves these via Google Cloud Secret Manager; the
self-hosted path reads them from process environment variables.

This package defines the ``SecretManager`` protocol and ships the env-var
implementation. The GCP implementation is stubbed pending the Phase B
channel migration, which is where call sites actually need it wired in.
"""

from unity.gateway.secrets.base import SecretManager, SecretNotFoundError
from unity.gateway.secrets.env import EnvSecretManager

__all__ = ["EnvSecretManager", "SecretManager", "SecretNotFoundError"]
