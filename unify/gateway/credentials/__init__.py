"""Pluggable credential store for gateway transports.

Channel routers (Twilio, Microsoft Graph, Discord, ...) need API keys,
OAuth tokens, webhook signing secrets, and similar credentials to talk
to their external services. The hosted code path resolves these via
Google Cloud Secret Manager; the self-hosted path reads them from
process environment variables.

This package is **distinct from** ``unify.secret_manager`` -- they
serve different purposes:

* ``unify.gateway.credentials`` (this package) holds **operator
  infrastructure credentials** used by the gateway processes to talk
  to external messaging providers. Examples: ``TWILIO_ACCOUNT_SID``,
  ``MICROSOFT_ACCESS_TOKEN``, ``DISCORD_BOT_TOKEN``. These are a
  deployment/operator concern; the assistant runtime never sees them.

* ``unify.secret_manager`` holds **assistant-owned secrets** -- the
  credentials the assistant uses on the user's behalf (the user's
  GitHub token, Salesforce API key, etc.). These are exposed to the
  assistant through ``primitives.secrets.ask/update``.

The two systems never touch each other; the assistant cannot read
gateway credentials and the gateway cannot read assistant secrets.

This package defines the ``CredentialStore`` protocol and ships the
env-var implementation. The GCP implementation is stubbed pending the
Phase B channel migration, which is where call sites actually need it
wired in.
"""

from unify.gateway.credentials.base import (
    CredentialNotFoundError,
    CredentialStore,
)
from unify.gateway.credentials.env import EnvCredentialStore

__all__ = ["CredentialNotFoundError", "CredentialStore", "EnvCredentialStore"]
