"""Bot Framework inbound JWT verification.

Hand-rolled validation of the ``Authorization: Bearer <jwt>`` header the
Bot Connector attaches to every inbound activity, using ``PyJWT`` +
``httpx`` rather than the heavy Bot Framework SDK — the same
"verify-the-provider-signature-ourselves" shape as the Slack HMAC check.

Trust chain:

1. Fetch the Bot Connector OpenID metadata to discover the signing-key
   (JWKS) endpoint.
2. Select the signing key by the token's ``kid`` (cached across calls).
3. Verify RS256 signature, ``iss`` (Bot Connector emitter), ``aud`` (our
   bot app id), and ``exp`` / ``nbf``.

The signing keys rotate, so the JWKS client is rebuilt on a TTL and (as a
fallback) whenever a ``kid`` is not found in the cached key set.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.request

import jwt
from jwt import PyJWKClient

logger = logging.getLogger("unify.gateway.adapters.ms_teams_bot_auth")

# Public-cloud Bot Connector metadata + token issuer. (Government / sovereign
# clouds use different hosts; add them here if we ever deploy there.)
_OPENID_CONFIG_URL = "https://login.botframework.com/v1/.well-known/openidconfiguration"
_ISSUER = "https://api.botframework.com"

_JWK_CLIENT_TTL_SECONDS = 24 * 3600

_jwk_client: PyJWKClient | None = None
_jwk_client_built_at: float = 0.0


class BotFrameworkAuthError(Exception):
    """Raised when an inbound Bot Framework token fails verification."""


def _fetch_jwks_uri() -> str:
    with urllib.request.urlopen(_OPENID_CONFIG_URL, timeout=10) as resp:
        config = json.load(resp)
    jwks_uri = config.get("jwks_uri")
    if not jwks_uri:
        raise BotFrameworkAuthError("Bot Connector OpenID config has no jwks_uri.")
    return jwks_uri


def _get_jwk_client(*, force_refresh: bool = False) -> PyJWKClient:
    global _jwk_client, _jwk_client_built_at
    now = time.time()
    stale = now - _jwk_client_built_at > _JWK_CLIENT_TTL_SECONDS
    if _jwk_client is None or stale or force_refresh:
        _jwk_client = PyJWKClient(_fetch_jwks_uri())
        _jwk_client_built_at = now
    return _jwk_client


def verify_bot_framework_token(token: str, *, app_id: str) -> dict:
    """Verify an inbound Bot Framework JWT and return its claims.

    Raises :class:`BotFrameworkAuthError` on any failure (bad signature,
    wrong audience/issuer, expired, unknown key). Blocking — call from a
    worker thread in an async context.
    """
    if not token:
        raise BotFrameworkAuthError("Missing bearer token.")
    if not app_id:
        raise BotFrameworkAuthError("MS Teams bot app id is not configured.")

    def _decode(client: PyJWKClient) -> dict:
        signing_key = client.get_signing_key_from_jwt(token)
        return jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=app_id,
            issuer=_ISSUER,
            options={"require": ["exp", "iss", "aud"]},
        )

    try:
        return _decode(_get_jwk_client())
    except jwt.PyJWKClientError:
        # Key rotated out of the cached set — rebuild once and retry.
        try:
            return _decode(_get_jwk_client(force_refresh=True))
        except Exception as exc:  # noqa: BLE001 - normalize to our error type
            raise BotFrameworkAuthError(str(exc)) from exc
    except jwt.InvalidTokenError as exc:
        raise BotFrameworkAuthError(str(exc)) from exc


__all__ = ["BotFrameworkAuthError", "verify_bot_framework_token"]
