"""OAuth helpers for gateway adapter callbacks."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone

import httpx

from unify.settings import SETTINGS


class OAuthStateError(Exception):
    """Raised when an OAuth state parameter is invalid."""


def verify_oauth_state(encoded_state: str, signing_key: str) -> dict:
    """Decode and verify a signed OAuth state parameter."""

    try:
        state_bytes = base64.urlsafe_b64decode(encoded_state)
        state_data: dict = json.loads(state_bytes.decode())
    except Exception as exc:
        raise OAuthStateError("Invalid state parameter") from exc
    provided_sig = state_data.pop("_sig", "")
    if not provided_sig:
        raise OAuthStateError("Missing state signature")
    canonical = json.dumps(state_data, sort_keys=True)
    expected_sig = hmac.new(
        signing_key.encode(),
        canonical.encode(),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(provided_sig, expected_sig):
        raise OAuthStateError("Invalid state signature")
    return state_data


async def upsert_assistant_secrets(
    *,
    assistant_id: str,
    api_key: str,
    secrets: dict[str, str],
) -> bool:
    """Upsert assistant secrets in Orchestra."""

    base = f"{SETTINGS.ORCHESTRA_URL}/assistant/{assistant_id}/secret"
    headers = {"Authorization": f"Bearer {api_key}"}
    success = True
    async with httpx.AsyncClient() as client:
        for name, value in secrets.items():
            if not value:
                continue
            response = await client.put(
                f"{base}/{name}",
                json={"secret_value": value},
                headers=headers,
                timeout=30.0,
            )
            if response.status_code == 404:
                response = await client.post(
                    base,
                    json={"secret_name": name, "secret_value": value},
                    headers=headers,
                    timeout=30.0,
                )
            if response.status_code not in (200, 201):
                success = False
    return success


def _expires_at(expires_in: int | None) -> str:
    return (
        datetime.now(tz=timezone.utc) + timedelta(seconds=expires_in or 3600)
    ).isoformat()


async def exchange_google_code_for_tokens(
    client_id: str,
    client_secret: str,
    code: str,
    redirect_uri: str,
) -> dict:
    """Exchange a Google authorization code for access and refresh tokens."""

    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "code": code,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )
    response.raise_for_status()
    data = response.json()
    data["expires_at"] = _expires_at(data.get("expires_in"))
    return data


async def get_google_user_info(access_token: str) -> dict:
    """Fetch Google profile information for an access token."""

    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    response.raise_for_status()
    return response.json()


async def exchange_microsoft_code_for_tokens(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    code: str,
    redirect_uri: str,
) -> dict:
    """Exchange a Microsoft authorization code for tokens."""

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "code": code,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )
    response.raise_for_status()
    data = response.json()
    data["expires_at"] = _expires_at(data.get("expires_in"))
    return data


async def get_microsoft_user_info(access_token: str) -> dict:
    """Fetch Microsoft Graph profile information for an access token."""

    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://graph.microsoft.com/v1.0/me",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    response.raise_for_status()
    return response.json()


__all__ = [
    "OAuthStateError",
    "exchange_google_code_for_tokens",
    "exchange_microsoft_code_for_tokens",
    "get_google_user_info",
    "get_microsoft_user_info",
    "upsert_assistant_secrets",
    "verify_oauth_state",
]
