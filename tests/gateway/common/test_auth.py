"""Tests for ``droid.gateway.common.auth``.

Covers the admin-bearer dependency (matching
``communication/dependencies.py::auth_admin_key``) and the
user-API-key helper used by unillm + any future SDK-style channels.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from droid.gateway.common.auth import (
    admin_auth_dependency,
    auth_admin_key,
    authenticate_user_api_key,
    extract_api_key,
)


@pytest.fixture
def _admin_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from droid.gateway.common import auth

    stub_secret = SimpleNamespace(get_secret_value=lambda: "test-admin-key")
    monkeypatch.setattr(
        auth,
        "SETTINGS",
        SimpleNamespace(
            ORCHESTRA_ADMIN_KEY=stub_secret,
            ORCHESTRA_URL="https://orchestra.example.com/v0",
        ),
    )


def _bearer(token: str) -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


# ---------------------------------------------------------------------------
# auth_admin_key
# ---------------------------------------------------------------------------


class TestAuthAdminKey:
    def test_matching_key_returns_none(self, _admin_settings: None) -> None:
        # No exception means the dependency passes.
        result = auth_admin_key(
            request=MagicMock(),
            credentials=_bearer("test-admin-key"),
        )
        assert result is None

    def test_mismatch_raises_403(self, _admin_settings: None) -> None:
        with pytest.raises(HTTPException) as ctx:
            auth_admin_key(
                request=MagicMock(),
                credentials=_bearer("wrong-key"),
            )
        assert ctx.value.status_code == 403

    def test_empty_expected_key_rejects_all_requests(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If ORCHESTRA_ADMIN_KEY is unset, no presented key can match.

        Important for safety: an empty expected key would otherwise
        match an empty presented key via secrets.compare_digest.
        """
        from droid.gateway.common import auth

        empty_secret = SimpleNamespace(get_secret_value=lambda: "")
        monkeypatch.setattr(
            auth,
            "SETTINGS",
            SimpleNamespace(
                ORCHESTRA_ADMIN_KEY=empty_secret,
                ORCHESTRA_URL="x",
            ),
        )
        with pytest.raises(HTTPException) as ctx:
            auth_admin_key(
                request=MagicMock(),
                credentials=_bearer(""),
            )
        assert ctx.value.status_code == 403


def test_admin_auth_dependency_shape() -> None:
    """Dependency list is one Depends() wrapping auth_admin_key."""
    assert len(admin_auth_dependency) == 1
    dep = admin_auth_dependency[0]
    assert dep.dependency is auth_admin_key


# ---------------------------------------------------------------------------
# authenticate_user_api_key
# ---------------------------------------------------------------------------


def _async_client_returning(response_mock: MagicMock) -> MagicMock:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.get.return_value = response_mock
    return client


class TestAuthenticateUserApiKey:
    @pytest.mark.asyncio
    async def test_200_returns_user_info(self, _admin_settings: None) -> None:
        resp = MagicMock(status_code=200)
        resp.json.return_value = {"user_id": "u-1"}
        with patch(
            "droid.gateway.common.auth.httpx.AsyncClient",
            return_value=_async_client_returning(resp),
        ):
            result = await authenticate_user_api_key("sk-test")
        assert result == {"user_id": "u-1"}

    @pytest.mark.asyncio
    async def test_401_raises_401(self, _admin_settings: None) -> None:
        resp = MagicMock(status_code=401)
        with patch(
            "droid.gateway.common.auth.httpx.AsyncClient",
            return_value=_async_client_returning(resp),
        ):
            with pytest.raises(HTTPException) as ctx:
                await authenticate_user_api_key("sk-bad")
        assert ctx.value.status_code == 401

    @pytest.mark.asyncio
    async def test_orchestra_outage_returns_401(
        self,
        _admin_settings: None,
    ) -> None:
        """Deny-on-outage: any non-200 -> 401, including 5xx."""
        resp = MagicMock(status_code=503)
        with patch(
            "droid.gateway.common.auth.httpx.AsyncClient",
            return_value=_async_client_returning(resp),
        ):
            with pytest.raises(HTTPException) as ctx:
                await authenticate_user_api_key("sk-x")
        assert ctx.value.status_code == 401


# ---------------------------------------------------------------------------
# extract_api_key
# ---------------------------------------------------------------------------


class TestExtractApiKey:
    def test_extracts_bearer_token(self) -> None:
        request = MagicMock()
        request.headers.get.return_value = "Bearer sk-foo"
        assert extract_api_key(request) == "sk-foo"

    def test_missing_header_raises_401(self) -> None:
        request = MagicMock()
        request.headers.get.return_value = ""
        with pytest.raises(HTTPException) as ctx:
            extract_api_key(request)
        assert ctx.value.status_code == 401

    def test_wrong_scheme_raises_401(self) -> None:
        request = MagicMock()
        request.headers.get.return_value = "Basic dXNlcjpwYXNz"
        with pytest.raises(HTTPException) as ctx:
            extract_api_key(request)
        assert ctx.value.status_code == 401
