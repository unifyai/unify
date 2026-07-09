"""Tests for ``unify.gateway.common.auth``.

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

from unify.gateway.common.auth import (
    admin_auth_dependency,
    admin_or_user_auth_dependency,
    auth_admin_key,
    auth_admin_or_user_key,
    authenticate_user_api_key,
    extract_api_key,
    require_assistant_ownership,
    require_gateway_admin,
)


@pytest.fixture
def _admin_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from unify.gateway.common import auth

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
        from unify.gateway.common import auth

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
            "unify.gateway.common.auth.httpx.AsyncClient",
            return_value=_async_client_returning(resp),
        ):
            result = await authenticate_user_api_key("sk-test")
        assert result == {"user_id": "u-1"}

    @pytest.mark.asyncio
    async def test_401_raises_401(self, _admin_settings: None) -> None:
        resp = MagicMock(status_code=401)
        with patch(
            "unify.gateway.common.auth.httpx.AsyncClient",
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
            "unify.gateway.common.auth.httpx.AsyncClient",
            return_value=_async_client_returning(resp),
        ):
            with pytest.raises(HTTPException) as ctx:
                await authenticate_user_api_key("sk-x")
        assert ctx.value.status_code == 401


# ---------------------------------------------------------------------------
# extract_api_key
# ---------------------------------------------------------------------------


def _blank_request() -> SimpleNamespace:
    """Request stand-in with a writable, initially-empty ``state``."""
    return SimpleNamespace(state=SimpleNamespace())


# ---------------------------------------------------------------------------
# auth_admin_or_user_key
# ---------------------------------------------------------------------------


class TestAuthAdminOrUserKey:
    @pytest.mark.asyncio
    async def test_admin_key_passes_and_marks_admin(
        self,
        _admin_settings: None,
    ) -> None:
        request = _blank_request()
        await auth_admin_or_user_key(
            request=request,
            credentials=_bearer("test-admin-key"),
        )
        assert request.state.gateway_is_admin is True
        assert not hasattr(request.state, "gateway_api_key")

    @pytest.mark.asyncio
    async def test_valid_user_key_passes_and_sets_state(
        self,
        _admin_settings: None,
    ) -> None:
        request = _blank_request()
        resp = MagicMock(status_code=200)
        resp.json.return_value = {"user_id": "u-1"}
        with patch(
            "unify.gateway.common.auth.httpx.AsyncClient",
            return_value=_async_client_returning(resp),
        ):
            await auth_admin_or_user_key(
                request=request,
                credentials=_bearer("sk-user"),
            )
        assert request.state.gateway_is_admin is False
        assert request.state.gateway_api_key == "sk-user"  # pragma: allowlist secret
        assert request.state.gateway_user == {"user_id": "u-1"}

    @pytest.mark.asyncio
    async def test_invalid_key_raises_401(self, _admin_settings: None) -> None:
        request = _blank_request()
        resp = MagicMock(status_code=401)
        with patch(
            "unify.gateway.common.auth.httpx.AsyncClient",
            return_value=_async_client_returning(resp),
        ):
            with pytest.raises(HTTPException) as ctx:
                await auth_admin_or_user_key(
                    request=request,
                    credentials=_bearer("sk-bad"),
                )
        assert ctx.value.status_code == 401
        assert not hasattr(request.state, "gateway_is_admin")


def test_admin_or_user_auth_dependency_shape() -> None:
    """Dependency list is one Depends() wrapping auth_admin_or_user_key."""
    assert len(admin_or_user_auth_dependency) == 1
    dep = admin_or_user_auth_dependency[0]
    assert dep.dependency is auth_admin_or_user_key


# ---------------------------------------------------------------------------
# require_assistant_ownership
# ---------------------------------------------------------------------------


def _user_key_request(api_key: str = "sk-user") -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(gateway_is_admin=False, gateway_api_key=api_key),
    )


class TestRequireAssistantOwnership:
    @pytest.mark.asyncio
    async def test_admin_caller_bypasses(self, _admin_settings: None) -> None:
        request = SimpleNamespace(state=SimpleNamespace(gateway_is_admin=True))
        # No httpx patching: any HTTP call would blow up the test.
        await require_assistant_ownership(request, 123)

    @pytest.mark.asyncio
    async def test_mount_without_dual_dependency_bypasses(
        self,
        _admin_settings: None,
    ) -> None:
        """Admin-only mounts never run the dual dependency; they stay trusted."""
        await require_assistant_ownership(_blank_request(), 123)

    @pytest.mark.asyncio
    async def test_owned_assistant_passes(self, _admin_settings: None) -> None:
        resp = MagicMock(status_code=200)
        resp.json.return_value = {"info": [{"agent_id": "123"}]}
        client = _async_client_returning(resp)
        with patch(
            "unify.gateway.common.auth.httpx.AsyncClient",
            return_value=client,
        ):
            await require_assistant_ownership(_user_key_request(), 123)
        _, kwargs = client.get.call_args
        assert kwargs["headers"] == {"Authorization": "Bearer sk-user"}
        assert kwargs["params"] == {"agent_id": "123"}

    @pytest.mark.asyncio
    async def test_not_owned_raises_403(self, _admin_settings: None) -> None:
        """Orchestra scopes results to the key's owner: 404 means not owned."""
        resp = MagicMock(status_code=404)
        with patch(
            "unify.gateway.common.auth.httpx.AsyncClient",
            return_value=_async_client_returning(resp),
        ):
            with pytest.raises(HTTPException) as ctx:
                await require_assistant_ownership(_user_key_request(), 123)
        assert ctx.value.status_code == 403

    @pytest.mark.asyncio
    async def test_empty_scoped_listing_raises_403(
        self,
        _admin_settings: None,
    ) -> None:
        resp = MagicMock(status_code=200)
        resp.json.return_value = {"info": []}
        with patch(
            "unify.gateway.common.auth.httpx.AsyncClient",
            return_value=_async_client_returning(resp),
        ):
            with pytest.raises(HTTPException) as ctx:
                await require_assistant_ownership(_user_key_request(), 123)
        assert ctx.value.status_code == 403

    @pytest.mark.asyncio
    async def test_missing_agent_id_raises_403(self, _admin_settings: None) -> None:
        with pytest.raises(HTTPException) as ctx:
            await require_assistant_ownership(_user_key_request(), None)
        assert ctx.value.status_code == 403

    @pytest.mark.asyncio
    async def test_orchestra_transport_error_raises_403(
        self,
        _admin_settings: None,
    ) -> None:
        client = AsyncMock()
        client.__aenter__.return_value = client
        client.get.side_effect = RuntimeError("connection refused")
        with patch(
            "unify.gateway.common.auth.httpx.AsyncClient",
            return_value=client,
        ):
            with pytest.raises(HTTPException) as ctx:
                await require_assistant_ownership(_user_key_request(), 123)
        assert ctx.value.status_code == 403


# ---------------------------------------------------------------------------
# require_gateway_admin
# ---------------------------------------------------------------------------


class TestRequireGatewayAdmin:
    def test_admin_caller_passes(self) -> None:
        request = SimpleNamespace(state=SimpleNamespace(gateway_is_admin=True))
        require_gateway_admin(request)

    def test_mount_without_dual_dependency_passes(self) -> None:
        require_gateway_admin(_blank_request())

    def test_user_key_caller_raises_403(self) -> None:
        request = _user_key_request()
        with pytest.raises(HTTPException) as ctx:
            require_gateway_admin(request)
        assert ctx.value.status_code == 403


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
