"""Tests for ``unity.gateway.common.graph``."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from unity.gateway.common.graph import (
    GRAPH_SCOPES,
    TokenCredentialFromSecret,
    _get_user_node,
    get_admin_graph_client,
    get_graph_client,
    graph_client_from_assistant,
)
from unity.gateway.credentials import EnvCredentialStore

# ---------------------------------------------------------------------------
# TokenCredentialFromSecret
# ---------------------------------------------------------------------------


def test_token_credential_returns_stored_token_with_far_future_expiry() -> None:
    cred = TokenCredentialFromSecret("stored-token-abc")
    tok = cred.get_token("any-scope")
    assert tok.token == "stored-token-abc"
    # Expires in the year 2286 -- effectively never, matching the
    # original communication.helpers behaviour
    assert tok.expires_on > 9_000_000_000


# ---------------------------------------------------------------------------
# get_admin_graph_client -- credential resolution
# ---------------------------------------------------------------------------


def test_get_admin_graph_client_raises_500_when_any_credential_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MS365_ADMIN_TENANT_ID", "tenant-id")
    monkeypatch.setenv("MS365_ADMIN_CLIENT_ID", "client-id")
    monkeypatch.delenv("MS365_ADMIN_CLIENT_SECRET", raising=False)
    with pytest.raises(HTTPException) as exc:
        get_admin_graph_client(EnvCredentialStore())
    assert exc.value.status_code == 500
    assert "MS365_ADMIN_CLIENT_SECRET" in exc.value.detail


def test_get_admin_graph_client_uses_client_secret_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MS365_ADMIN_TENANT_ID", "tenant-id")
    monkeypatch.setenv("MS365_ADMIN_CLIENT_ID", "client-id")
    monkeypatch.setenv("MS365_ADMIN_CLIENT_SECRET", "client-secret")
    with (
        patch("unity.gateway.common.graph.ClientSecretCredential") as MockCred,
        patch("unity.gateway.common.graph.GraphServiceClient") as MockClient,
    ):
        get_admin_graph_client(EnvCredentialStore())
    MockCred.assert_called_once_with(
        tenant_id="tenant-id",
        client_id="client-id",
        client_secret="client-secret",
    )
    MockClient.assert_called_once_with(
        credentials=MockCred.return_value,
        scopes=GRAPH_SCOPES,
    )


def test_get_admin_graph_client_lazy_credential_store_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no store is supplied, EnvCredentialStore() is used by default."""
    monkeypatch.setenv("MS365_ADMIN_TENANT_ID", "tenant-id")
    monkeypatch.setenv("MS365_ADMIN_CLIENT_ID", "client-id")
    monkeypatch.setenv("MS365_ADMIN_CLIENT_SECRET", "client-secret")
    with (
        patch("unity.gateway.common.graph.ClientSecretCredential"),
        patch("unity.gateway.common.graph.GraphServiceClient"),
    ):
        get_admin_graph_client()  # no explicit store


# ---------------------------------------------------------------------------
# graph_client_from_assistant -- BYOD vs admin dispatch
# ---------------------------------------------------------------------------


def test_graph_client_from_assistant_uses_byod_token_when_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assistant = {"secrets": {"MICROSOFT_ACCESS_TOKEN": "user-oauth-token"}}
    with patch("unity.gateway.common.graph.GraphServiceClient") as MockClient:
        graph_client_from_assistant(assistant, "user@example.com")
    MockClient.assert_called_once()
    call_kwargs = MockClient.call_args.kwargs
    assert isinstance(call_kwargs["credentials"], TokenCredentialFromSecret)
    assert call_kwargs["scopes"] == GRAPH_SCOPES


def test_graph_client_from_assistant_falls_back_to_admin_without_byod_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MS365_ADMIN_TENANT_ID", "tenant-id")
    monkeypatch.setenv("MS365_ADMIN_CLIENT_ID", "client-id")
    monkeypatch.setenv("MS365_ADMIN_CLIENT_SECRET", "client-secret")
    assistant = {"secrets": {}}  # no MICROSOFT_ACCESS_TOKEN
    with (
        patch("unity.gateway.common.graph.ClientSecretCredential") as MockCred,
        patch("unity.gateway.common.graph.GraphServiceClient") as MockClient,
    ):
        graph_client_from_assistant(assistant, "user@example.com")
    MockCred.assert_called_once()  # admin path
    MockClient.assert_called_once()


# ---------------------------------------------------------------------------
# get_graph_client -- Orchestra lookup + dispatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_graph_client_uses_assistant_when_lookup_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assistant = {"secrets": {"MICROSOFT_ACCESS_TOKEN": "user-token"}}
    with (
        patch(
            "unity.gateway.common.graph.lookup_assistant",
            new=AsyncMock(return_value=assistant),
        ),
        patch("unity.gateway.common.graph.GraphServiceClient") as MockClient,
    ):
        await get_graph_client("user@example.com", EnvCredentialStore())
    MockClient.assert_called_once()
    assert isinstance(
        MockClient.call_args.kwargs["credentials"],
        TokenCredentialFromSecret,
    )


@pytest.mark.asyncio
async def test_get_graph_client_resolves_by_assistant_id_when_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assistant = {"secrets": {"MICROSOFT_ACCESS_TOKEN": "user-token"}}
    by_id = AsyncMock(return_value=assistant)
    by_email = AsyncMock(return_value=assistant)
    with (
        patch("unity.gateway.common.graph.lookup_assistant_by_id", new=by_id),
        patch("unity.gateway.common.graph.lookup_assistant", new=by_email),
        patch("unity.gateway.common.graph.GraphServiceClient") as MockClient,
    ):
        await get_graph_client(assistant_id=123, credentials=EnvCredentialStore())
    by_id.assert_awaited_once()
    by_email.assert_not_awaited()
    MockClient.assert_called_once()
    assert isinstance(
        MockClient.call_args.kwargs["credentials"],
        TokenCredentialFromSecret,
    )


@pytest.mark.asyncio
async def test_get_graph_client_falls_back_to_admin_when_lookup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MS365_ADMIN_TENANT_ID", "tenant-id")
    monkeypatch.setenv("MS365_ADMIN_CLIENT_ID", "client-id")
    monkeypatch.setenv("MS365_ADMIN_CLIENT_SECRET", "client-secret")
    with (
        patch(
            "unity.gateway.common.graph.lookup_assistant",
            new=AsyncMock(side_effect=RuntimeError("orchestra down")),
        ),
        patch("unity.gateway.common.graph.ClientSecretCredential"),
        patch("unity.gateway.common.graph.GraphServiceClient") as MockClient,
    ):
        await get_graph_client("user@example.com", EnvCredentialStore())
    MockClient.assert_called_once()  # admin client built via the fallback


# ---------------------------------------------------------------------------
# _get_user_node -- /me vs /users/{email} dispatch
# ---------------------------------------------------------------------------


def test_get_user_node_returns_me_when_byod_token_present() -> None:
    graph = MagicMock()
    assistant = {"secrets": {"MICROSOFT_ACCESS_TOKEN": "user-token"}}
    node = _get_user_node(graph, "user@example.com", assistant)
    assert node is graph.me


def test_get_user_node_returns_users_by_id_when_no_byod_token() -> None:
    graph = MagicMock()
    assistant = {"secrets": {}}
    node = _get_user_node(graph, "user@example.com", assistant)
    graph.users.by_user_id.assert_called_once_with("user@example.com")
    assert node is graph.users.by_user_id.return_value
