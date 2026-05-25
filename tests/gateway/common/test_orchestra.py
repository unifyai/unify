"""Tests for ``unity.gateway.common.orchestra``."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from unity.gateway.common import orchestra
from unity.gateway.credentials import EnvCredentialStore


@pytest.fixture
def _orchestra_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        orchestra,
        "SETTINGS",
        SimpleNamespace(ORCHESTRA_URL="https://orchestra.example.com/v0"),
    )


@pytest.mark.asyncio
async def test_lookup_assistant_returns_first_match_on_200(
    monkeypatch: pytest.MonkeyPatch,
    _orchestra_settings: None,
) -> None:
    monkeypatch.setenv("ORCHESTRA_ADMIN_KEY", "test-admin-key")
    credentials = EnvCredentialStore()

    fake_response = MagicMock(status_code=200)
    fake_response.json.return_value = {
        "info": [
            {"email": "user@example.com", "secrets": {"X": "1"}},
            {"email": "user@example.com", "secrets": {"X": "2"}},
        ],
    }
    fake_client = AsyncMock()
    fake_client.__aenter__.return_value = fake_client
    fake_client.get.return_value = fake_response
    with patch(
        "unity.gateway.common.orchestra.httpx.AsyncClient",
        return_value=fake_client,
    ):
        result = await orchestra.lookup_assistant("user@example.com", credentials)

    assert result == {"email": "user@example.com", "secrets": {"X": "1"}}
    call = fake_client.get.call_args
    assert call.args[0] == "https://orchestra.example.com/v0/admin/assistant"
    assert call.kwargs["params"] == {"email": "user@example.com"}
    assert call.kwargs["headers"]["Authorization"] == "Bearer test-admin-key"


@pytest.mark.asyncio
async def test_lookup_assistant_raises_500_when_admin_key_missing(
    monkeypatch: pytest.MonkeyPatch,
    _orchestra_settings: None,
) -> None:
    monkeypatch.delenv("ORCHESTRA_ADMIN_KEY", raising=False)
    credentials = EnvCredentialStore()
    with pytest.raises(HTTPException) as exc:
        await orchestra.lookup_assistant("user@example.com", credentials)
    assert exc.value.status_code == 500
    assert "ORCHESTRA_ADMIN_KEY" in exc.value.detail


@pytest.mark.asyncio
async def test_lookup_assistant_raises_404_on_non_200(
    monkeypatch: pytest.MonkeyPatch,
    _orchestra_settings: None,
) -> None:
    monkeypatch.setenv("ORCHESTRA_ADMIN_KEY", "test-admin-key")
    credentials = EnvCredentialStore()

    fake_response = MagicMock(status_code=500)
    fake_client = AsyncMock()
    fake_client.__aenter__.return_value = fake_client
    fake_client.get.return_value = fake_response
    with patch(
        "unity.gateway.common.orchestra.httpx.AsyncClient",
        return_value=fake_client,
    ):
        with pytest.raises(HTTPException) as exc:
            await orchestra.lookup_assistant("unknown@example.com", credentials)

    assert exc.value.status_code == 404
    assert "unknown@example.com" in exc.value.detail


@pytest.mark.asyncio
async def test_lookup_assistant_raises_404_on_empty_info(
    monkeypatch: pytest.MonkeyPatch,
    _orchestra_settings: None,
) -> None:
    monkeypatch.setenv("ORCHESTRA_ADMIN_KEY", "test-admin-key")
    credentials = EnvCredentialStore()

    fake_response = MagicMock(status_code=200)
    fake_response.json.return_value = {"info": []}
    fake_client = AsyncMock()
    fake_client.__aenter__.return_value = fake_client
    fake_client.get.return_value = fake_response
    with patch(
        "unity.gateway.common.orchestra.httpx.AsyncClient",
        return_value=fake_client,
    ):
        with pytest.raises(HTTPException) as exc:
            await orchestra.lookup_assistant("user@example.com", credentials)

    assert exc.value.status_code == 404
