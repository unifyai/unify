"""Twilio adapter shared-phone routing tests."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import unity.gateway.adapters.twilio as twilio


def _response(status_code: int, payload: dict | None = None) -> MagicMock:
    resp = MagicMock(status_code=status_code)
    resp.json.return_value = payload or {}
    resp.raise_for_status = MagicMock()
    return resp


@pytest.mark.asyncio
async def test_resolve_phone_route_calls_orchestra_phone_resolve(monkeypatch):
    stub_secret = SimpleNamespace(get_secret_value=lambda: "admin-key")
    monkeypatch.setattr(
        twilio,
        "SETTINGS",
        SimpleNamespace(
            ORCHESTRA_URL="https://orchestra.example.com/v0",
            ORCHESTRA_ADMIN_KEY=stub_secret,
        ),
    )
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.get.return_value = _response(
        200,
        {"assistant_id": 101, "role": "owner"},
    )
    monkeypatch.setattr(twilio.httpx, "AsyncClient", lambda **_kwargs: client)

    route = await twilio.resolve_phone_route("+15550800000", "+15550000001")

    assert route == {"assistant_id": 101, "role": "owner"}
    client.get.assert_awaited_once_with(
        "https://orchestra.example.com/v0/admin/phone/resolve",
        params={"pool_number": "+15550800000", "sender": "+15550000001"},
        headers={"Authorization": "Bearer admin-key"},
    )


@pytest.mark.asyncio
async def test_resolve_phone_route_returns_none_on_404(monkeypatch):
    stub_secret = SimpleNamespace(get_secret_value=lambda: "admin-key")
    monkeypatch.setattr(
        twilio,
        "SETTINGS",
        SimpleNamespace(
            ORCHESTRA_URL="https://orchestra.example.com/v0",
            ORCHESTRA_ADMIN_KEY=stub_secret,
        ),
    )
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.get.return_value = _response(404)
    monkeypatch.setattr(twilio.httpx, "AsyncClient", lambda **_kwargs: client)

    assert await twilio.resolve_phone_route("+15550800000", "+15550000001") is None
