"""Behavioural tests for ``unify.gateway.channels.ms_teams_bot.views``.

The Bot Connector send is the single choke point for every live assistant
reply into Teams, so it is where AI-authored content is labelled: the posted
activity must carry the schema.org ``AIGeneratedContent`` entity that makes
Teams render its native "AI generated" caption (Store certification requires
AI-generated content be disclosed to users). The outbound HTTP call, install
resolution, and Connector-token mint are mocked -- this is a transport/shape
test, not a Bot Framework behaviour test.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from unify.gateway.channels.ms_teams_bot import auth_router
from unify.gateway.channels.ms_teams_bot import views as bot_views


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI()
    app.include_router(auth_router, prefix="/ms-teams-bot")
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _connector_response() -> MagicMock:
    resp = MagicMock(status_code=201)
    resp.content = b'{"id": "act-1"}'
    resp.json.return_value = {"id": "act-1"}
    return resp


def _capturing_async_client(response_mock: MagicMock) -> MagicMock:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.post.return_value = response_mock
    return client


def test_send_labels_reply_as_ai_generated(client: TestClient) -> None:
    connector = _capturing_async_client(_connector_response())
    with (
        patch.object(
            bot_views,
            "require_assistant_ownership",
            new=AsyncMock(return_value=None),
        ),
        patch.object(
            bot_views,
            "_resolve_install",
            new=AsyncMock(return_value={"id": 1}),
        ),
        patch.object(
            bot_views,
            "_resolve_service_url",
            new=AsyncMock(return_value="https://svc.example.com"),
        ),
        patch.object(
            bot_views,
            "_mint_connector_token",
            new=AsyncMock(return_value="connector-token"),
        ),
        patch.object(bot_views.httpx, "AsyncClient", return_value=connector),
    ):
        resp = client.post(
            "/ms-teams-bot/send",
            json={
                "tenant_id": "tenant-1",
                "conversation_id": "conv-1",
                "body": "Here is your answer.",
            },
        )

    assert resp.status_code == 200
    # The activity POSTed to the Bot Connector must carry the AI-generated label.
    activity = connector.post.await_args.kwargs["json"]
    assert activity["type"] == "message"
    assert activity["text"] == "Here is your answer."
    entities = activity["entities"]
    assert any(
        e.get("additionalType") == ["AIGeneratedContent"]
        and e.get("type") == "https://schema.org/Message"
        for e in entities
    )


def _lookup_client(status_code: int, payload: dict | None = None) -> MagicMock:
    resp = MagicMock(status_code=status_code)
    resp.json.return_value = payload or {}
    resp.text = ""
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.get.return_value = resp
    return client


def test_conversation_route_lookup_returns_full_routing_pair(
    client: TestClient,
) -> None:
    """An outbound-only caller needs tenant and conversation together; a
    second round trip for the tenant is what this endpoint exists to avoid."""
    orchestra = _lookup_client(
        200,
        {
            "conversation_id": "conv-1",
            "tenant_id": "tenant-1",
            "sender_is_owner": True,
        },
    )
    with (
        patch.object(
            bot_views,
            "require_assistant_ownership",
            new=AsyncMock(return_value=None),
        ),
        patch.object(bot_views.httpx, "AsyncClient", return_value=orchestra),
    ):
        resp = client.get(
            "/ms-teams-bot/conversation-route",
            params={"assistant_id": "42", "owner_only": "true"},
        )

    assert resp.status_code == 200
    assert resp.json()["tenant_id"] == "tenant-1"
    assert resp.json()["conversation_id"] == "conv-1"
    forwarded = orchestra.get.await_args.kwargs["params"]
    assert forwarded["assistant_id"] == "42"
    assert forwarded["conversation_type"] == "personal"
    assert forwarded["owner_only"] == "true"


def test_conversation_route_lookup_propagates_not_found(client: TestClient) -> None:
    """No live conversation is an answer, not an error to retry."""
    orchestra = _lookup_client(404)
    with (
        patch.object(
            bot_views,
            "require_assistant_ownership",
            new=AsyncMock(return_value=None),
        ),
        patch.object(bot_views.httpx, "AsyncClient", return_value=orchestra),
    ):
        resp = client.get(
            "/ms-teams-bot/conversation-route",
            params={"assistant_id": "42"},
        )

    assert resp.status_code == 404


def test_conversation_route_lookup_requires_ownership(client: TestClient) -> None:
    """The route table is admin-scoped; an assistant may only read its own."""
    orchestra = _lookup_client(200, {"conversation_id": "conv-1"})
    ownership = AsyncMock(return_value=None)
    with (
        patch.object(bot_views, "require_assistant_ownership", new=ownership),
        patch.object(bot_views.httpx, "AsyncClient", return_value=orchestra),
    ):
        client.get(
            "/ms-teams-bot/conversation-route",
            params={"assistant_id": "42"},
        )

    assert ownership.await_args.args[1] == "42"
