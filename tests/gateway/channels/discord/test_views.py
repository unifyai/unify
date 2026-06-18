"""Behavioural tests for ``droid.gateway.channels.discord``.

Discord is the most complex channel (WebSocket gateway + pool
manager + admin API). Tests cover:

- views router contract + happy-path / error per endpoint
- bot_manager helpers (registry, status, get_bot_token)
- gateway helpers (_assistant_topic, _default_contacts,
  shared dedup)

The WebSocket gateway loop itself is integration territory; we
verify importability and the pure helpers but do not exercise the
aiohttp ws_connect path here.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from droid.gateway.channels.discord import bot_manager, router
from droid.gateway.channels.discord.gateway import (
    GatewayConnection,
)
from droid.gateway.channels.discord.gateway import (
    _assistant_topic,
    _ensure_job_running,
)
from droid.gateway.common import pubsub as shared_pubsub

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_bot_pool() -> None:
    """Bot pool is module-level state; clear before + after each test."""
    bot_manager._bots.clear()
    yield
    bot_manager._bots.clear()


@pytest.fixture(autouse=True)
def _reset_dedup_cache() -> None:
    """Same for the inbound message dedup cache."""

    shared_pubsub._seen_ids.clear()
    yield
    shared_pubsub._seen_ids.clear()


@pytest.fixture
def _settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub SETTINGS in views.py (covers the SecretStr-shaped admin key)."""
    from droid.gateway.channels.discord import views as discord_views

    stub_secret = SimpleNamespace(get_secret_value=lambda: "test-admin-key")
    monkeypatch.setattr(
        discord_views,
        "SETTINGS",
        SimpleNamespace(
            ORCHESTRA_URL="https://orchestra.example.com/v0",
            ORCHESTRA_ADMIN_KEY=stub_secret,
        ),
    )


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI()
    app.include_router(router, prefix="/discord")
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


# ---------------------------------------------------------------------------
# Router contract
# ---------------------------------------------------------------------------


def test_router_exposes_expected_paths() -> None:
    paths = sorted(
        (r.path, sorted(r.methods)) for r in router.routes  # type: ignore[attr-defined]
    )
    assert paths == [
        ("/create", ["POST"]),
        ("/delete", ["DELETE"]),
        ("/send", ["POST"]),
        ("/status", ["GET"]),
        ("/sync", ["POST"]),
    ]


def test_router_importable_from_package_root() -> None:
    from droid.gateway.channels.discord import router as exported

    assert exported is router


def test_bot_manager_importable() -> None:
    from droid.gateway.channels.discord import bot_manager as bm

    assert hasattr(bm, "connect_bot")
    assert hasattr(bm, "disconnect_bot")
    assert hasattr(bm, "sync_from_orchestra")


def test_gateway_importable() -> None:
    from droid.gateway.channels.discord.gateway import (
        BOT_INTENTS,
        DISCORD_API_BASE,
        DISCORD_GATEWAY_URL,
        GatewayConnection,
    )

    assert "discord.com/api" in DISCORD_API_BASE
    assert DISCORD_GATEWAY_URL.startswith("wss://")
    assert BOT_INTENTS > 0
    # GUILDS (1 << 0) is required for thread MESSAGE_CREATE delivery;
    # DIRECT_MESSAGES (1 << 12) and GUILD_MESSAGES (1 << 9) for DMs and guild
    # channels. MESSAGE_CONTENT (privileged, 1 << 15) must not be requested or
    # the gateway gets a fatal 4014 close when it is disabled in the portal.
    assert BOT_INTENTS & (1 << 0)
    assert BOT_INTENTS & (1 << 12)
    assert BOT_INTENTS & (1 << 9)
    assert not BOT_INTENTS & (1 << 15)
    assert GatewayConnection is not None


# ---------------------------------------------------------------------------
# gateway.py pure helpers
# ---------------------------------------------------------------------------


class TestAssistantTopic:
    def test_uses_env_suffix_from_settings(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from droid.gateway.channels.discord import gateway as gw

        monkeypatch.setattr(
            gw,
            "SETTINGS",
            SimpleNamespace(ENV_SUFFIX="-staging"),
        )
        assert _assistant_topic("42") == "droid-42-staging"

    def test_empty_suffix_for_production(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from droid.gateway.channels.discord import gateway as gw

        monkeypatch.setattr(gw, "SETTINGS", SimpleNamespace(ENV_SUFFIX=""))
        assert _assistant_topic("42") == "droid-42"


class TestEnsureJobRunning:
    """The /infra/job/start contract requires self_contact_id and
    boss_contact_id as form fields; omitting them returns 422."""

    @pytest.mark.asyncio
    async def test_payload_includes_contact_ids(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from droid.gateway.channels.discord import gateway as gw

        stub_secret = SimpleNamespace(get_secret_value=lambda: "admin-key")
        monkeypatch.setattr(
            gw,
            "SETTINGS",
            SimpleNamespace(
                conversation=SimpleNamespace(COMMS_URL="https://comms.example.com"),
                ORCHESTRA_ADMIN_KEY=stub_secret,
            ),
        )

        captured: dict = {}

        async def _post(url, *args, **kwargs):
            captured["url"] = url
            captured["data"] = kwargs["data"]
            return MagicMock(status_code=200)

        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post = _post

        with patch.object(gw.httpx, "AsyncClient", return_value=httpx_client):
            await _ensure_job_running(
                {
                    "assistant_id": "42",
                    "api_key": "key-42",  # pragma: allowlist secret
                    "self_contact_id": 789,
                    "boss_contact_id": 790,
                },
            )

        assert captured["url"].endswith("/infra/job/start")
        assert captured["data"]["assistant_id"] == "42"
        assert captured["data"]["self_contact_id"] == "789"
        assert captured["data"]["boss_contact_id"] == "790"


class TestAlreadyPublished:
    def test_first_call_returns_false(self) -> None:
        assert shared_pubsub.already_published("discord", "msg-1") is False

    def test_second_call_returns_true(self) -> None:
        shared_pubsub.already_published("discord", "msg-1")
        assert shared_pubsub.already_published("discord", "msg-1") is True

    def test_expired_entries_are_cleaned(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Seed an entry that's already expired
        shared_pubsub._seen_ids["discord"] = {
            "old-msg": time.time() - shared_pubsub._DEDUP_TTL - 100,
        }
        shared_pubsub.already_published("discord", "new-msg")
        # The old expired key should have been swept on this call
        assert "old-msg" not in shared_pubsub._seen_ids["discord"]
        assert "new-msg" in shared_pubsub._seen_ids["discord"]


# ---------------------------------------------------------------------------
# bot_manager helpers
# ---------------------------------------------------------------------------


class TestBotManager:
    @pytest.mark.asyncio
    async def test_connect_bot_registers_and_starts(self) -> None:
        with patch(
            "droid.gateway.channels.discord.bot_manager.GatewayConnection",
        ) as MockConn:
            instance = MagicMock()
            instance.start = AsyncMock()
            instance.connected = True
            MockConn.return_value = instance

            await bot_manager.connect_bot("bot-1", "token-1")

        assert "bot-1" in bot_manager._bots
        assert bot_manager.get_bot_token("bot-1") == "token-1"
        instance.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_bot_skips_if_already_connected(self) -> None:
        existing = MagicMock(connected=True)
        bot_manager._bots["bot-1"] = ("existing-token", existing)
        with patch(
            "droid.gateway.channels.discord.bot_manager.GatewayConnection",
        ) as MockConn:
            await bot_manager.connect_bot("bot-1", "different-token")
            MockConn.assert_not_called()

    @pytest.mark.asyncio
    async def test_disconnect_bot_removes_from_pool(self) -> None:
        conn = MagicMock()
        conn.stop = AsyncMock()
        bot_manager._bots["bot-1"] = ("token", conn)

        await bot_manager.disconnect_bot("bot-1")

        assert "bot-1" not in bot_manager._bots
        conn.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_disconnect_bot_is_noop_when_not_present(self) -> None:
        await bot_manager.disconnect_bot("not-here")  # no-op

    def test_get_bot_token_returns_none_when_missing(self) -> None:
        assert bot_manager.get_bot_token("not-here") is None

    def test_get_all_status_reflects_pool(self) -> None:
        conn = MagicMock(connected=True, _fatal_close_code=None)
        bot_manager._bots["bot-1"] = ("token", conn)
        status = bot_manager.get_all_status()
        assert status == {
            "bot-1": {"connected": True, "fatal_close_code": None},
        }


# ---------------------------------------------------------------------------
# POST /send
# ---------------------------------------------------------------------------


class TestSend:
    def test_neither_to_nor_channel_returns_400(
        self,
        client: TestClient,
        _settings: None,
    ) -> None:
        resp = client.post(
            "/discord/send",
            json={"body": "hi", "assistant_id": 42},
        )
        assert resp.status_code == 400

    def test_channel_message_requires_bot_id(
        self,
        client: TestClient,
        _settings: None,
    ) -> None:
        resp = client.post(
            "/discord/send",
            json={
                "body": "hi",
                "assistant_id": 42,
                "channel_id": "chan-1",
            },
        )
        assert resp.status_code == 400
        assert "bot_id" in resp.json()["detail"]

    def test_returns_503_when_bot_not_connected(
        self,
        client: TestClient,
        _settings: None,
    ) -> None:
        """No matching bot in the pool -> 503."""
        resp = client.post(
            "/discord/send",
            json={
                "body": "hi",
                "assistant_id": 42,
                "channel_id": "chan-1",
                "bot_id": "missing-bot",
            },
        )
        assert resp.status_code == 503

    def test_channel_message_success(
        self,
        client: TestClient,
        _settings: None,
    ) -> None:
        """Channel send: skips DM-open, posts to messages endpoint."""
        bot_manager._bots["pool-bot-1"] = ("bot-token", MagicMock())
        msg_resp = MagicMock(status_code=200)
        msg_resp.json.return_value = {"id": "msg-discord-1"}
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post.return_value = msg_resp

        with patch(
            "droid.gateway.channels.discord.views.httpx.AsyncClient",
            return_value=httpx_client,
        ):
            resp = client.post(
                "/discord/send",
                json={
                    "body": "Hello channel",
                    "assistant_id": 42,
                    "channel_id": "chan-discord-1",
                    "bot_id": "pool-bot-1",
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["message_id"] == "msg-discord-1"
        assert body["channel_id"] == "chan-discord-1"

    def test_dm_message_resolves_route_then_opens_dm_then_posts(
        self,
        client: TestClient,
        _settings: None,
    ) -> None:
        """DM flow: Orchestra route -> open DM channel -> post message."""
        bot_manager._bots["pool-bot-1"] = ("bot-token", MagicMock())

        route_resp = MagicMock(status_code=200)
        route_resp.json.return_value = {"pool_bot_id": "pool-bot-1"}
        dm_resp = MagicMock(status_code=200)
        dm_resp.json.return_value = {"id": "dm-channel-1"}
        msg_resp = MagicMock(status_code=200)
        msg_resp.json.return_value = {"id": "msg-1"}

        post_responses = [route_resp, dm_resp, msg_resp]

        async def _post(*args, **kwargs):
            return post_responses.pop(0)

        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post = _post

        with patch(
            "droid.gateway.channels.discord.views.httpx.AsyncClient",
            return_value=httpx_client,
        ):
            resp = client.post(
                "/discord/send",
                json={
                    "body": "Hello DM",
                    "assistant_id": 42,
                    "to": "user-discord-id",
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["channel_id"] == "dm-channel-1"
        assert body["message_id"] == "msg-1"

    def test_orchestra_route_failure_propagates(
        self,
        client: TestClient,
        _settings: None,
    ) -> None:
        """4xx from Orchestra route -> surfaces as same status code."""
        bot_manager._bots["pool-bot-1"] = ("bot-token", MagicMock())
        route_resp = MagicMock(status_code=404, text="No route")
        route_resp.json.return_value = {"detail": "No route found"}
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post.return_value = route_resp

        with patch(
            "droid.gateway.channels.discord.views.httpx.AsyncClient",
            return_value=httpx_client,
        ):
            resp = client.post(
                "/discord/send",
                json={"body": "x", "assistant_id": 42, "to": "user-id"},
            )

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /create
# ---------------------------------------------------------------------------


class TestCreate:
    def test_missing_bot_token_returns_400(self, client: TestClient) -> None:
        resp = client.post(
            "/discord/create",
            json={"bot_id": "bot-1", "assistant_id": 42},
        )
        assert resp.status_code == 400

    def test_success_calls_connect_bot(self, client: TestClient) -> None:
        with patch.object(
            bot_manager,
            "connect_bot",
            new=AsyncMock(),
        ) as mock_connect:
            resp = client.post(
                "/discord/create",
                json={"bot_id": "bot-1", "assistant_id": 42, "bot_token": "T"},
            )

        assert resp.status_code == 200
        assert resp.json() == {"success": True, "bot_id": "bot-1"}
        mock_connect.assert_awaited_once_with("bot-1", "T")


# ---------------------------------------------------------------------------
# POST /sync, DELETE /delete, GET /status
# ---------------------------------------------------------------------------


def test_sync_pool_calls_orchestra_sync(client: TestClient) -> None:
    with (
        patch.object(
            bot_manager,
            "sync_from_orchestra",
            new=AsyncMock(return_value=3),
        ),
        patch.object(
            bot_manager,
            "get_all_status",
            return_value={"bot-1": {"connected": True, "fatal_close_code": None}},
        ),
    ):
        resp = client.post("/discord/sync")
    assert resp.status_code == 200
    body = resp.json()
    assert body["synced"] == 3
    assert "bot-1" in body["pool"]


def test_delete_calls_disconnect_bot(client: TestClient) -> None:
    with patch.object(
        bot_manager,
        "disconnect_bot",
        new=AsyncMock(),
    ) as mock_disconnect:
        resp = client.request(
            "DELETE",
            "/discord/delete",
            json={"bot_id": "bot-1"},
        )
    assert resp.status_code == 200
    assert resp.json() == {"success": True}
    mock_disconnect.assert_awaited_once_with("bot-1")


def test_status_returns_pool_status(client: TestClient) -> None:
    with patch.object(
        bot_manager,
        "get_all_status",
        return_value={"bot-1": {"connected": True, "fatal_close_code": None}},
    ):
        resp = client.get("/discord/status")
    assert resp.status_code == 200
    assert "bot-1" in resp.json()


# ---------------------------------------------------------------------------
# GatewayConnection construction
# ---------------------------------------------------------------------------


def test_gateway_connection_initial_state() -> None:
    """Newly constructed GatewayConnection is not connected."""
    conn = GatewayConnection("bot-x", "token-x")
    assert conn.bot_id == "bot-x"
    assert conn.bot_token == "token-x"
    assert conn.connected is False
    assert conn._fatal_close_code is None
    assert conn._session_id is None
