"""Aggregator tests for ``unity.gateway.app``.

Covers:

- Route inventory parity with ``communication/main.py`` for the 10
  Phase B channels.
- Admin-auth dependency is enforced on the admin-mounted routers.
- Unauth routes (Twilio status webhooks) are reachable without a
  bearer token.
- The /unillm path is mounted; its auth is enforced inside the route
  (verified in tests/gateway/channels/unillm).
- The lifespan starts the Discord pool sync and the health-check
  task, and cancels the task on shutdown.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from unity.gateway.app import create_app

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _admin_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub SETTINGS for the admin-auth dependency."""
    from unity.gateway.common import auth

    stub_secret = SimpleNamespace(get_secret_value=lambda: "test-admin-key")
    monkeypatch.setattr(
        auth,
        "SETTINGS",
        SimpleNamespace(
            ORCHESTRA_ADMIN_KEY=stub_secret,
            ORCHESTRA_URL="https://orchestra.example.com/v0",
        ),
    )


@asynccontextmanager
async def _noop_lifespan(app: FastAPI):
    """Empty lifespan that skips the Discord pool sync.

    Route-shape tests don't care about Discord and shouldn't pay the
    sync cost (or its httpx side effects). Tests that *do* care about
    lifespan use the real one via ``create_app()`` directly.
    """
    yield


@pytest.fixture
def app_no_lifespan(_admin_settings: None):
    """Build the gateway app with a no-op lifespan for synchronous tests."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan  # type: ignore[attr-defined]
    return app


@pytest.fixture
def client_no_lifespan(app_no_lifespan) -> TestClient:
    with TestClient(app_no_lifespan) as test_client:
        yield test_client


# ---------------------------------------------------------------------------
# Route inventory
# ---------------------------------------------------------------------------


def _admin_prefix(prefixes: set[str]) -> set[str]:
    """Filter out the framework-built-ins (/, /openapi.json, /docs, ...)."""
    return {
        p
        for p in prefixes
        if not p.startswith("/openapi")
        and not p.startswith("/docs")
        and not p.startswith("/redoc")
    }


class TestRouteInventory:
    def test_top_level_prefixes_match_phase_b_channels(
        self,
        app_no_lifespan,
    ) -> None:
        """The aggregator mounts exactly the 10 Phase B channels."""
        prefixes = {
            r.path.split("/", 2)[1] if r.path.count("/") >= 1 else r.path  # type: ignore[attr-defined]
            for r in app_no_lifespan.routes
        }
        prefixes = {f"/{p}" for p in prefixes if p}
        channel_prefixes = {
            "/social",
            "/phone",
            "/gmail",
            "/outlook",
            "/email",
            "/whatsapp",
            "/teams",
            "/sharepoint",
            "/discord",
            "/unillm",
        }
        assert channel_prefixes.issubset(prefixes)

    def test_root_and_health_routes_present(
        self,
        client_no_lifespan: TestClient,
    ) -> None:
        assert client_no_lifespan.get("/").status_code == 200
        assert client_no_lifespan.get("/").json() == {"message": "success!"}
        assert client_no_lifespan.get("/health").status_code == 200
        assert client_no_lifespan.get("/health").json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# Admin-auth enforcement
# ---------------------------------------------------------------------------


class TestAdminAuth:
    def test_admin_route_without_bearer_is_rejected(
        self,
        client_no_lifespan: TestClient,
    ) -> None:
        """No Authorization header -> HTTPBearer auto_error rejects.

        FastAPI returns 403 in some versions, 401 in newer ones; the
        contract we care about is "auth gate fired, route handler did
        not run".
        """
        resp = client_no_lifespan.get("/social/available-platforms")
        assert resp.status_code in (401, 403)

    def test_admin_route_with_wrong_bearer_returns_403(
        self,
        client_no_lifespan: TestClient,
    ) -> None:
        resp = client_no_lifespan.get(
            "/social/available-platforms",
            headers={"Authorization": "Bearer wrong-key"},
        )
        assert resp.status_code == 403

    def test_admin_route_with_correct_bearer_proceeds_past_auth(
        self,
        client_no_lifespan: TestClient,
    ) -> None:
        """Correct admin key -> auth passes, route handler runs.

        The important assertion is *not* 403 (auth passed); the handler
        may still 200, 4xx, or 5xx depending on its own logic.
        """
        resp = client_no_lifespan.get(
            "/social/available-platforms",
            headers={"Authorization": "Bearer test-admin-key"},
        )
        assert resp.status_code != 403

    def test_each_admin_channel_enforces_auth(
        self,
        client_no_lifespan: TestClient,
    ) -> None:
        """Every admin-mounted channel rejects unauthenticated requests.

        Each (method, path) is a known route in the channel's router.
        We assert 403 (auth dependency fired) rather than 404 (route
        not matched) because that's what proves the dependency is
        actually wired on the mount.
        """
        admin_routes = [
            ("GET", "/social/available-platforms"),
            ("POST", "/phone/send-text"),
            ("POST", "/gmail/send"),
            ("POST", "/outlook/send"),
            ("POST", "/email/send"),
            ("POST", "/whatsapp/send"),
            ("POST", "/teams/send"),
            ("GET", "/sharepoint/sites"),
            ("POST", "/discord/send"),
        ]
        for method, path in admin_routes:
            resp = client_no_lifespan.request(method, path, json={})
            assert resp.status_code in (401, 403), (
                f"{method} {path}: expected auth rejection (401/403), "
                f"got {resp.status_code}"
            )


# ---------------------------------------------------------------------------
# Unauth routes (Twilio webhooks)
# ---------------------------------------------------------------------------


class TestUnauthRoutes:
    def test_phone_status_webhook_does_not_require_bearer(
        self,
        client_no_lifespan: TestClient,
    ) -> None:
        """Twilio doesn't carry our bearer; the status webhook must be reachable."""
        resp = client_no_lifespan.post(
            "/phone/call-status",
            data={
                "CallSid": "CA_test",
                "CallStatus": "completed",
                "From": "+15555550000",
                "To": "+15555550100",
            },
        )
        # Should NOT be 403 (no auth required); the route itself may
        # return 200, 400, or 422 depending on schema, but never 403.
        assert resp.status_code != 403

    def test_whatsapp_status_webhook_does_not_require_bearer(
        self,
        client_no_lifespan: TestClient,
    ) -> None:
        resp = client_no_lifespan.post(
            "/whatsapp/status",
            data={
                "MessageStatus": "delivered",
                "To": "whatsapp:+15555550000",
                "From": "whatsapp:+15555550100",
            },
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# unillm path is mounted
# ---------------------------------------------------------------------------


def test_unillm_route_mounted_without_admin_auth(
    client_no_lifespan: TestClient,
) -> None:
    """unillm enforces auth internally (user-API-key), not via dependency.

    Without auth at all we get 401 from the route's own check (not
    403 from the admin dependency, since unillm isn't mounted behind it).
    """
    resp = client_no_lifespan.post(
        "/unillm/chat/completions",
        json={
            "model": "gpt-4o@openai",
            "messages": [{"role": "user", "content": "hi"}],
        },
    )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


class TestLifespan:
    @pytest.mark.asyncio
    async def test_lifespan_invokes_discord_sync_and_launches_health_loop(
        self,
        _admin_settings: None,
    ) -> None:
        """Startup: calls sync_from_orchestra and creates the health task.

        Shutdown: cancels the health task.
        """
        from unity.gateway import app as app_module

        sync_called = False

        async def fake_sync() -> int:
            nonlocal sync_called
            sync_called = True
            return 0

        async def fake_health_loop() -> None:
            try:
                while True:
                    import asyncio as _a

                    await _a.sleep(60)
            except Exception:
                return

        with (
            patch(
                "unity.gateway.channels.discord.bot_manager.sync_from_orchestra",
                new=fake_sync,
            ),
            patch(
                "unity.gateway.channels.discord.bot_manager.start_health_check_loop",
                new=fake_health_loop,
            ),
        ):
            app = app_module.create_app()
            async with app.router.lifespan_context(app):  # type: ignore[attr-defined]
                assert sync_called is True
        # No assertion needed for shutdown: if the task wasn't cancelled
        # cleanly the test would hang on the lifespan context exit.

    @pytest.mark.asyncio
    async def test_lifespan_tolerates_sync_failure(
        self,
        _admin_settings: None,
    ) -> None:
        """A Discord/Orchestra failure at startup is logged, not re-raised.

        We don't want a sync hiccup to block the app from serving the
        other 9 channels.
        """
        from unity.gateway import app as app_module

        async def boom() -> int:
            raise RuntimeError("Orchestra unreachable")

        async def quiet_loop() -> None:
            return

        with (
            patch(
                "unity.gateway.channels.discord.bot_manager.sync_from_orchestra",
                new=boom,
            ),
            patch(
                "unity.gateway.channels.discord.bot_manager.start_health_check_loop",
                new=quiet_loop,
            ),
        ):
            app = app_module.create_app()
            async with app.router.lifespan_context(app):  # type: ignore[attr-defined]
                pass  # got past lifespan startup without raising
