"""Behavioural tests for ``unify.gateway.channels.whatsapp``.

No existing tests in ``communication/tests/whatsapp/`` to port (this
channel was tested through integration only); all greenfield in the
class-grouped Phase B.1 style.
"""

from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from unify.gateway.channels.whatsapp import auth_router, unauth_router
from unify.gateway.channels.whatsapp.views import render_greeting_template_text

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _wa_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TWILIO_WA_ACCOUNT_SID", "ACtestwasid")
    monkeypatch.setenv("TWILIO_WA_AUTH_TOKEN", "test_wa_token")
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    monkeypatch.setenv("LIVEKIT_API_KEY", "test_lk_key")
    monkeypatch.setenv("LIVEKIT_API_SECRET", "test_lk_secret")
    monkeypatch.setenv("LIVEKIT_SIP_URI", "test.sip.livekit.cloud")


@pytest.fixture
def _settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin SETTINGS for URL-shape assertions; use a SimpleNamespace stub.

    The real SETTINGS exposes ORCHESTRA_ADMIN_KEY as a SecretStr
    property; we wrap our stub value in the same shape so call sites
    that do `.get_secret_value()` keep working.
    """
    from unify.gateway.channels.whatsapp import views as wa_views

    stub_secret = SimpleNamespace(get_secret_value=lambda: "test-admin-key")
    monkeypatch.setattr(
        wa_views,
        "SETTINGS",
        SimpleNamespace(
            ORCHESTRA_URL="https://orchestra.example.com/v0",
            ORCHESTRA_ADMIN_KEY=stub_secret,
            conversation=SimpleNamespace(
                COMMS_URL="https://comms.example.com",
                ADAPTERS_URL="https://adapters.example.com",
            ),
        ),
    )


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI()
    app.include_router(auth_router, prefix="/whatsapp")
    app.include_router(unauth_router, prefix="/whatsapp")
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _async_httpx_response(
    *,
    status_code: int = 200,
    json_body: dict | None = None,
    text_body: str = "",
) -> MagicMock:
    """Build a MagicMock standing in for an httpx.Response."""
    resp = MagicMock(status_code=status_code, text=text_body)
    resp.json.return_value = json_body or {}
    return resp


def _mock_invite_message_delivery(
    twilio_client: MagicMock,
    *,
    error_code: int | None = None,
    status: str = "delivered",
) -> None:
    created_msg = MagicMock(sid="MM_invite_test")
    fetched_msg = MagicMock(error_code=error_code, status=status)
    twilio_client.messages.create.return_value = created_msg
    twilio_client.messages.return_value.fetch.return_value = fetched_msg


def test_send_closed_window_returns_template_delivered_body(client: TestClient):
    twilio_client = MagicMock()
    twilio_client.messages.create.return_value = MagicMock(sid="SM_template")

    with (
        patch(
            "unify.gateway.channels.whatsapp.views._resolve_route",
            new=AsyncMock(
                return_value={"pool_number": "+15550000001", "window_open": False},
            ),
        ),
        patch(
            "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
            return_value=twilio_client,
        ),
    ):
        response = client.post(
            "/whatsapp/send",
            json={
                "to": "+4915237826557",
                "body": "The clue is Blade Runner.",
                "assistant_id": 110,
                "user_name": "Daniel",
                "agent_name": "T-W1N",
            },
        )

    assert response.status_code == 200
    assert response.json() == {
        "success": True,
        "method": "template",
        "delivered_body": render_greeting_template_text("Daniel", "T-W1N"),
    }
    create_kwargs = twilio_client.messages.create.call_args.kwargs
    assert "body" not in create_kwargs
    assert create_kwargs["content_sid"]


def test_window_endpoint_reports_open(client: TestClient):
    with patch(
        "unify.gateway.channels.whatsapp.views._resolve_route",
        new=AsyncMock(
            return_value={"pool_number": "+15550000001", "window_open": True},
        ),
    ):
        response = client.get(
            "/whatsapp/window",
            params={"to": "+4915237826557", "assistant_id": 110},
        )

    assert response.status_code == 200
    assert response.json() == {"window_open": True}


def test_window_endpoint_reports_closed(client: TestClient):
    with patch(
        "unify.gateway.channels.whatsapp.views._resolve_route",
        new=AsyncMock(
            return_value={"pool_number": "+15550000001", "window_open": False},
        ),
    ):
        response = client.get(
            "/whatsapp/window",
            params={"to": "+4915237826557", "assistant_id": 110},
        )

    assert response.status_code == 200
    assert response.json() == {"window_open": False}


def test_send_open_window_returns_freeform_delivered_body(client: TestClient):
    twilio_client = MagicMock()
    twilio_client.messages.create.return_value = MagicMock(sid="SM_freeform")

    with (
        patch(
            "unify.gateway.channels.whatsapp.views._resolve_route",
            new=AsyncMock(
                return_value={"pool_number": "+15550000001", "window_open": True},
            ),
        ),
        patch(
            "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
            return_value=twilio_client,
        ),
    ):
        response = client.post(
            "/whatsapp/send",
            json={
                "to": "+4915237826557",
                "body": "The clue is Blade Runner.",
                "assistant_id": 110,
                "user_name": "Daniel",
                "agent_name": "T-W1N",
            },
        )

    assert response.status_code == 200
    assert response.json() == {
        "success": True,
        "method": "freeform",
        "delivered_body": "The clue is Blade Runner.",
    }
    assert (
        twilio_client.messages.create.call_args.kwargs["body"]
        == "The clue is Blade Runner."
    )


def _async_client_returning(response_mock: MagicMock) -> MagicMock:
    """Build an AsyncMock context-manager for httpx.AsyncClient.

    Every HTTP method on the returned client returns ``response_mock``.
    """
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.post.return_value = response_mock
    client.get.return_value = response_mock
    client.delete.return_value = response_mock
    return client


# ---------------------------------------------------------------------------
# Router contract
# ---------------------------------------------------------------------------


def test_auth_router_exposes_expected_paths() -> None:
    paths = sorted(
        (r.path, sorted(r.methods)) for r in auth_router.routes  # type: ignore[attr-defined]
    )
    assert paths == [
        ("/assign", ["POST"]),
        ("/create", ["POST"]),
        ("/delete", ["DELETE"]),
        ("/notify", ["POST"]),
        ("/send", ["POST"]),
        ("/send-call", ["POST"]),
        ("/window", ["GET"]),
    ]


def test_unauth_router_exposes_expected_paths() -> None:
    paths = sorted(
        (r.path, sorted(r.methods)) for r in unauth_router.routes  # type: ignore[attr-defined]
    )
    assert paths == [
        ("/status", ["POST"]),
    ]


def test_routers_importable_from_package_root() -> None:
    from unify.gateway.channels.whatsapp import auth_router as a, unauth_router as u

    assert a is auth_router
    assert u is unauth_router


# ---------------------------------------------------------------------------
# POST /status (unauth)
# ---------------------------------------------------------------------------


class TestStatus:
    def test_returns_status_payload(self, client: TestClient) -> None:
        resp = client.post(
            "/whatsapp/status",
            data={
                "MessageStatus": "delivered",
                "To": "whatsapp:+15555550000",
                "From": "whatsapp:+15555550100",
                "MessageSid": "SM_test_sid",
            },
        )
        assert resp.status_code == 200
        assert resp.json() == {"status": True, "message_status": "delivered"}

    def test_forwards_to_orchestra_when_callback_id_present(
        self,
        client: TestClient,
        _settings: None,
    ) -> None:
        """When callback_id is set, the channel POSTs to Orchestra's notification-status."""
        orchestra_post = AsyncMock()
        orchestra_post.return_value = _async_httpx_response(status_code=200)
        client_mock = AsyncMock()
        client_mock.__aenter__.return_value = client_mock
        client_mock.post = orchestra_post

        with patch(
            "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
            return_value=client_mock,
        ):
            resp = client.post(
                "/whatsapp/status?callback_id=cb-123",
                data={
                    "MessageStatus": "delivered",
                    "To": "whatsapp:+15555550000",
                    "From": "whatsapp:+15555550100",
                    "MessageSid": "SM_test",
                },
            )
        assert resp.status_code == 200
        orchestra_post.assert_awaited_once()
        call = orchestra_post.await_args
        assert call.args[0].endswith("/admin/whatsapp/notification-status")
        assert call.kwargs["json"]["callback_id"] == "cb-123"
        # 'whatsapp:' prefix stripped from to
        assert call.kwargs["json"]["to"] == "+15555550000"
        assert call.kwargs["headers"]["Authorization"] == "Bearer test-admin-key"

    def test_no_forward_when_callback_id_absent(
        self,
        client: TestClient,
        _settings: None,
    ) -> None:
        """No callback_id -> no Orchestra POST (forward is opt-in)."""
        with patch(
            "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
        ) as MockClient:
            resp = client.post(
                "/whatsapp/status",
                data={
                    "MessageStatus": "delivered",
                    "To": "whatsapp:+15555550000",
                    "From": "whatsapp:+15555550100",
                },
            )
        assert resp.status_code == 200
        MockClient.assert_not_called()

    def test_failed_invite_37010_recovers_with_direct_call(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        twilio_client = MagicMock()
        user_call = MagicMock(sid="CA_user")
        sip_call = MagicMock(sid="CA_sip")
        twilio_client.calls.create.side_effect = [sip_call, user_call]

        perm_resp = _async_httpx_response(
            status_code=200,
            json_body={"permitted": False, "status": "pending"},
        )
        resolve_resp = _async_httpx_response(
            status_code=200,
            json_body={"assistant_id": 42},
        )
        accepted_resp = _async_httpx_response(
            status_code=200,
            json_body={
                "status": "accepted",
                "permitted": True,
                "expires_at": "2999-01-01T00:00:00+00:00",
            },
        )
        session_resp = _async_httpx_response(status_code=200, json_body={"id": 123})
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.get.side_effect = [perm_resp, resolve_resp]
        httpx_client.post.side_effect = [accepted_resp, session_resp]
        httpx_client.delete.return_value = _async_httpx_response(status_code=404)

        wake_mock = AsyncMock()

        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=httpx_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.ensure_phone_dispatch_rule",
                new=AsyncMock(),
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.make_sip_uri",
                return_value="sip:+15555550111@test.sip.livekit.cloud",
            ),
            patch(
                "unify.gateway.channels.whatsapp.views._wake_and_publish_outbound_whatsapp_call_sent",
                wake_mock,
            ),
        ):
            resp = client.post(
                "/whatsapp/status",
                data={
                    "MessageStatus": "failed",
                    "To": "whatsapp:+15555550000",
                    "From": "whatsapp:+15555550111",
                    "MessageSid": "MM_failed_invite",
                    "ErrorCode": "37010",
                },
            )

        assert resp.status_code == 200
        assert twilio_client.calls.create.call_count == 2
        wake_mock.assert_not_awaited()
        accepted_call = httpx_client.post.await_args_list[0].kwargs["json"]
        assert accepted_call["status"] == "accepted"
        assert accepted_call["source"] == "twilio_permanent_permission"

    def test_failed_invite_37010_wakes_runtime_when_gateway_context_present(
        self,
        app: FastAPI,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        twilio_client = MagicMock()
        user_call = MagicMock(sid="CA_user")
        sip_call = MagicMock(sid="CA_sip")
        twilio_client.calls.create.side_effect = [sip_call, user_call]

        perm_resp = _async_httpx_response(
            status_code=200,
            json_body={"permitted": False, "status": "pending"},
        )
        resolve_resp = _async_httpx_response(
            status_code=200,
            json_body={"assistant_id": 42},
        )
        accepted_resp = _async_httpx_response(
            status_code=200,
            json_body={
                "status": "accepted",
                "permitted": True,
                "expires_at": "2999-01-01T00:00:00+00:00",
            },
        )
        session_resp = _async_httpx_response(status_code=200, json_body={"id": 123})
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.get.side_effect = [perm_resp, resolve_resp]
        httpx_client.post.side_effect = [accepted_resp, session_resp]
        httpx_client.delete.return_value = _async_httpx_response(status_code=404)

        wake_mock = AsyncMock()
        app.state.gateway_context = object()

        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=httpx_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.ensure_phone_dispatch_rule",
                new=AsyncMock(),
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.make_sip_uri",
                return_value="sip:+15555550111@test.sip.livekit.cloud",
            ),
            patch(
                "unify.gateway.channels.whatsapp.views._wake_and_publish_outbound_whatsapp_call_sent",
                wake_mock,
            ),
        ):
            resp = client.post(
                "/whatsapp/status",
                data={
                    "MessageStatus": "failed",
                    "To": "whatsapp:+15555550000",
                    "From": "whatsapp:+15555550111",
                    "MessageSid": "MM_failed_invite",
                    "ErrorCode": "37010",
                },
            )

        assert resp.status_code == 200
        wake_mock.assert_awaited_once()
        wake_kwargs = wake_mock.await_args.kwargs
        assert wake_kwargs["assistant_id"] == 42
        assert wake_kwargs["contact_number"] == "+15555550000"
        assert wake_kwargs["room_name"] == "unity_42_whatsapp_call"

    def test_delayed_37010_on_invite_poll_places_direct_call(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        twilio_client = MagicMock()
        created_msg = MagicMock(sid="MM_invite_test")
        twilio_client.messages.create.return_value = created_msg

        queued_msg = MagicMock(error_code=None, status="queued")
        failed_msg = MagicMock(error_code=37010, status="failed")
        twilio_client.messages.return_value.fetch.side_effect = [
            queued_msg,
            failed_msg,
        ]

        user_call = MagicMock(sid="CA_user")
        sip_call = MagicMock(sid="CA_sip")
        twilio_client.calls.create.side_effect = [sip_call, user_call]

        route_resp = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": True},
        )
        perm_resp = _async_httpx_response(
            status_code=200,
            json_body={"permitted": False, "status": "unknown"},
        )
        accepted_resp = _async_httpx_response(
            status_code=200,
            json_body={
                "status": "accepted",
                "permitted": True,
                "expires_at": "2999-01-01T00:00:00+00:00",
            },
        )
        session_resp = _async_httpx_response(status_code=200, json_body={"id": 123})
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post.side_effect = [route_resp, accepted_resp, session_resp]
        httpx_client.get.return_value = perm_resp
        httpx_client.delete.return_value = _async_httpx_response(status_code=404)

        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=httpx_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.ensure_phone_dispatch_rule",
                new=AsyncMock(),
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.make_sip_uri",
                return_value="sip:+15555550111@test.sip.livekit.cloud",
            ),
        ):
            resp = client.post(
                "/whatsapp/send-call",
                json={
                    "to": "+15555550000",
                    "assistant_id": 42,
                    "room_name": "unity_42_whatsapp_call",
                },
            )

        assert resp.status_code == 200
        assert resp.json()["method"] == "direct"
        assert twilio_client.calls.create.call_count == 2


# ---------------------------------------------------------------------------
# POST /send
# ---------------------------------------------------------------------------


class TestSend:
    def test_freeform_send_when_window_open(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        """Open 24-hour window -> direct messages.create with body."""
        twilio_client = MagicMock()
        route_response = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": True},
        )
        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=_async_client_returning(route_response),
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
        ):
            resp = client.post(
                "/whatsapp/send",
                json={
                    "to": "+15555550000",
                    "body": "hello from test",
                    "assistant_id": 42,
                },
            )

        assert resp.status_code == 200
        assert resp.json() == {
            "success": True,
            "method": "freeform",
            "delivered_body": "hello from test",
        }
        kwargs = twilio_client.messages.create.call_args.kwargs
        assert kwargs["to"] == "whatsapp:+15555550000"
        assert kwargs["from_"] == "whatsapp:+15555550111"
        assert kwargs["body"] == "hello from test"
        assert "content_sid" not in kwargs

    def test_template_send_when_window_closed(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        """Closed window -> GREETING template; freeform body is dropped."""
        from unify.gateway.channels.whatsapp.views import GREETING_TEMPLATE_SID

        twilio_client = MagicMock()
        route_response = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": False},
        )
        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=_async_client_returning(route_response),
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
        ):
            resp = client.post(
                "/whatsapp/send",
                json={
                    "to": "+15555550000",
                    "body": "ignored when window is closed",
                    "assistant_id": 42,
                    "user_name": "Alice",
                    "agent_name": "Unity",
                },
            )

        assert resp.status_code == 200
        assert resp.json() == {
            "success": True,
            "method": "template",
            "delivered_body": render_greeting_template_text("Alice", "Unity"),
        }
        kwargs = twilio_client.messages.create.call_args.kwargs
        assert kwargs["content_sid"] == GREETING_TEMPLATE_SID
        assert "body" not in kwargs
        variables = json.loads(kwargs["content_variables"])
        assert variables == {"user_name": "Alice", "agent_name": "Unity"}

    def test_freeform_media_url_passes_through_for_http_urls(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        """HTTP(S) media URLs go straight to Twilio without GCS signing."""
        twilio_client = MagicMock()
        route_response = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": True},
        )
        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=_async_client_returning(route_response),
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
        ):
            client.post(
                "/whatsapp/send",
                json={
                    "to": "+15555550000",
                    "body": "see attached",
                    "assistant_id": 42,
                    "media_url": "https://example.com/image.jpg",
                },
            )

        kwargs = twilio_client.messages.create.call_args.kwargs
        assert kwargs["media_url"] == ["https://example.com/image.jpg"]

    def test_route_failure_bubbles_up_as_orchestra_status(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        """Orchestra route-resolution failures surface as the same status code."""
        route_response = _async_httpx_response(
            status_code=503,
            json_body={"detail": "Orchestra unavailable"},
        )
        with patch(
            "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
            return_value=_async_client_returning(route_response),
        ):
            resp = client.post(
                "/whatsapp/send",
                json={
                    "to": "+15555550000",
                    "body": "x",
                    "assistant_id": 42,
                },
            )
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# POST /send-call
# ---------------------------------------------------------------------------


class TestSendCall:
    def test_direct_call_when_permitted(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        """Permitted -> two Twilio calls (user + SIP) + conference name returned."""
        twilio_client = MagicMock()
        user_call = MagicMock(sid="CA_user")
        sip_call = MagicMock(sid="CA_sip")
        twilio_client.calls.create.side_effect = [sip_call, user_call]

        # Route + call-permission responses
        route_resp = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": True},
        )
        perm_resp = _async_httpx_response(
            status_code=200,
            json_body={"permitted": True},
        )
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        session_resp = _async_httpx_response(
            status_code=200,
            json_body={"id": 123},
        )
        httpx_client.post.side_effect = [route_resp, session_resp]
        httpx_client.get.return_value = perm_resp

        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=httpx_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.ensure_phone_dispatch_rule",
                new=AsyncMock(),
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.make_sip_uri",
                return_value="sip:+15555550111@test.sip.livekit.cloud",
            ),
        ):
            resp = client.post(
                "/whatsapp/send-call",
                json={
                    "to": "+15555550000",
                    "assistant_id": 42,
                    "room_name": "unity_42_whatsapp_call",
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["method"] == "direct"
        assert body["pool_number"] == "+15555550111"
        assert body["conference_name"].startswith("Unity_WA_15555550111_")
        assert twilio_client.calls.create.call_count == 2
        assert httpx_client.post.await_args_list[1].kwargs["json"][
            "provider_call_sid"
        ] == ("CA_user")
        twiml = twilio_client.calls.create.call_args_list[1].kwargs["twiml"]
        # Agent-initiated call: no conference wait audio on either leg. The
        # first participant into a Twilio conference hears the wait audio
        # until the second joins, so a ring-tone here meant the callee could
        # answer and immediately hear ringing.
        assert 'waitUrl=""' in twiml
        assert "ring-tone" not in twiml
        # No join beep either: the default conference beep plays an artificial
        # "call answered" tone at the callee the moment they pick up.
        assert 'beep="false"' in twiml
        # Hosted mode: the answered-status callback points at the public adapters.
        assert twilio_client.calls.create.call_args_list[1].kwargs[
            "status_callback"
        ] == ("https://adapters.example.com/twilio/whatsapp-call-status")

    def test_invite_template_when_not_permitted(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        """Not permitted -> VOICE_CALL_REQUEST template message."""
        from unify.gateway.channels.whatsapp.views import (
            VOICE_CALL_REQUEST_TEMPLATE_SID,
        )

        twilio_client = MagicMock()
        _mock_invite_message_delivery(twilio_client)

        route_resp = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": True},
        )
        perm_resp = _async_httpx_response(
            status_code=200,
            json_body={"permitted": False},
        )
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post.return_value = route_resp
        httpx_client.get.return_value = perm_resp

        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=httpx_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
        ):
            resp = client.post(
                "/whatsapp/send-call",
                json={
                    "to": "+15555550000",
                    "assistant_id": 42,
                    "room_name": "unity_42_wa_call",
                },
            )

        assert resp.status_code == 200
        assert resp.json() == {
            "success": True,
            "method": "invite",
            "pool_number": "+15555550111",
        }
        kwargs = twilio_client.messages.create.call_args.kwargs
        assert kwargs["content_sid"] == VOICE_CALL_REQUEST_TEMPLATE_SID

    def test_permanent_permission_on_invite_places_direct_call(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        twilio_client = MagicMock()
        _mock_invite_message_delivery(
            twilio_client,
            error_code=37010,
            status="failed",
        )
        user_call = MagicMock(sid="CA_user")
        sip_call = MagicMock(sid="CA_sip")
        twilio_client.calls.create.side_effect = [sip_call, user_call]

        route_resp = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": True},
        )
        perm_resp = _async_httpx_response(
            status_code=200,
            json_body={"permitted": False, "status": "unknown"},
        )
        accepted_resp = _async_httpx_response(
            status_code=200,
            json_body={
                "status": "accepted",
                "permitted": True,
                "expires_at": "2999-01-01T00:00:00+00:00",
            },
        )
        session_resp = _async_httpx_response(status_code=200, json_body={"id": 123})
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post.side_effect = [route_resp, accepted_resp, session_resp]
        httpx_client.get.return_value = perm_resp
        httpx_client.delete.return_value = _async_httpx_response(status_code=404)

        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=httpx_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.ensure_phone_dispatch_rule",
                new=AsyncMock(),
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.make_sip_uri",
                return_value="sip:+15555550111@test.sip.livekit.cloud",
            ),
        ):
            resp = client.post(
                "/whatsapp/send-call",
                json={
                    "to": "+15555550000",
                    "assistant_id": 42,
                    "room_name": "unity_42_whatsapp_call",
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["method"] == "direct"
        assert twilio_client.calls.create.call_count == 2
        accepted_call = httpx_client.post.await_args_list[1].kwargs["json"]
        assert accepted_call["status"] == "accepted"
        assert accepted_call["source"] == "twilio_permanent_permission"

    def test_recent_pending_invite_is_not_resent(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        twilio_client = MagicMock()
        route_resp = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": True},
        )
        perm_resp = _async_httpx_response(
            status_code=200,
            json_body={
                "permitted": False,
                "status": "pending",
                "requested_at": datetime.now().isoformat(),
            },
        )
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post.return_value = route_resp
        httpx_client.get.return_value = perm_resp

        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=httpx_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
        ):
            resp = client.post(
                "/whatsapp/send-call",
                json={
                    "to": "+15555550000",
                    "assistant_id": 42,
                    "room_name": "unity_42_wa_call",
                },
            )

        assert resp.status_code == 200
        assert resp.json() == {
            "success": True,
            "method": "invite_pending",
            "pool_number": "+15555550111",
        }
        twilio_client.messages.create.assert_not_called()

    def test_rejected_permission_does_not_send_invite_or_call(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        twilio_client = MagicMock()
        route_resp = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": True},
        )
        perm_resp = _async_httpx_response(
            status_code=200,
            json_body={"permitted": False, "status": "rejected"},
        )
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post.return_value = route_resp
        httpx_client.get.return_value = perm_resp

        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=httpx_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
        ):
            resp = client.post(
                "/whatsapp/send-call",
                json={
                    "to": "+15555550000",
                    "assistant_id": 42,
                    "room_name": "unity_42_wa_call",
                },
            )

        assert resp.status_code == 200
        assert resp.json() == {
            "success": True,
            "method": "rejected",
            "pool_number": "+15555550111",
        }
        twilio_client.messages.create.assert_not_called()
        twilio_client.calls.create.assert_not_called()

    def test_unknown_interaction_requires_reconciliation(
        self,
        client,
        _wa_credentials,
        _settings,
    ):
        twilio_client = MagicMock()
        route_resp = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": True},
        )
        perm_resp = _async_httpx_response(
            status_code=200,
            json_body={"permitted": False, "status": "unknown_interaction"},
        )
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post.return_value = route_resp
        httpx_client.get.return_value = perm_resp

        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=httpx_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
        ):
            resp = client.post(
                "/whatsapp/send-call",
                json={
                    "to": "+15555550000",
                    "assistant_id": 42,
                    "room_name": "unity_42_wa_call",
                },
            )

        assert resp.status_code == 200
        assert resp.json() == {
            "success": True,
            "method": "needs_reconciliation",
            "pool_number": "+15555550111",
        }
        twilio_client.messages.create.assert_not_called()
        twilio_client.calls.create.assert_not_called()

    def test_pending_probe_disabled_outside_local(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        twilio_client = MagicMock()
        route_resp = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": True},
        )
        perm_resp = _async_httpx_response(
            status_code=200,
            json_body={
                "permitted": False,
                "status": "pending",
                "requested_at": datetime.now().isoformat(),
            },
        )
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post.return_value = route_resp
        httpx_client.get.return_value = perm_resp

        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=httpx_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
        ):
            resp = client.post(
                "/whatsapp/send-call",
                json={
                    "to": "+15555550000",
                    "assistant_id": 42,
                    "room_name": "unity_42_wa_call",
                    "allow_permission_probe": True,
                },
            )

        assert resp.status_code == 200
        assert resp.json() == {
            "success": True,
            "method": "invite_pending",
            "pool_number": "+15555550111",
        }
        twilio_client.messages.create.assert_not_called()
        twilio_client.calls.create.assert_not_called()

    def test_pending_local_probe_success_marks_accepted(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        monkeypatch.setenv("SELF_HOST", "1")
        monkeypatch.setenv(
            "COMMS_BRIDGE_PERMISSION_CACHE",
            str(tmp_path / "wa-permissions.json"),
        )
        twilio_client = MagicMock()
        twilio_client.calls.create.side_effect = [
            MagicMock(sid="CA_sip"),
            MagicMock(sid="CA_user"),
        ]
        route_resp = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": True},
        )
        perm_resp = _async_httpx_response(
            status_code=200,
            json_body={
                "permitted": False,
                "status": "pending",
                "requested_at": datetime.now().isoformat(),
            },
        )
        intent_resp = _async_httpx_response(
            status_code=200,
            json_body={"context": "Call briefing"},
        )
        accepted_resp = _async_httpx_response(
            status_code=200,
            json_body={
                "status": "accepted",
                "expires_at": "2999-01-01T00:00:00+00:00",
            },
        )
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        session_resp = _async_httpx_response(status_code=200, json_body={"id": 123})
        httpx_client.post.side_effect = [route_resp, session_resp, accepted_resp]
        httpx_client.get.side_effect = [perm_resp, intent_resp]

        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=httpx_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.ensure_phone_dispatch_rule",
                new=AsyncMock(),
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.make_sip_uri",
                return_value="sip:+15555550111@test.sip.livekit.cloud",
            ),
        ):
            resp = client.post(
                "/whatsapp/send-call",
                json={
                    "to": "+15555550000",
                    "assistant_id": 42,
                    "room_name": "unity_42_wa_call",
                    "allow_permission_probe": True,
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["method"] == "direct"
        assert body["permission_probe"] is True
        assert twilio_client.calls.create.call_count == 2
        assert httpx_client.post.await_args_list[2].kwargs["json"] == {
            "pool_number": "+15555550111",
            "contact_number": "+15555550000",
            "status": "accepted",
            "source": "local_permission_probe",
        }

    def test_clean_reset_local_probe_creates_pending_and_calls_direct(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        monkeypatch.setenv("SELF_HOST", "1")
        monkeypatch.setenv(
            "COMMS_BRIDGE_PERMISSION_CACHE",
            str(tmp_path / "wa-permissions.json"),
        )
        twilio_client = MagicMock()
        twilio_client.calls.create.side_effect = [
            MagicMock(sid="CA_sip"),
            MagicMock(sid="CA_user"),
        ]
        route_resp = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": True},
        )
        unknown_perm_resp = _async_httpx_response(
            status_code=200,
            json_body={"permitted": False, "status": "unknown"},
        )
        pending_resp = _async_httpx_response(
            status_code=200,
            json_body={
                "permitted": False,
                "status": "pending",
                "requested_at": datetime.now().isoformat(),
            },
        )
        intent_resp = _async_httpx_response(status_code=200, json_body={"id": 55})
        call_session_resp = _async_httpx_response(
            status_code=200,
            json_body={"id": 123},
        )
        accepted_resp = _async_httpx_response(
            status_code=200,
            json_body={
                "status": "accepted",
                "expires_at": "2999-01-01T00:00:00+00:00",
            },
        )
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post.side_effect = [
            route_resp,
            pending_resp,
            intent_resp,
            call_session_resp,
            accepted_resp,
        ]
        httpx_client.get.side_effect = [unknown_perm_resp, pending_resp, intent_resp]

        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=httpx_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.ensure_phone_dispatch_rule",
                new=AsyncMock(),
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.make_sip_uri",
                return_value="sip:+15555550111@test.sip.livekit.cloud",
            ),
        ):
            resp = client.post(
                "/whatsapp/send-call",
                json={
                    "to": "+15555550000",
                    "assistant_id": 42,
                    "room_name": "unity_42_wa_call",
                    "allow_permission_probe": True,
                    "pending_call_opener": "Call briefing",
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["method"] == "direct"
        assert body["permission_probe"] is True
        assert (
            httpx_client.post.await_args_list[1].kwargs["json"]["status"] == "pending"
        )
        assert httpx_client.post.await_args_list[2].kwargs["json"]["context"] == (
            "Call briefing"
        )
        assert httpx_client.post.await_args_list[4].kwargs["json"] == {
            "pool_number": "+15555550111",
            "contact_number": "+15555550000",
            "status": "accepted",
            "source": "local_permission_probe",
        }

    def test_pending_local_probe_permission_failure_does_not_mark_accepted(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("SELF_HOST", "1")
        twilio_client = MagicMock()
        twilio_client.calls.create.side_effect = RuntimeError(
            "permission not approved",
        )
        route_resp = _async_httpx_response(
            status_code=200,
            json_body={"pool_number": "+15555550111", "window_open": True},
        )
        perm_resp = _async_httpx_response(
            status_code=200,
            json_body={
                "permitted": False,
                "status": "pending",
                "requested_at": datetime.now().isoformat(),
            },
        )
        intent_resp = _async_httpx_response(
            status_code=200,
            json_body={"context": "Call briefing"},
        )
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post.return_value = route_resp
        httpx_client.get.side_effect = [perm_resp, intent_resp]

        with (
            patch(
                "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
                return_value=httpx_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
                return_value=twilio_client,
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.ensure_phone_dispatch_rule",
                new=AsyncMock(),
            ),
            patch(
                "unify.gateway.channels.whatsapp.views.make_sip_uri",
                return_value="sip:+15555550111@test.sip.livekit.cloud",
            ),
        ):
            resp = client.post(
                "/whatsapp/send-call",
                json={
                    "to": "+15555550000",
                    "assistant_id": 42,
                    "room_name": "unity_42_wa_call",
                    "allow_permission_probe": True,
                },
            )

        assert resp.status_code == 200
        assert resp.json() == {
            "success": True,
            "method": "needs_permission",
            "pool_number": "+15555550111",
        }
        assert len(httpx_client.post.await_args_list) == 1


# ---------------------------------------------------------------------------
# POST /notify
# ---------------------------------------------------------------------------


class TestNotify:
    def test_sends_number_change_template_to_each_recipient(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        from unify.gateway.channels.whatsapp.views import NUMBER_CHANGE_TEMPLATE_SID

        twilio_client = MagicMock()
        m1 = MagicMock(sid="SM_one")
        m2 = MagicMock(sid="SM_two")
        twilio_client.messages.create.side_effect = [m1, m2]

        with patch(
            "unify.gateway.channels.whatsapp.views.build_twilio_wa_client",
            return_value=twilio_client,
        ):
            resp = client.post(
                "/whatsapp/notify",
                json={
                    "from_number": "+15555550111",
                    "old_contact": "+15555550111",
                    "new_contact": "+15555550222",
                    "recipients": [
                        {
                            "to": "+15555550000",
                            "user_name": "Alice",
                            "agent_name": "Unity",
                        },
                        {
                            "to": "+15555550001",
                            "user_name": "Bob",
                            "agent_name": "Unity",
                        },
                    ],
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["results"]["+15555550000"] == {"sid": "SM_one", "status": "sent"}
        assert body["results"]["+15555550001"] == {"sid": "SM_two", "status": "sent"}
        # Both calls used the NUMBER_CHANGE template
        for call in twilio_client.messages.create.call_args_list:
            assert call.kwargs["content_sid"] == NUMBER_CHANGE_TEMPLATE_SID


# ---------------------------------------------------------------------------
# DELETE /delete + POST /assign
# ---------------------------------------------------------------------------


class TestDelete:
    def test_calls_senders_api_with_sid(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        delete_resp = _async_httpx_response(status_code=204)
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.delete.return_value = delete_resp

        with patch(
            "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
            return_value=httpx_client,
        ):
            resp = client.request(
                "DELETE",
                "/whatsapp/delete",
                json={"sid": "XE_test_sender"},
            )

        assert resp.status_code == 200
        assert resp.json() == {"success": True}
        call = httpx_client.delete.await_args
        assert "XE_test_sender" in call.args[0]

    def test_propagates_twilio_failure(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        delete_resp = _async_httpx_response(
            status_code=500,
            text_body="Sender service unavailable",
        )
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.delete.return_value = delete_resp

        with patch(
            "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
            return_value=httpx_client,
        ):
            resp = client.request(
                "DELETE",
                "/whatsapp/delete",
                json={"sid": "XE_test"},
            )

        assert resp.status_code == 500


class TestAssign:
    def test_forwards_to_orchestra(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        assign_resp = _async_httpx_response(
            status_code=200,
            json_body={"assigned": True, "pool_number": "+15555550111"},
        )
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post.return_value = assign_resp

        with patch(
            "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
            return_value=httpx_client,
        ):
            resp = client.post("/whatsapp/assign", json={"assistant_id": 42})

        assert resp.status_code == 200
        assert resp.json() == {"assigned": True, "pool_number": "+15555550111"}
        call = httpx_client.post.await_args
        assert call.args[0].endswith("/admin/whatsapp/assign")
        assert call.kwargs["json"] == {"assistant_id": 42}

    def test_propagates_orchestra_error(
        self,
        client: TestClient,
        _wa_credentials: None,
        _settings: None,
    ) -> None:
        assign_resp = _async_httpx_response(
            status_code=409,
            json_body={"detail": "No pool numbers available"},
        )
        httpx_client = AsyncMock()
        httpx_client.__aenter__.return_value = httpx_client
        httpx_client.post.return_value = assign_resp

        with patch(
            "unify.gateway.channels.whatsapp.views.httpx.AsyncClient",
            return_value=httpx_client,
        ):
            resp = client.post("/whatsapp/assign", json={"assistant_id": 42})

        assert resp.status_code == 409
        assert resp.json()["detail"] == "No pool numbers available"


# ---------------------------------------------------------------------------
# Voice-app SID resolution
# ---------------------------------------------------------------------------


class TestVoiceAppSid:
    def test_returns_staging_sid_when_deploy_env_staging(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unify.gateway.channels.whatsapp.views import _whatsapp_voice_app_sid
        from unify.gateway.credentials import EnvCredentialStore

        monkeypatch.setenv("DEPLOY_ENV", "staging")
        sid = _whatsapp_voice_app_sid(EnvCredentialStore())
        assert sid == "APbf0903608f1a02e93bebcc90e2ea17db"

    def test_returns_production_sid_otherwise(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unify.gateway.channels.whatsapp.views import _whatsapp_voice_app_sid
        from unify.gateway.credentials import EnvCredentialStore

        monkeypatch.delenv("DEPLOY_ENV", raising=False)
        sid = _whatsapp_voice_app_sid(EnvCredentialStore())
        assert sid == "AP5e48f55135a987a482661a37db8ac68f"
