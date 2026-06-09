from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from unity.gateway.app import create_app
from unity.gateway.context import GatewayContext
from unity.gateway.public_url import StaticPublicUrlProvider
from unity.gateway.runtime import LocalRuntimeActivator
from unity.gateway.scheduler import LocalScheduler

ADMIN_KEY = "test-admin-key"
ADMIN_HEADERS = {"Authorization": f"Bearer {ADMIN_KEY}"}


class FakeCredentials:
    async def get(self, name: str) -> str:
        return f"credential:{name}"

    def get_optional(self, name: str, default: str = "") -> str:
        del name
        return default


class FakeStorage:
    async def write_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ):
        return SimpleNamespace(
            key=f"gs://local-bucket/{key}",
            size_bytes=len(data),
            content_type=content_type,
            metadata={},
        )

    async def read_bytes(self, key: str) -> bytes:
        return key.encode()

    async def signed_url(self, key: str, *, expires_in: int = 3600) -> str:
        del expires_in
        return f"https://signed.local/{key}"

    async def delete(self, key: str) -> None:
        del key


@dataclass
class FakeEnvelopeSink:
    published: list[tuple[str, dict[str, Any], str]] = field(default_factory=list)

    async def publish(
        self,
        assistant_id: str,
        envelope: dict[str, Any],
        *,
        thread: str = "",
    ) -> str:
        self.published.append((assistant_id, envelope, thread))
        return "message-id"


@pytest.fixture
def gateway_context() -> GatewayContext:
    return GatewayContext(
        credentials=FakeCredentials(),
        storage=FakeStorage(),
        envelope_sink=FakeEnvelopeSink(),
        runtime_activator=LocalRuntimeActivator(),
        public_url_provider=StaticPublicUrlProvider(
            comms_base_url="http://gateway.local",
            adapters_base_url="http://gateway.local",
        ),
        scheduler=LocalScheduler(),
    )


@asynccontextmanager
async def _noop_lifespan(app: FastAPI):
    yield


@pytest.fixture
def app(gateway_context: GatewayContext, monkeypatch: pytest.MonkeyPatch) -> FastAPI:
    from unity.gateway.adapters import common
    from unity.gateway.common import auth

    stub_secret = SimpleNamespace(get_secret_value=lambda: ADMIN_KEY)
    settings = SimpleNamespace(
        ORCHESTRA_ADMIN_KEY=stub_secret,
        ORCHESTRA_URL="http://orchestra.local/v0",
        conversation=SimpleNamespace(
            COMMS_URL="http://gateway.local",
            ADAPTERS_URL="http://gateway.local",
        ),
    )
    monkeypatch.setattr(auth, "SETTINGS", settings)

    async def fake_get_assistant(*, assistant_id=None, **kwargs):
        return {
            "assistant_id": assistant_id or "123",
            "boss_contact_id": 456,
            "self_contact_id": 789,
            "assistant_first_name": "Unity",
            "assistant_surname": "Assistant",
            "assistant_email": "unity@example.com",
            "assistant_number": "+15555550123",
            "assistant_whatsapp_number": "",
            "assistant_discord_bot_id": "",
            "assistant_slack_bot_user_id": "",
            "user_first_name": "Owner",
            "user_surname": "User",
            "user_email": "owner@example.com",
            "user_number": "+15555550456",
            "user_whatsapp_number": "",
            "user_discord_id": "",
            "user_slack_user_id": "",
        }

    monkeypatch.setattr(common, "get_assistant", fake_get_assistant)
    gateway_app = create_app(gateway_context=gateway_context)
    gateway_app.router.lifespan_context = _noop_lifespan  # type: ignore[attr-defined]
    return gateway_app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    with TestClient(app) as test_client:
        yield test_client


def test_console_can_read_phone_country_metadata(client: TestClient) -> None:
    response = client.get("/phone/available-countries", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.json()["success"] is True
    assert "US" in response.json()["countries"]


def test_twilio_whatsapp_reject_ambiguous_returns_closed_response(
    client: TestClient,
    gateway_context: GatewayContext,
) -> None:
    with patch(
        "unity.gateway.adapters.twilio.resolve_whatsapp_route",
        new=AsyncMock(return_value={"action": "reject_ambiguous"}),
    ):
        response = client.post(
            "/twilio/whatsapp",
            data={
                "To": "whatsapp:+15550810001",
                "From": "whatsapp:+15550810002",
                "Body": "Hello",
            },
        )

    assert response.status_code == 200
    assert "text/xml" in response.headers["content-type"]
    assert "This number is not accepting new messages." in response.text
    assert gateway_context.envelope_sink.published == []


def test_console_can_create_phone_with_empty_body(client: TestClient) -> None:
    twilio_client = MagicMock()
    number = MagicMock(phone_number="+15555550123")
    incoming = MagicMock(phone_number="+15555550123", sid="PN_created")
    service = MagicMock(friendly_name="Unity")
    twilio_client.available_phone_numbers.return_value.local.list.return_value = [
        number,
    ]
    twilio_client.available_phone_numbers.return_value.mobile.list.return_value = []
    twilio_client.incoming_phone_numbers.create.return_value = incoming
    twilio_client.messaging.v1.services.list.return_value = [service]

    livekit_api = MagicMock()
    livekit_api.sip.create_sip_inbound_trunk = AsyncMock()
    livekit_api.aclose = AsyncMock()

    with (
        patch(
            "unity.gateway.channels.phone.views.build_twilio_client",
            return_value=twilio_client,
        ),
        patch(
            "unity.gateway.channels.phone.views.get_livekit_api",
            return_value=livekit_api,
        ),
    ):
        response = client.post("/phone/create", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.json() == {"success": True, "phoneNumber": "+15555550123"}


def test_console_can_delete_phone_with_snake_case_body(client: TestClient) -> None:
    twilio_client = MagicMock()
    listed = MagicMock(sid="PN_existing")
    twilio_client.incoming_phone_numbers.list.return_value = [listed]

    with (
        patch(
            "unity.gateway.channels.phone.views.build_twilio_client",
            return_value=twilio_client,
        ),
        patch(
            "unity.gateway.channels.phone.views._delete_sip_trunk_for_phone_number",
            new=AsyncMock(return_value=True),
        ),
    ):
        response = client.request(
            "DELETE",
            "/phone/delete",
            headers=ADMIN_HEADERS,
            json={"phone_number": "+15555550123"},
        )

    assert response.status_code == 200
    assert response.json()["success"] is True
    assert response.json()["sid"] == "PN_existing"


def test_console_can_read_social_platform_metadata(client: TestClient) -> None:
    response = client.get("/social/available-platforms", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.json() == {"success": True, "platforms": {"whatsapp": 10.0}}


def test_console_can_send_social_verification_with_snake_case_body(
    client: TestClient,
) -> None:
    twilio_client = MagicMock()
    service = MagicMock(friendly_name="Unity", sid="MG_test")
    twilio_client.messaging.v1.services.list.return_value = [service]

    with patch(
        "unity.gateway.channels.social.views.build_twilio_client",
        return_value=twilio_client,
    ):
        response = client.post(
            "/social/verify",
            headers=ADMIN_HEADERS,
            json={"platform": "phone", "account_identifier": "+15555550123"},
        )

    assert response.status_code == 200
    assert response.json()["verification_code"].isdigit()
    assert "sent_at" in response.json()


def test_console_can_create_whatsapp_sender_with_snake_case_body(
    client: TestClient,
) -> None:
    sender_response = MagicMock(status_code=200, text="")
    sender_response.json.return_value = {"sid": "XE_sender"}
    httpx_client = AsyncMock()
    httpx_client.__aenter__.return_value = httpx_client
    httpx_client.post.return_value = sender_response

    with (
        patch(
            "unity.gateway.channels.whatsapp.views.httpx.AsyncClient",
            return_value=httpx_client,
        ),
        patch(
            "unity.gateway.channels.whatsapp.views._twilio_whatsapp_auth_headers",
            return_value={"Authorization": "Basic test"},
        ),
        patch(
            "unity.gateway.channels.whatsapp.views._attach_voice_app",
            new=AsyncMock(return_value=True),
        ),
    ):
        response = client.post(
            "/whatsapp/create",
            headers=ADMIN_HEADERS,
            json={
                "phone_number": "+15555550123",
                "first_name": "Test",
                "last_name": "User",
            },
        )

    assert response.status_code == 200
    assert response.json()["sid"] == "XE_sender"


def test_console_can_delete_whatsapp_sender(client: TestClient) -> None:
    delete_response = MagicMock(status_code=204, text="")
    httpx_client = AsyncMock()
    httpx_client.__aenter__.return_value = httpx_client
    httpx_client.delete.return_value = delete_response

    with (
        patch(
            "unity.gateway.channels.whatsapp.views.httpx.AsyncClient",
            return_value=httpx_client,
        ),
        patch(
            "unity.gateway.channels.whatsapp.views._twilio_whatsapp_auth_headers",
            return_value={"Authorization": "Basic test"},
        ),
    ):
        response = client.request(
            "DELETE",
            "/whatsapp/delete",
            headers=ADMIN_HEADERS,
            json={"sid": "XE_sender"},
        )

    assert response.status_code == 200
    assert response.json() == {"success": True}


def test_console_attachment_upload_response_has_expected_aliases(
    client: TestClient,
) -> None:
    response = client.post(
        "/unify/attachment",
        headers=ADMIN_HEADERS,
        files={"file": ("hello.txt", b"hello", "text/plain")},
        data={"assistant_id": "123"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["filename"] == "hello.txt"
    assert body["gs_url"].startswith("gs://local-bucket/attachments/123/")
    assert body["signed_url"].startswith("https://signed.local/")
    assert body["content_type"] == "text/plain"
    assert body["size_bytes"] == 5


def test_console_message_dispatch_publishes_runtime_event(
    client: TestClient,
    gateway_context: GatewayContext,
) -> None:
    response = client.post(
        "/unify/message",
        headers=ADMIN_HEADERS,
        json={
            "assistant_id": "123",
            "contact_id": 456,
            "body": "hello",
            "attachments": [],
        },
    )

    assert response.status_code == 200
    sink = gateway_context.envelope_sink
    assert isinstance(sink, FakeEnvelopeSink)
    _assistant_id, envelope, thread = sink.published[-1]
    assert thread == "inbound"
    assert envelope["thread"] == "unify_message"
    assert envelope["event"]["body"] == "hello"


def test_console_meet_dispatch_publishes_runtime_event(
    client: TestClient,
    gateway_context: GatewayContext,
) -> None:
    response = client.post(
        "/unify/meet",
        headers=ADMIN_HEADERS,
        json={
            "assistant_id": "123",
            "room_name": "room-1",
            "livekit_agent_name": "room-1",
        },
    )

    assert response.status_code == 200
    sink = gateway_context.envelope_sink
    assert isinstance(sink, FakeEnvelopeSink)
    _assistant_id, envelope, _thread = sink.published[-1]
    assert envelope["thread"] == "unify_meet"
    assert envelope["event"]["livekit_room"] == "room-1"


def test_console_system_event_publishes_runtime_event(
    client: TestClient,
    gateway_context: GatewayContext,
) -> None:
    response = client.post(
        "/unity/system-event",
        headers=ADMIN_HEADERS,
        json={
            "assistant_id": "123",
            "event_type": "user_remote_control_started",
            "message": "started",
        },
    )

    assert response.status_code == 200
    sink = gateway_context.envelope_sink
    assert isinstance(sink, FakeEnvelopeSink)
    _assistant_id, envelope, _thread = sink.published[-1]
    assert envelope["thread"] == "unity_system_event"
    assert envelope["event"]["event_type"] == "user_remote_control_started"
