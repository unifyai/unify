from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from droid.gateway.app import create_app
from droid.gateway.context import GatewayContext
from droid.gateway.public_url import StaticPublicUrlProvider
from droid.gateway.runtime import LocalRuntimeActivator
from droid.gateway.scheduler import LocalScheduler


class FakeCredentials:
    async def get(self, name: str) -> str:
        return f"credential:{name}"


class FakeStorage:
    async def write_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ):
        return SimpleNamespace(
            key=key,
            size_bytes=len(data),
            content_type=content_type,
            metadata={},
        )

    async def read_bytes(self, key: str) -> bytes:
        return key.encode()

    async def signed_url(self, key: str, *, expires_in: int = 3600) -> str:
        del expires_in
        return f"local://{key}"

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


@pytest.fixture
def app(gateway_context: GatewayContext, monkeypatch: pytest.MonkeyPatch) -> FastAPI:
    from droid.gateway.adapters import common
    from droid.gateway.common import auth

    monkeypatch.setattr(
        auth,
        "SETTINGS",
        SimpleNamespace(
            ORCHESTRA_ADMIN_KEY=SimpleNamespace(
                get_secret_value=lambda: "test-admin-key",
            ),
            ORCHESTRA_URL="http://orchestra.local/v0",
        ),
    )

    async def fake_get_assistant(assistant_id: str):
        assert assistant_id == "123"
        return {
            "assistant_id": 123,
            "boss_contact_id": 456,
            "self_contact_id": 789,
            "assistant_first_name": "Droid",
            "assistant_surname": "Assistant",
            "assistant_email": "droid@example.com",
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

    app = create_app(gateway_context=gateway_context)
    return app


def test_api_message_publishes_gateway_envelope(
    app: FastAPI,
    gateway_context: GatewayContext,
) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/message",
            headers={"Authorization": "Bearer test-admin-key"},
            json={
                "assistant_id": "123",
                "api_message_id": "api-msg-1",
                "body": "hello from console",
                "tags": ["local-e2e"],
            },
        )

    assert response.status_code == 200
    sink = gateway_context.envelope_sink
    assert isinstance(sink, FakeEnvelopeSink)
    assert len(sink.published) == 1
    assistant_id, envelope, thread = sink.published[0]
    assert assistant_id == "123"
    assert thread == "inbound"
    assert envelope["thread"] == "api_message"
    assert envelope["event"] == {
        "api_message_id": "api-msg-1",
        "body": "hello from console",
        "contact_id": 456,
        "assistant_id": "123",
        "tags": ["local-e2e"],
    }


def test_task_trigger_system_event_publishes_gateway_envelope(
    app: FastAPI,
    gateway_context: GatewayContext,
) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/droid/system-event",
            headers={"Authorization": "Bearer test-admin-key"},
            json={
                "assistant_id": "123",
                "event_type": "task_trigger",
                "message": "Task 17 triggered via REST API.",
                "extra_event_fields": {
                    "type": "task_trigger",
                    "task_id": 17,
                    "source_task_log_id": 2001,
                    "source_ref": "req-abc",
                    "task_label": "Review report",
                    "task_summary": "Review the weekly report.",
                },
            },
        )

    assert response.status_code == 200
    sink = gateway_context.envelope_sink
    assert isinstance(sink, FakeEnvelopeSink)
    assistant_id, envelope, thread = sink.published[-1]
    assert assistant_id == "123"
    assert thread == "inbound"
    assert envelope["thread"] == "droid_system_event"
    assert envelope["event"]["event_type"] == "task_trigger"
    assert envelope["event"]["task_id"] == 17
    assert envelope["event"]["source_task_log_id"] == 2001
    assert envelope["event"]["source_ref"] == "req-abc"
    assert envelope["event"]["task_label"] == "Review report"
