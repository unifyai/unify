from __future__ import annotations

import base64
import json
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from unify.gateway.app import create_app
from unify.settings import SETTINGS


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("ORCHESTRA_ADMIN_KEY", "test-admin-key")
    app = create_app()
    return TestClient(app)


def test_gmail_notification_skips_universal_coordinator_mailbox(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator_email = SETTINGS.UNITY_COORDINATOR_EMAIL_ADDRESS
    payload = {
        "emailAddress": coordinator_email,
        "historyId": "12345",
    }
    envelope = {
        "message": {
            "data": base64.b64encode(json.dumps(payload).encode()).decode(),
        },
    }

    with patch(
        "unify.gateway.adapters.google.get_assistant",
        new=AsyncMock(),
    ) as mock_get_assistant:
        response = client.post("/email/gmail", json=envelope)

    assert response.status_code == 200
    mock_get_assistant.assert_not_called()
