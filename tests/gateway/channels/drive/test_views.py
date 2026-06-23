"""Behavioural tests for ``droid.gateway.channels.drive``.

Focused on credential/identity resolution: the picker identifies the connected
account by ``assistant_id`` (preferred, and the only identity that works for a
Coordinator), falling back to ``user_email``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from droid.gateway.channels.drive import router


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/drive")
    return TestClient(app)


def _fake_drive_service() -> MagicMock:
    service = MagicMock()
    service.drives.return_value.list.return_value.execute.return_value = {"drives": []}
    return service


def test_roots_resolves_by_assistant_id_when_provided(client: TestClient) -> None:
    assistant = {"secrets": {"GOOGLE_ACCESS_TOKEN": "tok"}}
    by_id = AsyncMock(return_value=assistant)
    by_email = AsyncMock(return_value=assistant)
    with (
        patch("droid.gateway.channels.drive.views.lookup_assistant_by_id", new=by_id),
        patch("droid.gateway.channels.drive.views.lookup_assistant", new=by_email),
        patch(
            "droid.gateway.channels.drive.views.build",
            return_value=_fake_drive_service(),
        ),
    ):
        resp = client.get("/drive/roots", params={"assistant_id": "123"})

    assert resp.status_code == 200, resp.text
    by_id.assert_awaited_once()
    by_email.assert_not_awaited()
    assert resp.json()["roots"][0]["drive_id"] == "my-drive"


def test_roots_resolves_by_email_when_no_assistant_id(client: TestClient) -> None:
    assistant = {"secrets": {"GOOGLE_ACCESS_TOKEN": "tok"}}
    by_id = AsyncMock(return_value=assistant)
    by_email = AsyncMock(return_value=assistant)
    with (
        patch("droid.gateway.channels.drive.views.lookup_assistant_by_id", new=by_id),
        patch("droid.gateway.channels.drive.views.lookup_assistant", new=by_email),
        patch(
            "droid.gateway.channels.drive.views.build",
            return_value=_fake_drive_service(),
        ),
    ):
        resp = client.get("/drive/roots", params={"user_email": "u@x.com"})

    assert resp.status_code == 200, resp.text
    by_email.assert_awaited_once()
    by_id.assert_not_awaited()


def test_roots_requires_an_identity(client: TestClient) -> None:
    resp = client.get("/drive/roots")
    assert resp.status_code == 400


def test_roots_409_when_no_connected_token(client: TestClient) -> None:
    with patch(
        "droid.gateway.channels.drive.views.lookup_assistant_by_id",
        new=AsyncMock(return_value={"secrets": {}}),
    ):
        resp = client.get("/drive/roots", params={"assistant_id": "123"})
    assert resp.status_code == 409
