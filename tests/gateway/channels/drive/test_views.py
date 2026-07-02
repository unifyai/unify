"""Behavioural tests for ``unify.gateway.channels.drive``.

Focused on credential/identity resolution: the picker identifies the connected
account by ``assistant_id`` (preferred, and the only identity that works for a
Coordinator), falling back to ``user_email``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from unify.gateway.channels.drive import router


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/drive")
    return TestClient(app)


def _fake_drive_service() -> MagicMock:
    service = MagicMock()
    service.drives.return_value.list.return_value.execute.return_value = {"drives": []}
    return service


def _drive_service_with_files(files: list[dict]) -> MagicMock:
    """A Drive service whose ``files().list().execute()`` returns *files*."""
    service = _fake_drive_service()
    service.files.return_value.list.return_value.execute.return_value = {
        "files": files,
        "nextPageToken": None,
    }
    return service


def test_roots_resolves_by_assistant_id_when_provided(client: TestClient) -> None:
    assistant = {"secrets": {"GOOGLE_ACCESS_TOKEN": "tok"}}
    by_id = AsyncMock(return_value=assistant)
    by_email = AsyncMock(return_value=assistant)
    with (
        patch("unify.gateway.channels.drive.views.lookup_assistant_by_id", new=by_id),
        patch("unify.gateway.channels.drive.views.lookup_assistant", new=by_email),
        patch(
            "unify.gateway.channels.drive.views.build",
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
        patch("unify.gateway.channels.drive.views.lookup_assistant_by_id", new=by_id),
        patch("unify.gateway.channels.drive.views.lookup_assistant", new=by_email),
        patch(
            "unify.gateway.channels.drive.views.build",
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
        "unify.gateway.channels.drive.views.lookup_assistant_by_id",
        new=AsyncMock(return_value={"secrets": {}}),
    ):
        resp = client.get("/drive/roots", params={"assistant_id": "123"})
    assert resp.status_code == 409


def test_children_lists_my_drive_root(client: TestClient) -> None:
    """My Drive root children use the ``root`` alias and the personal drive space.

    This is the Drive analogue of the SharePoint root-children listing: it
    exercises the actual ``files().list`` query/kwargs the picker depends on,
    rather than only mocking a happy-path return.
    """
    files = [
        {
            "id": "f1",
            "name": "Folder",
            "mimeType": "application/vnd.google-apps.folder",
            "parents": ["root"],
        },
        {
            "id": "f2",
            "name": "notes.txt",
            "mimeType": "text/plain",
            "parents": ["root"],
        },
    ]
    service = _drive_service_with_files(files)
    assistant = {"secrets": {"GOOGLE_ACCESS_TOKEN": "tok"}}
    with (
        patch(
            "unify.gateway.channels.drive.views.lookup_assistant_by_id",
            new=AsyncMock(return_value=assistant),
        ),
        patch("unify.gateway.channels.drive.views.build", return_value=service),
    ):
        resp = client.get(
            "/drive/children",
            params={"assistant_id": "123", "drive_id": "my-drive", "item_id": "root"},
        )
    assert resp.status_code == 200, resp.text
    items = resp.json()["items"]
    assert [i["kind"] for i in items] == ["folder", "file"]
    assert all(i["drive_id"] == "my-drive" for i in items)
    kwargs = service.files.return_value.list.call_args.kwargs
    assert kwargs["q"] == "'root' in parents and trashed = false"
    assert kwargs["spaces"] == "drive"
    assert "driveId" not in kwargs


def test_children_scopes_query_to_shared_drive(client: TestClient) -> None:
    """A shared drive scopes the query with corpora/driveId + all-drives flags."""
    service = _drive_service_with_files([])
    assistant = {"secrets": {"GOOGLE_ACCESS_TOKEN": "tok"}}
    with (
        patch(
            "unify.gateway.channels.drive.views.lookup_assistant_by_id",
            new=AsyncMock(return_value=assistant),
        ),
        patch("unify.gateway.channels.drive.views.build", return_value=service),
    ):
        resp = client.get(
            "/drive/children",
            params={
                "assistant_id": "123",
                "drive_id": "shared-1",
                "item_id": "folder-9",
            },
        )
    assert resp.status_code == 200, resp.text
    kwargs = service.files.return_value.list.call_args.kwargs
    assert kwargs["q"] == "'folder-9' in parents and trashed = false"
    assert kwargs["driveId"] == "shared-1"
    assert kwargs["corpora"] == "drive"
    assert kwargs["includeItemsFromAllDrives"] is True
    assert kwargs["supportsAllDrives"] is True
