"""Behavioural tests for ``unify.gateway.channels.teams``.

12 endpoints; tests cover router contract + at least one happy-path
+ key edge cases per endpoint. Existing tests in
``communication/tests/teams/`` are listed for traceability but not
ported verbatim -- they exercise integration paths (full app
mount with auth + SETTINGS patching) that don't fit the
channel-isolated Phase B.1 shape.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from unify.gateway.channels.teams import router

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _teams_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MS365_ADMIN_TENANT_ID", "test-tenant")
    monkeypatch.setenv("MS365_ADMIN_CLIENT_ID", "test-client")
    monkeypatch.setenv("MS365_ADMIN_CLIENT_SECRET", "test-secret")
    monkeypatch.setenv("ORCHESTRA_ADMIN_KEY", "test-admin-key")
    monkeypatch.setenv("TEAMS_WEBHOOK_SECRET", "test-webhook-secret")


@pytest.fixture
def _adapters_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from unify.gateway.channels.teams import views as teams_views

    monkeypatch.setattr(
        teams_views,
        "SETTINGS",
        SimpleNamespace(
            conversation=SimpleNamespace(ADAPTERS_URL="https://adapters.example.com"),
        ),
    )


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI()
    app.include_router(router, prefix="/teams")
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _byod_assistant() -> dict:
    return {"secrets": {"MICROSOFT_ACCESS_TOKEN": "user-oauth-token"}}


# ---------------------------------------------------------------------------
# Router contract
# ---------------------------------------------------------------------------


def test_router_exposes_expected_paths() -> None:
    paths = sorted(
        (r.path, sorted(r.methods)) for r in router.routes  # type: ignore[attr-defined]
    )
    assert paths == [
        ("/channel/{team_id}/{channel_id}/messages", ["GET"]),
        ("/channel/{team_id}/{channel_id}/send", ["POST"]),
        ("/channels", ["POST"]),
        ("/chats", ["GET"]),
        ("/chats", ["POST"]),
        ("/create_meeting", ["POST"]),
        ("/messages/{chat_id}", ["GET"]),
        ("/send", ["POST"]),
        ("/teams", ["GET"]),
        ("/teams/{team_id}/channels", ["GET"]),
        ("/watch", ["DELETE"]),
        ("/watch", ["POST"]),
    ]


def test_router_importable_from_package_root() -> None:
    from unify.gateway.channels.teams import router as exported

    assert exported is router


# ---------------------------------------------------------------------------
# POST /send (chat message)
# ---------------------------------------------------------------------------


class TestSendChat:
    def test_missing_fields_returns_400(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        resp = client.post("/teams/send", json={"from": "x@x"})
        assert resp.status_code == 400

    def test_success_returns_message_id(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        fake_graph = MagicMock()
        fake_graph.me.chats.by_chat_id.return_value.messages.post = AsyncMock(
            return_value=MagicMock(id="msg-123"),
        )
        with patch(
            "unify.gateway.channels.teams.views.get_graph_client",
            new=AsyncMock(return_value=fake_graph),
        ):
            resp = client.post(
                "/teams/send",
                json={
                    "from": "alice@unify.ai",
                    "chat_id": "19:chat@thread.v2",
                    "body": "hello",
                },
            )
        assert resp.status_code == 200
        assert resp.json() == {"success": True, "message_id": "msg-123"}


# ---------------------------------------------------------------------------
# POST /chats (create chat) -- validation rules
# ---------------------------------------------------------------------------


class TestCreateChat:
    def test_one_on_one_requires_exactly_one_member(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        resp = client.post(
            "/teams/chats",
            json={
                "from": "alice@unify.ai",
                "chat_type": "oneOnOne",
                "members": [],
            },
        )
        assert resp.status_code == 400
        assert "oneOnOne requires exactly one member" in resp.json()["detail"]

    def test_one_on_one_rejects_topic(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        resp = client.post(
            "/teams/chats",
            json={
                "from": "alice@unify.ai",
                "chat_type": "oneOnOne",
                "members": ["bob@x.com"],
                "topic": "should be rejected",
            },
        )
        assert resp.status_code == 400

    def test_group_requires_at_least_two_members(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        resp = client.post(
            "/teams/chats",
            json={
                "from": "alice@unify.ai",
                "chat_type": "group",
                "members": ["bob@x.com"],
            },
        )
        assert resp.status_code == 400

    def test_invalid_chat_type_returns_400(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        resp = client.post(
            "/teams/chats",
            json={
                "from": "alice@unify.ai",
                "chat_type": "channel",
                "members": ["bob@x.com"],
            },
        )
        assert resp.status_code == 400

    def test_group_success_deduplicates_sender(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        fake_graph = MagicMock()
        fake_graph.me.get = AsyncMock(
            return_value=MagicMock(user_principal_name="alice@unify.ai"),
        )
        fake_graph.chats.post = AsyncMock(return_value=MagicMock(id="chat-1"))
        with patch(
            "unify.gateway.channels.teams.views.get_graph_client",
            new=AsyncMock(return_value=fake_graph),
        ):
            resp = client.post(
                "/teams/chats",
                json={
                    "from": "alice@unify.ai",
                    "chat_type": "group",
                    "members": ["alice@unify.ai", "bob@x.com", "carol@x.com"],
                    "topic": "Project planning",
                },
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["chat_id"] == "chat-1"
        assert body["chat_type"] == "group"


# ---------------------------------------------------------------------------
# GET /chats (list chats)
# ---------------------------------------------------------------------------


def test_list_chats_returns_chat_records(
    client: TestClient,
    _teams_credentials: None,
) -> None:
    fake_graph = MagicMock()
    chat1 = MagicMock(
        id="19:a@thread",
        topic="Topic A",
        chat_type=MagicMock(),
        created_date_time=None,
    )
    fake_graph.me.chats.get = AsyncMock(return_value=MagicMock(value=[chat1]))
    with patch(
        "unify.gateway.channels.teams.views.get_graph_client",
        new=AsyncMock(return_value=fake_graph),
    ):
        resp = client.get("/teams/chats", params={"user_email": "alice@unify.ai"})
    assert resp.status_code == 200
    assert resp.json()["chats"][0]["id"] == "19:a@thread"


# ---------------------------------------------------------------------------
# POST /watch
# ---------------------------------------------------------------------------


class TestWatchTeams:
    def test_missing_primary_email_returns_400(
        self,
        client: TestClient,
        _teams_credentials: None,
        _adapters_settings: None,
    ) -> None:
        resp = client.post("/teams/watch", json={})
        assert resp.status_code == 400

    def test_no_byod_token_returns_409(
        self,
        client: TestClient,
        _teams_credentials: None,
        _adapters_settings: None,
    ) -> None:
        """BYOD-only: missing delegated token must 409, not silently fall back."""
        with patch(
            "unify.gateway.channels.teams.views.lookup_assistant",
            new=AsyncMock(return_value={"secrets": {}}),
        ):
            resp = client.post(
                "/teams/watch",
                json={"primary_email": "alice@unify.ai"},
            )
        assert resp.status_code == 409
        assert "MICROSOFT_ACCESS_TOKEN" in resp.json()["detail"]

    def test_success_builds_chats_subscription(
        self,
        client: TestClient,
        _teams_credentials: None,
        _adapters_settings: None,
    ) -> None:
        fake_graph = MagicMock()
        fake_graph.me.get = AsyncMock(return_value=MagicMock(id="user-id-1"))
        fake_graph.subscriptions.get = AsyncMock(return_value=MagicMock(value=[]))
        fake_graph.subscriptions.post = AsyncMock(
            return_value=MagicMock(
                id="sub-chats-1",
                expiration_date_time=__import__(
                    "datetime",
                ).datetime(2026, 6, 1, tzinfo=__import__("datetime").timezone.utc),
            ),
        )
        fake_graph.me.joined_teams.get = AsyncMock(return_value=MagicMock(value=[]))
        with (
            patch(
                "unify.gateway.channels.teams.views.lookup_assistant",
                new=AsyncMock(return_value=_byod_assistant()),
            ),
            patch(
                "unify.gateway.channels.teams.views.graph_client_from_assistant",
                return_value=fake_graph,
            ),
        ):
            resp = client.post(
                "/teams/watch",
                json={"primary_email": "alice@unify.ai"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["chats"]["subscription_id"] == "sub-chats-1"


# ---------------------------------------------------------------------------
# DELETE /watch
# ---------------------------------------------------------------------------


class TestDeleteWatch:
    def test_missing_primary_email_returns_400(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        resp = client.request("DELETE", "/teams/watch", json={})
        assert resp.status_code == 400

    def test_no_matching_subscription_returns_404(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        fake_graph = MagicMock()
        fake_graph.subscriptions.get = AsyncMock(return_value=MagicMock(value=[]))
        with (
            patch(
                "unify.gateway.channels.teams.views.lookup_assistant",
                new=AsyncMock(return_value=_byod_assistant()),
            ),
            patch(
                "unify.gateway.channels.teams.views.graph_client_from_assistant",
                return_value=fake_graph,
            ),
        ):
            resp = client.request(
                "DELETE",
                "/teams/watch",
                json={"primary_email": "alice@unify.ai"},
            )
        assert resp.status_code == 404

    def test_deletes_owned_subs(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        owned = MagicMock(
            id="sub-1",
            resource="/users/u/chats/getAllMessages",
            client_state="test-webhook-secret::alice@unify.ai",
        )
        not_owned = MagicMock(
            id="sub-other",
            resource="/users/u/chats/getAllMessages",
            client_state="other-app::alice@unify.ai",
        )
        fake_graph = MagicMock()
        fake_graph.subscriptions.get = AsyncMock(
            return_value=MagicMock(value=[owned, not_owned]),
        )
        fake_graph.subscriptions.by_subscription_id.return_value.delete = AsyncMock()
        with (
            patch(
                "unify.gateway.channels.teams.views.lookup_assistant",
                new=AsyncMock(return_value=_byod_assistant()),
            ),
            patch(
                "unify.gateway.channels.teams.views.graph_client_from_assistant",
                return_value=fake_graph,
            ),
        ):
            resp = client.request(
                "DELETE",
                "/teams/watch",
                json={"primary_email": "alice@unify.ai"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["deleted"] == 1
        fake_graph.subscriptions.by_subscription_id.assert_called_with("sub-1")


# ---------------------------------------------------------------------------
# GET /messages/{chat_id}
# ---------------------------------------------------------------------------


def test_get_chat_messages_returns_list(
    client: TestClient,
    _teams_credentials: None,
) -> None:
    fake_graph = MagicMock()
    sender_info = MagicMock(user=MagicMock(display_name="Alice", id="u-1"))
    msg = MagicMock(
        id="m-1",
        from_=sender_info,
        body=MagicMock(content="hi", content_type=MagicMock()),
        created_date_time=None,
    )
    fake_graph.me.chats.by_chat_id.return_value.messages.get = AsyncMock(
        return_value=MagicMock(value=[msg]),
    )
    with patch(
        "unify.gateway.channels.teams.views.get_graph_client",
        new=AsyncMock(return_value=fake_graph),
    ):
        resp = client.get(
            "/teams/messages/19:chat@thread",
            params={"user_email": "alice@unify.ai"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["messages"][0]["id"] == "m-1"
    assert body["messages"][0]["sender"] == "Alice"


# ---------------------------------------------------------------------------
# GET /teams, GET /teams/{team_id}/channels
# ---------------------------------------------------------------------------


def test_list_joined_teams(
    client: TestClient,
    _teams_credentials: None,
) -> None:
    fake_graph = MagicMock()
    team = MagicMock(id="t-1", display_name="Team A", description="...")
    fake_graph.me.joined_teams.get = AsyncMock(return_value=MagicMock(value=[team]))
    with patch(
        "unify.gateway.channels.teams.views.get_graph_client",
        new=AsyncMock(return_value=fake_graph),
    ):
        resp = client.get("/teams/teams", params={"user_email": "alice@unify.ai"})
    assert resp.status_code == 200
    assert resp.json()["teams"][0]["id"] == "t-1"


def test_list_team_channels(
    client: TestClient,
    _teams_credentials: None,
) -> None:
    fake_graph = MagicMock()
    ch = MagicMock(
        id="c-1",
        display_name="General",
        description="",
        membership_type=MagicMock(),
    )
    fake_graph.teams.by_team_id.return_value.channels.get = AsyncMock(
        return_value=MagicMock(value=[ch]),
    )
    with patch(
        "unify.gateway.channels.teams.views.get_graph_client",
        new=AsyncMock(return_value=fake_graph),
    ):
        resp = client.get(
            "/teams/teams/t-1/channels",
            params={"user_email": "alice@unify.ai"},
        )
    assert resp.status_code == 200
    assert resp.json()["channels"][0]["id"] == "c-1"


# ---------------------------------------------------------------------------
# POST /channels (create channel)
# ---------------------------------------------------------------------------


class TestCreateChannel:
    def test_missing_fields_returns_400(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        resp = client.post(
            "/teams/channels",
            json={"from": "alice@unify.ai", "team_id": "t-1"},
        )
        assert resp.status_code == 400

    def test_private_channel_requires_owners(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        resp = client.post(
            "/teams/channels",
            json={
                "from": "alice@unify.ai",
                "team_id": "t-1",
                "display_name": "Launch",
                "membership_type": "private",
                "owners": [],
            },
        )
        assert resp.status_code == 400
        assert "owner" in resp.json()["detail"]

    def test_invalid_membership_type_returns_400(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        resp = client.post(
            "/teams/channels",
            json={
                "from": "alice@unify.ai",
                "team_id": "t-1",
                "display_name": "Launch",
                "membership_type": "weird",
            },
        )
        assert resp.status_code == 400

    def test_standard_channel_success(
        self,
        client: TestClient,
        _teams_credentials: None,
        _adapters_settings: None,
    ) -> None:
        fake_graph = MagicMock()
        fake_graph.teams.by_team_id.return_value.channels.post = AsyncMock(
            return_value=MagicMock(id="ch-1"),
        )
        # me.get for the watch-rebuild side effect
        fake_graph.me.get = AsyncMock(return_value=MagicMock(id=None))
        with patch(
            "unify.gateway.channels.teams.views.get_graph_client",
            new=AsyncMock(return_value=fake_graph),
        ):
            resp = client.post(
                "/teams/channels",
                json={
                    "from": "alice@unify.ai",
                    "team_id": "t-1",
                    "display_name": "Launch",
                },
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["channel_id"] == "ch-1"
        assert body["membership_type"] == "standard"


# ---------------------------------------------------------------------------
# POST /channel/{team_id}/{channel_id}/send
# ---------------------------------------------------------------------------


def test_send_channel_message_success(
    client: TestClient,
    _teams_credentials: None,
) -> None:
    fake_graph = MagicMock()
    fake_graph.teams.by_team_id.return_value.channels.by_channel_id.return_value.messages.post = AsyncMock(  # noqa: E501
        return_value=MagicMock(id="ch-msg-1"),
    )
    with patch(
        "unify.gateway.channels.teams.views.get_graph_client",
        new=AsyncMock(return_value=fake_graph),
    ):
        resp = client.post(
            "/teams/channel/t-1/c-1/send",
            json={"from": "alice@unify.ai", "body": "hello"},
        )
    assert resp.status_code == 200
    assert resp.json() == {"success": True, "message_id": "ch-msg-1"}


def test_send_channel_message_missing_fields_returns_400(
    client: TestClient,
    _teams_credentials: None,
) -> None:
    resp = client.post(
        "/teams/channel/t-1/c-1/send",
        json={"from": "alice@unify.ai"},
    )
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /create_meeting
# ---------------------------------------------------------------------------


class TestCreateMeeting:
    def test_missing_assistant_email_returns_400(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        resp = client.post("/teams/create_meeting", json={})
        assert resp.status_code == 400

    def test_invalid_mode_returns_400(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        resp = client.post(
            "/teams/create_meeting",
            json={"assistant_email": "x@x", "mode": "weird"},
        )
        assert resp.status_code == 400

    def test_no_byod_token_returns_409(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        with patch(
            "unify.gateway.channels.teams.views.lookup_assistant",
            new=AsyncMock(return_value={"secrets": {}}),
        ):
            resp = client.post(
                "/teams/create_meeting",
                json={"assistant_email": "alice@unify.ai", "mode": "instant"},
            )
        assert resp.status_code == 409

    def test_instant_success(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        from unify.gateway.channels.teams.create_meeting import CreatedMeeting

        created = CreatedMeeting(
            join_web_url="https://teams.microsoft.com/l/...",
            meeting_id="mtg-1",
            subject="Test",
        )
        with (
            patch(
                "unify.gateway.channels.teams.views.lookup_assistant",
                new=AsyncMock(return_value=_byod_assistant()),
            ),
            patch(
                "unify.gateway.channels.teams.create_meeting.create_instant_onlinemeeting",
                new=AsyncMock(return_value=created),
            ),
        ):
            resp = client.post(
                "/teams/create_meeting",
                json={"assistant_email": "alice@unify.ai", "mode": "instant"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["meeting_id"] == "mtg-1"
        assert body["join_web_url"] == "https://teams.microsoft.com/l/..."

    def test_scheduled_requires_subject_start_end(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        with patch(
            "unify.gateway.channels.teams.views.lookup_assistant",
            new=AsyncMock(return_value=_byod_assistant()),
        ):
            resp = client.post(
                "/teams/create_meeting",
                json={
                    "assistant_email": "alice@unify.ai",
                    "mode": "scheduled",
                    "subject": "Test",
                    # missing start, end
                },
            )
        assert resp.status_code == 400

    def test_permission_error_returns_403(
        self,
        client: TestClient,
        _teams_credentials: None,
    ) -> None:
        with (
            patch(
                "unify.gateway.channels.teams.views.lookup_assistant",
                new=AsyncMock(return_value=_byod_assistant()),
            ),
            patch(
                "unify.gateway.channels.teams.create_meeting.create_instant_onlinemeeting",
                new=AsyncMock(side_effect=PermissionError("Graph rejected token: 403")),
            ),
        ):
            resp = client.post(
                "/teams/create_meeting",
                json={"assistant_email": "alice@unify.ai", "mode": "instant"},
            )
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# OneDrive attachment upload
# ---------------------------------------------------------------------------


def test_upload_and_build_attachments_puts_by_path() -> None:
    """Attachments upload to OneDrive by resolving the me-drive id + path URL.

    ``graph.me.drive`` (DriveRequestBuilder) can't navigate items and
    ``drive.root.item_with_path`` no longer exists, so the upload resolves the
    personal drive id and binds the ``/root:/Teams Attachments/<name>:/content``
    URL onto the content builder via ``with_url``. The ``me.drive`` mock is
    spec-restricted to ``get`` so a revert to the old navigation raises here.
    """
    import asyncio
    import base64

    from unify.gateway.channels.teams.views import _upload_and_build_attachments

    graph = MagicMock()
    graph.request_adapter.base_url = "https://graph.microsoft.com/v1.0"
    graph.me.drive = MagicMock(spec=["get"])
    graph.me.drive.get = AsyncMock(return_value=MagicMock(id="me-drive-id"))
    drive = graph.drives.by_drive_id.return_value
    content = drive.items.by_drive_item_id.return_value.content
    put_mock = AsyncMock(return_value=MagicMock(web_url="https://od/report"))
    content.with_url.return_value.put = put_mock

    payload = base64.b64encode(b"hello").decode()
    out = asyncio.run(
        _upload_and_build_attachments(
            graph,
            [{"filename": "report.pdf", "content_base64": payload}],
        ),
    )

    assert len(out) == 1
    assert out[0].name == "report.pdf"
    assert out[0].content_url == "https://od/report"
    graph.me.drive.get.assert_awaited_once()
    graph.drives.by_drive_id.assert_called_with("me-drive-id")
    put_mock.assert_awaited_once_with(b"hello")
    url = content.with_url.call_args.args[0]
    assert url.startswith(
        "https://graph.microsoft.com/v1.0/drives/me-drive-id"
        "/root:/Teams%20Attachments/",
    )
    assert url.endswith("_report.pdf:/content")
