"""Behavioural tests for ``unify.gateway.channels.outlook``.

No existing tests in ``communication/tests/outlook/`` to port
(outlook was tested through integration only); these tests are
greenfield, following the Phase B.1 channel-isolated shape with the
class-grouping convention for >5-endpoint channels established by
phone (Phase B.2).
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from unify.gateway.channels.outlook import router

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _outlook_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MS365_ADMIN_TENANT_ID", "test-tenant")
    monkeypatch.setenv("MS365_ADMIN_CLIENT_ID", "test-client")
    monkeypatch.setenv("MS365_ADMIN_CLIENT_SECRET", "test-secret")
    monkeypatch.setenv("ORCHESTRA_ADMIN_KEY", "test-admin-key")
    monkeypatch.setenv("OUTLOOK_WEBHOOK_SECRET", "test-webhook-secret")


@pytest.fixture
def _adapters_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin SETTINGS.conversation.ADAPTERS_URL for default-webhook-URL assertions."""
    from unify.gateway.channels.outlook import views as outlook_views

    monkeypatch.setattr(
        outlook_views,
        "SETTINGS",
        SimpleNamespace(
            conversation=SimpleNamespace(
                ADAPTERS_URL="https://adapters.example.com",
            ),
        ),
    )


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI()
    app.include_router(router, prefix="/outlook")
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _byod_assistant() -> dict:
    """Assistant record with a per-user OAuth token (BYOD)."""
    return {"secrets": {"MICROSOFT_ACCESS_TOKEN": "user-oauth-token"}}


def _admin_assistant() -> dict:
    """Assistant record without a per-user token (admin-fallback)."""
    return {"secrets": {}}


# ---------------------------------------------------------------------------
# Router contract
# ---------------------------------------------------------------------------


def test_router_exposes_expected_paths() -> None:
    paths = sorted(
        (r.path, sorted(r.methods)) for r in router.routes  # type: ignore[attr-defined]
    )
    assert paths == [
        ("/attachment", ["GET"]),
        ("/delete", ["DELETE"]),
        ("/send", ["POST"]),
        ("/watch", ["DELETE"]),
        ("/watch", ["POST"]),
    ]


def test_router_importable_from_package_root() -> None:
    from unify.gateway.channels.outlook import router as exported

    assert exported is router


# ---------------------------------------------------------------------------
# DELETE /delete
# ---------------------------------------------------------------------------


class TestDeleteOutlookUser:
    def test_missing_primary_email_returns_400(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        resp = client.request("DELETE", "/outlook/delete", json={})
        assert resp.status_code == 400
        assert resp.json()["detail"] == "Missing primary_email"

    def test_success(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        fake_graph = MagicMock()
        fake_graph.users.by_user_id.return_value.delete = AsyncMock()
        with patch(
            "unify.gateway.channels.outlook.views.get_admin_graph_client",
            return_value=fake_graph,
        ):
            resp = client.request(
                "DELETE",
                "/outlook/delete",
                json={"primary_email": "user@example.com"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["deleted"] is True
        assert data["already_absent"] is False
        fake_graph.users.by_user_id.assert_called_with("user@example.com")

    def test_does_not_exist_treated_as_already_absent(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        """The 'does not exist' substring match is the legacy idempotency hook."""
        fake_graph = MagicMock()
        fake_graph.users.by_user_id.return_value.delete = AsyncMock(
            side_effect=Exception("Resource 'user@example.com' does not exist"),
        )
        with patch(
            "unify.gateway.channels.outlook.views.get_admin_graph_client",
            return_value=fake_graph,
        ):
            resp = client.request(
                "DELETE",
                "/outlook/delete",
                json={"primary_email": "user@example.com"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["deleted"] is False
        assert data["already_absent"] is True

    def test_not_found_string_treated_as_already_absent(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        """Second idempotency path: 'not found' substring."""
        fake_graph = MagicMock()
        fake_graph.users.by_user_id.return_value.delete = AsyncMock(
            side_effect=Exception("MS Graph 404: user not found"),
        )
        with patch(
            "unify.gateway.channels.outlook.views.get_admin_graph_client",
            return_value=fake_graph,
        ):
            resp = client.request(
                "DELETE",
                "/outlook/delete",
                json={"primary_email": "user@example.com"},
            )
        assert resp.status_code == 200
        assert resp.json()["already_absent"] is True

    def test_other_errors_bubble_up_as_500(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        fake_graph = MagicMock()
        fake_graph.users.by_user_id.return_value.delete = AsyncMock(
            side_effect=Exception("internal server error"),
        )
        with patch(
            "unify.gateway.channels.outlook.views.get_admin_graph_client",
            return_value=fake_graph,
        ):
            resp = client.request(
                "DELETE",
                "/outlook/delete",
                json={"primary_email": "user@example.com"},
            )
        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# POST /send
# ---------------------------------------------------------------------------


class TestSendOutlookEmail:
    def test_missing_required_fields_returns_400(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        resp = client.post("/outlook/send", json={"subject": "x"})
        assert resp.status_code == 400
        assert "Missing required fields" in resp.json()["detail"]

    def test_basic_send_via_byod_path(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        """BYOD path uses graph.me and goes through send_mail.post."""
        fake_graph = MagicMock()
        fake_graph.me.send_mail.post = AsyncMock()
        with (
            patch(
                "unify.gateway.channels.outlook.views.lookup_assistant",
                new=AsyncMock(return_value=_byod_assistant()),
            ),
            patch(
                "unify.gateway.channels.outlook.views.graph_client_from_assistant",
                return_value=fake_graph,
            ),
        ):
            resp = client.post(
                "/outlook/send",
                json={
                    "from": "sender@example.com",
                    "to": "recipient@example.com",
                    "subject": "Subject",
                    "body": "Body content",
                },
            )
        assert resp.status_code == 200
        assert resp.json() == {"success": True}
        fake_graph.me.send_mail.post.assert_awaited_once()

    def test_send_via_admin_path_uses_users_by_email_node(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        """Admin (no BYOD token) path addresses graph.users.by_user_id(email)."""
        fake_graph = MagicMock()
        fake_graph.users.by_user_id.return_value.send_mail.post = AsyncMock()
        with (
            patch(
                "unify.gateway.channels.outlook.views.lookup_assistant",
                new=AsyncMock(return_value=_admin_assistant()),
            ),
            patch(
                "unify.gateway.channels.outlook.views.graph_client_from_assistant",
                return_value=fake_graph,
            ),
        ):
            resp = client.post(
                "/outlook/send",
                json={
                    "from": "sender@example.com",
                    "to": "recipient@example.com",
                    "subject": "Subject",
                    "body": "Body",
                },
            )
        assert resp.status_code == 200
        fake_graph.users.by_user_id.assert_called_with("sender@example.com")

    def test_reply_without_attachment_uses_reply_post(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        """The in_reply_to path without attachment uses messages.reply.post."""
        fake_graph = MagicMock()
        fake_graph.me.messages.by_message_id.return_value.reply.post = AsyncMock()
        with (
            patch(
                "unify.gateway.channels.outlook.views.lookup_assistant",
                new=AsyncMock(return_value=_byod_assistant()),
            ),
            patch(
                "unify.gateway.channels.outlook.views.graph_client_from_assistant",
                return_value=fake_graph,
            ),
        ):
            resp = client.post(
                "/outlook/send",
                json={
                    "from": "sender@example.com",
                    "to": "recipient@example.com",
                    "subject": "Re: original",
                    "body": "Reply body",
                    "in_reply_to": "msg-id-1",
                },
            )
        assert resp.status_code == 200
        fake_graph.me.messages.by_message_id.assert_called_with("msg-id-1")
        # reply.post path used (not send_mail / createReply)
        fake_graph.me.messages.by_message_id.return_value.reply.post.assert_awaited()

    def test_reply_with_attachment_uses_create_reply_then_send(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        """Reply + attachment uses createReply -> attach -> send (3-step flow)."""
        fake_graph = MagicMock()
        draft = MagicMock(id="draft-id-1")
        fake_graph.me.messages.by_message_id.return_value.create_reply.post = AsyncMock(
            return_value=draft,
        )
        fake_graph.me.messages.by_message_id.return_value.attachments.post = AsyncMock()
        fake_graph.me.messages.by_message_id.return_value.send.post = AsyncMock()
        with (
            patch(
                "unify.gateway.channels.outlook.views.lookup_assistant",
                new=AsyncMock(return_value=_byod_assistant()),
            ),
            patch(
                "unify.gateway.channels.outlook.views.graph_client_from_assistant",
                return_value=fake_graph,
            ),
        ):
            content_b64 = base64.b64encode(b"reply attachment").decode("utf-8")
            resp = client.post(
                "/outlook/send",
                json={
                    "from": "sender@example.com",
                    "to": "recipient@example.com",
                    "subject": "Re: with file",
                    "body": "See attached",
                    "in_reply_to": "original-msg-id",
                    "attachment": {
                        "filename": "file.pdf",
                        "content_base64": content_b64,
                    },
                },
            )
        assert resp.status_code == 200
        # All three steps invoked: create_reply, attach, send
        fake_graph.me.messages.by_message_id.return_value.create_reply.post.assert_awaited()
        fake_graph.me.messages.by_message_id.return_value.attachments.post.assert_awaited()
        fake_graph.me.messages.by_message_id.return_value.send.post.assert_awaited()

    def test_send_failure_bubbles_up_as_500(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        fake_graph = MagicMock()
        fake_graph.me.send_mail.post = AsyncMock(side_effect=Exception("graph down"))
        with (
            patch(
                "unify.gateway.channels.outlook.views.lookup_assistant",
                new=AsyncMock(return_value=_byod_assistant()),
            ),
            patch(
                "unify.gateway.channels.outlook.views.graph_client_from_assistant",
                return_value=fake_graph,
            ),
        ):
            resp = client.post(
                "/outlook/send",
                json={
                    "from": "sender@example.com",
                    "to": "recipient@example.com",
                    "subject": "x",
                    "body": "y",
                },
            )
        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# POST /watch
# ---------------------------------------------------------------------------


class TestWatchOutlookEmail:
    def test_missing_primary_email_returns_400(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        resp = client.post("/outlook/watch", json={})
        assert resp.status_code == 400

    def test_creates_subscription_and_returns_id(
        self,
        client: TestClient,
        _outlook_credentials: None,
        _adapters_settings: None,
    ) -> None:
        fake_graph = MagicMock()
        fake_graph.subscriptions.get = AsyncMock(return_value=MagicMock(value=[]))
        result = MagicMock(
            id="sub-id-1",
            expiration_date_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
        )
        fake_graph.subscriptions.post = AsyncMock(return_value=result)

        with patch(
            "unify.gateway.channels.outlook.views.get_graph_client",
            new=AsyncMock(return_value=fake_graph),
        ):
            resp = client.post(
                "/outlook/watch",
                json={"primary_email": "user@example.com"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["subscription_id"] == "sub-id-1"
        assert "2026-06-01" in data["expiration"]

    def test_default_webhook_url_uses_adapters_setting(
        self,
        client: TestClient,
        _outlook_credentials: None,
        _adapters_settings: None,
    ) -> None:
        """Default webhook URL = SETTINGS.conversation.ADAPTERS_URL + /microsoft/router."""
        fake_graph = MagicMock()
        fake_graph.subscriptions.get = AsyncMock(return_value=MagicMock(value=[]))
        result = MagicMock(
            id="sub-id-1",
            expiration_date_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
        )
        fake_graph.subscriptions.post = AsyncMock(return_value=result)

        with patch(
            "unify.gateway.channels.outlook.views.get_graph_client",
            new=AsyncMock(return_value=fake_graph),
        ):
            client.post(
                "/outlook/watch",
                json={"primary_email": "user@example.com"},
            )

        # subscription.post called with a Subscription whose
        # notification_url is the default-adapters URL
        sub_arg = fake_graph.subscriptions.post.await_args.args[0]
        assert sub_arg.notification_url == (
            "https://adapters.example.com/microsoft/router"
        )

    def test_explicit_webhook_url_overrides_default(
        self,
        client: TestClient,
        _outlook_credentials: None,
        _adapters_settings: None,
    ) -> None:
        fake_graph = MagicMock()
        fake_graph.subscriptions.get = AsyncMock(return_value=MagicMock(value=[]))
        result = MagicMock(
            id="sub-id-1",
            expiration_date_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
        )
        fake_graph.subscriptions.post = AsyncMock(return_value=result)

        with patch(
            "unify.gateway.channels.outlook.views.get_graph_client",
            new=AsyncMock(return_value=fake_graph),
        ):
            client.post(
                "/outlook/watch",
                json={
                    "primary_email": "user@example.com",
                    "webhook_url": "https://custom-webhook.example.com/hook",
                },
            )

        sub_arg = fake_graph.subscriptions.post.await_args.args[0]
        assert sub_arg.notification_url == "https://custom-webhook.example.com/hook"

    def test_existing_subscription_for_same_resource_is_deleted_first(
        self,
        client: TestClient,
        _outlook_credentials: None,
        _adapters_settings: None,
    ) -> None:
        """Stale subscription on the same resource is purged before recreating."""
        existing = MagicMock(
            id="stale-sub-1",
            resource="users/user@example.com/mailFolders/inbox/messages",
        )
        fake_graph = MagicMock()
        fake_graph.subscriptions.get = AsyncMock(
            return_value=MagicMock(value=[existing]),
        )
        fake_graph.subscriptions.by_subscription_id.return_value.delete = AsyncMock()
        new_result = MagicMock(
            id="new-sub-1",
            expiration_date_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
        )
        fake_graph.subscriptions.post = AsyncMock(return_value=new_result)

        with patch(
            "unify.gateway.channels.outlook.views.get_graph_client",
            new=AsyncMock(return_value=fake_graph),
        ):
            resp = client.post(
                "/outlook/watch",
                json={"primary_email": "user@example.com"},
            )

        assert resp.status_code == 200
        fake_graph.subscriptions.by_subscription_id.assert_called_with("stale-sub-1")
        fake_graph.subscriptions.by_subscription_id.return_value.delete.assert_awaited()

    def test_validation_timeout_triggers_one_retry(
        self,
        client: TestClient,
        _outlook_credentials: None,
        _adapters_settings: None,
    ) -> None:
        """The MS Graph 'validation timeout' race is retried exactly once."""
        fake_graph = MagicMock()
        fake_graph.subscriptions.get = AsyncMock(return_value=MagicMock(value=[]))

        result = MagicMock(
            id="sub-after-retry",
            expiration_date_time=datetime(2026, 6, 1, tzinfo=timezone.utc),
        )
        fake_graph.subscriptions.post = AsyncMock(
            side_effect=[
                Exception("Subscription validation request timeout"),
                result,
            ],
        )

        with patch(
            "unify.gateway.channels.outlook.views.get_graph_client",
            new=AsyncMock(return_value=fake_graph),
        ):
            resp = client.post(
                "/outlook/watch",
                json={"primary_email": "user@example.com"},
            )

        assert resp.status_code == 200
        assert resp.json()["subscription_id"] == "sub-after-retry"
        assert fake_graph.subscriptions.post.await_count == 2

    def test_persistent_validation_timeout_eventually_500s(
        self,
        client: TestClient,
        _outlook_credentials: None,
        _adapters_settings: None,
    ) -> None:
        fake_graph = MagicMock()
        fake_graph.subscriptions.get = AsyncMock(return_value=MagicMock(value=[]))
        fake_graph.subscriptions.post = AsyncMock(
            side_effect=Exception("validation request timeout, again"),
        )
        with patch(
            "unify.gateway.channels.outlook.views.get_graph_client",
            new=AsyncMock(return_value=fake_graph),
        ):
            resp = client.post(
                "/outlook/watch",
                json={"primary_email": "user@example.com"},
            )
        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# DELETE /watch
# ---------------------------------------------------------------------------


class TestDeleteOutlookWatch:
    def test_missing_primary_email_returns_400(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        resp = client.request("DELETE", "/outlook/watch", json={})
        assert resp.status_code == 400

    def test_deletes_matching_subscription(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        existing = MagicMock(
            id="sub-1",
            resource="users/user@example.com/mailFolders/inbox/messages",
        )
        fake_graph = MagicMock()
        fake_graph.subscriptions.get = AsyncMock(
            return_value=MagicMock(value=[existing]),
        )
        fake_graph.subscriptions.by_subscription_id.return_value.delete = AsyncMock()

        with patch(
            "unify.gateway.channels.outlook.views.get_graph_client",
            new=AsyncMock(return_value=fake_graph),
        ):
            resp = client.request(
                "DELETE",
                "/outlook/watch",
                json={"primary_email": "user@example.com"},
            )

        assert resp.status_code == 200
        assert resp.json() == {
            "success": True,
            "primary_email": "user@example.com",
        }
        fake_graph.subscriptions.by_subscription_id.assert_called_with("sub-1")

    def test_no_matching_subscription_returns_404(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        fake_graph = MagicMock()
        fake_graph.subscriptions.get = AsyncMock(return_value=MagicMock(value=[]))

        with patch(
            "unify.gateway.channels.outlook.views.get_graph_client",
            new=AsyncMock(return_value=fake_graph),
        ):
            resp = client.request(
                "DELETE",
                "/outlook/watch",
                json={"primary_email": "user@example.com"},
            )

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /attachment
# ---------------------------------------------------------------------------


class TestGetOutlookAttachment:
    def test_returns_bytes_with_attachment_content_type(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        fake_graph = MagicMock()
        attachment = MagicMock(
            content_bytes=b"file-bytes",
            content_type="application/pdf",
        )
        attachment.name = "report.pdf"
        fake_graph.me.messages.by_message_id.return_value.attachments.by_attachment_id.return_value.get = AsyncMock(  # noqa: E501
            return_value=attachment,
        )

        with (
            patch(
                "unify.gateway.channels.outlook.views.lookup_assistant",
                new=AsyncMock(return_value=_byod_assistant()),
            ),
            patch(
                "unify.gateway.channels.outlook.views.graph_client_from_assistant",
                return_value=fake_graph,
            ),
        ):
            resp = client.get(
                "/outlook/attachment",
                params={
                    "user_email": "user@example.com",
                    "message_id": "msg-1",
                    "attachment_id": "att-1",
                    "filename": "report.pdf",
                },
            )

        assert resp.status_code == 200
        assert resp.content == b"file-bytes"
        assert resp.headers["content-type"] == "application/pdf"
        assert "filename=report.pdf" in resp.headers["content-disposition"]

    def test_default_filename_when_unspecified(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        fake_graph = MagicMock()
        # NB: `name` is a reserved MagicMock constructor kwarg (it sets the
        # mock's display name, not the .name attribute). Set it via assignment
        # after construction so the route sees a real string value.
        attachment = MagicMock(content_bytes=b"x", content_type=None)
        attachment.name = "from-graph.pdf"
        fake_graph.me.messages.by_message_id.return_value.attachments.by_attachment_id.return_value.get = AsyncMock(  # noqa: E501
            return_value=attachment,
        )

        with (
            patch(
                "unify.gateway.channels.outlook.views.lookup_assistant",
                new=AsyncMock(return_value=_byod_assistant()),
            ),
            patch(
                "unify.gateway.channels.outlook.views.graph_client_from_assistant",
                return_value=fake_graph,
            ),
        ):
            resp = client.get(
                "/outlook/attachment",
                params={
                    "user_email": "user@example.com",
                    "message_id": "msg-1",
                    "attachment_id": "att-1",
                },
            )

        assert resp.status_code == 200
        # When no filename param, the attachment's own name is used
        assert "filename=from-graph.pdf" in resp.headers["content-disposition"]
        # Default content_type when Graph didn't supply one
        assert resp.headers["content-type"] == "application/octet-stream"

    def test_missing_content_returns_404(
        self,
        client: TestClient,
        _outlook_credentials: None,
    ) -> None:
        fake_graph = MagicMock()
        attachment = MagicMock(
            content_bytes=None,
            content_type="text/plain",
        )
        attachment.name = "empty.txt"
        fake_graph.me.messages.by_message_id.return_value.attachments.by_attachment_id.return_value.get = AsyncMock(  # noqa: E501
            return_value=attachment,
        )

        with (
            patch(
                "unify.gateway.channels.outlook.views.lookup_assistant",
                new=AsyncMock(return_value=_byod_assistant()),
            ),
            patch(
                "unify.gateway.channels.outlook.views.graph_client_from_assistant",
                return_value=fake_graph,
            ),
        ):
            resp = client.get(
                "/outlook/attachment",
                params={
                    "user_email": "user@example.com",
                    "message_id": "msg-1",
                    "attachment_id": "att-1",
                },
            )

        assert resp.status_code == 404
