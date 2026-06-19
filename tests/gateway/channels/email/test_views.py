"""Behavioural tests for ``droid.gateway.channels.email``.

No existing tests in ``communication/tests/email/`` to port (the
dispatcher was only tested end-to-end with real gmail/outlook
fixtures); these tests are greenfield and focus on the dispatch
decision: which provider gets the forwarded request given which
assistant configuration.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from droid.gateway.channels.email import router
from droid.gateway.channels.email.views import _is_outlook_assistant

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _orchestra_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORCHESTRA_ADMIN_KEY", "test-admin-key")


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI()
    app.include_router(router, prefix="/email")
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
        ("/attachment", ["GET"]),
        ("/send", ["POST"]),
    ]


def test_router_importable_from_package_root() -> None:
    from droid.gateway.channels.email import router as exported

    assert exported is router


# ---------------------------------------------------------------------------
# _is_outlook_assistant -- provider-sniffing rules
# ---------------------------------------------------------------------------


class TestIsOutlookAssistant:
    def test_canonical_microsoft_365_provider_returns_true(self) -> None:
        assert _is_outlook_assistant({"email_provider": "microsoft_365"}) is True

    def test_canonical_google_workspace_provider_returns_false(self) -> None:
        assert _is_outlook_assistant({"email_provider": "google_workspace"}) is False

    def test_unknown_provider_returns_false(self) -> None:
        """Strict match: only microsoft_365 routes to outlook."""
        assert _is_outlook_assistant({"email_provider": "yahoo"}) is False

    def test_legacy_microsoft_token_sniff_returns_true_when_no_provider_set(
        self,
    ) -> None:
        """Pre-email_provider assistants: MICROSOFT_ACCESS_TOKEN presence = outlook."""
        assistant = {"secrets": {"MICROSOFT_ACCESS_TOKEN": "abc123"}}
        assert _is_outlook_assistant(assistant) is True

    def test_legacy_no_token_no_provider_returns_false_default_gmail(self) -> None:
        """No provider field and no MS token: assume gmail (the historical default)."""
        assert _is_outlook_assistant({}) is False
        assert _is_outlook_assistant({"secrets": {}}) is False

    def test_canonical_provider_wins_over_token_sniff(self) -> None:
        """Explicit google_workspace overrides a stale MS token in secrets."""
        assistant = {
            "email_provider": "google_workspace",
            "secrets": {"MICROSOFT_ACCESS_TOKEN": "stale-token"},
        }
        assert _is_outlook_assistant(assistant) is False


# ---------------------------------------------------------------------------
# POST /send -- request validation
# ---------------------------------------------------------------------------


class TestSendValidation:
    def test_missing_from_returns_400(
        self,
        client: TestClient,
        _orchestra_credentials: None,
    ) -> None:
        resp = client.post(
            "/email/send",
            json={"to": "recipient@example.com", "body": "hi"},
        )
        assert resp.status_code == 400
        assert "from" in resp.json()["detail"].lower()

    def test_empty_from_returns_400(
        self,
        client: TestClient,
        _orchestra_credentials: None,
    ) -> None:
        resp = client.post(
            "/email/send",
            json={"from": "", "to": "x@x.com", "body": "hi"},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /send -- dispatch routing
# ---------------------------------------------------------------------------


class TestSendDispatch:
    def test_routes_to_outlook_for_microsoft_365_assistant(
        self,
        client: TestClient,
        _orchestra_credentials: None,
    ) -> None:
        outlook_handler = AsyncMock(return_value={"success": True, "via": "outlook"})
        gmail_handler = AsyncMock(return_value={"success": True, "via": "gmail"})
        with (
            patch(
                "droid.gateway.channels.email.views.lookup_assistant",
                new=AsyncMock(return_value={"email_provider": "microsoft_365"}),
            ),
            patch(
                "droid.gateway.channels.email.views.send_outlook_email",
                new=outlook_handler,
            ),
            patch(
                "droid.gateway.channels.email.views.gmail_send_email",
                new=gmail_handler,
            ),
        ):
            resp = client.post(
                "/email/send",
                json={
                    "from": "alice@unify.ai",
                    "to": "bob@example.com",
                    "subject": "x",
                    "body": "y",
                },
            )
        assert resp.status_code == 200
        outlook_handler.assert_awaited_once()
        gmail_handler.assert_not_called()

    def test_routes_to_gmail_for_google_workspace_assistant(
        self,
        client: TestClient,
        _orchestra_credentials: None,
    ) -> None:
        outlook_handler = AsyncMock(return_value={"success": True, "via": "outlook"})
        gmail_handler = AsyncMock(return_value={"success": True, "via": "gmail"})
        with (
            patch(
                "droid.gateway.channels.email.views.lookup_assistant",
                new=AsyncMock(return_value={"email_provider": "google_workspace"}),
            ),
            patch(
                "droid.gateway.channels.email.views.send_outlook_email",
                new=outlook_handler,
            ),
            patch(
                "droid.gateway.channels.email.views.gmail_send_email",
                new=gmail_handler,
            ),
        ):
            resp = client.post(
                "/email/send",
                json={
                    "from": "alice@unify.ai",
                    "to": "bob@example.com",
                    "subject": "x",
                    "body": "y",
                },
            )
        assert resp.status_code == 200
        gmail_handler.assert_awaited_once()
        outlook_handler.assert_not_called()

    def test_routes_to_outlook_via_legacy_token_sniff(
        self,
        client: TestClient,
        _orchestra_credentials: None,
    ) -> None:
        """Pre-email_provider assistants: routed via MICROSOFT_ACCESS_TOKEN presence."""
        outlook_handler = AsyncMock(return_value={"success": True, "via": "outlook"})
        gmail_handler = AsyncMock(return_value={"success": True, "via": "gmail"})
        legacy_assistant = {"secrets": {"MICROSOFT_ACCESS_TOKEN": "legacy-token"}}
        with (
            patch(
                "droid.gateway.channels.email.views.lookup_assistant",
                new=AsyncMock(return_value=legacy_assistant),
            ),
            patch(
                "droid.gateway.channels.email.views.send_outlook_email",
                new=outlook_handler,
            ),
            patch(
                "droid.gateway.channels.email.views.gmail_send_email",
                new=gmail_handler,
            ),
        ):
            resp = client.post(
                "/email/send",
                json={
                    "from": "alice@unify.ai",
                    "to": "bob@example.com",
                    "body": "y",
                },
            )
        assert resp.status_code == 200
        outlook_handler.assert_awaited_once()

    def test_default_routes_to_gmail_when_no_provider_or_token(
        self,
        client: TestClient,
        _orchestra_credentials: None,
    ) -> None:
        outlook_handler = AsyncMock()
        gmail_handler = AsyncMock(return_value={"success": True, "via": "gmail"})
        with (
            patch(
                "droid.gateway.channels.email.views.lookup_assistant",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "droid.gateway.channels.email.views.send_outlook_email",
                new=outlook_handler,
            ),
            patch(
                "droid.gateway.channels.email.views.gmail_send_email",
                new=gmail_handler,
            ),
        ):
            resp = client.post(
                "/email/send",
                json={
                    "from": "alice@unify.ai",
                    "to": "bob@example.com",
                    "body": "y",
                },
            )
        assert resp.status_code == 200
        gmail_handler.assert_awaited_once()
        outlook_handler.assert_not_called()

    def test_forwards_body_bytes_through_to_provider_handler(
        self,
        client: TestClient,
        _orchestra_credentials: None,
    ) -> None:
        """The cloned ASGI request carries the original body unchanged."""
        captured = {}

        async def fake_gmail_send(forwarded_request):
            captured["body"] = await forwarded_request.body()
            return {"success": True}

        with (
            patch(
                "droid.gateway.channels.email.views.lookup_assistant",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "droid.gateway.channels.email.views.gmail_send_email",
                new=fake_gmail_send,
            ),
        ):
            client.post(
                "/email/send",
                json={
                    "from": "alice@unify.ai",
                    "to": "bob@example.com",
                    "subject": "Subject line",
                    "body": "Body content",
                },
            )

        import json as _json

        forwarded_body = _json.loads(captured["body"])
        assert forwarded_body == {
            "from": "alice@unify.ai",
            "to": "bob@example.com",
            "subject": "Subject line",
            "body": "Body content",
        }

    def test_shared_coordinator_email_routes_to_gmail_without_lookup(
        self,
        client: TestClient,
        _orchestra_credentials: None,
    ) -> None:
        lookup = AsyncMock(side_effect=AssertionError("lookup should be skipped"))
        outlook_handler = AsyncMock()
        gmail_handler = AsyncMock(return_value={"success": True, "via": "gmail"})
        with (
            patch(
                "droid.gateway.channels.email.views.lookup_assistant",
                new=lookup,
            ),
            patch(
                "droid.gateway.channels.email.views.send_outlook_email",
                new=outlook_handler,
            ),
            patch(
                "droid.gateway.channels.email.views.gmail_send_email",
                new=gmail_handler,
            ),
        ):
            resp = client.post(
                "/email/send",
                json={
                    "from": "marty@unify.ai",
                    "to": "owner@example.com",
                    "body": "hello",
                },
            )
        assert resp.status_code == 200
        gmail_handler.assert_awaited_once()
        outlook_handler.assert_not_called()
        lookup.assert_not_called()


# ---------------------------------------------------------------------------
# GET /attachment -- dispatch routing
# ---------------------------------------------------------------------------


class TestAttachmentDispatch:
    def test_routes_to_outlook_for_microsoft_365_assistant(
        self,
        client: TestClient,
        _orchestra_credentials: None,
    ) -> None:
        outlook_handler = AsyncMock(return_value=b"outlook-bytes")
        gmail_handler = AsyncMock(return_value=b"gmail-bytes")
        with (
            patch(
                "droid.gateway.channels.email.views.lookup_assistant",
                new=AsyncMock(return_value={"email_provider": "microsoft_365"}),
            ),
            patch(
                "droid.gateway.channels.email.views.get_outlook_attachment",
                new=outlook_handler,
            ),
            patch(
                "droid.gateway.channels.email.views.gmail_get_attachment",
                new=gmail_handler,
            ),
        ):
            client.get(
                "/email/attachment",
                params={
                    "receiver_email": "alice@unify.ai",
                    "message_id": "msg-1",
                    "attachment_id": "att-1",
                    "filename": "doc.pdf",
                },
            )
        outlook_handler.assert_awaited_once_with(
            user_email="alice@unify.ai",
            message_id="msg-1",
            attachment_id="att-1",
            filename="doc.pdf",
        )
        gmail_handler.assert_not_called()

    def test_routes_to_gmail_for_google_workspace_assistant_and_translates_arg_names(
        self,
        client: TestClient,
        _orchestra_credentials: None,
    ) -> None:
        """Gmail's handler uses gmail_message_id; outlook uses message_id.

        The dispatcher must translate the parameter name when delegating
        to gmail (the on-wire route parameter stays `message_id`).
        """
        outlook_handler = AsyncMock()
        gmail_handler = AsyncMock(return_value=b"gmail-bytes")
        with (
            patch(
                "droid.gateway.channels.email.views.lookup_assistant",
                new=AsyncMock(return_value={"email_provider": "google_workspace"}),
            ),
            patch(
                "droid.gateway.channels.email.views.get_outlook_attachment",
                new=outlook_handler,
            ),
            patch(
                "droid.gateway.channels.email.views.gmail_get_attachment",
                new=gmail_handler,
            ),
        ):
            client.get(
                "/email/attachment",
                params={
                    "receiver_email": "alice@unify.ai",
                    "message_id": "msg-1",
                    "attachment_id": "att-1",
                },
            )
        # gmail's handler uses receiver_email + gmail_message_id; the
        # dispatcher does the parameter-name translation here.
        gmail_handler.assert_awaited_once_with(
            receiver_email="alice@unify.ai",
            gmail_message_id="msg-1",
            attachment_id="att-1",
            filename=None,
        )
        outlook_handler.assert_not_called()

    def test_default_provider_routes_to_gmail(
        self,
        client: TestClient,
        _orchestra_credentials: None,
    ) -> None:
        gmail_handler = AsyncMock(return_value=b"x")
        with (
            patch(
                "droid.gateway.channels.email.views.lookup_assistant",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "droid.gateway.channels.email.views.gmail_get_attachment",
                new=gmail_handler,
            ),
        ):
            client.get(
                "/email/attachment",
                params={
                    "receiver_email": "alice@unify.ai",
                    "message_id": "msg-1",
                    "attachment_id": "att-1",
                },
            )
        gmail_handler.assert_awaited_once()

    def test_shared_coordinator_email_attachment_routes_to_gmail_without_lookup(
        self,
        client: TestClient,
        _orchestra_credentials: None,
    ) -> None:
        lookup = AsyncMock(side_effect=AssertionError("lookup should be skipped"))
        outlook_handler = AsyncMock()
        gmail_handler = AsyncMock(return_value=b"gmail-bytes")
        with (
            patch(
                "droid.gateway.channels.email.views.lookup_assistant",
                new=lookup,
            ),
            patch(
                "droid.gateway.channels.email.views.get_outlook_attachment",
                new=outlook_handler,
            ),
            patch(
                "droid.gateway.channels.email.views.gmail_get_attachment",
                new=gmail_handler,
            ),
        ):
            client.get(
                "/email/attachment",
                params={
                    "receiver_email": "marty@unify.ai",
                    "message_id": "msg-1",
                    "attachment_id": "att-1",
                    "filename": "doc.pdf",
                },
            )
        gmail_handler.assert_awaited_once_with(
            receiver_email="marty@unify.ai",
            gmail_message_id="msg-1",
            attachment_id="att-1",
            filename="doc.pdf",
        )
        outlook_handler.assert_not_called()
        lookup.assert_not_called()
