"""Behavioural tests for ``unify.gateway.channels.social``.

This is the reference test layout for the Phase B channel migration.
Other channels should mirror its structure: pin the router contract,
exercise each endpoint via FastAPI's ``TestClient``, mock the
underlying vendor SDK, and verify the missing-credential failure
mode.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from unify.gateway.channels.social import router
from unify.gateway.channels.social.views import (
    DEFAULT_WHATSAPP_VERIFICATION_FROM_NUMBER,
    MESSAGING_SERVICE_NAME,
    VerificationRequest,
    _get_messaging_service_sid,
    _reset_messaging_service_sid_cache,
)
from unify.gateway.common.twilio import build_twilio_client, build_twilio_wa_client
from unify.gateway.credentials import EnvCredentialStore

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_module_state():
    """Clear cached state between tests."""
    _reset_messaging_service_sid_cache()
    yield
    _reset_messaging_service_sid_cache()


@pytest.fixture
def app() -> FastAPI:
    """Mount the social router under the same prefix production uses."""
    app = FastAPI()
    app.include_router(router, prefix="/social")
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


@pytest.fixture
def _twilio_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide valid-looking Twilio credentials in the environment."""
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "ACtestaccountsid")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "test_auth_token")
    monkeypatch.setenv("TWILIO_WA_ACCOUNT_SID", "ACtestwaaccountsid")
    monkeypatch.setenv("TWILIO_WA_AUTH_TOKEN", "test_wa_auth_token")


# ---------------------------------------------------------------------------
# Router contract (the seam Phase B/C will rely on)
# ---------------------------------------------------------------------------


def test_router_exposes_expected_route_paths() -> None:
    """Pin the routes so an accidental rename surfaces immediately."""
    paths = sorted(
        (r.path, sorted(r.methods)) for r in router.routes  # type: ignore[attr-defined]
    )
    assert paths == [
        ("/available-platforms", ["GET"]),
        ("/verify", ["POST"]),
    ]


def test_router_is_importable_from_package_root() -> None:
    """Mirroring `from communication.social.views import router`."""
    from unify.gateway.channels.social import router as exported_router

    assert exported_router is router


# ---------------------------------------------------------------------------
# GET /available-platforms
# ---------------------------------------------------------------------------


def test_available_platforms_returns_documented_shape(client: TestClient) -> None:
    response = client.get("/social/available-platforms")
    assert response.status_code == 200
    assert response.json() == {
        "success": True,
        "platforms": {"whatsapp": 10.0},
    }


# ---------------------------------------------------------------------------
# POST /verify - request validation
# ---------------------------------------------------------------------------


def test_verify_request_rejects_unknown_platform(
    client: TestClient,
    _twilio_credentials: None,
) -> None:
    response = client.post(
        "/social/verify",
        json={"platform": "telegram", "account_identifier": "+15555550000"},
    )
    assert response.status_code == 400
    assert "not supported" in response.json()["detail"]


def test_verify_request_requires_platform_field(client: TestClient) -> None:
    response = client.post(
        "/social/verify",
        json={"account_identifier": "+15555550000"},
    )
    assert response.status_code == 422


def test_verify_request_requires_account_identifier_field(
    client: TestClient,
) -> None:
    response = client.post("/social/verify", json={"platform": "phone"})
    assert response.status_code == 422


def test_verification_request_model_pins_required_fields() -> None:
    schema = VerificationRequest.model_json_schema()
    assert set(schema["required"]) == {"platform", "account_identifier"}


# ---------------------------------------------------------------------------
# POST /verify - whatsapp path
# ---------------------------------------------------------------------------


def test_verify_whatsapp_sends_via_twilio_with_expected_args(
    client: TestClient,
    _twilio_credentials: None,
) -> None:
    fake_wa_client = MagicMock(name="TwilioWaClient")
    with patch(
        "unify.gateway.channels.social.views.build_twilio_wa_client",
        return_value=fake_wa_client,
    ):
        response = client.post(
            "/social/verify",
            json={
                "platform": "whatsapp",
                "account_identifier": "+15555550000",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert "verification_code" in body
    assert len(body["verification_code"]) == 6
    assert body["verification_code"].isdigit()
    assert "sent_at" in body

    fake_wa_client.messages.create.assert_called_once()
    call_kwargs = fake_wa_client.messages.create.call_args.kwargs
    assert call_kwargs["to"] == "whatsapp:+15555550000"
    assert (
        call_kwargs["from_"] == f"whatsapp:{DEFAULT_WHATSAPP_VERIFICATION_FROM_NUMBER}"
    )
    assert call_kwargs["content_sid"].startswith("HX")  # Twilio content template SID
    # The generated code is the same one returned in the response.
    import json as _json

    assert _json.loads(call_kwargs["content_variables"]) == {
        "1": body["verification_code"],
    }


def test_verify_whatsapp_uses_requested_sender_override(
    client: TestClient,
    _twilio_credentials: None,
) -> None:
    fake_wa_client = MagicMock(name="TwilioWaClient")
    with patch(
        "unify.gateway.channels.social.views.build_twilio_wa_client",
        return_value=fake_wa_client,
    ):
        response = client.post(
            "/social/verify",
            json={
                "platform": "whatsapp",
                "account_identifier": "+15555550000",
                "from_number": "whatsapp:+447414266034",
            },
        )

    assert response.status_code == 200
    fake_wa_client.messages.create.assert_called_once()
    call_kwargs = fake_wa_client.messages.create.call_args.kwargs
    assert call_kwargs["to"] == "whatsapp:+15555550000"
    assert call_kwargs["from_"] == "whatsapp:+447414266034"


def test_verify_whatsapp_propagates_twilio_failure_as_500(
    client: TestClient,
    _twilio_credentials: None,
) -> None:
    fake_wa_client = MagicMock(name="TwilioWaClient")
    fake_wa_client.messages.create.side_effect = RuntimeError("twilio down")
    with patch(
        "unify.gateway.channels.social.views.build_twilio_wa_client",
        return_value=fake_wa_client,
    ):
        response = client.post(
            "/social/verify",
            json={
                "platform": "whatsapp",
                "account_identifier": "+15555550000",
            },
        )

    assert response.status_code == 500
    assert "WhatsApp" in response.json()["detail"]


# ---------------------------------------------------------------------------
# POST /verify - phone path
# ---------------------------------------------------------------------------


def test_verify_phone_sends_via_twilio_messaging_service(
    client: TestClient,
    _twilio_credentials: None,
) -> None:
    fake_client = MagicMock(name="TwilioClient")
    # Mock the messaging service discovery to return our SID.
    fake_service = MagicMock()
    fake_service.friendly_name = MESSAGING_SERVICE_NAME
    fake_service.sid = "MGtestmessagingservicesid"
    fake_client.messaging.v1.services.list.return_value = [fake_service]

    with patch(
        "unify.gateway.channels.social.views.build_twilio_client",
        return_value=fake_client,
    ):
        response = client.post(
            "/social/verify",
            json={"platform": "phone", "account_identifier": "+15555550000"},
        )

    assert response.status_code == 200
    body = response.json()
    assert len(body["verification_code"]) == 6

    fake_client.messages.create.assert_called_once()
    call_kwargs = fake_client.messages.create.call_args.kwargs
    assert call_kwargs["to"] == "+15555550000"
    assert call_kwargs["messaging_service_sid"] == "MGtestmessagingservicesid"
    assert body["verification_code"] in call_kwargs["body"]
    assert "Your Unify verification code" in call_kwargs["body"]


def test_verify_phone_propagates_twilio_failure_as_500(
    client: TestClient,
    _twilio_credentials: None,
) -> None:
    fake_client = MagicMock(name="TwilioClient")
    fake_service = MagicMock()
    fake_service.friendly_name = MESSAGING_SERVICE_NAME
    fake_service.sid = "MGsid"
    fake_client.messaging.v1.services.list.return_value = [fake_service]
    fake_client.messages.create.side_effect = RuntimeError("twilio down")

    with patch(
        "unify.gateway.channels.social.views.build_twilio_client",
        return_value=fake_client,
    ):
        response = client.post(
            "/social/verify",
            json={"platform": "phone", "account_identifier": "+15555550000"},
        )

    assert response.status_code == 500
    assert "phone" in response.json()["detail"].lower()


def test_verify_phone_raises_when_messaging_service_missing(
    client: TestClient,
    _twilio_credentials: None,
) -> None:
    """If no MessagingService named 'Unity' exists, we 500 with a clear error."""
    fake_client = MagicMock(name="TwilioClient")
    fake_other = MagicMock()
    fake_other.friendly_name = "SomeOtherService"
    fake_other.sid = "MGother"
    fake_client.messaging.v1.services.list.return_value = [fake_other]

    with patch(
        "unify.gateway.channels.social.views.build_twilio_client",
        return_value=fake_client,
    ):
        response = client.post(
            "/social/verify",
            json={"platform": "phone", "account_identifier": "+15555550000"},
        )

    assert response.status_code == 500


# ---------------------------------------------------------------------------
# Credential resolution via CredentialStore
# ---------------------------------------------------------------------------


def testbuild_twilio_client_uses_credential_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "ACfromcredentialstore")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tokenfromcredentialstore")
    credentials = EnvCredentialStore()

    with patch("twilio.rest.Client") as MockClient:
        build_twilio_client(credentials)
    MockClient.assert_called_once_with(
        "ACfromcredentialstore",
        "tokenfromcredentialstore",
    )


def testbuild_twilio_client_raises_when_credentials_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TWILIO_ACCOUNT_SID", raising=False)
    monkeypatch.delenv("TWILIO_AUTH_TOKEN", raising=False)
    credentials = EnvCredentialStore()

    with pytest.raises(RuntimeError, match="TWILIO_ACCOUNT_SID"):
        build_twilio_client(credentials)


def testbuild_twilio_wa_client_uses_separate_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """WhatsApp credentials are distinct env vars; SMS creds shouldn't satisfy them."""
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "ACsms")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "smstoken")
    monkeypatch.delenv("TWILIO_WA_ACCOUNT_SID", raising=False)
    monkeypatch.delenv("TWILIO_WA_AUTH_TOKEN", raising=False)
    credentials = EnvCredentialStore()

    with pytest.raises(RuntimeError, match="TWILIO_WA_ACCOUNT_SID"):
        build_twilio_wa_client(credentials)


def test_get_messaging_service_sid_caches_result(
    _twilio_credentials: None,
) -> None:
    """First lookup hits Twilio; subsequent lookups return cached value."""
    credentials = EnvCredentialStore()
    fake_client = MagicMock(name="TwilioClient")
    fake_service = MagicMock()
    fake_service.friendly_name = MESSAGING_SERVICE_NAME
    fake_service.sid = "MGcached"
    fake_client.messaging.v1.services.list.return_value = [fake_service]

    with patch(
        "unify.gateway.channels.social.views.build_twilio_client",
        return_value=fake_client,
    ):
        sid1 = _get_messaging_service_sid(credentials)
        sid2 = _get_messaging_service_sid(credentials)

    assert sid1 == sid2 == "MGcached"
    # Twilio listing was only invoked once due to the cache.
    assert fake_client.messaging.v1.services.list.call_count == 1
