"""Behavioural tests for ``unify.gateway.channels.phone``.

Includes the 4 scenarios faithfully ported from
``communication/tests/phone/test_send_call.py`` (the only existing
phone tests in the source repo) plus router-contract and
per-endpoint tests for the other 9 endpoints. The ported scenarios
are tagged in their docstrings so the institutional knowledge
they encode (SIP URI shape, E.164 normalisation, TwiML URL format)
stays attributable.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from unify.conversation_manager.domains import comms_utils as _comms_utils  # noqa: F401
from unify.gateway.channels.phone import auth_router, unauth_router

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _phone_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "ACtestaccountsid")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "test_auth_token")
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    monkeypatch.setenv("LIVEKIT_API_KEY", "test_lk_key")
    monkeypatch.setenv("LIVEKIT_API_SECRET", "test_lk_secret")
    monkeypatch.setenv("LIVEKIT_SIP_URI", "test.sip.livekit.cloud")


@pytest.fixture
def _settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin SETTINGS.conversation.COMMS_URL / ADAPTERS_URL for URL-shape assertions."""
    from unify.gateway.channels.phone import views as phone_views

    stub = SimpleNamespace(
        conversation=SimpleNamespace(
            COMMS_URL="https://comms.example.com",
            ADAPTERS_URL="https://adapters.example.com",
        ),
    )
    monkeypatch.setattr(phone_views, "SETTINGS", stub)


@pytest.fixture
def app() -> FastAPI:
    """Mount both routers at the same prefix the aggregator will use."""
    app = FastAPI()
    app.include_router(auth_router, prefix="/phone")
    app.include_router(unauth_router, prefix="/phone")
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


# ---------------------------------------------------------------------------
# Router contract
# ---------------------------------------------------------------------------


def test_auth_router_exposes_expected_paths() -> None:
    paths = sorted(
        (r.path, sorted(r.methods)) for r in auth_router.routes  # type: ignore[attr-defined]
    )
    assert paths == [
        ("/available-countries", ["GET"]),
        ("/create", ["POST"]),
        ("/delete", ["DELETE"]),
        ("/dispatch-livekit-agent", ["POST"]),
        ("/end-conference", ["POST"]),
        ("/hang-up", ["POST"]),
        ("/hang-up-call", ["POST"]),
        ("/send-call", ["POST"]),
        ("/send-text", ["POST"]),
    ]


def test_unauth_router_exposes_expected_paths() -> None:
    paths = sorted(
        (r.path, sorted(r.methods)) for r in unauth_router.routes  # type: ignore[attr-defined]
    )
    assert paths == [
        ("/conference-status", ["POST"]),
        ("/twiml", ["POST"]),
    ]


def test_routers_are_importable_from_package_root() -> None:
    from unify.gateway.channels.phone import auth_router as a, unauth_router as u

    assert a is auth_router
    assert u is unauth_router


# ---------------------------------------------------------------------------
# POST /send-call -- 4 scenarios ported from communication/tests/phone/test_send_call.py
# ---------------------------------------------------------------------------


def _build_send_call_mocks(call_sid: str = "CA_test_call_sid") -> MagicMock:
    mock_client = MagicMock(name="TwilioClient")
    mock_call = MagicMock()
    mock_call.sid = call_sid
    mock_client.calls.create.return_value = mock_call
    return mock_client


def test_send_call_sip_uri_uses_phone_number(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    """PORTED: The SIP URI user part is the E.164 From number (trunk matching)."""
    mock_twilio = _build_send_call_mocks()
    with (
        patch(
            "unify.gateway.channels.phone.views.build_twilio_client",
            return_value=mock_twilio,
        ),
        patch(
            "unify.gateway.channels.phone.views.ensure_phone_dispatch_rule",
            new=AsyncMock(),
        ),
    ):
        resp = client.post(
            "/phone/send-call",
            json={
                "From": "+12526595494",
                "To": "+19206146850",
                "room_name": "unity_568_phone",
            },
        )

    assert resp.status_code == 200
    sip_to = mock_twilio.calls.create.call_args.kwargs.get("to")
    assert sip_to.startswith("sip:+12526595494@"), sip_to
    assert sip_to.endswith(".sip.livekit.cloud")


def test_send_call_twiml_url_contains_phone_number(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    """PORTED: The TwiML URL must reference the recipient's phone number."""
    mock_twilio = _build_send_call_mocks()
    with (
        patch(
            "unify.gateway.channels.phone.views.build_twilio_client",
            return_value=mock_twilio,
        ),
        patch(
            "unify.gateway.channels.phone.views.ensure_phone_dispatch_rule",
            new=AsyncMock(),
        ),
    ):
        resp = client.post(
            "/phone/send-call",
            json={
                "From": "+12526595494",
                "To": "+19206146850",
                "room_name": "unity_568_phone",
            },
        )

    assert resp.status_code == 200
    twiml_url = mock_twilio.calls.create.call_args.kwargs.get("url")
    assert "phone_number=+19206146850" in twiml_url
    assert "comms.example.com/phone/twiml" in twiml_url


def test_send_call_returns_call_sid(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    """PORTED: Response carries the Twilio call SID."""
    mock_twilio = _build_send_call_mocks()
    with (
        patch(
            "unify.gateway.channels.phone.views.build_twilio_client",
            return_value=mock_twilio,
        ),
        patch(
            "unify.gateway.channels.phone.views.ensure_phone_dispatch_rule",
            new=AsyncMock(),
        ),
    ):
        resp = client.post(
            "/phone/send-call",
            json={
                "From": "+12526595494",
                "To": "+19206146850",
                "room_name": "unity_568_phone",
            },
        )

    data = resp.json()
    assert data["success"] is True
    assert data["call_sid"] == "CA_test_call_sid"


def test_send_call_sip_uri_uses_e164_format(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    """PORTED: The SIP URI preserves E.164 format with the + prefix."""
    mock_twilio = _build_send_call_mocks()
    with (
        patch(
            "unify.gateway.channels.phone.views.build_twilio_client",
            return_value=mock_twilio,
        ),
        patch(
            "unify.gateway.channels.phone.views.ensure_phone_dispatch_rule",
            new=AsyncMock(),
        ),
    ):
        resp = client.post(
            "/phone/send-call",
            json={
                "From": "+447427857991",
                "To": "+442012345678",
                "room_name": "unity_42_phone",
            },
        )

    assert resp.status_code == 200
    sip_to = mock_twilio.calls.create.call_args.kwargs.get("to")
    assert sip_to == "sip:+447427857991@test.sip.livekit.cloud"


def test_send_call_invokes_ensure_phone_dispatch_rule_with_twilio_number_and_room(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    """Routing setup happens before the outbound call dispatch."""
    mock_twilio = _build_send_call_mocks()
    mock_ensure = AsyncMock()
    with (
        patch(
            "unify.gateway.channels.phone.views.build_twilio_client",
            return_value=mock_twilio,
        ),
        patch(
            "unify.gateway.channels.phone.views.ensure_phone_dispatch_rule",
            new=mock_ensure,
        ),
    ):
        client.post(
            "/phone/send-call",
            json={
                "From": "+12526595494",
                "To": "+19206146850",
                "room_name": "unity_42_phone",
            },
        )

    assert mock_ensure.await_count == 1
    args, kwargs = mock_ensure.await_args
    # Signature is (twilio_number, room_name, credentials)
    assert args[0] == "+12526595494"
    assert args[1] == "unity_42_phone"


# ---------------------------------------------------------------------------
# POST /send-text
# ---------------------------------------------------------------------------


def test_send_text_invokes_twilio_messages_create_with_supplied_fields(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    mock_twilio = MagicMock()
    with patch(
        "unify.gateway.channels.phone.views.build_twilio_client",
        return_value=mock_twilio,
    ):
        resp = client.post(
            "/phone/send-text",
            json={
                "From": "+12526595494",
                "To": "+19206146850",
                "Body": "hello from unit test",
            },
        )

    assert resp.status_code == 200
    assert resp.json() == {"success": True}
    mock_twilio.messages.create.assert_called_once_with(
        to="+19206146850",
        from_="+12526595494",
        body="hello from unit test",
    )


# ---------------------------------------------------------------------------
# POST /dispatch-livekit-agent
# ---------------------------------------------------------------------------


def test_dispatch_livekit_agent_calls_create_room_with_room_name(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    mock_create = AsyncMock()
    with patch(
        "unify.gateway.channels.phone.views.create_room_and_dispatch_agent",
        new=mock_create,
    ):
        resp = client.post(
            "/phone/dispatch-livekit-agent",
            json={"room_name": "unity_42_phone"},
        )

    assert resp.status_code == 200
    assert resp.json() == {"success": True}
    args, _ = mock_create.await_args
    assert args[0] == "unity_42_phone"
    assert args[1] == "unity_42_phone"  # agent_name == room_name


def test_dispatch_livekit_agent_falls_back_to_legacy_livekit_agent_name(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    """Backward-compat: older callers send `livekit_agent_name`."""
    mock_create = AsyncMock()
    with patch(
        "unify.gateway.channels.phone.views.create_room_and_dispatch_agent",
        new=mock_create,
    ):
        resp = client.post(
            "/phone/dispatch-livekit-agent",
            json={"livekit_agent_name": "legacy_agent_name"},
        )

    assert resp.status_code == 200
    args, _ = mock_create.await_args
    assert args[0] == "legacy_agent_name"


# ---------------------------------------------------------------------------
# GET /available-countries
# ---------------------------------------------------------------------------


def test_available_countries_returns_documented_list(client: TestClient) -> None:
    resp = client.get("/phone/available-countries")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    for code in ("US", "GB", "AU", "CA", "FI", "NL", "PR", "TH", "PL"):
        assert code in body["countries"]


# ---------------------------------------------------------------------------
# DELETE /delete
# ---------------------------------------------------------------------------


def test_delete_phone_number_missing_phone_field_returns_400(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    resp = client.request("DELETE", "/phone/delete", json={})
    assert resp.status_code == 400
    assert "PhoneNumber" in resp.json()["detail"]


def test_delete_phone_number_idempotent_when_already_absent(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    """Pre-existing contract: a number already deleted in Twilio still 200s."""
    mock_twilio = MagicMock()
    mock_twilio.incoming_phone_numbers.list.return_value = []

    with (
        patch(
            "unify.gateway.channels.phone.views.build_twilio_client",
            return_value=mock_twilio,
        ),
        patch(
            "unify.gateway.channels.phone.views._delete_sip_trunk_for_phone_number",
            new=AsyncMock(return_value=False),
        ),
    ):
        resp = client.request(
            "DELETE",
            "/phone/delete",
            json={"PhoneNumber": "+15555550000"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["already_absent"] is True
    assert body["deleted"] is False


def test_delete_phone_number_returns_deleted_true_when_twilio_succeeds(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    mock_twilio = MagicMock()
    listed = MagicMock(sid="PN_test_sid")
    mock_twilio.incoming_phone_numbers.list.return_value = [listed]

    with (
        patch(
            "unify.gateway.channels.phone.views.build_twilio_client",
            return_value=mock_twilio,
        ),
        patch(
            "unify.gateway.channels.phone.views._delete_sip_trunk_for_phone_number",
            new=AsyncMock(return_value=True),
        ),
    ):
        resp = client.request(
            "DELETE",
            "/phone/delete",
            json={"PhoneNumber": "+15555550000"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["sid"] == "PN_test_sid"
    assert body["deleted"] is True


# ---------------------------------------------------------------------------
# POST /twiml (unauth)
# ---------------------------------------------------------------------------


def test_twiml_returns_xml_with_dial_to_recipient_phone_number(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    resp = client.post(
        "/phone/twiml?phone_number=15555550000",
        data={"From": "+12526595494"},
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "text/xml; charset=utf-8"
    body = resp.text
    assert "+15555550000" in body
    assert "+12526595494" in body  # caller_id
    assert "adapters.example.com/twilio/call-status" in body


def test_twiml_returns_400_when_phone_number_query_missing(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    resp = client.post("/phone/twiml", data={"From": "+12526595494"})
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /conference-status (unauth)
# ---------------------------------------------------------------------------


def test_conference_status_unmutes_all_participants_on_end_event(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    """On conference end, all remaining participants get unmuted."""
    mock_twilio = MagicMock()
    p1 = MagicMock(sid="PA_one")
    p2 = MagicMock(sid="PA_two")
    mock_twilio.conferences.return_value.participants.list.return_value = [p1, p2]

    with patch(
        "unify.gateway.channels.phone.views.build_twilio_client",
        return_value=mock_twilio,
    ):
        resp = client.post(
            "/phone/conference-status",
            data={
                "StatusCallbackEvent": "end",
                "ConferenceSid": "CF_test_conf",
            },
        )

    assert resp.status_code == 200
    # Two unmute calls expected (one per participant).
    update_calls = [
        c
        for c in mock_twilio.conferences.return_value.participants.return_value.update.call_args_list
    ]
    # Two updates, both with muted=False.
    assert len(update_calls) == 2
    for call in update_calls:
        assert call.kwargs == {"muted": False}


def test_conference_status_no_op_on_non_end_event(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    mock_twilio = MagicMock()
    with patch(
        "unify.gateway.channels.phone.views.build_twilio_client",
        return_value=mock_twilio,
    ):
        resp = client.post(
            "/phone/conference-status",
            data={
                "StatusCallbackEvent": "start",
                "ConferenceSid": "CF_test_conf",
            },
        )

    assert resp.status_code == 200
    # No participant list lookup happens for non-end events.
    mock_twilio.conferences.return_value.participants.list.assert_not_called()


# ---------------------------------------------------------------------------
# POST /end-conference + /hang-up (sanity)
# ---------------------------------------------------------------------------


def test_end_conference_marks_conference_completed(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    mock_twilio = MagicMock()
    conf = MagicMock(sid="CF_test")
    mock_twilio.conferences.list.return_value = [conf]
    mock_twilio.conferences.return_value.update.return_value = MagicMock(
        status="completed",
    )
    with patch(
        "unify.gateway.channels.phone.views.build_twilio_client",
        return_value=mock_twilio,
    ):
        resp = client.post(
            "/phone/end-conference",
            json={"ConferenceName": "Unity_+12526595494_2026"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body == {"success": True, "status": "completed"}
    mock_twilio.conferences.return_value.update.assert_called_once_with(
        status="completed",
    )


def test_hang_up_removes_participant_from_active_conference(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    mock_twilio = MagicMock()
    conf = MagicMock(sid="CF_test")
    mock_twilio.conferences.list.return_value = [conf]
    with patch(
        "unify.gateway.channels.phone.views.build_twilio_client",
        return_value=mock_twilio,
    ):
        resp = client.post(
            "/phone/hang-up",
            json={"CallSid": "CA_user", "ConferenceName": "Unity_test"},
        )

    assert resp.status_code == 200
    mock_twilio.conferences.return_value.participants.return_value.delete.assert_called_once()


def test_hang_up_call_completes_call_by_sid(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    """/hang-up-call completes the given Twilio call SID (outbound teardown)."""
    mock_twilio = MagicMock()
    with patch(
        "unify.gateway.channels.phone.views.build_twilio_client",
        return_value=mock_twilio,
    ):
        resp = client.post(
            "/phone/hang-up-call",
            json={"CallSid": "CA_outbound"},
        )

    assert resp.status_code == 200
    assert resp.json() == {"success": True}
    mock_twilio.calls.assert_called_once_with("CA_outbound")
    mock_twilio.calls.return_value.update.assert_called_once_with(status="completed")


def test_hang_up_call_missing_sid_returns_400(
    client: TestClient,
    _phone_credentials: None,
    _settings: None,
) -> None:
    resp = client.post("/phone/hang-up-call", json={})
    assert resp.status_code == 400
