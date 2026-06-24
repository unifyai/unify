"""Tests for ``unity.gateway.common.livekit``.

Direct tests of the shared LiveKit helpers. Pin the credential
resolution + SIP URI construction + dispatch-rule semantics that
the phone (and upcoming whatsapp / teams) channel migrations rely
on.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unity.gateway.common.livekit import (
    get_livekit_api,
    make_sip_uri,
)
from unity.gateway.credentials import EnvCredentialStore

# ---------------------------------------------------------------------------
# get_livekit_api -- credential resolution
# ---------------------------------------------------------------------------


def test_get_livekit_api_uses_supplied_credential_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    monkeypatch.setenv("LIVEKIT_API_KEY", "API_test_key")
    monkeypatch.setenv("LIVEKIT_API_SECRET", "test_secret")
    credentials = EnvCredentialStore()

    with patch("unity.gateway.common.livekit.LiveKitAPI") as MockAPI:
        get_livekit_api(credentials)
    MockAPI.assert_called_once_with(
        url="wss://test.livekit.cloud",
        api_key="API_test_key",
        api_secret="test_secret",
    )


def test_get_livekit_api_raises_when_any_credential_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    monkeypatch.setenv("LIVEKIT_API_KEY", "API_test_key")
    monkeypatch.delenv("LIVEKIT_API_SECRET", raising=False)
    credentials = EnvCredentialStore()

    with pytest.raises(RuntimeError, match="LIVEKIT_API_SECRET"):
        get_livekit_api(credentials)


# ---------------------------------------------------------------------------
# make_sip_uri -- the contract communication/tests/phone/test_send_call.py
# encoded as expectations, ported here for the new home
# ---------------------------------------------------------------------------


def test_make_sip_uri_uses_e164_phone_number_as_user_part(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LiveKit trunk matching requires the user part to be the E.164 number."""
    monkeypatch.setenv("LIVEKIT_SIP_URI", "test.sip.livekit.cloud")
    credentials = EnvCredentialStore()
    assert (
        make_sip_uri("+12526595494", credentials)
        == "sip:+12526595494@test.sip.livekit.cloud"
    )


def test_make_sip_uri_preserves_e164_plus_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LIVEKIT_SIP_URI", "test.sip.livekit.cloud")
    credentials = EnvCredentialStore()
    assert (
        make_sip_uri("+447427857991", credentials)
        == "sip:+447427857991@test.sip.livekit.cloud"
    )


def test_make_sip_uri_adds_plus_prefix_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defensive normalisation: a raw number without + still produces a valid URI."""
    monkeypatch.setenv("LIVEKIT_SIP_URI", "test.sip.livekit.cloud")
    credentials = EnvCredentialStore()
    assert (
        make_sip_uri("12526595494", credentials)
        == "sip:+12526595494@test.sip.livekit.cloud"
    )


def test_make_sip_uri_with_empty_sip_domain_returns_uri_with_empty_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Match legacy behaviour: missing LIVEKIT_SIP_URI -> empty host (not error)."""
    monkeypatch.delenv("LIVEKIT_SIP_URI", raising=False)
    credentials = EnvCredentialStore()
    assert make_sip_uri("+12526595494", credentials) == "sip:+12526595494@"


# ---------------------------------------------------------------------------
# ensure_phone_dispatch_rule -- idempotency + best-effort behaviour
# ---------------------------------------------------------------------------


@pytest.fixture
def _livekit_credentials(monkeypatch: pytest.MonkeyPatch) -> EnvCredentialStore:
    monkeypatch.setenv("LIVEKIT_URL", "wss://test.livekit.cloud")
    monkeypatch.setenv("LIVEKIT_API_KEY", "API_test")
    monkeypatch.setenv("LIVEKIT_API_SECRET", "secret_test")
    monkeypatch.setenv("LIVEKIT_SIP_URI", "test.sip.livekit.cloud")
    return EnvCredentialStore()


def _fake_livekit_api() -> MagicMock:
    api = MagicMock(name="LiveKitAPI")
    api.aclose = AsyncMock()
    api.sip = MagicMock()
    api.sip.list_sip_inbound_trunk = AsyncMock()
    api.sip.list_sip_dispatch_rule = AsyncMock()
    api.sip.create_sip_dispatch_rule = AsyncMock()
    api.sip.delete_sip_dispatch_rule = AsyncMock()
    return api


@pytest.mark.asyncio
async def test_ensure_phone_dispatch_rule_creates_rule_when_no_match(
    _livekit_credentials: EnvCredentialStore,
) -> None:
    from unity.gateway.common.livekit import ensure_phone_dispatch_rule

    api = _fake_livekit_api()
    trunk = MagicMock(numbers=["+12526595494"], sip_trunk_id="ST_trunk1")
    api.sip.list_sip_inbound_trunk.return_value = MagicMock(items=[trunk])
    api.sip.list_sip_dispatch_rule.return_value = MagicMock(items=[])

    with patch(
        "unity.gateway.common.livekit.get_livekit_api",
        return_value=api,
    ):
        await ensure_phone_dispatch_rule(
            "+12526595494",
            "unity_42_phone",
            _livekit_credentials,
        )

    api.sip.create_sip_dispatch_rule.assert_called_once()
    api.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_phone_dispatch_rule_skips_when_matching_rule_exists(
    _livekit_credentials: EnvCredentialStore,
) -> None:
    """Idempotent: a matching rule means no action needed."""
    from unity.gateway.common.livekit import ensure_phone_dispatch_rule

    api = _fake_livekit_api()
    trunk = MagicMock(numbers=["+12526595494"], sip_trunk_id="ST_trunk1")
    api.sip.list_sip_inbound_trunk.return_value = MagicMock(items=[trunk])

    existing_rule = MagicMock(trunk_ids=["ST_trunk1"])
    existing_rule.rule.HasField.return_value = True
    existing_rule.rule.dispatch_rule_direct.room_name = "unity_42_phone"
    api.sip.list_sip_dispatch_rule.return_value = MagicMock(items=[existing_rule])

    with patch(
        "unity.gateway.common.livekit.get_livekit_api",
        return_value=api,
    ):
        await ensure_phone_dispatch_rule(
            "+12526595494",
            "unity_42_phone",
            _livekit_credentials,
        )

    api.sip.create_sip_dispatch_rule.assert_not_called()
    api.sip.delete_sip_dispatch_rule.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_phone_dispatch_rule_replaces_stale_rule(
    _livekit_credentials: EnvCredentialStore,
) -> None:
    """Stale rule (room name changed) is deleted, then a fresh one is created."""
    from unity.gateway.common.livekit import ensure_phone_dispatch_rule

    api = _fake_livekit_api()
    trunk = MagicMock(numbers=["+12526595494"], sip_trunk_id="ST_trunk1")
    api.sip.list_sip_inbound_trunk.return_value = MagicMock(items=[trunk])

    stale_rule = MagicMock(trunk_ids=["ST_trunk1"], sip_dispatch_rule_id="SR_stale")
    stale_rule.rule.HasField.return_value = True
    stale_rule.rule.dispatch_rule_direct.room_name = "unity_OLD_phone"  # stale
    api.sip.list_sip_dispatch_rule.return_value = MagicMock(items=[stale_rule])

    with patch(
        "unity.gateway.common.livekit.get_livekit_api",
        return_value=api,
    ):
        await ensure_phone_dispatch_rule(
            "+12526595494",
            "unity_NEW_phone",
            _livekit_credentials,
        )

    api.sip.delete_sip_dispatch_rule.assert_awaited_once_with("SR_stale")
    api.sip.create_sip_dispatch_rule.assert_called_once()


@pytest.mark.asyncio
async def test_ensure_phone_dispatch_rule_skips_when_no_inbound_trunk(
    _livekit_credentials: EnvCredentialStore,
) -> None:
    """No inbound trunk -> skip creation entirely (don't 500 the outbound flow)."""
    from unity.gateway.common.livekit import ensure_phone_dispatch_rule

    api = _fake_livekit_api()
    api.sip.list_sip_inbound_trunk.return_value = MagicMock(items=[])

    with patch(
        "unity.gateway.common.livekit.get_livekit_api",
        return_value=api,
    ):
        await ensure_phone_dispatch_rule(
            "+12526595494",
            "unity_42_phone",
            _livekit_credentials,
        )

    api.sip.create_sip_dispatch_rule.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_phone_dispatch_rule_swallows_livekit_errors(
    _livekit_credentials: EnvCredentialStore,
) -> None:
    """Best-effort: LiveKit failures don't propagate (caller's outbound flow keeps going)."""
    from unity.gateway.common.livekit import ensure_phone_dispatch_rule

    api = _fake_livekit_api()
    api.sip.list_sip_inbound_trunk.side_effect = RuntimeError("livekit down")

    with patch(
        "unity.gateway.common.livekit.get_livekit_api",
        return_value=api,
    ):
        await ensure_phone_dispatch_rule(
            "+12526595494",
            "unity_42_phone",
            _livekit_credentials,
        )

    api.aclose.assert_awaited_once()


# ---------------------------------------------------------------------------
# create_room_and_dispatch_agent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_room_and_dispatch_agent_passes_room_and_agent_through(
    _livekit_credentials: EnvCredentialStore,
) -> None:
    from unity.gateway.common.livekit import create_room_and_dispatch_agent

    api = _fake_livekit_api()
    api.agent_dispatch = MagicMock()
    api.agent_dispatch.create_dispatch = AsyncMock(
        return_value=MagicMock(id="DISPATCH_123"),
    )

    with patch(
        "unity.gateway.common.livekit.get_livekit_api",
        return_value=api,
    ):
        await create_room_and_dispatch_agent(
            "unity_42_phone",
            "unity_42_phone",
            _livekit_credentials,
        )

    api.agent_dispatch.create_dispatch.assert_awaited_once()
    api.aclose.assert_awaited_once()
