"""Tests for ``unify.gateway.common.twilio``.

Direct tests of the shared Twilio factories. The social channel's
test_views.py also exercises this surface indirectly; these tests
pin the behaviour at the module boundary so a future refactor
breaks here first.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from unify.gateway.common.twilio import build_twilio_client, build_twilio_wa_client
from unify.gateway.credentials import EnvCredentialStore


def test_build_twilio_client_uses_sms_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "ACtestsms")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "smstoken")
    credentials = EnvCredentialStore()

    with patch("twilio.rest.Client") as MockClient:
        build_twilio_client(credentials)
    MockClient.assert_called_once_with("ACtestsms", "smstoken")


def test_build_twilio_client_raises_with_clear_message_when_creds_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TWILIO_ACCOUNT_SID", raising=False)
    monkeypatch.delenv("TWILIO_AUTH_TOKEN", raising=False)
    credentials = EnvCredentialStore()

    with pytest.raises(RuntimeError, match="TWILIO_ACCOUNT_SID.*TWILIO_AUTH_TOKEN"):
        build_twilio_client(credentials)


def test_build_twilio_wa_client_uses_whatsapp_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TWILIO_WA_ACCOUNT_SID", "ACtestwa")
    monkeypatch.setenv("TWILIO_WA_AUTH_TOKEN", "watoken")
    credentials = EnvCredentialStore()

    with patch("twilio.rest.Client") as MockClient:
        build_twilio_wa_client(credentials)
    MockClient.assert_called_once_with("ACtestwa", "watoken")


def test_build_twilio_wa_client_sms_creds_do_not_satisfy_whatsapp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """WA credentials are distinct env vars; SMS creds must not silently substitute."""
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "ACsms")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "smstoken")
    monkeypatch.delenv("TWILIO_WA_ACCOUNT_SID", raising=False)
    monkeypatch.delenv("TWILIO_WA_AUTH_TOKEN", raising=False)
    credentials = EnvCredentialStore()

    with pytest.raises(
        RuntimeError,
        match="TWILIO_WA_ACCOUNT_SID.*TWILIO_WA_AUTH_TOKEN",
    ):
        build_twilio_wa_client(credentials)


def test_factories_use_supplied_credential_store_not_global_env_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The factories must read from the *supplied* store, not bypass to os.environ.

    Pinning this means a future hosted deploy can pass a
    GcpCredentialStore instance and the factory will honour it
    without needing process env vars set.
    """
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "AC_from_process_env_should_not_be_used")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "process_env_token")

    # Restrict the credential store to a non-overlapping prefix so it
    # cannot see TWILIO_*; the factory should then fail rather than
    # falling back to os.environ directly.
    restricted = EnvCredentialStore(prefix="NEVER_MATCHES_")
    with pytest.raises(RuntimeError, match="TWILIO_ACCOUNT_SID"):
        build_twilio_client(restricted)
