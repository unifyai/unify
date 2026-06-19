"""Behavioural tests for ``EnvCredentialStore``."""

from __future__ import annotations

import pytest

from droid.gateway.credentials import (
    CredentialNotFoundError,
    CredentialStore,
    EnvCredentialStore,
)


def test_env_credential_store_satisfies_credential_store_protocol() -> None:
    assert isinstance(EnvCredentialStore(), CredentialStore)


def test_get_returns_value_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DROID_TEST_CREDENTIAL", "value-1")
    store = EnvCredentialStore()
    assert store.get("DROID_TEST_CREDENTIAL") == "value-1"


def test_get_raises_for_missing_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DROID_GATEWAY_DEFINITELY_MISSING", raising=False)
    store = EnvCredentialStore()
    with pytest.raises(CredentialNotFoundError):
        store.get("DROID_GATEWAY_DEFINITELY_MISSING")


def test_get_optional_returns_default_for_missing_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DROID_GATEWAY_DEFINITELY_MISSING_2", raising=False)
    store = EnvCredentialStore()
    assert store.get_optional("DROID_GATEWAY_DEFINITELY_MISSING_2") == ""
    assert (
        store.get_optional("DROID_GATEWAY_DEFINITELY_MISSING_2", "fallback")
        == "fallback"
    )


def test_set_writes_through_to_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DROID_GATEWAY_SET_TEST", raising=False)
    store = EnvCredentialStore()
    store.set("DROID_GATEWAY_SET_TEST", "fresh")
    assert store.get("DROID_GATEWAY_SET_TEST") == "fresh"


def test_prefix_restricts_visibility_for_get(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DROID_GATEWAY_VISIBLE", "yes")
    monkeypatch.setenv("OTHER_HIDDEN", "no")
    store = EnvCredentialStore(prefix="DROID_GATEWAY_")
    assert store.get("DROID_GATEWAY_VISIBLE") == "yes"
    with pytest.raises(CredentialNotFoundError):
        store.get("OTHER_HIDDEN")


def test_prefix_restricts_visibility_for_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = EnvCredentialStore(prefix="DROID_GATEWAY_")
    with pytest.raises(CredentialNotFoundError):
        store.set("OTHER_HIDDEN", "value")


def test_list_names_with_prefix_returns_only_matching_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DROID_GATEWAY_A", "1")
    monkeypatch.setenv("DROID_GATEWAY_B", "2")
    monkeypatch.setenv("UNRELATED_C", "3")
    store = EnvCredentialStore(prefix="DROID_GATEWAY_")
    names = store.list_names()
    assert "DROID_GATEWAY_A" in names
    assert "DROID_GATEWAY_B" in names
    assert "UNRELATED_C" not in names


def test_list_names_without_prefix_returns_full_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DROID_GATEWAY_LIST_ALL", "1")
    store = EnvCredentialStore()
    assert "DROID_GATEWAY_LIST_ALL" in store.list_names()
