"""Behavioural tests for ``EnvSecretManager``."""

from __future__ import annotations

import pytest

from unity.gateway.secrets import (
    EnvSecretManager,
    SecretManager,
    SecretNotFoundError,
)


def test_env_secret_manager_satisfies_secret_manager_protocol() -> None:
    assert isinstance(EnvSecretManager(), SecretManager)


def test_get_returns_value_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("UNITY_TEST_SECRET", "value-1")
    sm = EnvSecretManager()
    assert sm.get("UNITY_TEST_SECRET") == "value-1"


def test_get_raises_for_missing_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("UNITY_GATEWAY_DEFINITELY_MISSING", raising=False)
    sm = EnvSecretManager()
    with pytest.raises(SecretNotFoundError):
        sm.get("UNITY_GATEWAY_DEFINITELY_MISSING")


def test_get_optional_returns_default_for_missing_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("UNITY_GATEWAY_DEFINITELY_MISSING_2", raising=False)
    sm = EnvSecretManager()
    assert sm.get_optional("UNITY_GATEWAY_DEFINITELY_MISSING_2") == ""
    assert (
        sm.get_optional("UNITY_GATEWAY_DEFINITELY_MISSING_2", "fallback") == "fallback"
    )


def test_set_writes_through_to_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("UNITY_GATEWAY_SET_TEST", raising=False)
    sm = EnvSecretManager()
    sm.set("UNITY_GATEWAY_SET_TEST", "fresh")
    assert sm.get("UNITY_GATEWAY_SET_TEST") == "fresh"


def test_prefix_restricts_visibility_for_get(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("UNITY_GATEWAY_VISIBLE", "yes")
    monkeypatch.setenv("OTHER_HIDDEN", "no")
    sm = EnvSecretManager(prefix="UNITY_GATEWAY_")
    assert sm.get("UNITY_GATEWAY_VISIBLE") == "yes"
    with pytest.raises(SecretNotFoundError):
        sm.get("OTHER_HIDDEN")


def test_prefix_restricts_visibility_for_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sm = EnvSecretManager(prefix="UNITY_GATEWAY_")
    with pytest.raises(SecretNotFoundError):
        sm.set("OTHER_HIDDEN", "value")


def test_list_names_with_prefix_returns_only_matching_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("UNITY_GATEWAY_A", "1")
    monkeypatch.setenv("UNITY_GATEWAY_B", "2")
    monkeypatch.setenv("UNRELATED_C", "3")
    sm = EnvSecretManager(prefix="UNITY_GATEWAY_")
    names = sm.list_names()
    assert "UNITY_GATEWAY_A" in names
    assert "UNITY_GATEWAY_B" in names
    assert "UNRELATED_C" not in names


def test_list_names_without_prefix_returns_full_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("UNITY_GATEWAY_LIST_ALL", "1")
    sm = EnvSecretManager()
    assert "UNITY_GATEWAY_LIST_ALL" in sm.list_names()
