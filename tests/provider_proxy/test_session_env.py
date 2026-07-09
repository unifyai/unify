from __future__ import annotations

import os

from unify.provider_proxy import session as sess
from unify.provider_proxy.session import (
    ProxySession,
    build_sandbox_env,
    scrub_platform_secrets_from_environ,
)
from unify.secret_manager.secret_manager import SecretManager


def test_build_sandbox_env_strips_tokens_and_adds_proxy(monkeypatch):
    monkeypatch.setenv("MICROSOFT_ACCESS_TOKEN", "secret-ms")
    monkeypatch.setenv("GOOGLE_REFRESH_TOKEN", "secret-g")
    monkeypatch.setenv("PATH", "/usr/bin")
    sess.set_session(ProxySession(host="127.0.0.1", port=5555, nonce="N"))

    env = build_sandbox_env()

    assert "MICROSOFT_ACCESS_TOKEN" not in env
    assert "GOOGLE_REFRESH_TOKEN" not in env
    assert env["PATH"] == "/usr/bin"
    assert env["MICROSOFT_GRAPH_BASE"].endswith("/microsoft/v1.0")
    assert env["WORKSPACE_PROXY_TOKEN"] == "N"


def test_build_sandbox_env_strips_platform_secrets(monkeypatch):
    monkeypatch.setenv("ORCHESTRA_ADMIN_KEY", "super-secret")
    monkeypatch.setenv("SHARED_UNIFY_KEY", "shared-secret")
    monkeypatch.setattr(sess, "current_session", lambda: None)

    env = build_sandbox_env()

    assert "ORCHESTRA_ADMIN_KEY" not in env
    assert "SHARED_UNIFY_KEY" not in env


def test_scrub_platform_secrets_from_environ_restores(monkeypatch):
    monkeypatch.setenv("ORCHESTRA_ADMIN_KEY", "super-secret")
    monkeypatch.setenv("SHARED_UNIFY_KEY", "shared-secret")

    with scrub_platform_secrets_from_environ():
        assert "ORCHESTRA_ADMIN_KEY" not in os.environ
        assert "SHARED_UNIFY_KEY" not in os.environ

    assert os.environ["ORCHESTRA_ADMIN_KEY"] == "super-secret"
    assert os.environ["SHARED_UNIFY_KEY"] == "shared-secret"


def test_build_sandbox_env_without_session_still_strips_tokens(monkeypatch):
    monkeypatch.setenv("GOOGLE_ACCESS_TOKEN", "secret-g")
    monkeypatch.setattr(sess, "current_session", lambda: None)

    env = build_sandbox_env()

    assert "GOOGLE_ACCESS_TOKEN" not in env
    assert "WORKSPACE_PROXY_TOKEN" not in env


def test_secret_manager_sensitive_token_names_are_raw_tokens_only():
    names = SecretManager._sensitive_oauth_token_names()
    assert names == {
        "MICROSOFT_ACCESS_TOKEN",
        "MICROSOFT_REFRESH_TOKEN",
        "GOOGLE_ACCESS_TOKEN",
        "GOOGLE_REFRESH_TOKEN",
    }
