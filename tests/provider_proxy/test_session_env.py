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


def test_build_sandbox_env_strips_provider_billing_credentials(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-v1-billing")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-billing")
    monkeypatch.setenv("ELEVEN_API_KEY", "eleven-billing")
    monkeypatch.setattr(sess, "current_session", lambda: None)

    env = build_sandbox_env()

    assert "OPENROUTER_API_KEY" not in env
    assert "ANTHROPIC_API_KEY" not in env
    assert "ELEVEN_API_KEY" not in env


def test_llm_billing_keys_are_withheld_in_process(monkeypatch):
    """User code must not be able to read the LLM billing credentials.

    These are the keys worth stealing: one pod holds a single set for every
    assistant it runs, so a copy is an uncapped spending instrument usable
    from anywhere, and the resulting spend is attributable to nobody. UniLLM
    now sends them with each request from settings captured at import, so
    nothing resolves them from the environment mid-call and withholding them
    here cannot strand inference in flight.
    """
    billing_key = "sk-or-v1-billing"  # pragma: allowlist secret
    monkeypatch.setenv("OPENROUTER_API_KEY", billing_key)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-billing")
    monkeypatch.setenv("ORCHESTRA_ADMIN_KEY", "super-secret")

    with scrub_platform_secrets_from_environ():
        assert "ORCHESTRA_ADMIN_KEY" not in os.environ
        assert "OPENROUTER_API_KEY" not in os.environ
        assert "ANTHROPIC_API_KEY" not in os.environ

    assert os.environ["OPENROUTER_API_KEY"] == billing_key


def test_credentials_read_from_the_environ_mid_call_are_left_alone(monkeypatch):
    """Only the keys UniLLM sends explicitly may be withheld.

    Speech and telephony SDKs still resolve their credentials at call time,
    so removing these would break a transcription or voice call that happened
    to overlap with user code. They stay stripped from subprocess sandboxes,
    where no such overlap exists.
    """
    speech = "dg-live"  # pragma: allowlist secret
    telephony = "lk-live"  # pragma: allowlist secret
    monkeypatch.setenv("DEEPGRAM_API_KEY", speech)
    monkeypatch.setenv("LIVEKIT_API_SECRET", telephony)

    with scrub_platform_secrets_from_environ():
        assert os.environ["DEEPGRAM_API_KEY"] == speech
        assert os.environ["LIVEKIT_API_SECRET"] == telephony


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
