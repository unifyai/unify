"""The broker-origin resolver: present only when a sidecar and a nonce both are.

Three call sites (voice, Tavily, Recall) fall back to their own env key when
this returns None, so a wrong answer here silently sends a real key nowhere or a
nonce to a provider that rejects it -- hence the check.
"""

from __future__ import annotations

from unify.common.broker import broker_origin


def test_none_without_a_gateway(monkeypatch):
    monkeypatch.delenv("UNILLM_LLM_GATEWAY_URL", raising=False)
    monkeypatch.setenv("UNIFY_KEY", "K")
    assert broker_origin() is None


def test_none_without_a_unify_key(monkeypatch):
    monkeypatch.setenv("UNILLM_LLM_GATEWAY_URL", "http://127.0.0.1:8787/llm")
    monkeypatch.delenv("UNIFY_KEY", raising=False)
    assert broker_origin() is None


def test_origin_strips_the_llm_path(monkeypatch):
    monkeypatch.setenv("UNILLM_LLM_GATEWAY_URL", "http://127.0.0.1:8787/llm")
    monkeypatch.setenv("UNIFY_KEY", "K")
    assert broker_origin() == "http://127.0.0.1:8787"


def test_none_on_a_malformed_gateway(monkeypatch):
    monkeypatch.setenv("UNILLM_LLM_GATEWAY_URL", "not-a-url")
    monkeypatch.setenv("UNIFY_KEY", "K")
    assert broker_origin() is None
