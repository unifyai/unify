from __future__ import annotations

from droid.gateway.context import create_default_gateway_context
from droid.gateway.envelope_sink import HttpEnvelopeSink, MissingEnvelopeSink


def test_default_context_requires_explicit_sink(monkeypatch) -> None:
    monkeypatch.delenv("DROID_GATEWAY_LOCAL_INGRESS_URL", raising=False)

    context = create_default_gateway_context()

    assert isinstance(context.envelope_sink, MissingEnvelopeSink)


def test_default_context_uses_local_ingress_http_sink(monkeypatch) -> None:
    monkeypatch.setenv("DROID_GATEWAY_LOCAL_INGRESS_URL", "http://127.0.0.1:9001")

    context = create_default_gateway_context()

    assert isinstance(context.envelope_sink, HttpEnvelopeSink)
    assert context.envelope_sink.base_url == "http://127.0.0.1:9001"
