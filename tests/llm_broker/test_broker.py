"""The pod-local broker: what it forwards, what it refuses, what it reports.

Exercised through the real routes with the provider and Orchestra stubbed,
because the behaviours worth pinning are the interactions -- refuse before
spending, forward bytes unaltered, report usage after -- rather than the
shape of any one function.
"""

from __future__ import annotations

import json

import httpx
import pytest
from fastapi.testclient import TestClient

from unify.llm_broker.app import build_app
from unify.llm_broker.settings import BrokerSettings
from unify.llm_broker.usage import usage_from_stream_tail

_SETTINGS = BrokerSettings(
    host="127.0.0.1",
    port=0,
    orchestra_url="https://orchestra.test/v0",
    openrouter_api_key="sk-openrouter",  # pragma: allowlist secret
    openrouter_api_base="https://openrouter.test/api/v1",
    anthropic_api_key="sk-anthropic",  # pragma: allowlist secret
    anthropic_api_base="https://anthropic.test",
    # Every call re-asks, so a test never passes because of a cached verdict.
    auth_ttl_s=0.0,
)

_AUTH = {"Authorization": "Bearer caller-key"}


class _Recorder:
    """Stands in for both Orchestra and the provider, and remembers calls."""

    def __init__(self, *, allowed=True, reason=None, provider_body=None, stream=None):
        self.allowed = allowed
        self.reason = reason
        self.provider_body = provider_body or {"usage": {"cost": 0.002}}
        self.stream = stream
        self.authorize_calls: list[dict] = []
        self.settle_calls: list[dict] = []
        self.provider_calls: list[dict] = []

    async def handle(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        payload = json.loads(request.content or b"{}")
        if url.endswith("/llm/authorize"):
            self.authorize_calls.append(payload)
            return httpx.Response(
                200,
                json={"allowed": self.allowed, "reason": self.reason},
            )
        if url.endswith("/llm/settle"):
            self.settle_calls.append(payload)
            return httpx.Response(200, json={"charged": 0.0024, "metered": True})

        self.provider_calls.append(
            {"url": url, "headers": dict(request.headers), "body": payload},
        )
        if self.stream is not None:
            return httpx.Response(200, content=self.stream)
        return httpx.Response(200, json=self.provider_body)


def _client(recorder: _Recorder) -> TestClient:
    app = build_app(_SETTINGS)
    transport = httpx.MockTransport(recorder.handle)
    app.state.broker._provider = httpx.AsyncClient(transport=transport)
    app.state.broker._control = httpx.AsyncClient(transport=transport)
    return TestClient(app)


class TestAuthorizationPrecedesSpending:
    def test_a_refused_account_never_reaches_the_provider(self):
        """The point of asking first: no call, so nothing to bill."""
        recorder = _Recorder(allowed=False, reason="Insufficient credits.")
        response = _client(recorder).post(
            "/llm/chat/completions",
            headers=_AUTH,
            json={"model": "openai/gpt-5.6-sol", "messages": []},
        )

        assert response.status_code == 402
        assert response.json()["detail"] == "Insufficient credits."
        assert recorder.provider_calls == []

    def test_an_unreachable_ledger_refuses_rather_than_proceeds(self):
        """Fail closed: an unreachable ledger cannot record what it allows."""

        async def unreachable(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("orchestra down", request=request)

        app = build_app(_SETTINGS)
        transport = httpx.MockTransport(unreachable)
        app.state.broker._control = httpx.AsyncClient(transport=transport)
        app.state.broker._provider = httpx.AsyncClient(transport=transport)

        response = TestClient(app).post(
            "/llm/chat/completions",
            headers=_AUTH,
            json={"model": "openai/gpt-5.6-sol", "messages": []},
        )
        assert response.status_code == 503

    def test_a_call_without_a_key_is_refused_before_anything_else(self):
        recorder = _Recorder()
        response = _client(recorder).post(
            "/llm/chat/completions",
            json={"model": "openai/gpt-5.6-sol", "messages": []},
        )

        assert response.status_code == 401
        assert recorder.authorize_calls == []


class TestTheCallerNeverSeesTheProviderKey:
    def test_the_caller_s_key_is_replaced_not_forwarded(self):
        """The whole point of the sidecar: the credential is substituted here."""
        recorder = _Recorder()
        _client(recorder).post(
            "/llm/chat/completions",
            headers=_AUTH,
            json={"model": "openai/gpt-5.6-sol", "messages": []},
        )

        sent = recorder.provider_calls[0]["headers"]
        assert sent["authorization"] == "Bearer sk-openrouter"
        assert "caller-key" not in sent["authorization"]

    def test_anthropic_receives_its_own_credential_header(self):
        recorder = _Recorder(provider_body={"usage": {"input_tokens": 10}})
        _client(recorder).post(
            "/llm/anthropic/v1/messages",
            headers=_AUTH,
            json={"model": "claude-opus-5", "messages": []},
        )

        sent = recorder.provider_calls[0]["headers"]
        assert sent["x-api-key"] == "sk-anthropic"
        assert sent["anthropic-version"] == "2023-06-01"


class TestUsageIsReportedNotPriced:
    def test_the_provider_s_usage_is_relayed_verbatim(self):
        """Orchestra prices it; a broker that priced could declare anything."""
        recorder = _Recorder(provider_body={"usage": {"cost": 0.00042}})
        _client(recorder).post(
            "/llm/chat/completions",
            headers=_AUTH,
            json={"model": "openai/gpt-5.6-sol", "messages": []},
        )

        assert len(recorder.settle_calls) == 1
        settled = recorder.settle_calls[0]
        assert settled["usage"] == {"cost": 0.00042}
        assert settled["model"] == "openai/gpt-5.6-sol"
        assert "charged" not in settled

    def test_cost_accounting_is_requested_on_the_way_out(self):
        """OpenRouter reports cost only when asked, and it is what we settle on."""
        recorder = _Recorder()
        _client(recorder).post(
            "/llm/chat/completions",
            headers=_AUTH,
            json={"model": "openai/gpt-5.6-sol", "messages": []},
        )

        assert recorder.provider_calls[0]["body"]["usage"] == {"include": True}

    def test_a_failed_provider_call_is_not_settled(self):
        """Nothing was served, so there is nothing to charge for."""

        async def failing(request: httpx.Request) -> httpx.Response:
            if str(request.url).endswith("/llm/authorize"):
                return httpx.Response(200, json={"allowed": True})
            return httpx.Response(500, json={"error": "provider exploded"})

        app = build_app(_SETTINGS)
        transport = httpx.MockTransport(failing)
        app.state.broker._provider = httpx.AsyncClient(transport=transport)
        app.state.broker._control = httpx.AsyncClient(transport=transport)

        response = TestClient(app).post(
            "/llm/chat/completions",
            headers=_AUTH,
            json={"model": "openai/gpt-5.6-sol", "messages": []},
        )
        assert response.status_code == 500


class TestStreaming:
    def test_provider_bytes_reach_the_caller_unaltered(self):
        """A voice turn reads these as they arrive; rewriting them adds delay."""
        body = (
            b'data: {"choices":[{"delta":{"content":"He"}}]}\n\n'
            b'data: {"choices":[{"delta":{"content":"llo"}}]}\n\n'
            b'data: {"usage":{"cost":0.001}}\n\ndata: [DONE]\n\n'
        )
        recorder = _Recorder(stream=body)
        response = _client(recorder).post(
            "/llm/chat/completions",
            headers=_AUTH,
            json={"model": "openai/gpt-5.6-sol", "messages": [], "stream": True},
        )

        assert response.content == body

    def test_a_stream_settles_from_its_terminal_usage(self):
        body = (
            b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n'
            b'data: {"usage":{"cost":0.00123}}\n\ndata: [DONE]\n\n'
        )
        recorder = _Recorder(stream=body)
        _client(recorder).post(
            "/llm/chat/completions",
            headers=_AUTH,
            json={"model": "openai/gpt-5.6-sol", "messages": [], "stream": True},
        )

        assert recorder.settle_calls[0]["usage"]["cost"] == pytest.approx(0.00123)


class TestUsageExtraction:
    def test_anthropic_counts_split_across_events_are_merged(self):
        """Input arrives on message_start, output on the final message_delta."""
        tail = (
            'data: {"type":"message_start","message":{"usage":'
            '{"input_tokens":900}}}\n\n'
            'data: {"type":"message_delta","usage":{"output_tokens":120}}\n\n'
        )
        assert usage_from_stream_tail(tail) == {
            "input_tokens": 900,
            "output_tokens": 120,
        }

    def test_a_truncated_leading_line_does_not_discard_the_rest(self):
        """The tail is a byte window, so its first line is usually incomplete."""
        tail = '_tokens":5}}\n\ndata: {"usage":{"output_tokens":7}}\n\n'
        assert usage_from_stream_tail(tail) == {"output_tokens": 7}

    def test_a_stream_with_no_usage_reports_nothing(self):
        assert usage_from_stream_tail('data: {"choices":[]}\n\n') is None
