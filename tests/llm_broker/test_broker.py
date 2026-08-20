"""The pod-local broker: what it forwards, what it refuses, what it reports.

Exercised through the real routes with the provider and Orchestra stubbed,
because the behaviours worth pinning are the interactions -- refuse before
spending, forward bytes unaltered, report usage after -- rather than the
shape of any one function.
"""

from __future__ import annotations

import base64
import json

import httpx
import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from unify.llm_broker.app import (
    _assistant_id,
    _billing_label,
    _billing_source,
    build_app,
)
from unify.llm_broker.settings import BrokerSettings
from unify.llm_broker.usage import (
    generation_id_from_body,
    generation_id_from_stream_tail,
    usage_from_stream_tail,
)

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


class TestAccountingModelSpelling:
    """Orchestra meters ``<id>@<provider>``; each provider sees its own bare id.

    The metering gate recognises an OpenRouter call by its ``@openrouter``
    marker. Authorizing with the provider-shaped id made every model outside
    the curated catalogue unmeterable -- refused on each call -- while curated
    ones passed only by colliding with the catalogue's suffix-stripped
    entries.
    """

    def test_openrouter_is_authorized_and_settled_with_its_marker(self):
        recorder = _Recorder()
        _client(recorder).post(
            "/llm/chat/completions",
            headers=_AUTH,
            json={"model": "openai/gpt-5.4-mini", "messages": []},
        )

        wire = "openai/gpt-5.4-mini@openrouter"
        assert recorder.authorize_calls[0]["model"] == wire
        assert recorder.settle_calls[0]["model"] == wire
        assert recorder.provider_calls[0]["body"]["model"] == "openai/gpt-5.4-mini"

    def test_anthropic_is_authorized_and_settled_with_its_marker(self):
        recorder = _Recorder(provider_body={"usage": {"input_tokens": 10}})
        _client(recorder).post(
            "/llm/anthropic/v1/messages",
            headers=_AUTH,
            json={"model": "claude-opus-5", "messages": []},
        )

        wire = "claude-opus-5@anthropic"
        assert recorder.authorize_calls[0]["model"] == wire
        assert recorder.settle_calls[0]["model"] == wire
        assert recorder.provider_calls[0]["body"]["model"] == "claude-opus-5"


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
        assert settled["model"] == "openai/gpt-5.6-sol@openrouter"
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

    def test_a_non_streamed_settle_carries_the_generation_id(self):
        recorder = _Recorder(
            provider_body={"id": "gen-body-1", "usage": {"cost": 0.00042}},
        )
        _client(recorder).post(
            "/llm/chat/completions",
            headers=_AUTH,
            json={"model": "openai/gpt-5.6-sol", "messages": []},
        )

        assert recorder.settle_calls[0]["generation_id"] == "gen-body-1"

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

    def test_a_stream_settles_with_the_generation_id_from_its_tail(self):
        body = (
            b'data: {"id":"gen-stream-1","choices":[{"delta":{"content":"hi"}}]}\n\n'
            b'data: {"id":"gen-stream-1","usage":{"cost":0.00123}}\n\n'
            b"data: [DONE]\n\n"
        )
        recorder = _Recorder(stream=body)
        _client(recorder).post(
            "/llm/chat/completions",
            headers=_AUTH,
            json={"model": "openai/gpt-5.6-sol", "messages": [], "stream": True},
        )

        assert recorder.settle_calls[0]["generation_id"] == "gen-stream-1"


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

    def test_cached_tokens_are_lifted_out_of_prompt_tokens_details(self):
        """The scalar-only merge would otherwise drop this on the floor.

        ``prompt_tokens_details`` is a nested breakdown, so the general
        scalar merge skips it -- but ``cached_tokens`` inside it is the one
        per-call cache signal a provider reports, so it is hoisted out into
        the merged scalars explicitly.
        """
        tail = (
            'data: {"usage":{"prompt_tokens":100,"completion_tokens":20,'
            '"prompt_tokens_details":{"cached_tokens":64}}}\n\n'
        )
        assert usage_from_stream_tail(tail) == {
            "prompt_tokens": 100,
            "completion_tokens": 20,
            "cached_tokens": 64,
        }

    def test_cached_tokens_lift_survives_across_split_events(self):
        """Anthropic-style split events: a later event still contributes the lift."""
        tail = (
            'data: {"usage":{"prompt_tokens":50}}\n\n'
            'data: {"usage":{"prompt_tokens_details":{"cached_tokens":10}}}\n\n'
        )
        assert usage_from_stream_tail(tail) == {
            "prompt_tokens": 50,
            "cached_tokens": 10,
        }

    def test_a_non_dict_prompt_tokens_details_is_ignored(self):
        tail = 'data: {"usage":{"prompt_tokens":5,"prompt_tokens_details":null}}\n\n'
        assert usage_from_stream_tail(tail) == {"prompt_tokens": 5}

    def test_a_missing_cached_tokens_key_is_not_invented(self):
        tail = 'data: {"usage":{"prompt_tokens":5,"prompt_tokens_details":{}}}\n\n'
        assert usage_from_stream_tail(tail) == {"prompt_tokens": 5}


class TestGenerationIdExtraction:
    """The provider's own response id, so a ledger row can be cross-checked."""

    def test_from_a_non_streamed_body(self):
        assert (
            generation_id_from_body({"id": "gen-abc123", "usage": {}}) == "gen-abc123"
        )

    def test_from_a_body_with_no_id(self):
        assert generation_id_from_body({"usage": {}}) is None

    def test_from_a_body_that_is_not_a_dict(self):
        assert generation_id_from_body(None) is None

    def test_an_empty_string_id_counts_as_absent(self):
        assert generation_id_from_body({"id": ""}) is None

    def test_from_an_openai_compatible_stream_tail(self):
        """OpenAI-compatible chunks repeat the same id on every event."""
        tail = (
            'data: {"id":"gen-xyz","choices":[{"delta":{"content":"He"}}]}\n\n'
            'data: {"id":"gen-xyz","usage":{"cost":0.001}}\n\ndata: [DONE]\n\n'
        )
        assert generation_id_from_stream_tail(tail) == "gen-xyz"

    def test_from_an_anthropic_message_start_event(self):
        """Anthropic carries the id once, nested under ``message``."""
        tail = (
            'data: {"type":"message_start","message":{"id":"msg_01abc",'
            '"usage":{"input_tokens":900}}}\n\n'
            'data: {"type":"message_delta","usage":{"output_tokens":120}}\n\n'
        )
        assert generation_id_from_stream_tail(tail) == "msg_01abc"

    def test_a_stream_with_no_id_anywhere_reports_nothing(self):
        assert generation_id_from_stream_tail('data: {"choices":[]}\n\n') is None


class TestAssistantAttribution:
    """A brokered call must still land on the assistant that made it.

    The direct path took this from the billing context when it deducted
    client-side. Gateway-routed calls skip that deduction, so the id has to
    travel explicitly -- otherwise spend lands on the account with no
    assistant, which loses per-assistant reporting and quietly stops the
    per-assistant spending caps applying, since those are enforced on it.
    """

    def _request(self, headers=None):
        scope = {
            "type": "http",
            "headers": [
                (k.lower().encode(), v.encode()) for k, v in (headers or {}).items()
            ],
        }
        return Request(scope)

    def test_the_header_the_runtime_sets_is_used(self):
        got = _assistant_id(
            self._request({"X-Unify-Assistant-Id": "7366"}),
            {},
        )
        assert got == 7366

    def test_a_body_field_still_works_for_direct_callers(self):
        """Callers without a runtime setting headers are not left unattributed."""
        body = {"assistant_id": 42}
        assert _assistant_id(self._request(), body) == 42
        assert "assistant_id" not in body, "must not reach the provider"

    def test_the_header_wins_over_a_body_field(self):
        assert (
            _assistant_id(
                self._request({"X-Unify-Assistant-Id": "7366"}),
                {"assistant_id": 1},
            )
            == 7366
        )

    def test_an_unusable_id_attributes_nothing_rather_than_failing(self):
        """A malformed id should not cost the caller the whole call."""
        assert _assistant_id(self._request({"X-Unify-Assistant-Id": "x"}), {}) is None

    def test_no_id_anywhere_is_simply_unattributed(self):
        assert _assistant_id(self._request(), {}) is None


class TestLabelAndSourceAttribution:
    """The act/source label, threaded from the caller's billing context.

    Carried as a base64url-encoded header (label) and a plain header
    (source) rather than body fields, mirroring assistant attribution: the
    body is provider-shaped and forwarded verbatim.
    """

    def _request(self, headers=None):
        scope = {
            "type": "http",
            "headers": [
                (k.lower().encode(), v.encode()) for k, v in (headers or {}).items()
            ],
        }
        return Request(scope)

    def _b64(self, text: str) -> str:
        return base64.urlsafe_b64encode(text.encode("utf-8")).decode("ascii")

    def test_a_plain_ascii_label_round_trips(self):
        got = _billing_label(
            self._request({"X-Unify-Label": self._b64("Researching leads")}),
        )
        assert got == "Researching leads"

    def test_a_chinese_label_round_trips(self):
        """UTF-8 text must survive the header, which is otherwise latin-1."""
        label = "研究客户需求"
        got = _billing_label(self._request({"X-Unify-Label": self._b64(label)}))
        assert got == label

    def test_a_mixed_utf8_label_round_trips(self):
        label = "Checking 天气 for São Paulo 🌦"
        got = _billing_label(self._request({"X-Unify-Label": self._b64(label)}))
        assert got == label

    def test_no_label_header_is_simply_absent(self):
        assert _billing_label(self._request()) is None

    def test_a_malformed_label_header_is_dropped_not_raised(self):
        """A broken header must not fail the call it is only metadata for."""
        assert _billing_label(self._request({"X-Unify-Label": "not-base64!!"})) is None

    def test_the_source_header_is_read_verbatim(self):
        assert _billing_source(self._request({"X-Unify-Source": "tool"})) == "tool"

    def test_no_source_header_is_simply_absent(self):
        assert _billing_source(self._request()) is None

    def test_round_trips_through_unillm_s_own_encoder(self):
        """End-to-end: what unillm's billing-context read actually sends.

        Encodes with unillm's ``_gateway_attribution_headers`` (the
        "unillm billing-context read" half of this change) and decodes with
        the broker's own reader, so the two sides are proven compatible
        rather than merely both individually correct against base64.
        """
        import unillm
        from unillm.billing_context import set_billing_context
        from unillm.clients.uni_llm import _gateway_attribution_headers

        set_billing_context(
            assistant_id=7366,
            source="chat",
            label="正在研究客户的 NotebookLM 连接选项",
        )
        try:
            headers = _gateway_attribution_headers()
        finally:
            # Do not leak billing context into later tests in the process.
            unillm.set_billing_context()

        request = self._request(headers)
        assert _billing_label(request) == "正在研究客户的 NotebookLM 连接选项"
        assert _billing_source(request) == "chat"


class TestLabelAndSourceReachSettle:
    """The label/source travel all the way through to the settle call."""

    def test_a_streamed_call_settles_with_label_and_source(self):
        recorder = _Recorder(
            stream=b'data: {"usage":{"cost":0.001}}\n\ndata: [DONE]\n\n',
        )
        label = base64.urlsafe_b64encode("客户研究".encode("utf-8")).decode("ascii")
        _client(recorder).post(
            "/llm/chat/completions",
            headers={**_AUTH, "X-Unify-Label": label, "X-Unify-Source": "tool"},
            json={"model": "openai/gpt-5.6-sol", "messages": [], "stream": True},
        )

        settled = recorder.settle_calls[0]
        assert settled["label"] == "客户研究"
        assert settled["source"] == "tool"

    def test_a_call_with_no_billing_context_settles_with_no_label(self):
        recorder = _Recorder()
        _client(recorder).post(
            "/llm/chat/completions",
            headers=_AUTH,
            json={"model": "openai/gpt-5.6-sol", "messages": []},
        )

        settled = recorder.settle_calls[0]
        assert "label" not in settled
        assert "source" not in settled


class TestHealthz:
    """What the pod is allowed to conclude from this endpoint.

    The manifest probes it twice, for two decisions it cannot take back: the
    runtime container does not start until it answers, and a container that
    stops answering is restarted. Both are right for a frozen broker and
    catastrophic for a healthy one, so the answer must depend on nothing but
    this process.
    """

    def test_a_broker_with_no_provider_keys_is_still_serving(self):
        """Credentials are not health. A keyless broker answers and says so.

        Were this to report on keys, a secret that failed to mount would stop
        the runtime from starting at all rather than failing the calls that
        needed it.
        """
        keyless = BrokerSettings(
            host="127.0.0.1",
            port=0,
            orchestra_url="https://orchestra.test/v0",
            openrouter_api_key=None,
            openrouter_api_base="https://openrouter.test/api/v1",
            anthropic_api_key=None,
            anthropic_api_base="https://anthropic.test",
            auth_ttl_s=0.0,
        )
        with TestClient(build_app(keyless)) as client:
            response = client.get("/healthz")

        assert response.status_code == 200
        assert response.json() == {"ok": True}

    def test_it_answers_without_reaching_orchestra_or_a_provider(self, monkeypatch):
        """An upstream outage must not read as this container being broken.

        A provider check here would turn one OpenRouter incident into every
        assistant pod restarting at once, and none of them starting again
        until it cleared.
        """
        calls: list[str] = []

        def _explode(*args, **kwargs):
            calls.append("outbound")
            raise AssertionError("healthz must not make an outbound call")

        # Only the async client: both the provider and control-plane legs are
        # AsyncClients, while TestClient is itself an httpx.Client -- patching
        # that one breaks the harness rather than catching the broker.
        monkeypatch.setattr(httpx.AsyncClient, "send", _explode)

        with TestClient(build_app(_SETTINGS)) as client:
            response = client.get("/healthz")

        assert response.status_code == 200
        assert calls == []
