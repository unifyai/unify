"""The voice broker: where it sends the bytes, which credential it injects, and
that it refuses a caller presenting none.

The swap is the security-critical part -- the real provider key must reach the
upstream and never the caller -- so it is checked for all three providers,
whose credential sits in a different place each. The relay is exercised through
the real route against a stubbed upstream that echoes, which is enough to pin
that frames cross in both directions and that the upstream was opened with the
real key.
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from unify.llm_broker import voice
from unify.llm_broker.voice import (
    VoiceProvider,
    _has_caller_credential,
    _upstream_headers,
    _upstream_url,
    build_voice_router,
)


def _providers() -> dict[str, VoiceProvider]:
    return {
        "deepgram": VoiceProvider(
            "deepgram",
            "https://api.deepgram.com",
            "dg-real",
            "header",
            "Authorization",
            "Token {key}",
        ),
        "cartesia": VoiceProvider(
            "cartesia",
            "https://api.cartesia.ai",
            "ct-real",
            "query",
            "api_key",
            "{key}",
        ),
        "elevenlabs": VoiceProvider(
            "elevenlabs",
            "https://api.elevenlabs.io",
            "el-real",
            "header",
            "xi-api-key",
            "{key}",
        ),
    }


def test_deepgram_injects_token_header_and_keeps_query():
    p = _providers()["deepgram"]
    assert _upstream_url(p, "v1/listen", "model=nova-3&language=en-GB") == (
        "wss://api.deepgram.com/v1/listen?model=nova-3&language=en-GB"
    )
    assert _upstream_headers(p) == {"Authorization": "Token dg-real"}


def test_cartesia_swaps_the_key_in_the_query_not_a_header():
    p = _providers()["cartesia"]
    # The caller's placeholder key in the query is replaced with the real one,
    # every other parameter preserved; no auth header is added.
    assert _upstream_url(p, "tts/websocket", "api_key=NONCE&cartesia_version=2024") == (
        "wss://api.cartesia.ai/tts/websocket?api_key=ct-real&cartesia_version=2024"
    )
    assert _upstream_headers(p) == {}


def test_elevenlabs_injects_xi_api_key_header():
    p = _providers()["elevenlabs"]
    assert _upstream_headers(p) == {"xi-api-key": "el-real"}
    assert _upstream_url(
        p,
        "v1/text-to-speech/x/multi-stream-input",
        "o=pcm",
    ).startswith(
        "wss://api.elevenlabs.io/v1/text-to-speech/x/multi-stream-input?o=pcm",
    )


class _FakeUpstream:
    """Echoes what it is sent, so a frame in becomes a frame back out."""

    def __init__(self):
        self._q: asyncio.Queue = asyncio.Queue()
        self.sent: list = []
        self.closed = False

    async def send(self, message):
        self.sent.append(message)
        await self._q.put(message)

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self._q.get()

    async def close(self):
        self.closed = True


def _client(monkeypatch):
    opened: dict = {}

    async def fake_connect(url, additional_headers=None):
        opened["url"] = url
        opened["headers"] = dict(additional_headers or {})
        return _FakeUpstream()

    monkeypatch.setattr(voice.websockets, "connect", fake_connect)
    app = FastAPI()
    app.include_router(build_voice_router(_providers()))
    return TestClient(app), opened


def test_relay_opens_upstream_with_real_key_and_echoes(monkeypatch):
    client, opened = _client(monkeypatch)
    with client.websocket_connect(
        "/voice/deepgram/v1/listen?model=nova-3",
        headers={"Authorization": "Token NONCE"},
    ) as ws:
        ws.send_bytes(b"AUDIO-FRAME")
        assert ws.receive_bytes() == b"AUDIO-FRAME"
    # The upstream was opened at the provider with the real key, never the nonce.
    assert opened["url"] == "wss://api.deepgram.com/v1/listen?model=nova-3"
    assert opened["headers"] == {"Authorization": "Token dg-real"}


def test_missing_caller_credential_is_refused(monkeypatch):
    client, _ = _client(monkeypatch)
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/voice/deepgram/v1/listen"):
            pass


def test_unconfigured_provider_is_refused(monkeypatch):
    client, _ = _client(monkeypatch)
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect(
            "/voice/unknown/x",
            headers={"Authorization": "Token NONCE"},
        ):
            pass


def test_has_caller_credential_reads_the_right_place():
    class _WS:
        def __init__(self, headers, query):
            from starlette.datastructures import Headers
            from starlette.datastructures import URL

            self.headers = Headers(headers)
            self.url = URL(f"ws://x/p?{query}")

    dg = _providers()["deepgram"]
    ct = _providers()["cartesia"]
    assert _has_caller_credential(dg, _WS({"authorization": "Token n"}, "")) is True
    assert _has_caller_credential(dg, _WS({}, "api_key=n")) is False
    assert _has_caller_credential(ct, _WS({}, "api_key=n")) is True
    assert _has_caller_credential(ct, _WS({"authorization": "Token n"}, "")) is False
