"""The credential proxy: forwards to the right upstream, swaps in the real key,
and refuses a caller presenting none.

The swap is the point -- the real provider key must reach the upstream and never
the caller -- so it is checked against a stubbed transport that records what was
sent, for both providers, whose scheme differs (Bearer vs Token).
"""

from __future__ import annotations

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient

from unify.llm_broker.proxy import CredentialProxy, Proxy, build_proxy_router


def _app(monkeypatch, recorder):
    proxy = Proxy(
        {
            "tavily": CredentialProxy(
                "tavily",
                "https://api.tavily.com",
                "Bearer",
                "tv-real",
            ),
            "recall": CredentialProxy(
                "recall",
                "https://eu-central-1.recall.ai",
                "Token",
                "rc-real",
            ),
            "unset": CredentialProxy("unset", "https://x", "Bearer", None),
        },
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        recorder["method"] = request.method
        recorder["url"] = str(request.url)
        recorder["auth"] = request.headers.get("authorization")
        recorder["body"] = request.content
        return httpx.Response(200, json={"ok": True})

    monkeypatch.setattr(
        proxy,
        "_client",
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    app = FastAPI()
    app.include_router(build_proxy_router(proxy))
    return TestClient(app)


def test_tavily_forwards_with_bearer_and_preserves_path_and_body(monkeypatch):
    rec: dict = {}
    client = _app(monkeypatch, rec)
    r = client.post(
        "/proxy/tavily/search?x=1",
        headers={"Authorization": "Bearer NONCE"},
        content=b'{"query":"hi"}',
    )
    assert r.status_code == 200
    assert rec["url"] == "https://api.tavily.com/search?x=1"
    assert rec["auth"] == "Bearer tv-real"  # the nonce was replaced
    assert rec["body"] == b'{"query":"hi"}'


def test_recall_uses_token_scheme_and_region_host(monkeypatch):
    rec: dict = {}
    client = _app(monkeypatch, rec)
    r = client.post(
        "/proxy/recall/api/v1/bot",
        headers={"Authorization": "Token NONCE"},
    )
    assert r.status_code == 200
    assert rec["url"] == "https://eu-central-1.recall.ai/api/v1/bot"
    assert rec["auth"] == "Token rc-real"


def test_missing_credential_is_refused(monkeypatch):
    client = _app(monkeypatch, {})
    assert client.get("/proxy/tavily/search").status_code == 401


def test_unconfigured_provider_is_refused(monkeypatch):
    client = _app(monkeypatch, {})
    # Present as a provider but with no key mounted -> unavailable, not forwarded.
    assert (
        client.get("/proxy/unset/x", headers={"Authorization": "Bearer n"}).status_code
        == 503
    )
    assert (
        client.get("/proxy/nope/x", headers={"Authorization": "Bearer n"}).status_code
        == 503
    )
