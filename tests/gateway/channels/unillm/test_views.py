"""Behavioural tests for ``unity.gateway.channels.unillm``.

Smallest channel; one endpoint (POST /chat/completions). Tests
cover router contract + the two inlined auth helpers + both code
paths (stream + non-stream). The actual UniLLM call is mocked --
this is a transport / auth proxy test, not an LLM behaviour test.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from unity.gateway.channels.unillm import router

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _orchestra_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin ORCHESTRA_URL for the inlined auth helper."""
    from unity.gateway.channels.unillm import views as unillm_views

    monkeypatch.setattr(
        unillm_views,
        "SETTINGS",
        SimpleNamespace(ORCHESTRA_URL="https://orchestra.example.com/v0"),
    )


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI()
    app.include_router(router, prefix="/unillm")
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _ok_orchestra_response() -> MagicMock:
    """Mock httpx response: 200 + user-info JSON body."""
    resp = MagicMock(status_code=200)
    resp.json.return_value = {"user_id": "u-1", "email": "test@example.com"}
    return resp


def _orchestra_failure_response(status_code: int = 401) -> MagicMock:
    return MagicMock(status_code=status_code)


def _async_httpx_client(response_mock: MagicMock) -> MagicMock:
    """AsyncMock context-manager for httpx.AsyncClient."""
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.get.return_value = response_mock
    return client


# ---------------------------------------------------------------------------
# Router contract
# ---------------------------------------------------------------------------


def test_router_exposes_expected_paths() -> None:
    paths = sorted(
        (r.path, sorted(r.methods)) for r in router.routes  # type: ignore[attr-defined]
    )
    assert paths == [("/chat/completions", ["POST"])]


def test_router_importable_from_package_root() -> None:
    from unity.gateway.channels.unillm import router as exported

    assert exported is router


def test_schema_re_exported_from_package() -> None:
    """The schema module is part of the channel and importable."""
    from unity.gateway.channels.unillm.schema import (
        ChatCompletionRequest,
        ChatMessage,
        ContentPart,
    )

    assert ChatCompletionRequest is not None
    assert ChatMessage is not None
    assert ContentPart is not None


# ---------------------------------------------------------------------------
# Auth helpers (inlined from communication/dependencies.py)
# ---------------------------------------------------------------------------


class TestExtractApiKey:
    def test_returns_bearer_token(self) -> None:
        from unity.gateway.channels.unillm.views import _extract_api_key

        request = MagicMock()
        request.headers.get.return_value = "Bearer sk-test-1234"
        assert _extract_api_key(request) == "sk-test-1234"

    def test_missing_authorization_raises_401(self) -> None:
        from fastapi import HTTPException

        from unity.gateway.channels.unillm.views import _extract_api_key

        request = MagicMock()
        request.headers.get.return_value = ""
        with pytest.raises(HTTPException) as ctx:
            _extract_api_key(request)
        assert ctx.value.status_code == 401

    def test_wrong_scheme_raises_401(self) -> None:
        from fastapi import HTTPException

        from unity.gateway.channels.unillm.views import _extract_api_key

        request = MagicMock()
        request.headers.get.return_value = "Basic dXNlcjpwYXNz"
        with pytest.raises(HTTPException) as ctx:
            _extract_api_key(request)
        assert ctx.value.status_code == 401


class TestAuthenticateUserApiKey:
    @pytest.mark.asyncio
    async def test_200_returns_user_info_dict(
        self,
        _orchestra_settings: None,
    ) -> None:
        from unity.gateway.channels.unillm.views import _authenticate_user_api_key

        with patch(
            "unity.gateway.channels.unillm.views.httpx.AsyncClient",
            return_value=_async_httpx_client(_ok_orchestra_response()),
        ):
            result = await _authenticate_user_api_key("sk-test")
        assert result == {"user_id": "u-1", "email": "test@example.com"}

    @pytest.mark.asyncio
    async def test_401_from_orchestra_raises_401(
        self,
        _orchestra_settings: None,
    ) -> None:
        from fastapi import HTTPException

        from unity.gateway.channels.unillm.views import _authenticate_user_api_key

        with patch(
            "unity.gateway.channels.unillm.views.httpx.AsyncClient",
            return_value=_async_httpx_client(_orchestra_failure_response(401)),
        ):
            with pytest.raises(HTTPException) as ctx:
                await _authenticate_user_api_key("sk-bad")
        assert ctx.value.status_code == 401

    @pytest.mark.asyncio
    async def test_500_from_orchestra_raises_401(
        self,
        _orchestra_settings: None,
    ) -> None:
        """Orchestra outages look like auth failures (safest default for a
        credential-gated endpoint).
        """
        from fastapi import HTTPException

        from unity.gateway.channels.unillm.views import _authenticate_user_api_key

        with patch(
            "unity.gateway.channels.unillm.views.httpx.AsyncClient",
            return_value=_async_httpx_client(_orchestra_failure_response(500)),
        ):
            with pytest.raises(HTTPException) as ctx:
                await _authenticate_user_api_key("sk-x")
        assert ctx.value.status_code == 401


# ---------------------------------------------------------------------------
# POST /chat/completions -- end-to-end via TestClient
# ---------------------------------------------------------------------------


class TestChatCompletions:
    def test_missing_api_key_returns_401(
        self,
        client: TestClient,
        _orchestra_settings: None,
    ) -> None:
        resp = client.post(
            "/unillm/chat/completions",
            json={
                "model": "gpt-4o@openai",
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        assert resp.status_code == 401

    def test_invalid_api_key_returns_401(
        self,
        client: TestClient,
        _orchestra_settings: None,
    ) -> None:
        with patch(
            "unity.gateway.channels.unillm.views.httpx.AsyncClient",
            return_value=_async_httpx_client(_orchestra_failure_response(401)),
        ):
            resp = client.post(
                "/unillm/chat/completions",
                json={
                    "model": "gpt-4o@openai",
                    "messages": [{"role": "user", "content": "hi"}],
                },
                headers={"Authorization": "Bearer sk-bad"},
            )
        assert resp.status_code == 401

    def test_missing_required_fields_returns_422(
        self,
        client: TestClient,
        _orchestra_settings: None,
    ) -> None:
        """Pydantic validation: model + messages are required."""
        with patch(
            "unity.gateway.channels.unillm.views.httpx.AsyncClient",
            return_value=_async_httpx_client(_ok_orchestra_response()),
        ):
            resp = client.post(
                "/unillm/chat/completions",
                json={"messages": [{"role": "user", "content": "hi"}]},
                headers={"Authorization": "Bearer sk-test"},
            )
        assert resp.status_code == 422

    def test_non_stream_returns_unillm_response_dict(
        self,
        client: TestClient,
        _orchestra_settings: None,
    ) -> None:
        """Non-stream path: builds AsyncUnify, awaits generate, returns dict."""
        fake_response = MagicMock()
        fake_response.model_dump.return_value = {
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "model": "gpt-4o@openai",
            "choices": [
                {"index": 0, "message": {"role": "assistant", "content": "ok"}},
            ],
        }
        fake_client = MagicMock()
        fake_client.generate = AsyncMock(return_value=fake_response)

        with (
            patch(
                "unity.gateway.channels.unillm.views.httpx.AsyncClient",
                return_value=_async_httpx_client(_ok_orchestra_response()),
            ),
            patch(
                "unity.gateway.channels.unillm.views.unillm.AsyncUnify",
                return_value=fake_client,
            ) as MockAsync,
        ):
            resp = client.post(
                "/unillm/chat/completions",
                json={
                    "model": "gpt-4o@openai",
                    "messages": [{"role": "user", "content": "hi"}],
                },
                headers={"Authorization": "Bearer sk-test"},
            )

        assert resp.status_code == 200
        assert resp.json()["id"] == "chatcmpl-1"
        # The user API key was forwarded into the AsyncUnify constructor.
        kwargs = MockAsync.call_args.kwargs
        assert kwargs["api_key"] == "sk-test"
        # AsyncUnify was called as positional model + kwargs.
        assert MockAsync.call_args.args[0] == "gpt-4o@openai"

    def test_stream_returns_sse_with_done_marker(
        self,
        client: TestClient,
        _orchestra_settings: None,
    ) -> None:
        """Stream path: SSE with one data line per chunk + final [DONE]."""
        chunk1 = MagicMock()
        chunk1.model_dump.return_value = {"id": "c-1", "choices": [{"delta": {}}]}
        chunk2 = MagicMock()
        chunk2.model_dump.return_value = {"id": "c-2", "choices": [{"delta": {}}]}

        async def _iter_chunks(messages):
            yield chunk1
            yield chunk2

        fake_client = MagicMock()
        fake_client.generate = _iter_chunks

        with (
            patch(
                "unity.gateway.channels.unillm.views.httpx.AsyncClient",
                return_value=_async_httpx_client(_ok_orchestra_response()),
            ),
            patch(
                "unity.gateway.channels.unillm.views.unillm.AsyncUnify",
                return_value=fake_client,
            ),
        ):
            resp = client.post(
                "/unillm/chat/completions",
                json={
                    "model": "gpt-4o@openai",
                    "messages": [{"role": "user", "content": "hi"}],
                    "stream": True,
                },
                headers={"Authorization": "Bearer sk-test"},
            )

        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        body = resp.text
        assert 'data: {"id": "c-1"' in body
        assert 'data: {"id": "c-2"' in body
        assert "data: [DONE]" in body

    def test_max_tokens_falls_back_to_max_completion_tokens(
        self,
        client: TestClient,
        _orchestra_settings: None,
    ) -> None:
        """max_completion_tokens takes precedence; max_tokens is the fallback."""
        fake_response = MagicMock()
        fake_response.model_dump.return_value = {"id": "x"}
        fake_client = MagicMock()
        fake_client.generate = AsyncMock(return_value=fake_response)

        with (
            patch(
                "unity.gateway.channels.unillm.views.httpx.AsyncClient",
                return_value=_async_httpx_client(_ok_orchestra_response()),
            ),
            patch(
                "unity.gateway.channels.unillm.views.unillm.AsyncUnify",
                return_value=fake_client,
            ) as MockAsync,
        ):
            # Only max_tokens supplied -> AsyncUnify gets max_completion_tokens=42
            client.post(
                "/unillm/chat/completions",
                json={
                    "model": "gpt-4o@openai",
                    "messages": [{"role": "user", "content": "hi"}],
                    "max_tokens": 42,
                },
                headers={"Authorization": "Bearer sk-test"},
            )
        assert MockAsync.call_args.kwargs["max_completion_tokens"] == 42
