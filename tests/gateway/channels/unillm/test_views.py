"""Behavioural tests for ``unify.gateway.channels.unillm``.

Smallest channel; one endpoint (POST /chat/completions). Tests
cover router contract + the two inlined auth helpers + both code
paths (stream + non-stream). The actual UniLLM call is mocked --
this is a transport / auth proxy test, not an LLM behaviour test.

Spending enforcement is covered here too: this endpoint spends real
money on a bare user API key, and it shipped for a time with no gates
at all because the limit-check hook was never installed in a gateway
process. The ``TestSpendingEnforcement`` cases pin that shut.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from unify.gateway.channels.unillm import router

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _orchestra_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin ORCHESTRA_URL for the inlined auth helper."""
    from unify.gateway.channels.unillm import views as unillm_views

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
    from unify.gateway.channels.unillm import router as exported

    assert exported is router


def test_schema_re_exported_from_package() -> None:
    """The schema module is part of the channel and importable."""
    from unify.gateway.channels.unillm.schema import (
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
        from unify.gateway.channels.unillm.views import _extract_api_key

        request = MagicMock()
        request.headers.get.return_value = "Bearer sk-test-1234"
        assert _extract_api_key(request) == "sk-test-1234"

    def test_missing_authorization_raises_401(self) -> None:
        from fastapi import HTTPException

        from unify.gateway.channels.unillm.views import _extract_api_key

        request = MagicMock()
        request.headers.get.return_value = ""
        with pytest.raises(HTTPException) as ctx:
            _extract_api_key(request)
        assert ctx.value.status_code == 401

    def test_wrong_scheme_raises_401(self) -> None:
        from fastapi import HTTPException

        from unify.gateway.channels.unillm.views import _extract_api_key

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
        from unify.gateway.channels.unillm.views import _authenticate_user_api_key

        with patch(
            "unify.gateway.channels.unillm.views.httpx.AsyncClient",
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

        from unify.gateway.channels.unillm.views import _authenticate_user_api_key

        with patch(
            "unify.gateway.channels.unillm.views.httpx.AsyncClient",
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

        from unify.gateway.channels.unillm.views import _authenticate_user_api_key

        with patch(
            "unify.gateway.channels.unillm.views.httpx.AsyncClient",
            return_value=_async_httpx_client(_orchestra_failure_response(500)),
        ):
            with pytest.raises(HTTPException) as ctx:
                await _authenticate_user_api_key("sk-x")
        assert ctx.value.status_code == 401


# ---------------------------------------------------------------------------
# POST /chat/completions -- end-to-end via TestClient
# ---------------------------------------------------------------------------


class TestChatCompletions:
    @pytest.fixture(autouse=True)
    def _limits_allowed(self):
        """Hold the spending gate open for the transport/auth cases.

        These assert proxying behaviour, not billing. Without this they
        reach a real ``/user/spend`` and their outcome depends on whether
        a local Orchestra happens to be up.
        """
        from unillm.limit_hooks import LimitCheckResponse

        async def _allow(_request):
            return LimitCheckResponse(allowed=True)

        with patch("unillm.limit_hooks._LIMIT_CHECK_HOOK", _allow):
            yield

    def test_missing_api_key_returns_401(
        self,
        client: TestClient,
        _orchestra_settings: None,
    ) -> None:
        resp = client.post(
            "/unillm/chat/completions",
            json={
                "model": "openai/gpt-4o@openrouter",
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
            "unify.gateway.channels.unillm.views.httpx.AsyncClient",
            return_value=_async_httpx_client(_orchestra_failure_response(401)),
        ):
            resp = client.post(
                "/unillm/chat/completions",
                json={
                    "model": "openai/gpt-4o@openrouter",
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
            "unify.gateway.channels.unillm.views.httpx.AsyncClient",
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
            "model": "openai/gpt-4o@openrouter",
            "choices": [
                {"index": 0, "message": {"role": "assistant", "content": "ok"}},
            ],
        }
        fake_client = MagicMock()
        fake_client.generate = AsyncMock(return_value=fake_response)

        with (
            patch(
                "unify.gateway.channels.unillm.views.httpx.AsyncClient",
                return_value=_async_httpx_client(_ok_orchestra_response()),
            ),
            patch(
                "unify.gateway.channels.unillm.views.unillm.AsyncUnify",
                return_value=fake_client,
            ) as MockAsync,
        ):
            resp = client.post(
                "/unillm/chat/completions",
                json={
                    "model": "openai/gpt-4o@openrouter",
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
        assert MockAsync.call_args.args[0] == "openai/gpt-4o@openrouter"

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
                "unify.gateway.channels.unillm.views.httpx.AsyncClient",
                return_value=_async_httpx_client(_ok_orchestra_response()),
            ),
            patch(
                "unify.gateway.channels.unillm.views.unillm.AsyncUnify",
                return_value=fake_client,
            ),
        ):
            resp = client.post(
                "/unillm/chat/completions",
                json={
                    "model": "openai/gpt-4o@openrouter",
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
                "unify.gateway.channels.unillm.views.httpx.AsyncClient",
                return_value=_async_httpx_client(_ok_orchestra_response()),
            ),
            patch(
                "unify.gateway.channels.unillm.views.unillm.AsyncUnify",
                return_value=fake_client,
            ) as MockAsync,
        ):
            # Only max_tokens supplied -> AsyncUnify gets max_completion_tokens=42
            client.post(
                "/unillm/chat/completions",
                json={
                    "model": "openai/gpt-4o@openrouter",
                    "messages": [{"role": "user", "content": "hi"}],
                    "max_tokens": 42,
                },
                headers={"Authorization": "Bearer sk-test"},
            )
        assert MockAsync.call_args.kwargs["max_completion_tokens"] == 42


# ---------------------------------------------------------------------------
# Spending enforcement
# ---------------------------------------------------------------------------


@pytest.fixture
def _hook_installed():
    """Register the real limit-check callback for the duration of a test.

    ``install_multi_tenant_limit_check_hook`` no-ops when platform billing
    is off, so tests register directly rather than depending on the
    ambient DEPLOY_ENV.
    """
    import unillm

    from unify.spending_limits import check_spending_limits_callback

    previous = unillm.get_limit_check_hook()
    unillm.set_limit_check_hook(check_spending_limits_callback)
    try:
        yield
    finally:
        unillm.set_limit_check_hook(previous)


def _denied(reason: str = "Insufficient credits: balance is $0.00."):
    """Patch the limit hook to a flat denial."""
    from unillm.limit_hooks import LimitCheckResponse

    async def _hook(_request):
        return LimitCheckResponse(allowed=False, reason=reason)

    return patch("unillm.limit_hooks._LIMIT_CHECK_HOOK", _hook)


class TestSpendingEnforcement:
    def test_channel_import_installs_the_hook_when_billing_is_on(self) -> None:
        """The module that mounts the route also installs the gates.

        Without this the proxy silently enforces nothing, because UniLLM
        treats a missing hook as 'allowed'. Re-invokes the installer
        rather than relying on import order, since another test may have
        swapped the hook out.
        """
        import unillm

        from unify.spending_limits import (
            check_spending_limits_callback,
            install_multi_tenant_limit_check_hook,
        )

        previous = unillm.get_limit_check_hook()
        unillm.set_limit_check_hook(None)
        try:
            with patch(
                "unify.spending_limits._charges_billing",
                return_value=True,
            ):
                install_multi_tenant_limit_check_hook()
            assert unillm.get_limit_check_hook() is check_spending_limits_callback
        finally:
            unillm.set_limit_check_hook(previous)

    def test_installer_does_not_require_a_process_unify_key(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The regression that caused the outage.

        ``install_limit_check_hook`` bails when ``UNIFY_KEY`` is unset,
        which is the normal state of a gateway process — the caller's key
        arrives per request. The multi-tenant installer must not.
        """
        import unillm

        from unify.spending_limits import install_multi_tenant_limit_check_hook

        monkeypatch.delenv("UNIFY_KEY", raising=False)
        previous = unillm.get_limit_check_hook()
        unillm.set_limit_check_hook(None)
        try:
            with patch(
                "unify.spending_limits._charges_billing",
                return_value=True,
            ):
                install_multi_tenant_limit_check_hook()
            assert unillm.get_limit_check_hook() is not None
        finally:
            unillm.set_limit_check_hook(previous)

    def test_non_stream_denial_returns_402(
        self,
        client: TestClient,
        _orchestra_settings: None,
    ) -> None:
        fake_client = MagicMock()
        fake_client.generate = AsyncMock(
            side_effect=AssertionError("LLM must not be called when denied"),
        )

        with (
            patch(
                "unify.gateway.channels.unillm.views.httpx.AsyncClient",
                return_value=_async_httpx_client(_ok_orchestra_response()),
            ),
            patch(
                "unify.gateway.channels.unillm.views.unillm.AsyncUnify",
                return_value=fake_client,
            ),
            _denied(),
        ):
            resp = client.post(
                "/unillm/chat/completions",
                json={
                    "model": "openai/gpt-4o@openrouter",
                    "messages": [{"role": "user", "content": "hi"}],
                },
                headers={"Authorization": "Bearer sk-test"},
            )

        assert resp.status_code == 402
        assert "Insufficient credits" in resp.json()["detail"]

    def test_stream_denial_returns_402_before_any_bytes(
        self,
        client: TestClient,
        _orchestra_settings: None,
    ) -> None:
        """A denied stream must fail as a 402, not a 200 carrying an error.

        Once StreamingResponse is returned the status line is committed,
        so the gates run before the response object is constructed.
        """
        fake_client = MagicMock()
        fake_client.generate = MagicMock(
            side_effect=AssertionError("LLM must not be called when denied"),
        )

        with (
            patch(
                "unify.gateway.channels.unillm.views.httpx.AsyncClient",
                return_value=_async_httpx_client(_ok_orchestra_response()),
            ),
            patch(
                "unify.gateway.channels.unillm.views.unillm.AsyncUnify",
                return_value=fake_client,
            ),
            _denied("Daily trial spend limit reached ($25.00 of $25.00 today)."),
        ):
            resp = client.post(
                "/unillm/chat/completions",
                json={
                    "model": "openai/gpt-4o@openrouter",
                    "messages": [{"role": "user", "content": "hi"}],
                    "stream": True,
                },
                headers={"Authorization": "Bearer sk-test"},
            )

        assert resp.status_code == 402
        assert "Daily trial spend limit" in resp.json()["detail"]
        assert "data:" not in resp.text

    def test_limits_resolve_against_the_caller_not_the_process_key(
        self,
        monkeypatch: pytest.MonkeyPatch,
        client: TestClient,
        _orchestra_settings: None,
        _hook_installed: None,
    ) -> None:
        """The wallet checked must be the caller's, not the gateway's.

        A shared process key would bill and gate every tenant against one
        account, which on a public endpoint is the whole ballgame.
        """
        monkeypatch.setenv("UNIFY_KEY", "sk-gateway-process-key")
        seen: dict = {}

        async def _capture_user_limit(user_id, month):
            from unify.spending_limits import _get_api_key

            seen["api_key"] = _get_api_key()
            seen["user_id"] = user_id
            return SimpleNamespace(
                exceeded=False,
                credit_balance=10.0,
                billing_mode="CREDITS",
                account_suspended=False,
                never_paid=False,
                limit_type=None,
                limit_value=None,
                current_spend=None,
                entity_id=None,
                entity_name=None,
            )

        fake_response = MagicMock()
        fake_response.model_dump.return_value = {"id": "chatcmpl-1"}
        fake_client = MagicMock()
        fake_client.generate = AsyncMock(return_value=fake_response)

        with (
            patch(
                "unify.gateway.channels.unillm.views.httpx.AsyncClient",
                return_value=_async_httpx_client(_ok_orchestra_response()),
            ),
            patch(
                "unify.gateway.channels.unillm.views.unillm.AsyncUnify",
                return_value=fake_client,
            ),
            patch(
                "unify.spending_limits._check_user_limit",
                _capture_user_limit,
            ),
        ):
            resp = client.post(
                "/unillm/chat/completions",
                json={
                    "model": "openai/gpt-4o@openrouter",
                    "messages": [{"role": "user", "content": "hi"}],
                },
                headers={"Authorization": "Bearer sk-caller"},
            )

        assert resp.status_code == 200
        assert seen["api_key"] == "sk-caller"  # pragma: allowlist secret
        assert seen["user_id"] == "u-1"

    def test_caller_context_restores_the_process_key_on_exit(self) -> None:
        """A leaked contextvar would mis-bill the next request on the loop."""
        import asyncio

        from unify.spending_limits import _get_api_key, caller_context

        async def _run() -> tuple:
            import os

            os.environ["UNIFY_KEY"] = "sk-process"
            async with caller_context("sk-tenant", user_id="u-9"):
                inside = _get_api_key()
            return inside, _get_api_key()

        inside, after = asyncio.run(_run())
        assert inside == "sk-tenant"
        assert after == "sk-process"

    def test_unverifiable_limits_fail_closed_for_a_proxy_caller(
        self,
        client: TestClient,
        _orchestra_settings: None,
        _hook_installed: None,
    ) -> None:
        """An Orchestra error must not become free LLM access.

        The runtime fails open on the same error (availability for a
        paying customer mid-task); a public endpoint spending against a
        wallet it just failed to read must not.
        """
        from unisdk.async_admin import SpendRequestError

        # Raised from inside the spend client so the real handler in
        # ``_check_user_limit`` is what classifies it, not the gather.
        async def _boom(month):
            raise SpendRequestError(
                url="/user/spend",
                method="GET",
                status=503,
                body="upstream unavailable",
            )

        fake_client = MagicMock()
        fake_client.generate = AsyncMock(
            side_effect=AssertionError("LLM must not be called when unverified"),
        )

        with (
            patch(
                "unify.gateway.channels.unillm.views.httpx.AsyncClient",
                return_value=_async_httpx_client(_ok_orchestra_response()),
            ),
            patch(
                "unify.gateway.channels.unillm.views.unillm.AsyncUnify",
                return_value=fake_client,
            ),
            patch(
                "unify.spending_limits._get_spend_client",
                return_value=SimpleNamespace(get_user_spend=_boom),
            ),
        ):
            resp = client.post(
                "/unillm/chat/completions",
                json={
                    "model": "openai/gpt-4o@openrouter",
                    "messages": [{"role": "user", "content": "hi"}],
                },
                headers={"Authorization": "Bearer sk-test"},
            )

        assert resp.status_code == 402
        assert "verify account spending limits" in resp.json()["detail"]

    def test_missing_limit_is_not_treated_as_a_failed_check(
        self,
        client: TestClient,
        _orchestra_settings: None,
        _hook_installed: None,
    ) -> None:
        """A clean 404 means 'no cap set', which must still allow the call.

        Conflating it with an unreachable Orchestra would deny every
        account that has never configured a spending limit.
        """
        from unisdk.async_admin import SpendRequestError

        async def _not_found(month):
            raise SpendRequestError(
                url="/user/spend",
                method="GET",
                status=404,
                body="no limit",
            )

        fake_response = MagicMock()
        fake_response.model_dump.return_value = {"id": "chatcmpl-1"}
        fake_client = MagicMock()
        fake_client.generate = AsyncMock(return_value=fake_response)

        with (
            patch(
                "unify.gateway.channels.unillm.views.httpx.AsyncClient",
                return_value=_async_httpx_client(_ok_orchestra_response()),
            ),
            patch(
                "unify.gateway.channels.unillm.views.unillm.AsyncUnify",
                return_value=fake_client,
            ),
            patch(
                "unify.spending_limits._get_spend_client",
                return_value=SimpleNamespace(get_user_spend=_not_found),
            ),
        ):
            resp = client.post(
                "/unillm/chat/completions",
                json={
                    "model": "openai/gpt-4o@openrouter",
                    "messages": [{"role": "user", "content": "hi"}],
                },
                headers={"Authorization": "Bearer sk-test"},
            )

        assert resp.status_code == 200

    def test_unverifiable_limits_still_fail_open_for_the_runtime(self) -> None:
        """No caller context means the assistant runtime: availability wins."""
        import asyncio

        from unillm.limit_hooks import LimitCheckRequest

        from unify.spending_limits import check_spending_limits_callback

        async def _boom(user_id, month):
            raise RuntimeError("orchestra down")

        async def _run():
            import os

            os.environ["UNIFY_KEY"] = "sk-runtime"
            with patch("unify.spending_limits._check_user_limit", _boom):
                return await check_spending_limits_callback(
                    LimitCheckRequest(
                        model="openai/gpt-4o@openrouter",
                        endpoint="chat",
                    ),
                )

        assert asyncio.run(_run()).allowed is True

    def test_free_tier_account_is_denied_api_access(
        self,
        client: TestClient,
        _orchestra_settings: None,
        _hook_installed: None,
    ) -> None:
        """Console-only free credits, enforced at the proxy.

        Orchestra decides; the runtime just carries the verdict on the
        existing spend payload.
        """

        async def _no_api_access(month):
            return {
                "cumulative_spend": 0,
                "limit": None,
                "credit_balance": 50.0,
                "billing_mode": "CREDITS",
                "account_suspended": False,
                "api_access_allowed": False,
            }

        fake_client = MagicMock()
        fake_client.generate = AsyncMock(
            side_effect=AssertionError("LLM must not be called when denied"),
        )

        with (
            patch(
                "unify.gateway.channels.unillm.views.httpx.AsyncClient",
                return_value=_async_httpx_client(_ok_orchestra_response()),
            ),
            patch(
                "unify.gateway.channels.unillm.views.unillm.AsyncUnify",
                return_value=fake_client,
            ),
            patch(
                "unify.spending_limits._get_spend_client",
                return_value=SimpleNamespace(get_user_spend=_no_api_access),
            ),
        ):
            resp = client.post(
                "/unillm/chat/completions",
                json={
                    "model": "openai/gpt-4o@openrouter",
                    "messages": [{"role": "user", "content": "hi"}],
                },
                headers={"Authorization": "Bearer sk-test"},
            )

        assert resp.status_code == 402
        assert "only be spent through the Unify Console" in resp.json()["detail"]

    def test_console_driven_runtime_work_is_never_api_gated(self) -> None:
        """The gate must not touch the Console's own path.

        The runtime has no caller context, so a false ``api_access_allowed``
        has to be inert there — otherwise turning the flag on would stop
        every free account from using the product at all, which is the
        opposite of the intent.
        """
        import asyncio
        import os

        from unillm.limit_hooks import LimitCheckRequest

        from unify.spending_limits import check_spending_limits_callback

        async def _no_api_access(user_id, month):
            return SimpleNamespace(
                exceeded=False,
                credit_balance=50.0,
                billing_mode="CREDITS",
                account_suspended=False,
                never_paid=False,
                check_failed=False,
                api_access_allowed=False,
                limit_type=None,
                limit_value=None,
                current_spend=None,
                entity_id=None,
                entity_name=None,
            )

        async def _run():
            os.environ["UNIFY_KEY"] = "sk-runtime"
            with patch("unify.spending_limits._check_user_limit", _no_api_access):
                return await check_spending_limits_callback(
                    LimitCheckRequest(
                        model="openai/gpt-4o@openrouter",
                        endpoint="chat",
                    ),
                )

        assert asyncio.run(_run()).allowed is True
