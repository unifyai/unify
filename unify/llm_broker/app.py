"""The broker's HTTP surface: authorise, stream to the provider, report usage.

Two routes, shaped like the providers themselves rather than like a common
abstraction, because the callers are provider SDKs: an OpenAI-compatible
chat-completions route for OpenRouter traffic, and Anthropic's Messages API
for native Claude traffic. Each forwards its caller's body essentially
unchanged, so features we have never heard of keep working.

Every request follows the same three steps: ask Orchestra whether the account
may spend, stream the provider's bytes straight back to the caller, then
report what the provider said it used. Only the first and last touch
Orchestra, and neither waits on the model.
"""

from __future__ import annotations

import logging
import time
from typing import Any, AsyncIterator, Optional

import httpx
from fastapi import APIRouter, FastAPI, Request
from starlette.responses import JSONResponse, StreamingResponse

from unify.llm_broker.settings import BrokerSettings, load_settings
from unify.llm_broker.usage import (
    USAGE_TAIL_LIMIT,
    usage_from_body,
    usage_from_stream_tail,
)
from unify.llm_broker.proxy import Proxy, build_proxy_router
from unify.llm_broker.voice import build_voice_router

LOGGER = logging.getLogger(__name__)

#: Generations run long; the connect timeout stays short so a provider that is
#: down fails fast instead of holding the caller for the full read window.
_PROVIDER_TIMEOUT = httpx.Timeout(600.0, connect=10.0)
#: Metadata calls. A slow answer here delays a turn, so it is bounded tightly
#: and treated as a refusal rather than waited on.
_CONTROL_TIMEOUT = httpx.Timeout(5.0, connect=2.0)


class _AuthorizationCache:
    """Short-lived memory of positive verdicts, keyed by caller and model.

    Only successes are remembered. A refusal is always re-asked, so an
    account that has just been suspended or run dry is refused on its next
    call rather than at the end of a cache window.
    """

    def __init__(self, ttl_s: float) -> None:
        self._ttl_s = ttl_s
        self._entries: dict[tuple[str, str], float] = {}

    def is_fresh(self, key: str, model: str) -> bool:
        if self._ttl_s <= 0:
            return False
        expires_at = self._entries.get((key, model))
        return expires_at is not None and expires_at > time.monotonic()

    def remember(self, key: str, model: str) -> None:
        if self._ttl_s > 0:
            self._entries[(key, model)] = time.monotonic() + self._ttl_s


def _caller_key(request: Request) -> Optional[str]:
    """The Unify API key the caller is spending as.

    The broker never substitutes an identity of its own: whatever the caller
    presents is what Orchestra meters, so a call made by sandbox code is
    charged to the same assistant as one made by the runtime.
    """
    header = request.headers.get("authorization") or ""
    if header.lower().startswith("bearer "):
        return header[7:].strip() or None
    return request.headers.get("x-api-key") or None


def _assistant_id(request: Request, body: dict) -> Optional[int]:
    """Which assistant to attribute this call to.

    Read from a header the runtime sets, because the body is provider-shaped
    and forwarded verbatim -- an id left in it would reach a provider that
    rejects unknown fields. Without this the call still runs and is still
    charged, but lands on the account with no assistant, which loses
    per-assistant reporting and silently stops the per-assistant spending
    caps from applying, since those are enforced against this id.

    The body form is still accepted for direct callers that have no runtime
    setting the header.
    """
    raw = request.headers.get("x-unify-assistant-id") or body.pop("assistant_id", None)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        LOGGER.warning("LLM broker: unusable assistant id %r; not attributing", raw)
        return None


def _refusal(status_code: int, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"detail": detail})


class Broker:
    """Holds the provider credentials and the clients that use them."""

    def __init__(self, settings: BrokerSettings) -> None:
        self.settings = settings
        self._auth_cache = _AuthorizationCache(settings.auth_ttl_s)
        self._provider = httpx.AsyncClient(timeout=_PROVIDER_TIMEOUT)
        self._control = httpx.AsyncClient(timeout=_CONTROL_TIMEOUT)

    async def aclose(self) -> None:
        await self._provider.aclose()
        await self._control.aclose()

    async def authorize(
        self,
        *,
        caller_key: str,
        model: str,
        assistant_id: Optional[int],
    ) -> Optional[JSONResponse]:
        """Return a refusal to send back, or ``None`` to proceed.

        Fails closed. If Orchestra cannot be reached the call is refused
        rather than allowed through unmetered -- an unreachable ledger is
        exactly when an unchecked call is most likely to be the one that
        should have been stopped.
        """
        if self._auth_cache.is_fresh(caller_key, model):
            return None
        if not self.settings.orchestra_url:
            return _refusal(503, "LLM broker is not configured (no Orchestra URL).")

        try:
            response = await self._control.post(
                self.settings.authorize_url,
                headers={"Authorization": f"Bearer {caller_key}"},
                json={"model": model, "assistant_id": assistant_id},
            )
        except httpx.RequestError as exc:
            LOGGER.warning("LLM broker: authorize unreachable: %s", exc)
            return _refusal(503, "Spending authorization is unavailable; try again.")

        if response.status_code == 401:
            return _refusal(401, "Invalid API key.")
        if response.status_code >= 400:
            LOGGER.warning(
                "LLM broker: authorize failed (%s)",
                response.status_code,
            )
            return _refusal(503, "Spending authorization failed; try again.")

        verdict = response.json()
        if not verdict.get("allowed"):
            # 402 carries the meaning the caller needs: the account, not the
            # request, is why this cannot run.
            return _refusal(402, verdict.get("reason") or "Spending refused.")

        self._auth_cache.remember(caller_key, model)
        return None

    async def settle(
        self,
        *,
        caller_key: str,
        model: str,
        usage: Optional[dict],
        assistant_id: Optional[int],
    ) -> None:
        """Report a completed call's usage.

        Best-effort by necessity -- the provider has already served and
        already billed us, so there is nothing to undo -- but never silent:
        an unreported call is spend with no ledger row, which is the failure
        this whole design exists to prevent, and it has to be visible in logs
        to be noticed at all.
        """
        if usage is None:
            LOGGER.warning("LLM broker: no usage to report (model=%s)", model)
            return
        try:
            response = await self._control.post(
                self.settings.settle_url,
                headers={"Authorization": f"Bearer {caller_key}"},
                json={
                    "model": model,
                    "usage": usage,
                    "assistant_id": assistant_id,
                },
            )
        except httpx.RequestError as exc:
            LOGGER.error(
                "LLM broker: usage NOT recorded (model=%s): %s",
                model,
                exc,
            )
            return
        if response.status_code >= 400:
            LOGGER.error(
                "LLM broker: usage NOT recorded (model=%s): settle returned %s",
                model,
                response.status_code,
            )

    async def relay(
        self,
        *,
        request: Request,
        url: str,
        headers: dict[str, str],
        body: dict,
        model: str,
        assistant_id: Optional[int],
        caller_key: str,
        stream: bool,
    ) -> Any:
        """Send the call to the provider and report what it used."""
        if not stream:
            try:
                response = await self._provider.post(url, headers=headers, json=body)
            except httpx.RequestError as exc:
                return _refusal(502, f"Upstream provider error: {exc}")

            payload = response.json() if response.content else {}
            if response.status_code < 400:
                await self.settle(
                    caller_key=caller_key,
                    model=model,
                    usage=usage_from_body(payload),
                    assistant_id=assistant_id,
                )
            return JSONResponse(status_code=response.status_code, content=payload)

        async def _stream() -> AsyncIterator[bytes]:
            tail = ""
            try:
                async with self._provider.stream(
                    "POST",
                    url,
                    headers=headers,
                    json=body,
                ) as upstream:
                    if upstream.status_code >= 400:
                        yield await upstream.aread()
                        return
                    async for chunk in upstream.aiter_bytes():
                        # Forwarded before anything is parsed: the caller is
                        # often a voice turn, and holding a chunk back to
                        # inspect it would be latency spent on accounting.
                        yield chunk
                        tail = (tail + chunk.decode("utf-8", "ignore"))[
                            -USAGE_TAIL_LIMIT:
                        ]
            except httpx.RequestError as exc:
                LOGGER.warning("LLM broker: stream error: %s", exc)
                return

            await self.settle(
                caller_key=caller_key,
                model=model,
                usage=usage_from_stream_tail(tail),
                assistant_id=assistant_id,
            )

        return StreamingResponse(_stream(), media_type="text/event-stream")


def build_router(broker: Broker) -> APIRouter:
    router = APIRouter()
    settings = broker.settings

    @router.post("/llm/chat/completions")
    async def chat_completions(request: Request) -> Any:
        """OpenAI-compatible calls, forwarded to OpenRouter."""
        caller_key = _caller_key(request)
        if not caller_key:
            return _refusal(401, "Missing API key.")
        if not settings.openrouter_api_key:
            return _refusal(503, "LLM broker holds no OpenRouter credential.")

        body = await request.json()
        model = str(body.get("model") or "").strip()
        if not model:
            return _refusal(400, "`model` is required.")
        # The body's model is provider-shaped and forwarded verbatim; the
        # ledger names the same model in accounting form. Orchestra recognises
        # an OpenRouter call by exactly this marker -- authorizing with the
        # bare id left the metering gate refusing every model its curated
        # catalogue did not happen to cover.
        accounting_model = f"{model}@openrouter"
        assistant_id = _assistant_id(request, body)

        refusal = await broker.authorize(
            caller_key=caller_key,
            model=accounting_model,
            assistant_id=assistant_id,
        )
        if refusal is not None:
            return refusal

        stream = bool(body.get("stream"))
        # Ask for cost on the way out: it is what Orchestra prices the call
        # from, and OpenRouter only reports it when a request opts in.
        body["usage"] = {"include": True}
        if stream:
            options = dict(body.get("stream_options") or {})
            options["include_usage"] = True
            body["stream_options"] = options

        return await broker.relay(
            request=request,
            url=f"{settings.openrouter_api_base.rstrip('/')}/chat/completions",
            headers={
                "Authorization": f"Bearer {settings.openrouter_api_key}",
                "Content-Type": "application/json",
            },
            body=body,
            model=accounting_model,
            assistant_id=assistant_id,
            caller_key=caller_key,
            stream=stream,
        )

    @router.post("/llm/anthropic/v1/messages")
    async def anthropic_messages(request: Request) -> Any:
        """Anthropic's own Messages API, forwarded to Anthropic."""
        caller_key = _caller_key(request)
        if not caller_key:
            return _refusal(401, "Missing API key.")
        if not settings.anthropic_api_key:
            return _refusal(503, "LLM broker holds no Anthropic credential.")

        body = await request.json()
        model = str(body.get("model") or "").strip()
        if not model:
            return _refusal(400, "`model` is required.")
        # Same split as the OpenRouter route: bare id to the provider,
        # accounting form to the ledger, so metering matches the curated
        # catalogue's own ``@anthropic`` entries rather than relying on
        # suffix-stripped comparison.
        accounting_model = f"{model}@anthropic"
        assistant_id = _assistant_id(request, body)

        refusal = await broker.authorize(
            caller_key=caller_key,
            model=accounting_model,
            assistant_id=assistant_id,
        )
        if refusal is not None:
            return refusal

        stream = bool(body.get("stream"))
        headers = {
            "x-api-key": settings.anthropic_api_key,
            "anthropic-version": request.headers.get(
                "anthropic-version",
                "2023-06-01",
            ),
            "content-type": "application/json",
            "accept": "text/event-stream" if stream else "application/json",
        }
        beta = request.headers.get("anthropic-beta")
        if beta:
            headers["anthropic-beta"] = beta

        return await broker.relay(
            request=request,
            url=f"{settings.anthropic_api_base.rstrip('/')}/v1/messages",
            headers=headers,
            body=body,
            model=accounting_model,
            assistant_id=assistant_id,
            caller_key=caller_key,
            stream=stream,
        )

    @router.get("/healthz")
    async def healthz() -> dict:
        """Readiness for the pod, not a report on which keys are present."""
        return {"ok": True}

    return router


def build_app(settings: Optional[BrokerSettings] = None) -> FastAPI:
    resolved = settings or load_settings()
    broker = Broker(resolved)
    app = FastAPI(title="Unify LLM broker", docs_url=None, redoc_url=None)
    proxy = Proxy(resolved.credential_proxies)
    app.state.broker = broker
    app.state.proxy = proxy
    app.include_router(build_router(broker))
    app.include_router(build_voice_router(resolved.voice_providers))
    app.include_router(build_proxy_router(proxy))

    @app.on_event("shutdown")
    async def _close() -> None:
        await broker.aclose()
        await proxy.aclose()

    return app
