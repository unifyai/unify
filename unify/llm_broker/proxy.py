"""Header-swap HTTP proxy for the non-LLM REST providers.

Tavily (web search) and Recall (meeting bots) are plain request/response REST
authenticated with an ``Authorization`` header. Like the LLM and voice legs
their keys are held only in the sidecar, so the pod's clients are pointed here
with the pod's UNIFY_KEY as a placeholder and this swaps in the real one before
forwarding. Unlike the LLM leg there is no spend to authorise or meter -- these
are not priced in the LLM ledger -- so it just forwards the call and returns the
provider's response unchanged.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Mapping, Optional

import httpx
from fastapi import APIRouter, Request
from starlette.responses import JSONResponse, Response

LOGGER = logging.getLogger(__name__)

#: Search crawls and bot dispatch are slower than a keystroke but not open-ended.
_TIMEOUT = httpx.Timeout(120.0, connect=10.0)

#: Headers that describe the caller's hop, not the payload; httpx sets its own.
_DROP_REQUEST_HEADERS = {"host", "authorization", "content-length", "connection"}
_DROP_RESPONSE_HEADERS = {
    "content-length",
    "content-encoding",
    "transfer-encoding",
    "connection",
}


@dataclass(frozen=True)
class CredentialProxy:
    """An upstream to forward to and the credential to forward it with."""

    name: str
    upstream_base: str
    auth_scheme: str  # e.g. "Bearer" (Tavily) or "Token" (Recall).
    api_key: Optional[str]


class Proxy:
    def __init__(self, providers: Mapping[str, CredentialProxy]) -> None:
        self.providers = providers
        self._client = httpx.AsyncClient(timeout=_TIMEOUT)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def forward(
        self,
        provider: CredentialProxy,
        request: Request,
        path: str,
    ) -> Response:
        url = f"{provider.upstream_base.rstrip('/')}/{path.lstrip('/')}"
        if request.url.query:
            url = f"{url}?{request.url.query}"
        headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower() not in _DROP_REQUEST_HEADERS
        }
        headers["Authorization"] = f"{provider.auth_scheme} {provider.api_key}"
        try:
            upstream = await self._client.request(
                request.method,
                url,
                headers=headers,
                content=await request.body(),
            )
        except httpx.RequestError as exc:
            LOGGER.warning(
                "credential proxy: upstream error (%s): %s",
                provider.name,
                exc,
            )
            return JSONResponse(
                status_code=502,
                content={"detail": f"Upstream error: {exc}"},
            )
        passthrough = {
            k: v
            for k, v in upstream.headers.items()
            if k.lower() not in _DROP_RESPONSE_HEADERS
        }
        return Response(
            content=upstream.content,
            status_code=upstream.status_code,
            headers=passthrough,
        )


def build_proxy_router(proxy: Proxy) -> APIRouter:
    router = APIRouter()

    @router.api_route(
        "/proxy/{provider}/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    )
    async def credential_proxy(provider: str, path: str, request: Request) -> Response:
        cfg = proxy.providers.get(provider)
        if cfg is None or not cfg.api_key:
            return JSONResponse(
                status_code=503,
                content={"detail": "Unknown or unconfigured provider."},
            )
        # The caller presents its nonce here; loopback binding is what keeps the
        # route reachable only from inside the pod, and the real key is never
        # here to lift. An absent header is a misconfigured caller.
        if not request.headers.get("authorization"):
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing credential."},
            )
        return await proxy.forward(cfg, request, path)

    return router
