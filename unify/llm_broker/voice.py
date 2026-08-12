"""WebSocket passthrough for the voice providers.

The voice pipeline's STT/TTS plugins (Deepgram, Cartesia, ElevenLabs) each open
a WebSocket straight to their provider and authenticate it with a key read from
the environment. Mounting those keys in the runtime container leaves them
readable by in-process sandbox code -- the same exposure the LLM broker exists
to remove -- so the keys live only here, and the plugins are pointed at this
sidecar over loopback instead.

This forwards the socket verbatim: it reads the caller's placeholder credential
off the handshake, opens the upstream socket with the real one injected, and
relays every frame in both directions without buffering or parsing. Audio is
latency-sensitive, so nothing here inspects a frame -- holding one back to look
at it would be delay spent on nothing, and unlike the LLM legs there is no usage
object to recover from the stream.

The credential lives in a different place per provider -- an ``Authorization:
Token`` header for Deepgram, an ``xi-api-key`` header for ElevenLabs, an
``api_key`` query parameter for Cartesia -- so each provider says where its
credential is and the swap happens there, not in one assumed location.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Mapping, Optional
from urllib.parse import urlencode, parse_qs

import websockets
from fastapi import APIRouter, WebSocket
from starlette.websockets import WebSocketDisconnect, WebSocketState

LOGGER = logging.getLogger(__name__)

#: A downed provider should fail the socket fast rather than hang the caller.
_UPSTREAM_OPEN_TIMEOUT_S = 10.0


@dataclass(frozen=True)
class VoiceProvider:
    """Where a provider's credential sits and where its bytes go.

    ``cred_in`` is ``"header"`` or ``"query"``. For a header, ``cred_name`` is
    the header and ``cred_template`` formats the value (Deepgram wants
    ``"Token {key}"``, ElevenLabs the bare key). For a query, ``cred_name`` is
    the parameter carrying the key.
    """

    name: str
    upstream_base: str
    api_key: Optional[str]
    cred_in: str
    cred_name: str
    cred_template: str = "{key}"


def _upstream_url(provider: VoiceProvider, path: str, query: str) -> str:
    """Build the wss:// upstream URL, swapping in the real key for query auth."""
    base = provider.upstream_base.rstrip("/")
    ws_base = base.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    suffix = path.lstrip("/")
    if provider.cred_in == "query":
        params = parse_qs(query, keep_blank_values=True)
        params[provider.cred_name] = [provider.api_key or ""]
        # doseq keeps every other parameter (model, version, ...) untouched.
        query = urlencode(params, doseq=True)
    url = f"{ws_base}/{suffix}" if suffix else ws_base
    return f"{url}?{query}" if query else url


def _upstream_headers(provider: VoiceProvider) -> dict[str, str]:
    if provider.cred_in == "header":
        return {provider.cred_name: provider.cred_template.format(key=provider.api_key)}
    return {}


def _has_caller_credential(provider: VoiceProvider, ws: WebSocket) -> bool:
    """The caller must present *a* credential; loopback binding is what keeps
    this reachable only from inside the pod, and the real key is never here to
    lift. An absent credential means a misconfigured caller, not an attacker."""
    if provider.cred_in == "header":
        return bool(ws.headers.get(provider.cred_name))
    return bool(parse_qs(ws.url.query, keep_blank_values=False).get(provider.cred_name))


async def _proxy(provider: VoiceProvider, client: WebSocket, path: str) -> None:
    url = _upstream_url(provider, path, client.url.query)
    try:
        upstream = await asyncio.wait_for(
            websockets.connect(url, additional_headers=_upstream_headers(provider)),
            timeout=_UPSTREAM_OPEN_TIMEOUT_S,
        )
    except (OSError, asyncio.TimeoutError, websockets.WebSocketException) as exc:
        LOGGER.warning(
            "voice broker: upstream connect failed (%s): %s",
            provider.name,
            exc,
        )
        await client.close(code=1011)
        return

    await client.accept()

    async def client_to_upstream() -> None:
        while True:
            message = await client.receive()
            if message["type"] == "websocket.disconnect":
                return
            if message.get("bytes") is not None:
                await upstream.send(message["bytes"])
            elif message.get("text") is not None:
                await upstream.send(message["text"])

    async def upstream_to_client() -> None:
        async for message in upstream:
            if isinstance(message, (bytes, bytearray)):
                await client.send_bytes(bytes(message))
            else:
                await client.send_text(message)

    tasks = [
        asyncio.create_task(client_to_upstream()),
        asyncio.create_task(upstream_to_client()),
    ]
    try:
        # Either side ending ends the call: a hung half is latency, not data.
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except WebSocketDisconnect:
        pass
    finally:
        for task in tasks:
            task.cancel()
        await upstream.close()
        if client.client_state != WebSocketState.DISCONNECTED:
            await client.close()


def build_voice_router(providers: Mapping[str, VoiceProvider]) -> APIRouter:
    router = APIRouter()

    @router.websocket("/voice/{provider}/{path:path}")
    async def voice(ws: WebSocket, provider: str, path: str) -> None:
        cfg = providers.get(provider)
        # 1008 = policy violation. Refuse before ``accept`` so a caller for an
        # unconfigured provider gets a handshake rejection, not an open socket.
        if cfg is None or not cfg.api_key:
            await ws.close(code=1008)
            return
        if not _has_caller_credential(cfg, ws):
            await ws.close(code=1008)
            return
        await _proxy(cfg, ws, path)

    return router
