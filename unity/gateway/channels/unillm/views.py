"""FastAPI route for the UniLLM OpenAI-compatible chat completions proxy.

Ports ``communication/unillm/views.py`` (plus the two auth helpers
from ``communication/dependencies.py`` inlined locally) into
``unity.gateway``. Translation applied:

* ``from communication.dependencies import authenticate_user_api_key,
  extract_api_key`` -> inlined below as ``_authenticate_user_api_key``
  + ``_extract_api_key``. unillm is the only user-API-keyed
  public-facing channel today; if a second channel ever needs the
  same pair we can promote them to ``unity.gateway.common.auth``.
* ``from communication.unillm.schema import ChatCompletionRequest``
  -> ``from unity.gateway.channels.unillm.schema import ...``
  (schema ported verbatim alongside this module).
* ``from common.settings import SETTINGS`` ->
  ``from unity.settings import SETTINGS``;
  ``SETTINGS.orchestra_url`` -> ``SETTINGS.ORCHESTRA_URL``.

Wire behaviour preserved bit-for-bit so the gateway aggregator can
mount this router at ``/unillm`` (or any path the deployment
chooses) and external SDK callers see no change.
"""

from __future__ import annotations

import json
import logging
from typing import AsyncGenerator

import httpx
import unillm
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from unity.gateway.channels.unillm.schema import ChatCompletionRequest
from unity.settings import SETTINGS

logger = logging.getLogger("unity.gateway.channels.unillm")

router = APIRouter()


# ---------------------------------------------------------------------------
# Auth helpers (inlined from communication/dependencies.py)
# ---------------------------------------------------------------------------


def _extract_api_key(request: Request) -> str:
    """Extract the Bearer token from the Authorization header."""
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:]
    raise HTTPException(status_code=401, detail="Missing API key.")


async def _authenticate_user_api_key(api_key: str) -> dict:
    """Validate a user API key against Orchestra's /user/basic-info endpoint.

    Returns the user info dict (contains user_id, email, etc.) on
    success. Raises HTTPException(401) on failure. Network failures
    surface as 401 because we cannot prove the key is valid; a 5xx
    Orchestra outage will look like an auth failure to the SDK
    consumer, which is the safest default for a credential-gated
    endpoint.
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{SETTINGS.ORCHESTRA_URL}/user/basic-info",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10.0,
        )

    if response.status_code != 200:
        logger.warning("API key authentication failed: %s", response.status_code)
        raise HTTPException(status_code=401, detail="Invalid API key.")

    return response.json()


# ---------------------------------------------------------------------------
# POST /chat/completions
# ---------------------------------------------------------------------------


@router.post("/chat/completions")
async def chat_completions(
    request_body: ChatCompletionRequest,
    request: Request,
):
    """OpenAI-compatible chat completions endpoint via UniLLM.

    Routes requests through UniLLM for caching, cost tracking, and
    multi-provider support. The caller's API key is extracted from
    the Authorization header and validated against Orchestra before
    forwarding to UniLLM.

    The model should be specified in UniLLM format: ``model@provider``
    (e.g. ``claude-sonnet-4-20250514@anthropic``, ``gpt-4o@openai``).
    """
    api_key = _extract_api_key(request)
    await _authenticate_user_api_key(api_key)

    messages = [msg.model_dump(exclude_none=True) for msg in request_body.messages]

    if request_body.stream:
        return await _stream_response(request_body, messages, api_key)
    return await _non_stream_response(request_body, messages, api_key)


async def _non_stream_response(
    request_body: ChatCompletionRequest,
    messages: list,
    api_key: str,
) -> dict:
    """Handle non-streaming chat completion."""
    client = unillm.AsyncUnify(
        request_body.model,
        api_key=api_key,
        temperature=request_body.temperature,
        max_completion_tokens=(
            request_body.max_completion_tokens or request_body.max_tokens
        ),
        top_p=request_body.top_p,
        frequency_penalty=request_body.frequency_penalty,
        presence_penalty=request_body.presence_penalty,
        stop=request_body.stop,
        seed=request_body.seed,
        tools=request_body.tools,
        tool_choice=request_body.tool_choice,
        response_format=request_body.response_format,
        return_full_completion=True,
    )

    response = await client.generate(messages=messages)
    return response.model_dump()


async def _stream_response(
    request_body: ChatCompletionRequest,
    messages: list,
    api_key: str,
) -> StreamingResponse:
    """Handle streaming chat completion with Server-Sent Events."""

    async def generate() -> AsyncGenerator[str, None]:
        client = unillm.AsyncUnify(
            request_body.model,
            api_key=api_key,
            stream=True,
            stream_options={"include_usage": True},
            temperature=request_body.temperature,
            max_completion_tokens=(
                request_body.max_completion_tokens or request_body.max_tokens
            ),
            top_p=request_body.top_p,
            frequency_penalty=request_body.frequency_penalty,
            presence_penalty=request_body.presence_penalty,
            stop=request_body.stop,
            seed=request_body.seed,
            tools=request_body.tools,
            tool_choice=request_body.tool_choice,
            response_format=request_body.response_format,
            return_full_completion=True,
        )

        async for chunk in client.generate(messages=messages):
            chunk_data = chunk.model_dump() if hasattr(chunk, "model_dump") else chunk
            yield f"data: {json.dumps(chunk_data)}\n\n"

        yield "data: [DONE]\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


__all__ = ["router"]
