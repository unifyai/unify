"""FastAPI route for the UniLLM OpenAI-compatible chat completions proxy.

Ports ``communication/unillm/views.py`` (plus the two auth helpers
from ``communication/dependencies.py`` inlined locally) into
``unify.gateway``. Translation applied:

* ``from communication.dependencies import authenticate_user_api_key,
  extract_api_key`` -> inlined below as ``_authenticate_user_api_key``
  + ``_extract_api_key``. unillm is the only user-API-keyed
  public-facing channel today; if a second channel ever needs the
  same pair we can promote them to ``unify.gateway.common.auth``.
* ``from communication.unillm.schema import ChatCompletionRequest``
  -> ``from unify.gateway.channels.unillm.schema import ...``
  (schema ported verbatim alongside this module).
* ``from common.settings import SETTINGS`` ->
  ``from unify.settings import SETTINGS``;
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
from unillm.limit_hooks import SpendingLimitExceededError

from unify.gateway.channels.unillm.schema import ChatCompletionRequest
from unify.settings import SETTINGS
from unify.spending_limits import (
    caller_context,
    install_multi_tenant_limit_check_hook,
)

logger = logging.getLogger("unify.gateway.channels.unillm")

router = APIRouter()

# This endpoint spends real money on a bare user API key, so it must run
# under the same spending gates as the assistant runtime (credit balance,
# trial daily burn ceiling, per-entity monthly caps). The runtime installs
# them in ``unify.init()``, which no gateway process calls — so install
# here, at the one import site that mounts this router. UniLLM treats a
# missing hook as "allowed", which is how this path previously enforced
# nothing at all.
install_multi_tenant_limit_check_hook()


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


def _ensure_non_system_message(messages: list) -> None:
    """Guarantee at least one non-system message in the payload.

    Anthropic rejects any request whose message list contains only
    system messages with ``BadRequestError: Anthropic requires at least
    one non-system message``. Browser-automation clients (Magnus/BAML)
    can render a system-only chat on their first planning call, before
    any observation exists, which deterministically fails and exhausts
    the client's retry budget. Appending a neutral continuation turn
    makes the request valid without altering intent, and is a no-op for
    the common case where a user/assistant/tool message is already
    present. Mutates ``messages`` in place.
    """
    if not any(msg.get("role") != "system" for msg in messages):
        messages.append({"role": "user", "content": "Please continue."})


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
    (e.g. ``claude-sonnet-4-20250514@anthropic``,
    ``openai/gpt-4o@openrouter``).
    """
    api_key = _extract_api_key(request)
    user_info = await _authenticate_user_api_key(api_key)

    messages = [msg.model_dump(exclude_none=True) for msg in request_body.messages]
    _ensure_non_system_message(messages)

    user_id = user_info.get("user_id")
    # Absent on Orchestra builds predating the field; None is also the
    # correct value for a personal key, and the user-spend check resolves
    # the wallet server-side either way.
    org_id = user_info.get("organization_id")

    # Assert the gates here rather than relying on the equivalent check
    # inside UniLLM's own call path. Two reasons: a denied stream has to
    # fail before StreamingResponse commits the status line, and this
    # endpoint's enforcement should not be contingent on the internal
    # wiring of a callee — that contingency is precisely how it came to
    # enforce nothing at all.
    await _assert_within_limits(
        model=request_body.model,
        api_key=api_key,
        user_id=user_id,
        org_id=org_id,
    )

    if request_body.stream:
        return await _stream_response(
            request_body,
            messages,
            api_key,
            user_id=user_id,
            org_id=org_id,
        )
    return await _non_stream_response(
        request_body,
        messages,
        api_key,
        user_id=user_id,
        org_id=org_id,
    )


async def _assert_within_limits(
    *,
    model: str,
    api_key: str,
    user_id: str | None,
    org_id: int | None,
) -> None:
    """Run the spending gates up-front, raising 402 if the caller is blocked.

    UniLLM runs the same check inside ``generate()``; this duplicate exists
    only so the streaming path can fail before any bytes are committed to
    the response. No-op when no hook is installed (self-host, local dev).
    """
    from unillm.limit_hooks import (
        LimitCheckRequest,
        check_limits,
        is_limit_check_enabled,
    )

    if not is_limit_check_enabled():
        return

    async with caller_context(api_key, user_id=user_id, org_id=org_id):
        result = await check_limits(
            LimitCheckRequest(model=model, endpoint="chat/completions"),
        )

    if not result.allowed:
        raise _limit_denied(SpendingLimitExceededError(result))


def _limit_denied(error: SpendingLimitExceededError) -> HTTPException:
    """Map a spending-limit denial onto 402 Payment Required.

    402 rather than 429: the caller is not being rate-limited, they are out
    of money (or out of trial allowance). OpenAI-compatible clients surface
    the body text, so the hook's reason string reaches the user unaltered.
    """
    return HTTPException(status_code=402, detail=str(error))


async def _non_stream_response(
    request_body: ChatCompletionRequest,
    messages: list,
    api_key: str,
    *,
    user_id: str | None = None,
    org_id: int | None = None,
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

    async with caller_context(api_key, user_id=user_id, org_id=org_id):
        try:
            response = await client.generate(messages=messages)
        except SpendingLimitExceededError as e:
            raise _limit_denied(e) from e
    return response.model_dump()


async def _stream_response(
    request_body: ChatCompletionRequest,
    messages: list,
    api_key: str,
    *,
    user_id: str | None = None,
    org_id: int | None = None,
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

        async with caller_context(api_key, user_id=user_id, org_id=org_id):
            async for chunk in client.generate(messages=messages):
                chunk_data = (
                    chunk.model_dump() if hasattr(chunk, "model_dump") else chunk
                )
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
