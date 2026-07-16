"""Classify and retry transient Orchestra ↔ client integration failures.

Orchestra ``run_tool`` already retries provider/Composio flakiness (HTTP 429/5xx,
GraphQL platform errors on reads). This module only retries **transport**
failures talking to Orchestra itself — never re-interpreting an ``ok`` provider
payload (that would multiply provider attempts).

Agents and task scripts must not wrap custom retry loops around
``primitives.integrations.*`` / ``unisdk.run_integration_tool`` — call once and
treat the envelope as final after this layer + Orchestra have absorbed retries.

Opt out: set ``UNIFY_INTEGRATION_TRANSPORT_RETRY_MAX_ATTEMPTS=1``.
"""

from __future__ import annotations

import asyncio
import os
import random
import re
import time
from typing import Any, Awaitable, Callable, Optional, TypeVar

T = TypeVar("T")

DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_BASE_DELAY_SECONDS = 0.5
DEFAULT_MAX_DELAY_SECONDS = 8.0

_TRANSIENT_MESSAGE_RE = re.compile(
    r"("
    r"timed?\s*out|timeout|temporar(?:y|ily)|unavailable|"
    r"connection\s*(?:reset|refused|aborted|error)|"
    r"broken\s*pipe|network|dns|name\s*resolution|"
    r"bad\s*gateway|gateway\s*timeout|service\s*unavailable|"
    r"too many requests|rate\s*limit|"
    r"expecting\s*value|empty\s*response|connection\s*broken|"
    r"502|503|504|429"
    r")",
    re.IGNORECASE,
)


def transport_retry_max_attempts() -> int:
    raw = os.environ.get("UNIFY_INTEGRATION_TRANSPORT_RETRY_MAX_ATTEMPTS")
    if raw is None or str(raw).strip() == "":
        return DEFAULT_MAX_ATTEMPTS
    return max(1, int(raw))


def is_transient_transport_message(message: Any) -> bool:
    return bool(_TRANSIENT_MESSAGE_RE.search(str(message or "")))


def is_transient_transport_envelope(payload: Any) -> bool:
    """True when Unify ops returned a request-failed envelope worth retrying."""

    if not isinstance(payload, dict):
        return False
    if payload.get("status") != "error":
        return False
    error = payload.get("error")
    if not isinstance(error, dict):
        return False
    if error.get("code") != "unify_integration_request_failed":
        return False
    return is_transient_transport_message(error.get("message"))


def compute_retry_delay_seconds(attempt_index: int) -> float:
    exp = min(
        DEFAULT_MAX_DELAY_SECONDS,
        DEFAULT_BASE_DELAY_SECONDS * (2 ** max(0, attempt_index)),
    )
    jitter = random.uniform(0.0, min(0.25, exp * 0.25))
    return min(DEFAULT_MAX_DELAY_SECONDS, exp + jitter)


def call_with_transport_retries(
    fn: Callable[[], T],
    *,
    max_attempts: Optional[int] = None,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    attempts = (
        max_attempts if max_attempts is not None else transport_retry_max_attempts()
    )
    attempts = max(1, int(attempts))
    last: T | None = None
    for index in range(attempts):
        value = fn()
        last = value
        if not is_transient_transport_envelope(value) or index >= attempts - 1:
            return value
        sleep(compute_retry_delay_seconds(index))
    assert last is not None
    return last


async def async_call_with_transport_retries(
    fn: Callable[[], Awaitable[T]],
    *,
    max_attempts: Optional[int] = None,
) -> T:
    attempts = (
        max_attempts if max_attempts is not None else transport_retry_max_attempts()
    )
    attempts = max(1, int(attempts))
    last: T | None = None
    for index in range(attempts):
        value = await fn()
        last = value
        if not is_transient_transport_envelope(value) or index >= attempts - 1:
            return value
        await asyncio.sleep(compute_retry_delay_seconds(index))
    assert last is not None
    return last
