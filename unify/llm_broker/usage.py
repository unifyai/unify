"""Find a provider's usage object so it can be reported for pricing.

Deliberately extraction only. What a call costs is decided by Orchestra,
which holds the ledger and the catalogue; a broker that priced its own calls
could declare any number, and Orchestra refusing to serve models it cannot
price would stop meaning anything. So this locates the provider's own usage
object and passes it on unaltered.

The two providers report it differently. OpenAI-compatible responses carry a
single ``usage`` object at the end. Anthropic splits it: ``message_start``
carries the input tokens and the final ``message_delta`` the output count, so
a stream has to merge what it finds rather than expect one event to hold both.
"""

from __future__ import annotations

import json
from typing import Any, Optional

#: Retained tail of a stream, bounded so a long generation is not buffered in
#: memory to read a usage block that only ever appears at the end.
USAGE_TAIL_LIMIT = 16_384


def usage_from_body(body: Any) -> Optional[dict]:
    """Pull the usage object out of a non-streamed response body."""
    if not isinstance(body, dict):
        return None
    usage = body.get("usage")
    return usage if isinstance(usage, dict) else None


def _iter_sse_payloads(tail: str) -> list[dict]:
    """Decode the JSON payloads of any complete ``data:`` lines in *tail*."""
    payloads: list[dict] = []
    for line in tail.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        data = line[len("data:") :].strip()
        if not data or data == "[DONE]":
            continue
        try:
            obj = json.loads(data)
        except json.JSONDecodeError:
            # A truncated first line is expected: the tail is a byte window,
            # not a message boundary.
            continue
        if isinstance(obj, dict):
            payloads.append(obj)
    return payloads


def usage_from_stream_tail(tail: str) -> Optional[dict]:
    """Merge whatever usage the tail of a stream carries.

    Merging rather than taking the last one seen is what makes this work for
    Anthropic, whose input and output counts arrive in different events. For
    an OpenAI-compatible stream there is only one usage object, so merging is
    a no-op.
    """
    merged: dict[str, Any] = {}
    for obj in _iter_sse_payloads(tail):
        sources = (obj.get("usage"), (obj.get("message") or {}).get("usage"))
        for source in sources:
            if not isinstance(source, dict):
                continue
            for name, value in source.items():
                # Scalars only: nested breakdowns (cost_details,
                # prompt_tokens_details) are informational, and merging them
                # key-by-key across events would interleave unrelated shapes.
                if isinstance(value, (int, float)):
                    merged[name] = value
    return merged or None
