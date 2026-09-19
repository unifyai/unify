from __future__ import annotations

import json
import time as _time
from dataclasses import dataclass
from typing import Union


def perf_counter() -> float:
    """Monotonic seconds from an arbitrary origin; a module-level wrapper so
    it can be monkey-patched."""
    return _time.perf_counter()


def _fmt(seconds: float, *, prefix: str = "") -> str:
    """Compact time string (``0s``, ``100ms``, ``2s50ms``, ``1m30s``,
    ``1h2m30s``) with *prefix* prepended. Milliseconds appear only under
    one minute."""
    if seconds < 0:
        return f"{prefix}0s"

    total_ms = int(round(seconds * 1000))
    if total_ms == 0:
        return f"{prefix}0s"

    h, remainder_ms = divmod(total_ms, 3_600_000)
    m, remainder_ms = divmod(remainder_ms, 60_000)
    s, ms = divmod(remainder_ms, 1000)

    parts: list[str] = []
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    if s:
        parts.append(f"{s}s")
    if ms and not h and not m:
        parts.append(f"{ms}ms")
    if not parts:
        parts.append("0s")
    return prefix + "".join(parts)


def format_offset(seconds: float) -> str:
    """Format *seconds* as a compact signed offset string."""
    return _fmt(seconds, prefix="+")


def format_duration(seconds: float) -> str:
    """Format *seconds* as a compact human-readable duration."""
    return _fmt(seconds)


_EXPLANATION_PROMPT = (
    "## Time Annotations\n"
    "This conversation includes inline timing metadata so you can reason "
    "about elapsed time and tool execution order.\n\n"
    "- **Tool results** from non-steering tools include a JSON envelope:\n"
    '  `{"tool_result": <result>, "metadata": {"called_at": "+1m30s", "duration": "2s45ms"}}`\n'
    "  `called_at` is the offset since conversation start when the tool was invoked; "
    "`duration` is wall-clock execution time.\n"
    "- **User messages** are prefixed with `[elapsed: +XmYs]` showing "
    "when the message was sent relative to conversation start.\n"
    '- **Pending tool placeholders** include `"meta:started"` with the '
    "invocation offset.\n\n"
    "Use these annotations when reasoning about timing, ordering, or how "
    "long operations took. Do NOT reproduce the annotations in your replies."
)


@dataclass
class TimeContext:
    """Wall-clock offsets of an async tool loop, anchored at the
    ``perf_counter()`` value captured at loop start, with the formatting
    and result-wrapping helpers the loop uses."""

    perf_counter_start: float

    def current_offset(self) -> str:
        return format_offset(perf_counter() - self.perf_counter_start)

    def offset_at(self, perf_time: float) -> str:
        return format_offset(perf_time - self.perf_counter_start)

    def duration_since(self, perf_time: float) -> str:
        return format_duration(perf_counter() - perf_time)

    def wrap_result(
        self,
        content: Union[str, list],
        scheduled_time: float,
    ) -> Union[str, list]:
        """Wrap serialized tool *content* (from ``serialize_tool_content``)
        with timing metadata relative to *scheduled_time*: a JSON string
        ``{"tool_result": <content>, "metadata": {"called_at", "duration"}}``
        for string content, or the original block list with a metadata text
        block prepended for image content."""
        called_at = self.offset_at(scheduled_time)
        duration = self.duration_since(scheduled_time)
        meta = {"called_at": called_at, "duration": duration}

        if isinstance(content, list):
            meta_block = {
                "type": "text",
                "text": json.dumps({"metadata": meta}),
            }
            return [meta_block, *content]

        envelope = {"tool_result": content, "metadata": meta}
        return json.dumps(envelope, indent=4)

    def prefix_user_message(self, text: str) -> str:
        return f"[elapsed: {self.current_offset()}] {text}"

    @staticmethod
    def build_explanation_prompt() -> str:
        """The static system-message content explaining the annotations."""
        return _EXPLANATION_PROMPT


def create_time_context() -> TimeContext:
    return TimeContext(perf_counter_start=perf_counter())
