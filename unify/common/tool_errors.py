"""Failures a tool call can be told about, raised rather than returned.

A tool that cannot run as written has two ways to say so: return a description
of the problem, or raise one. Both reach the caller — the loop feeds a raised
exception back as the tool result — but only a raise is visible to the loop
itself, and the loop is what notices a request arriving unchanged for the tenth
time. A returned error is indistinguishable from a successful call, so it resets
the very counter that would catch the repetition.

``ToolInputError`` is the raise, carrying the corrective text so the loop can
surface it verbatim instead of a traceback. Internal frames say nothing about
which argument to change; the message does.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

__all__ = ["ToolInputError"]


class ToolInputError(Exception):
    """A tool call that cannot run as written, described so the caller can fix it.

    Raise this for a request that is understood and refused — an argument that
    contradicts another, a resource that does not exist, a mode that does not
    apply. Not for a failure the caller cannot act on: an unexpected exception
    should stay unexpected, because a traceback is the useful artifact there.

    Args:
        message: What is wrong, in the caller's terms.
        suggestion: The change that would make the call work.
        received: The arguments as they arrived, so the caller can see which
            of them the message is about.
    """

    def __init__(
        self,
        message: str,
        *,
        suggestion: Optional[str] = None,
        received: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.message = message
        self.suggestion = suggestion
        self.received: Dict[str, Any] = dict(received or {})
        super().__init__(message)

    def as_tool_result(self) -> str:
        """Render the text the caller reads in place of a traceback."""
        lines = [self.message]
        if self.suggestion:
            lines.append(f"Suggestion: {self.suggestion}")
        if self.received:
            shown = ", ".join(f"{k}={v!r}" for k, v in self.received.items())
            lines.append(f"Received: {shown}")
        return "\n".join(lines)
