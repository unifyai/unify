from typing import Any, Callable, Dict, List
from dataclasses import dataclass, field
from datetime import datetime

__all__ = [
    "unwrap_tool_callable",
    "now",
    "get_assistant_timezone",
    "tool_name",
    "PromptParts",
]


def unwrap_tool_callable(fn: Callable) -> Callable:
    """Return the underlying callable for prompt/schema introspection.

    Tool tables often store ``ToolSpec`` wrappers, and sandbox instrumentation can
    introduce ``functools.wraps`` layers around the real callable. Prompt builders
    should inspect the original function signature/docstring rather than the
    wrapper metadata.
    """
    import inspect

    target = getattr(fn, "fn", fn)
    try:
        return inspect.unwrap(target)
    except Exception:
        return target


def get_assistant_timezone() -> str | None:
    """Return the assistant's configured IANA timezone, or ``None`` when unset."""
    from unify.session_details import SESSION_DETAILS

    return SESSION_DETAILS.assistant.timezone.strip() or None


def _utc_now() -> datetime:
    from datetime import timezone as dt_timezone

    return datetime.now(dt_timezone.utc)


def now(time_only: bool = False, as_string: bool = True) -> "str | datetime":
    """Return the current timestamp in the assistant's timezone.

    The assistant's session profile carries its ``timezone`` (an IANA
    identifier like "America/New_York"); UTC is used when it is unset or
    invalid.

    Args:
        time_only: If True and as_string=True, return only the time portion.
        as_string: If True, return formatted string. If False, return datetime object.

    Returns:
        If as_string=True: "Thursday, January 15, 2026 at 02:09 PM UTC" (or time only)
        If as_string=False: datetime object

    In tests, this function is monkeypatched by tests/conftest.py to return
    fixed or incrementing datetimes for cache consistency.
    """
    from zoneinfo import ZoneInfo

    tz_name = get_assistant_timezone() or "UTC"
    utc_now = _utc_now()
    try:
        local_dt = utc_now.astimezone(ZoneInfo(tz_name))
        label = tz_name
    except Exception:
        local_dt = utc_now
        label = "UTC"

    if not as_string:
        return local_dt
    if time_only:
        return local_dt.strftime("%I:%M %p ") + label
    return local_dt.strftime("%A, %B %d, %Y at %I:%M %p ") + label


def tool_name(tools: Dict[str, Callable], needle: str) -> str | None:
    """Best-effort lookup: find the first tool whose name contains ``needle``.

    Comparison is case-insensitive. Returns ``None`` if not found.
    """
    lowered = needle.lower()
    return next((name for name in tools if lowered in name.lower()), None)


@dataclass
class PromptParts:
    """Structured prompt builder with List[Dict] internal representation.

    Each part is stored as ``{"type": "text", "text": "...", "_static": True/False}``.
    The `add` method handles separator insertion (blank lines between blocks),
    and `flatten` joins the parts into the final prompt string.
    """

    _parts: List[Dict[str, Any]] = field(default_factory=list)

    def add(self, part: str, separator: bool = True, static: bool = True) -> None:
        """Add a part, optionally with a preceding blank line separator.

        Consecutive parts with the same `static` value are merged into a single
        content block. A new entry is created only when the `static` value
        differs from the previous content part. Empty parts are skipped.

        Parameters
        ----------
        part : str
            The content to add. Empty strings are ignored.
        separator : bool
            If True (default), adds ``\\n\\n`` before the part.
            If False, only a single newline is added.
        static : bool
            If True (default), the part is marked as static content.
            Set to False for dynamic content that may change between runs.
        """
        # Skip empty parts
        if not part:
            return

        if not self._parts:
            # First item - add directly without separator
            self._parts.append({"type": "text", "text": part, "_static": static})
        elif self._parts[-1]["_static"] == static:
            # Same static - merge with previous content
            joiner = "\n\n" if separator else "\n"
            self._parts[-1]["text"] += joiner + part
        else:
            # Different static - add new block
            content = ("\n\n" + part) if separator else "\n" + part
            self._parts.append({"type": "text", "text": content, "_static": static})

    def to_list(self) -> List[Dict[str, Any]]:
        """Return the internal structured parts."""
        return list(self._parts)

    def flatten(self) -> str:
        """Return the full prompt string: static parts first, dynamic last.

        Provider prompt caches key on the request prefix, so a dynamic
        block sitting mid-prompt re-bills every static part behind it each
        time it changes. Emitting ``static=False`` parts (current time,
        transient status blocks) after all static parts keeps the stable
        prefix byte-identical; volatile content only invalidates itself.
        Relative order within each group is preserved.
        """
        static = [p["text"] for p in self._parts if p.get("_static", True)]
        dynamic = [p["text"] for p in self._parts if not p.get("_static", True)]
        return "".join(static) + "".join(dynamic)

    def __str__(self) -> str:
        return self.flatten()
