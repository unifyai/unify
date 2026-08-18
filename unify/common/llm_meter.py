"""Per-run LLM token accounting split by purpose.

Every actor LLM client is created with a ``purpose`` — ``planning`` (the
CodeAct loop and its librarian), ``verification`` (verifier passes) or
``repair`` (repair loops) — carried in the client's ``origin`` tag. A single
unillm event listener reads that tag back from each ``LLMEvent`` and adds
the call's usage to the :class:`RunMeter` bound to the current context, so a
task run can report how many tokens went to planning, to verifying, and to
repairing. The listener never raises and never changes a request.
"""

from __future__ import annotations

import contextvars
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import unillm

from unify.common.llm_client import LLMPurpose, purpose_from_origin

PURPOSES: tuple[LLMPurpose, ...] = ("planning", "verification", "repair")


@dataclass
class RunMeter:
    """Prompt/completion tokens and cost per purpose for one run."""

    tokens: Dict[str, Dict[str, int]] = field(
        default_factory=lambda: {
            purpose: {"prompt": 0, "completion": 0} for purpose in PURPOSES
        },
    )
    calls: Dict[str, int] = field(default_factory=lambda: {p: 0 for p in PURPOSES})
    cost: Dict[str, float] = field(default_factory=lambda: {p: 0.0 for p in PURPOSES})
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(
        self,
        purpose: str,
        *,
        prompt_tokens: int,
        completion_tokens: int,
        cost: Optional[float] = None,
    ) -> None:
        if purpose not in self.tokens:
            purpose = "planning"
        with self._lock:
            self.tokens[purpose]["prompt"] += int(prompt_tokens or 0)
            self.tokens[purpose]["completion"] += int(completion_tokens or 0)
            self.calls[purpose] += 1
            if cost:
                self.cost[purpose] += float(cost)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "tokens": {k: dict(v) for k, v in self.tokens.items()},
                "calls": dict(self.calls),
                "cost": dict(self.cost),
            }

    def total(self, purpose: str) -> int:
        with self._lock:
            entry = self.tokens.get(purpose) or {}
            return int(entry.get("prompt", 0)) + int(entry.get("completion", 0))


current_run_meter: contextvars.ContextVar[Optional[RunMeter]] = contextvars.ContextVar(
    "current_run_meter",
    default=None,
)

_listener: Optional[Any] = None
_install_lock = threading.Lock()


def _usage_from_event(event: Any) -> tuple[int, int]:
    response = getattr(event, "response", None)
    if not isinstance(response, dict):
        return 0, 0
    usage = response.get("usage") or {}
    if not isinstance(usage, dict):
        return 0, 0
    return int(usage.get("prompt_tokens") or 0), int(
        usage.get("completion_tokens") or 0,
    )


def _on_llm_event(event: Any) -> None:
    meter = current_run_meter.get()
    if meter is None:
        return
    prompt_tokens, completion_tokens = _usage_from_event(event)
    purpose = purpose_from_origin(getattr(event, "origin", None)) or "planning"
    meter.add(
        purpose,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        cost=getattr(event, "provider_cost", None),
    )


def install_run_metering() -> None:
    """Register the process-wide listener once; idempotent."""
    global _listener
    with _install_lock:
        if _listener is None:
            _listener = unillm.add_llm_event_listener(_on_llm_event)


def new_run_meter() -> RunMeter:
    """Create a meter and make sure the listener that feeds it is installed."""
    install_run_metering()
    return RunMeter()


def handle_run_stats(handle: Any) -> Dict[str, Any]:
    """Verification and token accounting a run handle exposes for its execution row.

    Handles that carry ``run_stats`` report it verbatim; a bare loop handle
    that only carries ``run_meter`` reports the meter's token split.
    """
    stats = getattr(handle, "run_stats", None)
    out: Dict[str, Any] = dict(stats) if isinstance(stats, dict) else {}
    meter = getattr(handle, "run_meter", None)
    if isinstance(meter, RunMeter) and not out.get("tokens"):
        out["tokens"] = meter.snapshot()["tokens"]
    return out


__all__ = [
    "PURPOSES",
    "RunMeter",
    "current_run_meter",
    "handle_run_stats",
    "install_run_metering",
    "new_run_meter",
]
