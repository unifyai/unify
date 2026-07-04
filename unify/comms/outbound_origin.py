"""Track whether outbound comms are slow-brain direct tool sends."""

from __future__ import annotations

from contextvars import ContextVar, Token

_SLOW_BRAIN_DIRECT_OUTBOUND: ContextVar[bool] = ContextVar(
    "slow_brain_direct_outbound",
    default=False,
)


def slow_brain_direct_outbound_active() -> bool:
    return _SLOW_BRAIN_DIRECT_OUTBOUND.get()


def mark_slow_brain_direct_outbound() -> Token[bool]:
    return _SLOW_BRAIN_DIRECT_OUTBOUND.set(True)


def reset_slow_brain_direct_outbound(token: Token[bool]) -> None:
    _SLOW_BRAIN_DIRECT_OUTBOUND.reset(token)
