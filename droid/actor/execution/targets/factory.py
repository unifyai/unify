"""Resolve an :class:`ExecutionTarget` for a surface against the live session."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from ..surface import ExecutionSurface, _resolve_user_link, resolve_surface
from .assistant_desktop import AssistantDesktopTarget
from .base import ExecutionTarget, TargetUnavailableError
from .local import LocalTarget
from .user_desktop import UserDesktopTarget

if TYPE_CHECKING:
    from droid.function_manager.function_manager import FunctionManager

    from ..session import SessionExecutor


def get_target(
    surface: ExecutionSurface,
    *,
    user_id: str | None = None,
    session_executor: Optional["SessionExecutor"] = None,
    function_manager: Optional["FunctionManager"] = None,
) -> ExecutionTarget:
    """Build the execution target for ``surface``, or raise if it is unusable.

    Capability gating is delegated to :func:`resolve_surface`; an unavailable or
    not-ready surface raises :class:`TargetUnavailableError` with the resolver's
    reason so callers can surface a clear message to the model.
    """
    caps = resolve_surface(surface, user_id)
    if not caps.available:
        raise TargetUnavailableError(caps.reason or f"{surface.value} is unavailable")

    if surface is ExecutionSurface.LOCAL:
        if session_executor is None:
            raise ValueError("session_executor is required for the local target")
        return LocalTarget(session_executor)

    if surface is ExecutionSurface.ASSISTANT_DESKTOP:
        if not caps.ready:
            raise TargetUnavailableError(
                caps.reason or "assistant desktop is not ready",
            )
        if function_manager is None:
            raise ValueError(
                "function_manager is required for the assistant desktop target",
            )
        return AssistantDesktopTarget(
            function_manager,
            api_url=caps.api_url,
            os=caps.os,
        )

    if surface is ExecutionSurface.USER_DESKTOP:
        if not caps.ready:
            raise TargetUnavailableError(caps.reason or "user desktop is not ready")
        link, reason = _resolve_user_link(user_id)
        if link is None:
            raise TargetUnavailableError(reason or "no user desktop linked")
        return UserDesktopTarget(caps.user_id or link.owner_user_id, link, os=caps.os)

    raise ValueError(f"Unknown execution surface: {surface!r}")
