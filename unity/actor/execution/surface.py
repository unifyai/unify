"""Execution surfaces: the distinct machines Unity can run code and commands on.

Unity orchestrates work across three physically distinct machines, each with its
own exec transport and file-movement mechanism:

- ``LOCAL`` — the Unity host process itself (in-process Python / shell).
- ``ASSISTANT_DESKTOP`` — the assistant's managed VM, reached over its
  agent-service (``/api/exec``) with FileSync bisync for file movement.
- ``USER_DESKTOP`` — a user's own linked machine, reached over its agent-service
  (``/api/exec``) with on-demand SFTP to ``$HOME`` for file movement, gated by
  the user's live consent.

The rest of the actor stack selects between surfaces through the resolver here
rather than hard-coding any one path, and exposes the resolved capabilities to
the model so it can choose the right machine for a request.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from unity.session_details import SESSION_DETAILS


class ExecutionSurface(str, Enum):
    """A machine Unity can execute on."""

    LOCAL = "local"
    ASSISTANT_DESKTOP = "assistant_desktop"
    USER_DESKTOP = "user_desktop"


@dataclass(frozen=True)
class SurfaceCapabilities:
    """What a surface can do right now and how to reach it.

    ``available`` means the surface exists for this session; ``ready`` means it
    is also usable at this moment. Live consent for a user desktop is asserted at
    call time by the execution target (a revoke can race a resolve), so ``ready``
    here reflects link/tunnel presence, not the in-memory consent toggle.
    """

    surface: ExecutionSurface
    available: bool
    ready: bool
    can_python: bool
    can_shell: bool
    can_files: bool
    reason: str | None = None
    api_url: str | None = None
    os: str | None = None
    user_id: str | None = None


def _local_capabilities() -> SurfaceCapabilities:
    return SurfaceCapabilities(
        surface=ExecutionSurface.LOCAL,
        available=True,
        ready=True,
        can_python=True,
        can_shell=True,
        can_files=True,
    )


def _assistant_desktop_capabilities() -> SurfaceCapabilities:
    assistant = SESSION_DETAILS.assistant
    url = assistant.desktop_url
    available = assistant.has_managed_desktop
    return SurfaceCapabilities(
        surface=ExecutionSurface.ASSISTANT_DESKTOP,
        available=available,
        # VM readiness (boot / agent-service up) is awaited by the target's
        # ensure_ready; presence of an assigned desktop is what gates here.
        ready=available,
        can_python=available,
        can_shell=available,
        can_files=available,
        reason=(
            None if available else "No managed desktop is assigned to this assistant"
        ),
        api_url=url if available else None,
        os=assistant.desktop_mode if available else None,
    )


def _resolve_user_link(user_id: str | None):
    """Resolve the relevant user-desktop link, or return ``(None, reason)``.

    With an explicit ``user_id`` the matching link is used. Without one, a lone
    link is selected; multiple links are ambiguous and require a ``user_id``.
    """
    desktops = SESSION_DETAILS.assistant.user_desktops
    if not desktops:
        return None, "No user desktop is linked to this assistant"
    if user_id is not None:
        link = desktops.get(user_id)
        return (
            (link, None)
            if link
            else (None, f"No user desktop linked for user {user_id}")
        )
    if len(desktops) == 1:
        return next(iter(desktops.values())), None
    return None, "Multiple user desktops are linked; specify a user_id"


def _user_desktop_capabilities(user_id: str | None) -> SurfaceCapabilities:
    link, reason = _resolve_user_link(user_id)
    if link is None:
        return SurfaceCapabilities(
            surface=ExecutionSurface.USER_DESKTOP,
            available=False,
            ready=False,
            can_python=False,
            can_shell=False,
            can_files=False,
            reason=reason,
        )
    has_exec = bool(link.url)
    return SurfaceCapabilities(
        surface=ExecutionSurface.USER_DESKTOP,
        available=True,
        ready=has_exec,
        can_python=has_exec,
        can_shell=has_exec,
        can_files=link.filesys_available,
        reason=(
            None if has_exec else "User desktop has no reachable agent-service tunnel"
        ),
        api_url=link.url or None,
        os=link.os,
        user_id=link.owner_user_id,
    )


def resolve_surface(
    surface: ExecutionSurface,
    user_id: str | None = None,
) -> SurfaceCapabilities:
    """Resolve the capabilities of a single surface against the live session."""
    if surface is ExecutionSurface.LOCAL:
        return _local_capabilities()
    if surface is ExecutionSurface.ASSISTANT_DESKTOP:
        return _assistant_desktop_capabilities()
    if surface is ExecutionSurface.USER_DESKTOP:
        return _user_desktop_capabilities(user_id)
    raise ValueError(f"Unknown execution surface: {surface!r}")


def resolve_all(
    user_id: str | None = None,
) -> dict[ExecutionSurface, SurfaceCapabilities]:
    """Resolve every surface's capabilities for the current session."""
    return {
        ExecutionSurface.LOCAL: _local_capabilities(),
        ExecutionSurface.ASSISTANT_DESKTOP: _assistant_desktop_capabilities(),
        ExecutionSurface.USER_DESKTOP: _user_desktop_capabilities(user_id),
    }
