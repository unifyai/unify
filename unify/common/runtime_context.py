"""Canonical session-root resolution for production and pytest runs."""

from __future__ import annotations

import os

import unisdk

from unify.session_details import SESSION_DETAILS


def resolve_runtime_context_root(*, test: bool | None = None) -> str:
    """Return the authoritative Unify context root for this session.

    In production the root comes from ``SESSION_DETAILS`` (populated by
    startup): user-owned assistants live at ``{userId}/{agentId}`` while
    team-owned assistants have no personal root at all — their per-assistant
    internals live under ``Teams/{ownerTeamId}/Assistants/{agentId}``.
    In tests pytest establishes a per-test root via ``unisdk.set_context``
    before fixtures run; that active context is the session identity for the
    test.
    """
    if test is None:
        from unify.settings import SETTINGS

        test = SETTINGS.TEST

    if test:
        try:
            active = unisdk.get_active_context() or {}
            read_ctx = active.get("read")
            write_ctx = active.get("write")
            if read_ctx and read_ctx == write_ctx:
                return read_ctx
        except Exception:
            pass

    if SESSION_DETAILS.team_owned:
        return (
            f"Teams/{SESSION_DETAILS.owner_team_id}"
            f"/Assistants/{SESSION_DETAILS.assistant_context}"
        )
    env_owner = (os.environ.get("OWNER_TEAM_ID") or "").strip()
    if env_owner:
        raise RuntimeError(
            "OWNER_TEAM_ID is set but SESSION_DETAILS.owner_team_id is missing; "
            "refusing to bind a personal runtime root for a team-owned assistant.",
        )
    return f"{SESSION_DETAILS.user_context}/{SESSION_DETAILS.assistant_context}"


def bind_runtime_context_root(
    *,
    skip_create: bool = False,
    strict: bool = False,
) -> str:
    """Bind unisdk and ContextRegistry to the canonical session root."""
    from unify.common.context_registry import ContextRegistry
    from unify.common.hierarchical_logger import ICONS
    from unify.logger import LOGGER

    full_ctx = resolve_runtime_context_root()
    context_set = False
    try:
        active_ctx = unisdk.get_active_context() or {}
        if active_ctx.get("read") != full_ctx or active_ctx.get("write") != full_ctx:
            unisdk.set_context(full_ctx, skip_create=skip_create)
            context_set = True
    except Exception as exc:
        if strict:
            raise
        LOGGER.warning(
            f"{ICONS['managers_worker']} [ManagersWorker] Failed to set runtime context to {full_ctx}: {exc}",
        )
    if context_set:
        LOGGER.debug(
            f"{ICONS['managers_worker']} [ManagersWorker] Runtime context rebound to {full_ctx}",
        )
    ContextRegistry.set_base_context(full_ctx)
    return full_ctx
