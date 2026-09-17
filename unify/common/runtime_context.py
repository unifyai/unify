"""Canonical session-root resolution for production and pytest runs."""

from __future__ import annotations

from unify import db
from unify.session_details import SESSION_DETAILS


def resolve_runtime_context_root(*, test: bool | None = None) -> str:
    """Return the authoritative context root for this session.

    In production the root comes from ``SESSION_DETAILS``: an assistant lives
    at ``{userId}/{agentId}``. In tests pytest establishes a per-test root via
    ``db.set_context`` before fixtures run; that active context is the session
    identity for the test.
    """
    if test is None:
        from unify.settings import SETTINGS

        test = SETTINGS.TEST
    if test:
        active = db.get_active_context()
        read_ctx = active.get("read")
        write_ctx = active.get("write")
        if read_ctx and read_ctx == write_ctx:
            return read_ctx
    return f"{SESSION_DETAILS.user_context}/{SESSION_DETAILS.assistant_context}"


def bind_runtime_context_root(*, strict: bool = False) -> str:
    """Bind the store and ContextRegistry to the canonical session root."""
    from unify.common.context_registry import ContextRegistry

    full_ctx = resolve_runtime_context_root()
    active_ctx = db.get_active_context()
    if active_ctx.get("read") != full_ctx or active_ctx.get("write") != full_ctx:
        db.set_context(full_ctx, relative=False)
    ContextRegistry.set_base_context(full_ctx)
    if strict:
        active = db.get_active_context()
        if active.get("read") != full_ctx or active.get("write") != full_ctx:
            raise RuntimeError(
                f"Context root binding failed: expected {full_ctx!r}, active is {active!r}",
            )
    return full_ctx
