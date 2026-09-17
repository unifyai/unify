"""
unify/common/hierarchical_logger.py
===================================

Central icon registry and hierarchical logging infrastructure.

This module provides:
1. ``ICONS`` / ``DEFAULT_ICON`` -- the single source of truth for all emoji
   prefixes used across SessionLogger, LoopLogger and raw ``LOGGER`` calls.
2. ``SessionLogger`` -- session-level logger with consistent label formatting.
3. Integration with ``TOOL_LOOP_LINEAGE`` for nested hierarchy propagation.
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import Optional

from unify.logger import LOGGER

# ─────────────────────────────────────────────────────────────────────────────
# Session lineage context (parallel to TOOL_LOOP_LINEAGE for non-tool-loop components)
# ─────────────────────────────────────────────────────────────────────────────
SESSION_LINEAGE: ContextVar[list[str]] = ContextVar("SESSION_LINEAGE", default=[])


# ─────────────────────────────────────────────────────────────────────────────
# Central icon registry
#
# Every emoji prefix in the codebase MUST be defined here.  No other module
# should hardcode emoji characters in log calls.
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_ICON = "⬥"

ICONS = {
    # ── Chat ────────────────────────────────────────────────────────────
    "unify_message_received": "💬",
    "unify_message_sent": "💬",
    "direct_message": "💬",
    # ── Session / lifecycle ─────────────────────────────────────────────
    "session_start": "🚀",
    "session_end": "🏁",
    "lifecycle": "🚀",
    # ── LLM brain ───────────────────────────────────────────────────────
    "llm_thinking": "🧠",
    "llm_response": "🤖",
    "llm_error": "❌",
    # ── Async tool loop ─────────────────────────────────────────────────
    "system_message": "📋",
    "request": "➡️",
    "tool_seeding": "⬇️",
    "stop_requested": "🛑",
    "early_exit": "⏹️",
    "clarification": "❓",
    "notification": "🔔",
    "interjection": "💬",
    "wait": "🕒",
    "auto_cancel": "🔚",
    "completed": "✅",
    "pending": "⏳",
    "pause": "⏸️",
    "resume": "▶️",
    # ── State management ────────────────────────────────────────────────
    "state_update": "📋",
    "notification_injected": "🔔",
    "notification_unpinned": "🗑️",
    # ── Infrastructure ──────────────────────────────────────────────────
    "managers_worker": "⚙️",
    # ── Generic / misc ──────────────────────────────────────────────────
    "event": "📣",
    "ping": "🏓",
    "summarize": "📑",
    "info": "ℹ️",
    "warning": "⚠️",
    "error": "❌",
}


def get_icon(event_type: str) -> str:
    """Look up the emoji for *event_type*, falling back to ``DEFAULT_ICON``."""
    return ICONS.get(event_type.lower().replace(" ", "_"), DEFAULT_ICON)


# ─────────────────────────────────────────────────────────────────────────────
# SessionLogger - hierarchical logger for session-based components
# ─────────────────────────────────────────────────────────────────────────────


class SessionLogger:
    """
    Hierarchical logger for components like ConversationManager that operate
    as long-lived sessions rather than request-response tool loops.

    Provides consistent log formatting with the same `[label]` pattern as
    async tool loops, enabling unified log viewing across all components.

    Unlike async tool loops (which may have many concurrent instances and need
    unique suffixes), session-scoped components have exactly one instance per
    session — so the label is a fixed string with no suffix.

    Usage:
        logger = SessionLogger("ConversationManager")
        logger.info("unify_message_received", "Message from the user")
        # Output: 💬 [ConversationManager] Message from the user
    """

    def __init__(
        self,
        component_name: str,
        *,
        parent_lineage: Optional[list[str]] = None,
    ):
        """
        Initialize a session logger.

        Args:
            component_name: The name of the component (e.g., "ConversationManager")
            parent_lineage: Optional explicit parent lineage. If None, reads from
                TOOL_LOOP_LINEAGE or SESSION_LINEAGE context vars.
        """
        self._component_name = component_name

        # Determine parent lineage
        if parent_lineage is not None:
            self._parent_lineage = list(parent_lineage)
        else:
            # Check TOOL_LOOP_LINEAGE first (async tool loop context)
            try:
                from unify.common._async_tool.loop_config import TOOL_LOOP_LINEAGE

                tool_lineage = TOOL_LOOP_LINEAGE.get([])
                if tool_lineage:
                    self._parent_lineage = list(tool_lineage)
                else:
                    # Fall back to SESSION_LINEAGE
                    self._parent_lineage = list(SESSION_LINEAGE.get([]))
            except Exception:
                self._parent_lineage = list(SESSION_LINEAGE.get([]))

        # Build the label
        self._label = self._build_label()

    def _build_label(self) -> str:
        """Build the hierarchical label string."""
        parts = self._parent_lineage + [self._component_name]
        return "->".join(parts)

    @property
    def label(self) -> str:
        """The full hierarchical label for this session."""
        return self._label

    @property
    def lineage(self) -> list[str]:
        """The full lineage including this component."""
        return self._parent_lineage + [self._component_name]

    def child_lineage(self) -> list[str]:
        """
        Get the lineage to pass to child components/loops.

        This should be passed as `parent_lineage` to nested async tool loops
        or child SessionLoggers.
        """
        return self.lineage

    def _log(
        self,
        level: int,
        event_type: str,
        message: str,
        icon_override: Optional[str] = None,
    ) -> None:
        icon = icon_override or get_icon(event_type)
        LOGGER.log(level, f"{icon} [{self._label}] {message}")

    def info(
        self,
        event_type: str,
        message: str,
        *,
        icon_override: Optional[str] = None,
    ) -> None:
        """
        Log an info-level message with event-specific icon.

        Args:
            event_type: The type of event (used to select icon)
            message: The log message
            icon_override: Optional icon to use instead of event-type lookup
        """
        self._log(logging.INFO, event_type, message, icon_override)

    def debug(
        self,
        event_type: str,
        message: str,
        *,
        icon_override: Optional[str] = None,
    ) -> None:
        """Log a debug-level message with event-specific icon."""
        self._log(logging.DEBUG, event_type, message, icon_override)

    def warning(
        self,
        event_type: str,
        message: str,
        *,
        icon_override: Optional[str] = None,
    ) -> None:
        """Log a warning-level message with event-specific icon."""
        self._log(logging.WARNING, event_type, message, icon_override)

    def error(
        self,
        event_type: str,
        message: str,
        *,
        icon_override: Optional[str] = None,
    ) -> None:
        """Log an error-level message with event-specific icon."""
        self._log(logging.ERROR, event_type, message, icon_override)


def log_boundary_event(
    hierarchy_label: str,
    message: str,
    *,
    icon: str = "🛠️",
    level: str = "info",
) -> None:
    """Log a boundary event with hierarchical label.

    Format: ``{icon} [{hierarchy_label}] {message}``
    """
    try:
        txt = f"{icon} [{hierarchy_label}] {message}"
        log_fn = getattr(LOGGER, str(level).lower(), None)
        if not callable(log_fn):
            log_fn = LOGGER.info
        log_fn(txt)
    except Exception:
        # Best-effort logging; never fail execution.
        return
