"""Payload models for desktop primitive events."""

from __future__ import annotations

from pydantic import BaseModel, Field


class DesktopPrimitivePayload(BaseModel):
    """Fired when a primitives.computer.desktop.* method is called.

    Used by the ConversationManager to gate desktop fast-path tool exposure.
    """

    method: str = Field(
        description="Desktop method that was invoked (act, observe, etc.)",
    )


class UserDesktopFileAccessPayload(BaseModel):
    """Audit record for on-demand access to a user's own home filesystem.

    Emitted for every pull/push/list against a user's linked machine so the
    access is traceable: who was accessed, which path, and where a writeback
    copy landed.
    """

    user_id: str = Field(description="Owner of the machine that was accessed.")
    operation: str = Field(description="Access type: 'pull', 'push', or 'list'.")
    path: str = Field(description="Home-relative path that was accessed.")
    dest: str = Field(
        default="",
        description="Remote path of the writeback copy (push only).",
    )


class ComputerActCompletedPayload(BaseModel):
    """Fired when a visible computer session's act() completes (desktop or web-vm).

    Carries the instruction and the agent's summary of what was done so the CM
    can notify both the slow brain and fast brain during interactive
    screen-sharing sessions. The actual visual state is already captured by
    the regular screen-share screenshot pipeline on the next brain turn.
    """

    instruction: str = Field(description="The instruction that was executed.")
    summary: str = Field(description="Agent's description of what was done.")
