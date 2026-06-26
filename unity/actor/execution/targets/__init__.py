"""Execution targets: a uniform async interface over Unity's execution surfaces.

Each target wraps one :class:`~unity.actor.execution.surface.ExecutionSurface`
and exposes the same contract (run shell, run python, move files) so callers
never branch on surface internals. The local target is fully functional here;
the remote targets (managed VM, user desktop) and the resolving factory build on
this foundation.
"""

from .assistant_desktop import AssistantDesktopTarget
from .base import (
    ExecResult,
    ExecutionTarget,
    TargetUnavailableError,
    coerce_output,
)
from .exec_client import AgentServiceExecClient
from .factory import get_target
from .local import LocalTarget
from .user_desktop import UserDesktopTarget

__all__ = [
    "ExecResult",
    "ExecutionTarget",
    "TargetUnavailableError",
    "coerce_output",
    "AgentServiceExecClient",
    "LocalTarget",
    "AssistantDesktopTarget",
    "UserDesktopTarget",
    "get_target",
]
