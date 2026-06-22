"""Execution targets: a uniform async interface over droid's execution surfaces.

Each target wraps one :class:`~droid.actor.execution.surface.ExecutionSurface`
and exposes the same contract (run shell, run python, move files) so callers
never branch on surface internals. The local target is fully functional here;
the remote targets (managed VM, user desktop) and the resolving factory build on
this foundation.
"""

from .base import ExecResult, ExecutionTarget, coerce_output
from .exec_client import AgentServiceExecClient
from .local import LocalTarget

__all__ = [
    "ExecResult",
    "ExecutionTarget",
    "coerce_output",
    "AgentServiceExecClient",
    "LocalTarget",
]
