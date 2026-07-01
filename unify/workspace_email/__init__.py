"""Connected-workspace mailbox access.

Exposes :class:`WorkspaceEmailManager` (``primitives.workspace_email.*``), the
impersonation surface for sending from and reading the user's connected Google
Workspace / Microsoft 365 mailbox — distinct from the assistant's own managed
mailbox reached via ``primitives.comms.send_email``.
"""

from unify.workspace_email.workspace_email_manager import (
    WorkspaceEmailError,
    WorkspaceEmailManager,
)

__all__ = [
    "WorkspaceEmailError",
    "WorkspaceEmailManager",
]
