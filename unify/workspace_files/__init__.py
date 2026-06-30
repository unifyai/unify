"""Workspace file access: the single enforced door to a connected BYOD
Google Drive / Microsoft 365 account's files and folders.

All Drive / SharePoint / OneDrive access by the assistant flows through
``WorkspaceFilesManager`` (exposed as ``primitives.workspace_files.*``), which
masks any item disallowed by the per-assistant allowlist so that hidden files
and folders appear not to exist.
"""

from unify.workspace_files.policy import (
    WorkspaceFilePolicy,
    evaluate_access,
    get_policy_store,
)
from unify.workspace_files.workspace_files_manager import WorkspaceFilesManager

__all__ = [
    "WorkspaceFilePolicy",
    "WorkspaceFilesManager",
    "evaluate_access",
    "get_policy_store",
]
