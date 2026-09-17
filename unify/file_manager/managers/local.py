"""Local file manager over the workspace directory."""

from __future__ import annotations

from unify.file_manager.filesystem_adapters.local_adapter import LocalFileSystemAdapter
from unify.file_manager.managers.file_manager import FileManager
from unify.manager_registry import SingletonABCMeta


class LocalFileManager(FileManager, metaclass=SingletonABCMeta):
    """Local file manager over the workspace directory."""

    def __init__(
        self,
        root: str | None = None,
    ):
        super().__init__(adapter=LocalFileSystemAdapter(root))
