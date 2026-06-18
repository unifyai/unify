"""Local file manager with optional managed VM file sync."""

from __future__ import annotations

from droid.file_manager.filesystem_adapters.local_adapter import LocalFileSystemAdapter
from droid.file_manager.managers.file_manager import FileManager
from droid.manager_registry import SingletonABCMeta


class LocalFileManager(FileManager, metaclass=SingletonABCMeta):
    """Local file manager with optional managed VM file sync.

    Sync functionality is handled by the underlying LocalFileSystemAdapter.
    Access sync methods via: manager._adapter.start_sync(), etc.
    """

    def __init__(
        self,
        root: str | None = None,
        *,
        enable_sync: bool = True,
    ):
        super().__init__(adapter=LocalFileSystemAdapter(root, enable_sync=enable_sync))
