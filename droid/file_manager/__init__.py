"""
FileManager package providing file management abstractions and implementations.
"""

from typing import TYPE_CHECKING
from importlib import import_module

__all__ = [
    "BaseFileManager",
    # adapters
    "BaseFileSystemAdapter",
    "LocalFileSystemAdapter",
    # managers
    "FileManager",
    "LocalFileManager",
    # types
    "FileRecord",
    "FileSystemCapabilities",
    "FileReference",
    "FolderReference",
]

_lazy_map = {
    "BaseFileManager": "droid.file_manager.base",
    "BaseFileSystemAdapter": "droid.file_manager.filesystem_adapters",
    "LocalFileSystemAdapter": "droid.file_manager.filesystem_adapters",
    "FileManager": "droid.file_manager.managers.file_manager",
    "LocalFileManager": "droid.file_manager.managers.local",
    "FileRecord": "droid.file_manager.types",
    "FileSystemCapabilities": "droid.file_manager.types",
    "FileReference": "droid.file_manager.types",
    "FolderReference": "droid.file_manager.types",
}


def __getattr__(name: str):
    if name in _lazy_map:
        module = import_module(_lazy_map[name])
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals().keys()) + __all__)


if TYPE_CHECKING:
    from droid.file_manager.base import BaseFileManager
    from droid.file_manager.filesystem_adapters import (
        BaseFileSystemAdapter,
        LocalFileSystemAdapter,
    )
    from droid.file_manager.managers.file_manager import FileManager
    from droid.file_manager.managers.local import LocalFileManager
    from droid.file_manager.types import (
        FileRecord,
        FileSystemCapabilities,
        FileReference,
        FolderReference,
    )
