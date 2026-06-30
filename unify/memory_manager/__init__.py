"""
MemoryManager package providing memory management abstractions and implementations.
"""

from typing import TYPE_CHECKING
from importlib import import_module

__all__ = [
    "BaseMemoryManager",
    "MemoryManager",
    "SimulatedMemoryManager",
]

_lazy_map = {
    "BaseMemoryManager": "unify.memory_manager.base",
    "MemoryManager": "unify.memory_manager.memory_manager",
    "SimulatedMemoryManager": "unify.memory_manager.simulated",
}


def __getattr__(name: str):
    if name in _lazy_map:
        module = import_module(_lazy_map[name])
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals().keys()) + __all__)


if TYPE_CHECKING:
    from .base import BaseMemoryManager
    from .memory_manager import MemoryManager
    from .simulated import SimulatedMemoryManager
