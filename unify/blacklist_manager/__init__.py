"""
BlackListManager package providing blocked-contact abstractions and implementations.
"""

from typing import TYPE_CHECKING
from importlib import import_module

__all__ = [
    "BaseBlackListManager",
    "BlackListManager",
]

_lazy_map = {
    "BaseBlackListManager": "unify.blacklist_manager.base",
    "BlackListManager": "unify.blacklist_manager.blacklist_manager",
}


def __getattr__(name: str):
    if name in _lazy_map:
        module = import_module(_lazy_map[name])
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals().keys()) + __all__)


if TYPE_CHECKING:
    from .base import BaseBlackListManager
    from .blacklist_manager import BlackListManager
