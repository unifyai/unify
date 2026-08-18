from typing import TYPE_CHECKING
from importlib import import_module

__all__ = [
    "CodeActActor",
    "BaseCodeActActor",
]

_lazy_map = {
    "CodeActActor": "unify.actor.code_act_actor",
    "BaseCodeActActor": "unify.actor.base",
}


def __getattr__(name: str):
    if name in _lazy_map:
        module = import_module(_lazy_map[name])
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals().keys()) + __all__)


if TYPE_CHECKING:
    from .code_act_actor import CodeActActor
    from .base import BaseCodeActActor
