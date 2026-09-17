from __future__ import annotations

from abc import ABC
from typing import Dict, Callable, Optional, Type


class BaseStateManager(ABC):
    """
    Central marker base class for all state managers.

    This abstract base exists solely to provide a single common ancestor for
    manager interfaces such as FunctionManager and GuidanceManager.

    Purpose
    -------
    - Enable straightforward `isinstance(obj, BaseStateManager)` checks.
    - Allow expressive and maintainable type hints (e.g., unions or generics
      bounded to `BaseStateManager`).

    The class intentionally defines no abstract methods to avoid constraining
    individual manager contracts.

    Caller Context
    --------------
    When a manager invokes another manager's tool loop, the ``_as_caller_description``
    class attribute provides a one-liner describing this manager from the perspective
    of the callee. This is injected into the system message so the LLM understands
    who the "user" messages are coming from.
    """

    # Global registry of discovered manager classes keyed by class name
    _registry: Dict[str, Type["BaseStateManager"]] = {}

    # Override in subclasses to describe this manager when it's the caller of another.
    # Used to explain who the "user" is in nested tool loops.
    _as_caller_description: str = "another component of the assistant system"

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Register concrete subclasses by their class name for stable lookup
        # We intentionally key by the bare class name to match snapshot entrypoints.
        try:
            BaseStateManager._registry[cls.__name__] = cls
        except Exception:
            # Registration is best-effort; avoid raising at import time
            pass

    def __init__(self):
        self._tools = {}

    def warm_embeddings(self) -> None:
        """Pre-create embedding columns for commonly searched fields.

        Called after initialization to avoid cold-start latency on first
        search.  Override in concrete implementations that use vector search.
        """

    def add_tools(self, method: str, tools: Dict[str, Callable]):
        """
        Store tools for a given manager method. must be called in manager's __init__ method.
        Any tools added after the manager has been initialised may not be available for semantic cache re-execution.

        Parameters
        ----------
        method : str
            The name of the manager method to store tools for.
        tools : Dict[str, Callable]
            A dictionary of tools to store for the given manager method.
        """
        self._tools[method] = tools

    def get_tools(
        self,
        method: Optional[str] = None,
        include_sub_tools: bool = False,
    ) -> Dict[str, Callable]:
        """
        Get tools for a given manager method.

        Parameters
        ----------
        method : Optional[str], default ``None``
            The name of the manager method to get tools for. If ``None``, return all tools.
        """
        if method is None:
            ret = {}
            for sub_tools in self._tools.values():
                ret.update(sub_tools)
            return ret

        if include_sub_tools:
            # Return all sub tools that starts with `method.`
            ret = self._tools.get(method, {})
            for sub_tool in self._tools.keys():
                if sub_tool.startswith(f"{method}."):
                    ret.update(self._tools[sub_tool])
            return ret

        return self._tools.get(method, {})


def get_caller_description(manager_class_name: str) -> Optional[str]:
    """
    Look up the caller description for a manager by class name.

    Parameters
    ----------
    manager_class_name : str
        The class name of the manager (e.g., "FunctionManager").

    Returns
    -------
    str | None
        The ``_as_caller_description`` for the manager, or None if not found.
    """
    cls = BaseStateManager._registry.get(manager_class_name)
    if cls is None:
        return None
    return getattr(cls, "_as_caller_description", None)
