"""
Runtime primitives interface for state managers.

This module provides:
- `Primitives` - Scoped runtime interface for accessing state manager primitives
- `_AsyncPrimitiveWrapper` - Async wrapper for sync managers

All manager configuration (aliases, excluded methods, class paths) is defined in
`unify.function_manager.primitives.registry`. This module only handles runtime instantiation
and async wrapping.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from typing import Any, Callable, Optional, TYPE_CHECKING

from unify.function_manager.primitives.scope import (
    PrimitiveScope,
    VALID_MANAGER_ALIASES,
    default_runtime_scope,
)
from unify.function_manager.primitives.registry import (
    get_registry,
    _CLASS_PATH_TO_ALIAS,
)

if TYPE_CHECKING:
    from unify.contact_manager.contact_manager import ContactManager
    from unify.transcript_manager.transcript_manager import TranscriptManager
    from unify.secret_manager.secret_manager import SecretManager
    from unify.web_searcher.web_searcher import WebSearcher

logger = logging.getLogger(__name__)


# =============================================================================
# Async Wrapper for Sync Managers
# =============================================================================


class _AsyncPrimitiveWrapper:
    """
    Wrapper that provides async versions of sync manager methods.

    Delegates to the original manager without modifying it,
    ensuring internal code using the manager synchronously continues to work.

    Uses asyncio.to_thread() for sync methods to avoid blocking the event loop.
    """

    def __init__(self, manager: Any, manager_alias: str):
        """
        Initialize the wrapper.

        Args:
            manager: The original sync manager instance.
            manager_alias: The manager alias to look up primitive methods.
        """
        object.__setattr__(self, "_wrapped_manager", manager)
        object.__setattr__(self, "_manager_alias", manager_alias)
        # Get primitive methods from registry
        registry = get_registry()
        object.__setattr__(
            self,
            "_primitive_methods",
            set(registry.primitive_methods(manager_alias=manager_alias)),
        )

    def __dir__(self):
        """List primitive method names alongside the default attributes.

        Primitive methods are served by ``__getattr__``, which the default
        ``dir()`` cannot see — without this, ``dir(primitives.<manager>)``
        on a sync-wrapped manager hides its entire method surface.
        """
        return sorted(set(super().__dir__()) | self._primitive_methods)

    def __getattr__(self, name: str) -> Any:
        """
        Get an attribute - returns async wrapper for primitive methods, else delegates.
        """
        attr = getattr(self._wrapped_manager, name)

        # Only wrap methods that are in our primitive methods set
        if name not in self._primitive_methods:
            return attr

        # Non-callable attributes pass through directly
        if not callable(attr):
            return attr

        # Create async wrapper that uses to_thread for sync methods
        @functools.wraps(attr)
        async def async_method_wrapper(*args, **kwargs):
            if asyncio.iscoroutinefunction(attr):
                return await attr(*args, **kwargs)
            else:
                return await asyncio.to_thread(attr, *args, **kwargs)

        return async_method_wrapper


def _create_async_wrapper(manager: Any, manager_alias: str) -> _AsyncPrimitiveWrapper:
    """
    Create an async wrapper for a sync manager.

    Args:
        manager: The original sync manager instance.
        manager_alias: The manager alias for registry lookup.

    Returns:
        An async wrapper around the manager.
    """
    return _AsyncPrimitiveWrapper(manager, manager_alias)


# =============================================================================
# Manager Registry Key Mapping
# =============================================================================

# Maps manager_alias to ManagerRegistry getter method name.
# Empty string means direct construction (e.g. singleton via metaclass).
_ALIAS_TO_GETTER: dict[str, str] = {
    "contacts": "get_contact_manager",
    "ingestion": "get_ingestion_manager",
    "data": "get_data_manager",
    "transcripts": "get_transcript_manager",
    "secrets": "get_secret_manager",
    "web": "get_web_searcher",
    "files": "get_file_manager",
    "actor": "",
}

# Managers that need async wrapping (sync implementations)
_SYNC_MANAGERS: frozenset[str] = frozenset(
    {"data", "files", "ingestion"},
)


# =============================================================================


class Primitives:
    """
    Scoped runtime interface to all primitives (state managers).

    Only managers in the provided `primitive_scope` are accessible.
    Attempting to access an out-of-scope manager raises AttributeError.

    Managers are obtained via ManagerRegistry typed methods to respect
    IMPL settings (real vs simulated).

    Sync managers (DataManager, FileManager) are wrapped with async interfaces
    for consistency - the LLM can safely use `await` on all primitives.

    Usage:
        scope = PrimitiveScope(scoped_managers=frozenset({"files", "contacts"}))
        primitives = Primitives(primitive_scope=scope)

        # Accessible:
        await primitives.files.describe(file_path="...")
        await primitives.contacts.ask(text="...")

        # Raises AttributeError:
        primitives.web  # not in scope
    """

    def __init__(self, *, primitive_scope: Optional[PrimitiveScope] = None) -> None:
        """
        Initialize primitives with the given scope.

        Args:
            primitive_scope: Defines which managers are accessible.
                           If None, uses role-gated default runtime scope.
        """
        self._primitive_scope = primitive_scope or default_runtime_scope()
        # Lazy-initialized manager instances
        self._managers: dict[str, Any] = {}

    @property
    def primitive_scope(self) -> PrimitiveScope:
        """The scope controlling which managers are accessible."""
        return self._primitive_scope

    def _get_manager(self, alias: str) -> Any:
        """
        Get or create a manager instance by alias.

        Raises AttributeError if alias is not in scope.
        """
        if alias not in self._primitive_scope.scoped_managers:
            available = sorted(self._primitive_scope.scoped_managers)
            raise AttributeError(
                f"primitives.{alias} is not available in this scope. "
                f"Available managers: {available}",
            )

        if alias in self._managers:
            return self._managers[alias]

        getter_name = _ALIAS_TO_GETTER.get(alias)
        if getter_name is None:
            raise AttributeError(f"Unknown manager alias: {alias}")

        if getter_name == "":
            # Direct construction via primitive_class_path from the registry.
            from unify.function_manager.primitives.registry import _MANAGER_BY_ALIAS

            spec = _MANAGER_BY_ALIAS.get(alias)
            if spec is None:
                raise AttributeError(f"No ManagerSpec for alias: {alias}")
            cls = get_registry()._load_manager_class(spec.primitive_class_path)
            if cls is None:
                raise AttributeError(
                    f"Could not load class for alias {alias!r}: "
                    f"{spec.primitive_class_path}",
                )
            manager = cls()
        else:
            from unify.manager_registry import ManagerRegistry

            getter = getattr(ManagerRegistry, getter_name)
            manager = getter()

        # Wrap sync managers with async interface
        if alias in _SYNC_MANAGERS:
            manager = _create_async_wrapper(manager, alias)

        self._managers[alias] = manager
        return manager

    def __getattr__(self, name: str) -> Any:
        """Attribute access for manager retrieval."""
        if name in VALID_MANAGER_ALIASES:
            return self._get_manager(name)

        raise AttributeError(f"'Primitives' object has no attribute '{name}'")

    # Convenience properties for type hints (IDE support)
    # These are optional and just provide better autocomplete

    @property
    def contacts(self) -> "ContactManager":
        """Contact management primitives (ask, update)."""
        return self._get_manager("contacts")

    @property
    def data(self) -> "_AsyncPrimitiveWrapper":
        """Data operations primitives (filter, search, reduce, join, etc.)."""
        return self._get_manager("data")

    @property
    def transcripts(self) -> "TranscriptManager":
        """Transcript management primitives (ask)."""
        return self._get_manager("transcripts")

    @property
    def secrets(self) -> "SecretManager":
        """Secret management primitives (ask, update)."""
        return self._get_manager("secrets")

    @property
    def web(self) -> "WebSearcher":
        """Web search primitives (ask)."""
        return self._get_manager("web")

    @property
    def files(self) -> "_AsyncPrimitiveWrapper":
        """File management primitives (describe, reduce, filter_files, etc.)."""
        return self._get_manager("files")

    @property
    def actor(self) -> Any:
        """Actor delegation primitives (run)."""
        return self._get_manager("actor")


# =============================================================================
# Primitive Callable Resolution (for FunctionManager execution)
# =============================================================================


def get_primitive_callable(
    primitive_data: dict[str, Any],
    *,
    primitives: Optional[Primitives] = None,
) -> Optional[Callable]:
    """
    Resolve a primitive metadata dict to its actual callable.

    Uses the ``primitives`` instance (which handles all aliases uniformly)
    when available, falling back to a default-scoped Primitives instance.

    Args:
        primitive_data: Primitive metadata with primitive_class and primitive_method.
        primitives: Scoped Primitives instance for resolution.

    Returns:
        The callable method, or None if resolution fails.
    """
    class_path = primitive_data.get("primitive_class")
    method_name = primitive_data.get("primitive_method")

    if not class_path or not method_name:
        return None

    manager_alias = _CLASS_PATH_TO_ALIAS.get(class_path)
    if not manager_alias:
        return None

    # Use provided primitives instance, or construct a default-scoped one.
    if primitives is None:
        primitives = Primitives()

    manager = getattr(primitives, manager_alias, None)
    if manager is None:
        return None
    return getattr(manager, method_name, None)
