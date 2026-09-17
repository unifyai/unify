"""Primitives scoping and registry for state manager primitives.

This subpackage provides:
- `PrimitiveScope` - The single knob for controlling which managers are exposed
- `VALID_MANAGER_ALIASES` - Canonical set of valid manager aliases
- `ToolSurfaceRegistry` / `get_registry` - Central registry for manager configuration
- `ManagerSpec` - Per-manager configuration dataclass
- `Primitives` - Scoped runtime interface for accessing state manager primitives
- `get_primitive_callable` - Resolve primitive metadata to callables
- `collect_primitives` / `compute_primitives_hash` - Module-level convenience functions
"""

from unify.function_manager.primitives.scope import (
    PrimitiveScope,
    VALID_MANAGER_ALIASES,
    default_runtime_scope,
)
from unify.function_manager.primitives.registry import (
    ManagerSpec,
    ToolSurfaceRegistry,
    get_registry,
    collect_primitives,
    compute_primitives_hash,
    get_primitive_sources,
    _COMMON_EXCLUDED_METHODS,
    _CLASS_PATH_TO_ALIAS,
)
from unify.function_manager.primitives.runtime import (
    Primitives,
    get_primitive_callable,
    _AsyncPrimitiveWrapper,
    _create_async_wrapper,
)

__all__ = [
    # Scope
    "PrimitiveScope",
    "VALID_MANAGER_ALIASES",
    "default_runtime_scope",
    # Registry
    "ManagerSpec",
    "ToolSurfaceRegistry",
    "get_registry",
    "collect_primitives",
    "compute_primitives_hash",
    "get_primitive_sources",
    "_COMMON_EXCLUDED_METHODS",
    "_CLASS_PATH_TO_ALIAS",
    # Runtime
    "Primitives",
    "get_primitive_callable",
    "_AsyncPrimitiveWrapper",
    "_create_async_wrapper",
]
