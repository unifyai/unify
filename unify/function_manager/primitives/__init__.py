"""Primitives scoping and registry.

This subpackage provides:
- `PrimitiveScope` - The single knob for controlling which namespaces are exposed
- `VALID_MANAGER_ALIASES` - Canonical set of valid namespace aliases
- `ToolSurfaceRegistry` / `get_registry` - Central registry for namespace configuration
- `ManagerSpec` - Per-namespace configuration dataclass
- `Primitives` - Scoped runtime interface for accessing primitives
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
]
