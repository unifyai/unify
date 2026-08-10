"""
WorkflowManager package: the catalogue of installable workflow bundles.

Lazy attribute access keeps this package importable from ``unify.settings``
(which composes :mod:`unify.workflow_manager.settings`) without dragging in
the manager and its manager-registry dependencies at import time.
"""

from typing import TYPE_CHECKING
from importlib import import_module

__all__ = [
    "PENDING_SCOPING",
    "SCOPED_SURFACES",
    "WORKFLOW_LIBRARY",
    "BaseWorkflowManager",
    "Surface",
    "SurfaceRegistry",
    "UnscopedSurfaceError",
    "WorkflowBundle",
    "WorkflowCatalogEntry",
    "WorkflowContentEntry",
    "WorkflowInstallation",
    "WorkflowManager",
    "WorkflowRequest",
    "WorkflowRequirement",
    "bootstrap_workflow_catalog",
    "seed_builtin_workflows",
    "load_bundle",
    "load_catalog",
    "register_default_surfaces",
    "schedule_bootstrap_workflow_catalog",
]

_lazy_map = {
    "BaseWorkflowManager": "unify.workflow_manager.base",
    "WORKFLOW_LIBRARY": "unify.workflow_manager.bundle",
    "Surface": "unify.workflow_manager.bundle",
    "SurfaceRegistry": "unify.workflow_manager.bundle",
    "UnscopedSurfaceError": "unify.workflow_manager.bundle",
    "WorkflowBundle": "unify.workflow_manager.bundle",
    "WorkflowRequirement": "unify.workflow_manager.bundle",
    "bootstrap_workflow_catalog": "unify.workflow_manager.catalog",
    "seed_builtin_workflows": "unify.workflow_manager.builtins_catalog",
    "load_bundle": "unify.workflow_manager.catalog",
    "load_catalog": "unify.workflow_manager.catalog",
    "schedule_bootstrap_workflow_catalog": "unify.workflow_manager.catalog",
    "PENDING_SCOPING": "unify.workflow_manager.surfaces",
    "SCOPED_SURFACES": "unify.workflow_manager.surfaces",
    "register_default_surfaces": "unify.workflow_manager.surfaces",
    "WorkflowCatalogEntry": "unify.workflow_manager.types.catalog_entry",
    "WorkflowContentEntry": "unify.workflow_manager.types.content_entry",
    "WorkflowInstallation": "unify.workflow_manager.types.workflow",
    "WorkflowRequest": "unify.workflow_manager.types.request",
    "WorkflowManager": "unify.workflow_manager.workflow_manager",
}


def __getattr__(name: str):
    if name in _lazy_map:
        module = import_module(_lazy_map[name])
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals().keys()) + __all__)


if TYPE_CHECKING:
    from .base import BaseWorkflowManager
    from .bundle import (
        WORKFLOW_LIBRARY,
        Surface,
        SurfaceRegistry,
        UnscopedSurfaceError,
        WorkflowBundle,
        WorkflowRequirement,
    )
    from .builtins_catalog import seed_builtin_workflows
    from .catalog import (
        bootstrap_workflow_catalog,
        load_bundle,
        load_catalog,
        schedule_bootstrap_workflow_catalog,
    )
    from .surfaces import (
        PENDING_SCOPING,
        SCOPED_SURFACES,
        register_default_surfaces,
    )
    from .types.catalog_entry import WorkflowCatalogEntry
    from .types.content_entry import WorkflowContentEntry
    from .types.request import WorkflowRequest
    from .types.workflow import WorkflowInstallation
    from .workflow_manager import WorkflowManager
