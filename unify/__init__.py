"""
unify/__init__.py
==================

Package initialization for the unify assistant runtime.

The runtime must be explicitly initialized via init() before using managers:

    import unify
    unify.init()  # Activates the project, binds the context root, installs hooks

For code that may run before or after init(), use ensure_initialised() which
is a no-op if already initialized.

Logging is configured centrally in unify.logger (imported below).
"""

try:
    import onnxruntime as _ort

    _ort.set_default_logger_severity(4)  # FATAL — suppress thread affinity noise
except Exception:
    pass

from unify import db
from unify.common.context_registry import ContextRegistry

# Logging is configured entirely in unify.logger — import it so that
# the module-level setup (handler, formatter, library muting) runs once.
import unify.logger  # noqa: F401
from unify.common.startup_timing import startup_timing
from unify.logger import LOGGER

_INITIALISED = False


def init(
    project_name: str = db.DEFAULT_PROJECT,
    overwrite: bool = False,
) -> None:  # noqa: D401 – imperative name
    """Initialise the runtime.

    Reads SESSION_DETAILS.assistant.agent_id for the context path. All
    assistant identity and profile data lives on SESSION_DETAILS — this
    function only handles project activation, context setup and hooks.
    """

    global _INITIALISED
    if _INITIALISED:
        return

    from unify.settings import SETTINGS as _SETTINGS

    with startup_timing(LOGGER, "unify.init.validate_llm_providers"):
        _SETTINGS.validate_llm_providers()

    if db.active_project() != project_name:
        with startup_timing(LOGGER, "unify.init.activate", f"project={project_name}"):
            db.activate(project_name, overwrite)

    from unify.common.runtime_context import (
        bind_runtime_context_root,
        resolve_runtime_context_root,
    )

    with startup_timing(
        LOGGER,
        "unify.init.set_context",
        f"context={resolve_runtime_context_root()}",
    ):
        bind_runtime_context_root(strict=True)

    with startup_timing(LOGGER, "unify.init.context_registry_setup"):
        ContextRegistry.setup()

    from .events.llm_event_hook import install_llm_event_hook

    with startup_timing(LOGGER, "unify.init.install_llm_event_hook"):
        install_llm_event_hook()

    _INITIALISED = True


def ensure_initialised(
    project_name: str = db.DEFAULT_PROJECT,
    overwrite: bool = False,
) -> None:
    """Ensure the runtime is initialised if no active read/write contexts exist.

    If both read and write contexts are already configured, this is a no-op.
    Otherwise, it calls :pyfunc:`init` to set up project, context and hooks.
    """
    ctxs = db.get_active_context()
    if ctxs.get("read") and ctxs.get("write"):
        return
    init(project_name=project_name, overwrite=overwrite)


# What the package exports at top-level
__all__ = ["db", "init", "ensure_initialised"]
