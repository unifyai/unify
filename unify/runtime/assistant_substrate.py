"""Shared assistant runtime substrate for live and offline lanes.

Live assistants boot ConversationManager-owned managers on top of this
substrate. Offline tasks boot the same non-CM substrate so symbolic
entrypoints and actor code see matching project context, workspace root,
eager managers, deployment reconcile, and SecretManager→env hydration.

Intentionally excluded (live-only):
- ConversationManager / CommsManager construction
- CM↔task steering when a live session wakes
- Filesystem mount / VM / ComputerEnvironment (gated per-task via
  ``requires_filesystem`` / ``requires_computer``)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def bootstrap_assistant_substrate(
    *,
    project_name: str = "Assistants",
    warm_embeddings: bool = True,
    configure_event_bus: bool = True,
    prepare_workspace: bool = True,
    init_contact_manager: bool = True,
    init_transcript_manager: bool = True,
    init_memory_manager: bool = True,
    run_deployment_runtime: bool = True,
    reconcile_mode: str | None = "blocking",
) -> dict[str, Any] | None:
    """Initialize the non-ConversationManager assistant runtime substrate.

    Assumes ``SESSION_DETAILS`` is already populated. Does not construct
    ConversationManager, CommsManager, chat hydration, or live steering.

    Returns actor startup config from deployment bootstrap when available.
    """

    from unify.common.runtime_context import bind_runtime_context_root
    from unify.session_details import SESSION_DETAILS

    bind_runtime_context_root(skip_create=True, strict=True)

    if prepare_workspace:
        _prepare_local_workspace()

    import unify

    # Unconditional init so ContextRegistry.setup / EventBus hooks run even
    # when a parent process already set a unisdk read/write context.
    unify.init(project_name=project_name)

    if configure_event_bus:
        _configure_event_bus(SESSION_DETAILS.unify_key or None)

    if init_contact_manager or init_transcript_manager or init_memory_manager:
        _eager_session_managers(
            init_contact_manager=init_contact_manager,
            init_transcript_manager=init_transcript_manager,
            init_memory_manager=init_memory_manager,
            api_key=SESSION_DETAILS.unify_key or None,
        )

    startup_config: dict[str, Any] | None = None
    if run_deployment_runtime:
        startup_config = ensure_deployment_runtime_optional(
            reconcile_mode=reconcile_mode,
        )
        _apply_runtime_backends(startup_config)

    _eager_core_managers()
    _hydrate_secrets_to_environ()

    if warm_embeddings:
        _warm_embeddings()

    return startup_config


def ensure_deployment_runtime_optional(
    *,
    reconcile_mode: str | None = None,
) -> dict[str, Any] | None:
    """Run unify-deploy deployment bootstrap when the package is available.

    Offline Jobs and live assistants both mount ``_UNITY_STARTUP_HOOK_*``.
    Live invokes this via the CM startup hook; offline invokes it directly.
    Returns actor startup config when available, else ``None``.
    """

    try:
        from unify_deploy.runtime_bootstrap import ensure_deployment_runtime
    except ImportError:
        logger.warning(
            "unify_deploy.runtime_bootstrap unavailable; skipping deployment runtime",
        )
        return None

    from unify.session_details import SESSION_DETAILS

    return ensure_deployment_runtime(
        session_details=SESSION_DETAILS,
        cm=None,
        reconcile_mode=reconcile_mode,
    )


def _prepare_local_workspace() -> None:
    from unify.conversation_manager.workspace import ensure_local_workspace_dirs
    from unify.file_manager.settings import get_local_root

    root = Path(get_local_root()).expanduser().resolve()
    ensure_local_workspace_dirs(root)
    os.chdir(root)
    logger.info("Assistant workspace ready at %s", root)


def _configure_event_bus(api_key: str | None) -> None:
    from unify.events.event_bus import EVENT_BUS

    if api_key:
        EVENT_BUS._get_logger().session.headers["Authorization"] = f"Bearer {api_key}"
    EVENT_BUS.set_window("Comms", 100)


def _eager_session_managers(
    *,
    init_contact_manager: bool,
    init_transcript_manager: bool,
    init_memory_manager: bool,
    api_key: str | None,
) -> None:
    from unify.manager_registry import ManagerRegistry
    from unify.settings import SETTINGS

    contact_manager = None
    if init_contact_manager:
        try:
            contact_manager = ManagerRegistry.get_contact_manager(
                description="offline substrate",
            )
        except Exception:
            logger.warning("ContactManager eager init failed (degraded)", exc_info=True)

    transcript_manager = None
    if init_transcript_manager:
        try:
            kwargs: dict[str, Any] = {"description": "offline substrate"}
            if contact_manager is not None:
                kwargs["contact_manager"] = contact_manager
            transcript_manager = ManagerRegistry.get_transcript_manager(**kwargs)
            if api_key and hasattr(transcript_manager, "_get_logger"):
                transcript_manager._get_logger().session.headers[
                    "Authorization"
                ] = f"Bearer {api_key}"
        except Exception:
            logger.warning(
                "TranscriptManager eager init failed (degraded)",
                exc_info=True,
            )

    if init_memory_manager and SETTINGS.memory.ENABLED:
        try:
            from unify.memory_manager.memory_manager import MemoryManager

            mem_cfg = MemoryManager.MemoryConfig(
                contacts=SETTINGS.memory.CONTACTS,
                bios=SETTINGS.memory.BIOS,
                rolling_summaries=SETTINGS.memory.ROLLING_SUMMARIES,
                response_policies=SETTINGS.memory.RESPONSE_POLICIES,
                knowledge=SETTINGS.memory.KNOWLEDGE,
                tasks=SETTINGS.memory.TASKS,
            )
            ManagerRegistry.get_memory_manager(
                transcript_manager=transcript_manager,
                contact_manager=contact_manager,
                config=mem_cfg,
                loop=None,
            )
        except Exception:
            logger.warning("MemoryManager eager init failed (degraded)", exc_info=True)


def _eager_core_managers() -> None:
    from unify.manager_registry import ManagerRegistry

    try:
        fm = ManagerRegistry.get_file_manager()
        _ = fm._data_manager  # noqa: F841 — resolve while ContextVars are bound
    except Exception:
        logger.warning("FileManager eager init failed (degraded)", exc_info=True)

    try:
        ManagerRegistry.get_secret_manager()
    except Exception:
        logger.warning("SecretManager eager init failed (degraded)", exc_info=True)

    try:
        ManagerRegistry.get_function_manager()
    except Exception:
        logger.warning("FunctionManager eager init failed (degraded)", exc_info=True)


def _hydrate_secrets_to_environ() -> None:
    """Mirror Orchestra Secrets rows into os.environ after deployment reconcile."""

    from unify.manager_registry import ManagerRegistry

    try:
        sm = ManagerRegistry.get_secret_manager()
        sm._sync_dotenv()
    except Exception:
        logger.warning("Secrets→env hydrate failed (degraded)", exc_info=True)


def _warm_embeddings() -> None:
    from unify.manager_registry import ManagerRegistry

    try:
        ManagerRegistry.warm_all_embeddings()
        ManagerRegistry.get_function_manager().warm_embeddings()
    except Exception:
        logger.warning("Embedding warm-up failed (degraded)", exc_info=True)


def _apply_runtime_backends(startup_config: dict[str, Any] | None) -> None:
    if not startup_config:
        return
    runtime_backends = startup_config.get("runtime_backends")
    if not runtime_backends:
        return
    try:
        from unify.deploy_runtime import (
            DeployRuntimeBackends,
            register_deploy_runtime,
        )

        if isinstance(runtime_backends, DeployRuntimeBackends):
            register_deploy_runtime(runtime_backends)
        else:
            register_deploy_runtime(
                session=runtime_backends.get("session"),
                jobs=runtime_backends.get("jobs"),
                metrics=runtime_backends.get("metrics"),
                logs=runtime_backends.get("logs"),
            )
    except Exception:
        logger.warning("Deploy runtime backend registration failed", exc_info=True)


__all__ = [
    "bootstrap_assistant_substrate",
    "ensure_deployment_runtime_optional",
]
