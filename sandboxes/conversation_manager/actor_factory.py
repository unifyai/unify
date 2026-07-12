"""
Actor + primitives factory for the ConversationManager sandbox.

This is sandbox-only wiring glue. It centralizes:
- configuring real manager implementations via env vars
- constructing a CodeActActor with explicit environments and a computer backend

Important:
- We keep the existing `--real-comms` flag orthogonal to actor configuration.
  Real comms is guarded by explicit safety confirmations elsewhere.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, Literal, Optional

from sandboxes.conversation_manager.computer_activity import (
    ComputerActivity,
    install_computer_activity_hooks,
)
from sandboxes.conversation_manager.config_manager import ActorConfig
from unify.actor.code_act_actor import CodeActActor
from unify.actor.environments import ComputerEnvironment, StateManagerEnvironment
from unify.function_manager.primitives import ComputerPrimitives, Primitives
from unify.manager_registry import ManagerRegistry

LG = logging.getLogger("conversation_manager_sandbox")

ProgressCallback = Callable[[str], None]


@dataclass(frozen=True)
class ActorFactoryResult:
    actor: Any
    primitives: Optional[Primitives]
    computer_primitives: Optional[ComputerPrimitives]


class ActorFactory:
    """Create actors for sandbox configurations."""

    @classmethod
    def create_actor(
        cls,
        config: ActorConfig,
        *,
        args: Any,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> ActorFactoryResult:
        """
        Create the actor instance for the selected sandbox configuration.

        Returns the actor plus (for CodeAct) the constructed primitives objects that
        back the actor environments.
        """
        progress = progress_callback or (lambda _m: None)

        cls._apply_manager_impl_env(config.managers_mode)
        # Ensure no stale singleton managers leak across sandbox restarts.
        try:
            ManagerRegistry.clear()
        except Exception:
            pass

        progress("[init] Loading configuration...")
        progress(f"✓ Actor selected: {config.actor_type}")

        primitives = cls.build_primitives(
            progress_callback=progress,
        )

        computer_primitives = cls.create_computer_backend(
            args=args,
            progress_callback=progress,
        )

        envs = [StateManagerEnvironment(primitives)]
        if computer_primitives is not None:
            envs.append(ComputerEnvironment(computer_primitives))

        actor = CodeActActor(
            environments=envs,
            function_manager=ManagerRegistry.get_function_manager(),
            guidance_manager=ManagerRegistry.get_guidance_manager(),
            knowledge_manager=ManagerRegistry.get_knowledge_manager(),
        )

        return ActorFactoryResult(
            actor=actor,
            primitives=primitives,
            computer_primitives=computer_primitives,
        )

    @staticmethod
    def build_primitives(
        *,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> Primitives:
        """
        Return a Primitives instance for the real-managers sandbox configuration.

        Manager IMPL selection is already applied via env vars before this call
        so CM initialization and CodeAct share the same singleton instances from
        ManagerRegistry.
        """
        progress = progress_callback or (lambda _m: None)
        progress("[init] Managers mode: real")
        return Primitives()

    @staticmethod
    def create_computer_backend(
        *,
        args: Any,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> Optional[ComputerPrimitives]:
        """Create the Magnitude computer backend via agent-service."""
        progress = progress_callback or (lambda _m: None)

        container_url = getattr(args, "container_url", None) or getattr(
            args,
            "agent_server_url",
            None,
        )
        local_url = getattr(args, "local_url", None)
        progress("[computer] Connecting to agent-service...")
        cp = ComputerPrimitives(
            computer_mode="magnitude",
            container_url=str(container_url) if container_url else None,
            local_url=str(local_url) if local_url else None,
        )
        activity = ComputerActivity()
        setattr(args, "_computer_activity", activity)
        activity.mark_connected_sync(True)
        install_computer_activity_hooks(
            computer_primitives=cp,
            activity=activity,
            emit_line=getattr(args, "_computer_log_sink", None),
        )
        progress("✓ Computer ready (backend initialized)")
        return cp

    @staticmethod
    def _apply_manager_impl_env(mode: Literal["real"]) -> None:
        """
        Configure manager IMPL selection for sandbox runs.

        This controls what `ManagerRegistry.get_*()` returns when CM initializes.
        """
        impl = "real"

        # State managers
        os.environ["UNITY_CONTACT_IMPL"] = impl
        os.environ["UNITY_TRANSCRIPT_IMPL"] = impl
        os.environ["UNITY_TASK_IMPL"] = impl
        os.environ["UNITY_KNOWLEDGE_IMPL"] = impl
        os.environ["UNITY_GUIDANCE_IMPL"] = impl
        os.environ["UNITY_SECRET_IMPL"] = impl
        os.environ["UNITY_WEB_IMPL"] = impl
        os.environ["UNITY_FILE_IMPL"] = impl

        # Support managers commonly used by CM / primitives
        os.environ["UNITY_DATA_IMPL"] = impl
        os.environ["UNITY_FUNCTION_IMPL"] = impl
        os.environ["UNITY_CONVERSATION_IMPL"] = impl

        # Memory is optional; keep it aligned so behavior is predictable.
        os.environ["UNITY_MEMORY_IMPL"] = impl

        # Config manager for per-company actor configuration.
        os.environ["UNITY_CONFIG_IMPL"] = impl
