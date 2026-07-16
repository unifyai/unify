"""
TaskScheduler-specific settings.

These settings are composed into the global ProductionSettings.
Environment variables use the prefix UNITY_TASK_.
"""

import os

from pydantic_settings import BaseSettings, SettingsConfigDict


def _derive_local_scheduler_default() -> bool:
    """Derive whether the in-process LocalActivationScheduler should run.

    Resolution order, first match wins:
    1. ``UNITY_LOCAL_SCHEDULER`` set explicitly → use the parsed bool.
    2. ``UNITY_COMMS_URL`` empty/unset → local (Orchestra projection silently
       stops syncing to Communication, so something has to fire timers).
    3. ``UNITY_CONVERSATION_LOCAL_COMMS_MODE`` == ``"local"`` or
       ``UNITY_CONVERSATION_LOCAL_COMMS_ENABLED`` truthy → local.
    4. Otherwise hosted (the default for production deployments).

    Hosted deployments leave ``UNITY_COMMS_URL`` set, so this defaults to
    ``False`` there. The fresh ``unity`` install path runs with
    ``LOCAL_COMMS_MODE=local`` and an empty ``UNITY_COMMS_URL``, so it
    defaults to ``True`` and the scheduler fires.
    """

    explicit = os.environ.get("UNITY_LOCAL_SCHEDULER")
    if explicit is not None:
        return explicit.strip().lower() in {"1", "true", "yes", "on"}

    comms_url = os.environ.get("UNITY_COMMS_URL", "").strip()
    if not comms_url:
        return True

    local_mode = os.environ.get("UNITY_CONVERSATION_LOCAL_COMMS_MODE", "").strip()
    if local_mode.lower() == "local":
        return True
    local_enabled = os.environ.get(
        "UNITY_CONVERSATION_LOCAL_COMMS_ENABLED",
        "",
    ).strip()
    if local_enabled.lower() in {"1", "true", "yes", "on"}:
        return True
    return False


class TaskSettings(BaseSettings):
    """TaskScheduler settings.

    Attributes:
        IMPL: Implementation type - "real" or "simulated".
        ROUTER_TIMEOUT_SECONDS: Timeout for routing operations in seconds.
        LOCAL_VIEW_OFF: Disable local task view caching.
        SIM_ACTOR_DURATION: Duration for simulated actor in seconds.
        LOCAL_SCHEDULER_ENABLED: When True, an in-process
            ``LocalActivationScheduler`` watches the Orchestra-projected
            ``Tasks/Activations`` rows and fires due tasks directly on the
            event broker (live mode) or via subprocess invocation of
            ``unify.task_scheduler.offline_runner`` (offline mode). When
            False, scheduled activations are materialised by Communication's
            Cloud Tasks queues — the hosted path. Auto-derived from
            ``UNITY_COMMS_URL`` / ``LOCAL_COMMS_*`` env signals so a fresh
            local ``unity`` install gets ``True`` and a production deploy
            gets ``False``. Override explicitly with
            ``UNITY_LOCAL_SCHEDULER=true|false``.
        LOCAL_SCHEDULER_POLL_INTERVAL_SECONDS: How often the in-process
            scheduler re-reads ``Tasks/Activations`` to pick up rows added
            after boot (e.g. immediately after the user asks the agent to
            schedule something). Ignored when LOCAL_SCHEDULER_ENABLED is
            False. Default 60 seconds.
    """

    IMPL: str = "real"
    ROUTER_TIMEOUT_SECONDS: float = 60.0
    LOCAL_VIEW_OFF: bool = False
    SIM_ACTOR_DURATION: float = 20.0
    LOCAL_SCHEDULER_ENABLED: bool = _derive_local_scheduler_default()
    LOCAL_SCHEDULER_POLL_INTERVAL_SECONDS: float = 60.0
    PROVIDER_EVENT_DISPATCH_REQUEST_TTL_SECONDS: int = 300

    model_config = SettingsConfigDict(
        env_prefix="UNITY_TASK_",
        case_sensitive=True,
        extra="ignore",
    )
