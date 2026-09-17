"""
Actor-specific settings.

These settings are composed into the global ProductionSettings.
Environment variables use the prefix UNIFY_ACTOR_.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class ActorSettings(BaseSettings):
    """Actor settings.

    Attributes:
        IMPL: Implementation type - "code_act" or "simulated".
    """

    IMPL: str = "code_act"

    model_config = SettingsConfigDict(
        env_prefix="UNIFY_ACTOR_",
        case_sensitive=True,
        extra="ignore",
    )
