"""
WorkflowManager-specific settings.

These settings are composed into the global ProductionSettings.
Environment variables use the prefix UNIFY_WORKFLOW_.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkflowSettings(BaseSettings):
    """WorkflowManager settings.

    Attributes:
        ENABLED: Whether WorkflowManager is enabled.
        IMPL: Implementation type - "real".
    """

    ENABLED: bool = False
    IMPL: str = "real"

    model_config = SettingsConfigDict(
        env_prefix="UNIFY_WORKFLOW_",
        case_sensitive=True,
        extra="ignore",
    )
