"""
FunctionManager-specific settings.

These settings are composed into the global ProductionSettings.
Environment variables use the prefix DROID_FUNCTION_.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class FunctionSettings(BaseSettings):
    """FunctionManager settings.

    Attributes:
        IMPL: Implementation type - "real" or "simulated".
    """

    IMPL: str = "real"

    model_config = SettingsConfigDict(
        env_prefix="DROID_FUNCTION_",
        case_sensitive=True,
        extra="ignore",
    )
