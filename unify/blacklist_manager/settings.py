"""
BlackListManager-specific settings.

These settings are composed into the global ProductionSettings.
Environment variables use the prefix UNITY_BLACKLIST_.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class BlacklistSettings(BaseSettings):
    """BlackListManager settings.

    Attributes:
        IMPL: Implementation type - "real" (no simulated variant exists).
    """

    IMPL: str = "real"

    model_config = SettingsConfigDict(
        env_prefix="UNITY_BLACKLIST_",
        case_sensitive=True,
        extra="ignore",
    )
