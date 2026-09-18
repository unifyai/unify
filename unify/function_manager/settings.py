"""
FunctionManager-specific settings.

These settings are composed into the global ProductionSettings.
Environment variables use the prefix UNIFY_FUNCTION_; nested knobs use a
double underscore, e.g. ``UNIFY_FUNCTION_activation__enabled``.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .activation import ActivationSettings


class FunctionSettings(BaseSettings):
    """FunctionManager settings.

    Attributes:
        IMPL: Implementation type - "real" or "simulated".
        activation: Usage-weighted retrieval policy for stored functions.
    """

    IMPL: str = "real"
    activation: ActivationSettings = Field(default_factory=ActivationSettings)

    model_config = SettingsConfigDict(
        env_prefix="UNIFY_FUNCTION_",
        env_nested_delimiter="__",
        case_sensitive=True,
        extra="ignore",
    )
