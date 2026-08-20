"""
FunctionManager-specific settings.

These settings are composed into the global ProductionSettings.
Environment variables use the prefix UNIFY_FUNCTION_; nested verification
knobs use a double underscore, e.g. ``UNIFY_FUNCTION_verification__max_rewinds_per_run``.
"""

from typing import Dict, Optional

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .types.verification import SideEffectClass


class VerificationSettings(BaseModel):
    """Policy defaults for earning and keeping trust in stored functions.

    Every number here is a decision, not a placeholder; change it through
    settings, never at a call site.
    """

    model: Optional[str] = None
    tier0_always: bool = True
    required_passes: Dict[SideEffectClass, int] = Field(
        default_factory=lambda: {
            SideEffectClass.safe_noop: 0,
            SideEffectClass.read_only: 3,
            SideEffectClass.idempotent_effectful: 3,
            SideEffectClass.unsafe_effectful: 5,
        },
    )
    min_distinct_inputs: Dict[SideEffectClass, int] = Field(
        default_factory=lambda: {
            SideEffectClass.safe_noop: 1,
            SideEffectClass.read_only: 2,
            SideEffectClass.idempotent_effectful: 2,
            SideEffectClass.unsafe_effectful: 3,
        },
    )
    spot_check_rate: Dict[SideEffectClass, float] = Field(
        default_factory=lambda: {
            SideEffectClass.idempotent_effectful: 0.1,
            SideEffectClass.unsafe_effectful: 0.1,
        },
    )
    max_rewinds_per_run: int = 2
    pending_verdict_timeout_s: int = 120
    deliver_before_root_verdict: bool = False
    auto_promote_offline: bool = True
    max_fixtures_per_function: int = 5
    max_fixture_bytes: int = 8192
    max_guidance_chars: int = 6000
    unsure_warning_threshold: int = 3


class FunctionSettings(BaseSettings):
    """FunctionManager settings.

    Attributes:
        IMPL: Implementation type - "real" or "simulated".
        verification: Trust policy for compositional functions.
    """

    IMPL: str = "real"
    verification: VerificationSettings = Field(default_factory=VerificationSettings)

    model_config = SettingsConfigDict(
        env_prefix="UNIFY_FUNCTION_",
        env_nested_delimiter="__",
        case_sensitive=True,
        extra="ignore",
    )
