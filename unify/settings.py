"""
unify/settings.py
==================

Centralized runtime settings using pydantic-settings.

All settings can be overridden via environment variables or the ``.env`` file
in the working directory.
"""

from typing import Any

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from unify.actor.settings import ActorSettings
from unify.conversation_manager.settings import ConversationSettings
from unify.function_manager.settings import FunctionSettings
from unify.guidance_manager.settings import GuidanceSettings


def _parse_bool(v: Any) -> bool:
    """Parse a value as boolean."""
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in ("true", "yes", "1", "on")
    return bool(v)


class ProductionSettings(BaseSettings):
    """Runtime settings; test settings (TestingSettings) inherit from this class."""

    # ─────────────────────────────────────────────────────────────────────────
    # Local Workspace
    # ─────────────────────────────────────────────────────────────────────────
    # Root directory for local file operations, CodeActActor working directory
    # and virtual environments. Defaults to ``<UNIFY_HOME>/workspace`` (with
    # ``UNIFY_HOME`` defaulting to ``~/.unify``) when empty.
    UNIFY_LOCAL_ROOT: str = ""

    # ─────────────────────────────────────────────────────────────────────────
    # Core LLM Settings
    # ─────────────────────────────────────────────────────────────────────────
    UNIFY_MODEL: str = "openai/gpt-5.6-sol@openrouter"
    # Reasoning effort paired with UNIFY_MODEL when no per-assistant default is
    # set. Empty leaves per-call-site effort levels untouched.
    UNIFY_REASONING_EFFORT: str = "high"
    # Ceiling on output tokens for one actor turn. Unset, the provider ceiling
    # applies (128k on current OpenAI models), so a turn that degenerates into
    # repetition bills and blocks for the full window — observed at eight
    # minutes. Sized to bound that, not to shape normal turns: reasoning tokens
    # count toward it, so keep it well above what a long reasoning turn plus a
    # large code block needs. Set to 0 to restore the provider ceiling.
    UNIFY_MAX_OUTPUT_TOKENS: int = 32768

    # Ceiling on tool-calling iterations for one agent run. Unset, a run ends
    # only when the model decides to stop, so a loop that never converges
    # bills indefinitely. Sized to bound that rather than to shape normal
    # runs: a long agentic task uses far fewer steps than this, and a caller
    # that genuinely needs more can pass ``max_steps`` explicitly. Set to 0 to
    # restore unbounded iteration.
    UNIFY_MAX_TOOL_LOOP_STEPS: int = 300

    # ─────────────────────────────────────────────────────────────────────────
    # LLM Provider Credentials
    # ─────────────────────────────────────────────────────────────────────────
    ANTHROPIC_API_KEY: SecretStr = SecretStr("")
    DEEPSEEK_API_KEY: SecretStr = SecretStr("")
    # OpenRouter — used for ``*@openrouter`` endpoints (the default model) and
    # for embeddings when ``UNIFY_EMBED_MODEL`` names an ``@openrouter`` model.
    OPENROUTER_API_KEY: SecretStr = SecretStr("")
    UNIFY_VALIDATE_LLM_PROVIDERS: bool = True

    # ─────────────────────────────────────────────────────────────────────────
    # Builtins Catalogue
    # ─────────────────────────────────────────────────────────────────────────
    # Name of the project holding the builtins catalogues (function primitives
    # and guidance), seeded from the committed snapshots at start-up.
    UNIFY_BUILTINS_PROJECT: str = "Builtins"

    # ─────────────────────────────────────────────────────────────────────────
    # Logging / Observability
    # ─────────────────────────────────────────────────────────────────────────
    PYTEST_LOG_TO_FILE: bool = True
    # Directory for Unify LOGGER file output (async tool loop, managers, etc.)
    # When set, logs are written to {UNIFY_LOG_DIR}/unify.log
    # Default: None (console only)
    UNIFY_LOG_DIR: str = ""

    # ─────────────────────────────────────────────────────────────────────────
    # EventBus Publishing
    # ─────────────────────────────────────────────────────────────────────────
    # Controls whether EventBus persists published events to the store's
    # ``Events/*`` contexts. Disabled by default to reduce noise.
    EVENTBUS_PUBLISHING_ENABLED: bool = False

    # ─────────────────────────────────────────────────────────────────────────
    # Terminal Logging
    # ─────────────────────────────────────────────────────────────────────────
    UNIFY_TERMINAL_LOG: bool = True
    UNIFY_TERMINAL_LOG_LEVEL: str = "INFO"

    # ─────────────────────────────────────────────────────────────────────────
    # Debug Modes (performance overhead, development-only)
    # ─────────────────────────────────────────────────────────────────────────
    UNIFY_ASYNCIO_DEBUG: bool = False

    # ─────────────────────────────────────────────────────────────────────────
    # Test Infrastructure
    # ─────────────────────────────────────────────────────────────────────────
    # Log subdirectory for LLM I/O log files (datetime-prefixed for ordering)
    UNIFY_LOG_SUBDIR: str = ""
    # Terminal socket name for tmux isolation; also used as log subdir fallback
    # when UNIFY_LOG_SUBDIR is not set
    UNIFY_TEST_SOCKET: str = ""
    # Explicit repository root for log file placement (e.g., worktrees)
    UNIFY_LOG_ROOT: str = ""
    # Test mode flag
    TEST: bool = False

    # ─────────────────────────────────────────────────────────────────────────
    # Feature Flags
    # ─────────────────────────────────────────────────────────────────────────
    UNIFY_READONLY_ASK_GUARD: bool = True
    FIRST_ASK_TOOL_IS_SEARCH: bool = False
    FIRST_MUTATION_TOOL_IS_ASK: bool = False

    # ─────────────────────────────────────────────────────────────────────────
    # Composed Manager Settings
    # ─────────────────────────────────────────────────────────────────────────
    # Each manager owns its settings in its own settings.py file.
    # Access via SETTINGS.function.IMPL, SETTINGS.guidance.IMPL, etc.
    actor: ActorSettings = Field(default_factory=ActorSettings)
    conversation: ConversationSettings = Field(default_factory=ConversationSettings)
    function: FunctionSettings = Field(default_factory=FunctionSettings)
    guidance: GuidanceSettings = Field(default_factory=GuidanceSettings)

    # ─────────────────────────────────────────────────────────────────────────
    # Validators
    # ─────────────────────────────────────────────────────────────────────────
    @field_validator(
        "UNIFY_TERMINAL_LOG",
        "UNIFY_ASYNCIO_DEBUG",
        "EVENTBUS_PUBLISHING_ENABLED",
        "PYTEST_LOG_TO_FILE",
        "UNIFY_READONLY_ASK_GUARD",
        "FIRST_ASK_TOOL_IS_SEARCH",
        "FIRST_MUTATION_TOOL_IS_ASK",
        "TEST",
        "UNIFY_VALIDATE_LLM_PROVIDERS",
        mode="before",
    )
    @classmethod
    def parse_bool_fields(cls, v: Any) -> bool:
        return _parse_bool(v)

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore",
    )

    def validate_llm_providers(self) -> None:
        """Validate that the runtime has some way to reach an LLM provider.

        Raises:
            RuntimeError: If no provider credential is available.
        """
        if not self.UNIFY_VALIDATE_LLM_PROVIDERS:
            return
        available = {
            "ANTHROPIC_API_KEY": self.ANTHROPIC_API_KEY,
            "DEEPSEEK_API_KEY": self.DEEPSEEK_API_KEY,
            "OPENROUTER_API_KEY": self.OPENROUTER_API_KEY,
        }
        if not any(available.values()):
            raise RuntimeError(
                "At least one LLM provider credential is required. "
                "Set OPENROUTER_API_KEY, ANTHROPIC_API_KEY, "
                "and/or DEEPSEEK_API_KEY.",
            )


# Singleton instance for production code
SETTINGS = ProductionSettings()
