"""
unify/settings.py
==================

Centralized production environment settings using pydantic-settings.

These settings are used in the deployed system and are inherited by test settings.
All settings can be overridden via environment variables or .env file.
"""

from typing import Any, Literal

from pydantic import Field, field_validator, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from unify.actor.settings import ActorSettings
from unify.blacklist_manager.settings import BlacklistSettings
from unify.contact_manager.settings import ContactSettings
from unify.conversation_manager.settings import ConversationSettings
from unify.canvas_manager.settings import CanvasSettings
from unify.dashboard_manager.settings import DashboardSettings
from unify.data_manager.settings import DataSettings
from unify.file_manager.settings import FileSettings
from unify.function_manager.settings import FunctionSettings
from unify.guidance_manager.settings import GuidanceSettings
from unify.workflow_manager.settings import WorkflowSettings
from unify.image_manager.settings import ImageSettings
from unify.ingestion_manager.settings import IngestionSettings
from unify.knowledge_manager.settings import KnowledgeSettings
from unify.memory_manager.settings import MemorySettings
from unify.secret_manager.settings import SecretSettings
from unify.task_scheduler.settings import TaskSettings
from unify.transcript_manager.settings import TranscriptSettings
from unify.web_searcher.settings import WebSettings


def _parse_bool(v: Any) -> bool:
    """Parse a value as boolean."""
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in ("true", "yes", "1", "on")
    return bool(v)


def _parse_deploy_env(v: Any) -> str:
    """Parse the deployment environment setting."""
    if v is None:
        return "production"
    env = str(v).strip().lower() or "production"
    if env not in {"production", "staging"}:
        raise ValueError("DEPLOY_ENV must be one of production or staging")
    return env


class ProductionSettings(BaseSettings):
    """Production environment settings used in deployed system and tests.

    All settings can be overridden via environment variables.
    Test settings (TestingSettings) inherit from this class.
    """

    # ─────────────────────────────────────────────────────────────────────────
    # Local Workspace
    # ─────────────────────────────────────────────────────────────────────────
    # Root directory for local file operations, CodeActActor working directory,
    # virtual environments, and .env storage.  Defaults to ~/Unity/Local when
    # empty.  Override via UNITY_LOCAL_ROOT env var.
    UNITY_LOCAL_ROOT: str = ""

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

    # ─────────────────────────────────────────────────────────────────────────
    # LLM Provider Credentials
    # ─────────────────────────────────────────────────────────────────────────
    # OpenAI — speech-to-text and realtime voice only. OpenAI chat models are
    # reached as ``openai/<id>@openrouter``, so this cannot satisfy LLM access.
    OPENAI_API_KEY: SecretStr = SecretStr("")
    ANTHROPIC_API_KEY: SecretStr = SecretStr("")
    DEEPSEEK_API_KEY: SecretStr = SecretStr("")
    # OpenRouter — used for ``*@openrouter`` endpoints (platform defaults).
    OPENROUTER_API_KEY: SecretStr = SecretStr("")
    UNITY_VALIDATE_LLM_PROVIDERS: bool = True

    # ─────────────────────────────────────────────────────────────────────────
    # External Service Credentials
    # ─────────────────────────────────────────────────────────────────────────
    ORCHESTRA_ADMIN_KEY: SecretStr = SecretStr("")

    # Multi-tenant MS Teams (Bot Framework) app credentials. A single Azure
    # bot registration serves every tenant that installs it; the id + secret
    # mint Bot Connector tokens for outbound proactive replies and verify
    # inbound activity JWTs. Deployment-level secrets (not per-tenant).
    MS_TEAMS_BOT_APP_ID: str = ""
    MS_TEAMS_BOT_APP_SECRET: SecretStr = SecretStr("")

    # ─────────────────────────────────────────────────────────────────────────
    # Infrastructure URLs
    # ─────────────────────────────────────────────────────────────────────────
    ORCHESTRA_URL: str = "https://api.unify.ai/v0"
    # Console origin used to build user-facing links (canvas and dashboard
    # views). Per-environment deployments override this or every shared link
    # points at production Console regardless of where the row lives.
    CONSOLE_URL: str = "https://console.unify.ai"
    UNITY_COORDINATOR_EMAIL_ADDRESS: str = "twin@unify.ai"
    # Catch-all domain for multiplayer twin alias addresses, and the Workspace
    # mailbox that receives them. Alias addresses have no Workspace user of
    # their own: reads and sends delegate to the mailbox while the From header
    # keeps the alias.
    UNITY_TWIN_ALIAS_EMAIL_DOMAIN: str = "twins.unify.ai"
    UNITY_TWIN_ALIAS_MAILBOX: str = "twins@unify.ai"

    # ─────────────────────────────────────────────────────────────────────────
    # Builtins Catalogue
    # ─────────────────────────────────────────────────────────────────────────
    # Name of the public-read Unify project holding the global builtins
    # catalogues (function primitives and guidance), one copy platform-wide.
    UNITY_BUILTINS_PROJECT: str = "Builtins"

    # ─────────────────────────────────────────────────────────────────────────
    # Workflow Catalogue
    # ─────────────────────────────────────────────────────────────────────────
    # Root directory holding the curated workflow bundles (one directory
    # per workflow: manifest.yaml + guidance/ + tasks/ + ...). Empty means
    # no catalogue: the WorkflowManager is not built at boot.
    UNITY_WORKFLOWS_DIR: str = ""

    # ─────────────────────────────────────────────────────────────────────────
    # GCP Project
    # ─────────────────────────────────────────────────────────────────────────
    # GCP project ID for Pub/Sub topics and subscriptions. Override via
    # GCP_PROJECT_ID env var for local development with the Pub/Sub emulator
    # (e.g. "local-test-project" to match Communication's local.sh).
    GCP_PROJECT_ID: str = ""

    # ─────────────────────────────────────────────────────────────────────────
    # Logging / Observability
    # ─────────────────────────────────────────────────────────────────────────
    PYTEST_LOG_TO_FILE: bool = True
    # Directory for Unity LOGGER file output (async tool loop, managers, etc.)
    # When set, logs are written to {UNITY_LOG_DIR}/unity.log
    # Default: None (console only)
    UNITY_LOG_DIR: str = ""

    # ─────────────────────────────────────────────────────────────────────────
    # EventBus Publishing
    # ─────────────────────────────────────────────────────────────────────────
    # Controls whether EventBus publishes events (logging to Unify and local
    # subscriptions/callbacks). Disabled by default for local development to
    # reduce noise. Enable in production deployments.
    EVENTBUS_PUBLISHING_ENABLED: bool = False

    # Orchestra ``Events/*`` persistence mode when publishing is enabled:
    # - ``all``: write every published event to Orchestra (legacy behavior)
    # - ``allowlist``: write ManagerMethod/ToolLoop events whose method or
    #   tool name is listed in EVENTBUS_ORCHESTRA_PERSIST_TOOLS, **plus** the
    #   full ManagerMethod + ToolLoop tree when the payload carries task-run
    #   lineage (``run_key`` or ``task_id``+``run_key`` under an ActiveTask)
    # Pub/Sub Live Actions streaming is independent (see EVENTBUS_PUBSUB_STREAMING).
    EVENTBUS_ORCHESTRA_PERSIST_MODE: Literal["all", "allowlist"] = "all"

    # Comma-separated tool/method names when EVENTBUS_ORCHESTRA_PERSIST_MODE
    # is ``allowlist`` and the event is **not** under a execution lineage
    # (default: CodeAct action boundary + execution boundaries + tool results).
    EVENTBUS_ORCHESTRA_PERSIST_TOOLS: str = "act,execute_code,execute_function"

    # ─────────────────────────────────────────────────────────────────────────
    # EventBus Pub/Sub Streaming (Live Actions)
    # ─────────────────────────────────────────────────────────────────────────
    # When enabled, EventBus.publish() also streams ManagerMethod and ToolLoop
    # events to the assistant's GCP Pub/Sub topic with thread="action_event".
    # This enables real-time frontend rendering of the agent's activity tree
    # without polling Orchestra. Requires GCP credentials and a provisioned
    # Pub/Sub topic. Disabled by default; enable in production deployments.
    # Stream filters (stream_filters.py) apply here only — not to Orchestra.
    EVENTBUS_PUBSUB_STREAMING: bool = False

    # ─────────────────────────────────────────────────────────────────────────
    # OpenTelemetry Tracing
    # ─────────────────────────────────────────────────────────────────────────
    # Master switch for OTel tracing.
    # - UNITY_OTEL=false (default): OTel tracing disabled
    # - UNITY_OTEL=true: OTel tracing enabled, creates TracerProvider if needed
    # - UNITY_OTEL_ENDPOINT: OTLP endpoint for trace export (optional)
    # - UNITY_OTEL_LOG_DIR: Directory for file-based span export (optional)
    #
    # When enabled, manager operations and async tool loops create spans that
    # propagate trace context to downstream libraries (unillm, unify).
    #
    # File-based span export:
    # When UNITY_OTEL_LOG_DIR is set, spans are written to JSONL files keyed
    # by trace_id. This enables full-stack trace correlation with Orchestra
    # (which runs in a separate process but receives the traceparent header).
    UNITY_OTEL: bool = False
    UNITY_OTEL_ENDPOINT: str = ""
    UNITY_OTEL_LOG_DIR: str = ""

    # ─────────────────────────────────────────────────────────────────────────
    # Terminal Logging
    # ─────────────────────────────────────────────────────────────────────────
    UNITY_TERMINAL_LOG: bool = True
    UNITY_TERMINAL_LOG_LEVEL: str = "INFO"

    # ─────────────────────────────────────────────────────────────────────────
    # Debug Modes (performance overhead, development-only)
    # ─────────────────────────────────────────────────────────────────────────
    UNITY_ASYNCIO_DEBUG: bool = False

    # ─────────────────────────────────────────────────────────────────────────
    # Test Infrastructure
    # ─────────────────────────────────────────────────────────────────────────
    # Log subdirectory for LLM I/O log files (datetime-prefixed for ordering)
    UNITY_LOG_SUBDIR: str = ""
    # Terminal socket name for tmux isolation; also used as log subdir fallback
    # when UNITY_LOG_SUBDIR is not set
    UNITY_TEST_SOCKET: str = ""
    # Explicit repository root for log file placement (e.g., worktrees)
    UNITY_LOG_ROOT: str = ""
    # Test mode flag
    TEST: bool = False

    # ─────────────────────────────────────────────────────────────────────────
    # Feature Flags
    # ─────────────────────────────────────────────────────────────────────────
    UNITY_READONLY_ASK_GUARD: bool = True
    FIRST_ASK_TOOL_IS_SEARCH: bool = False
    FIRST_MUTATION_TOOL_IS_ASK: bool = False
    DEPLOY_ENV: Literal["production", "staging"] = "production"
    # Whether a Console web UI / onboarding front-end is present for this
    # deployment. Hosted and self-host run with a Console (default True); the
    # public local install runs against hosted Orchestra with no Console and
    # sets this False to suppress Console-UI knowledge and onboarding prompts.
    UNITY_CONSOLE_UI: bool = True

    # ─────────────────────────────────────────────────────────────────────────
    # Manager Configuration
    # ─────────────────────────────────────────────────────────────────────────
    # Foundational managers (cannot be disabled, only implementation switched):
    #   - Actor, ContactManager, TranscriptManager, TaskScheduler, ConversationManager
    # Optional managers (can be disabled via ENABLED=False):
    #   - KnowledgeManager, GuidanceManager, SecretManager,
    #     WebSearcher

    # ─────────────────────────────────────────────────────────────────────────
    # Composed Manager Settings
    # ─────────────────────────────────────────────────────────────────────────
    # Each manager owns its settings in its own settings.py file.
    # Access via SETTINGS.contact.IMPL, SETTINGS.transcript.IMPL, etc.
    actor: ActorSettings = Field(default_factory=ActorSettings)
    blacklist: BlacklistSettings = Field(default_factory=BlacklistSettings)
    contact: ContactSettings = Field(default_factory=ContactSettings)
    conversation: ConversationSettings = Field(default_factory=ConversationSettings)
    canvas: CanvasSettings = Field(default_factory=CanvasSettings)
    dashboard: DashboardSettings = Field(default_factory=DashboardSettings)
    data: DataSettings = Field(default_factory=DataSettings)
    file: FileSettings = Field(default_factory=FileSettings)
    function: FunctionSettings = Field(default_factory=FunctionSettings)
    guidance: GuidanceSettings = Field(default_factory=GuidanceSettings)
    image: ImageSettings = Field(default_factory=ImageSettings)
    ingestion: IngestionSettings = Field(default_factory=IngestionSettings)
    knowledge: KnowledgeSettings = Field(default_factory=KnowledgeSettings)
    memory: MemorySettings = Field(default_factory=MemorySettings)
    secret: SecretSettings = Field(default_factory=SecretSettings)
    task: TaskSettings = Field(default_factory=TaskSettings)
    transcript: TranscriptSettings = Field(default_factory=TranscriptSettings)
    web: WebSettings = Field(default_factory=WebSettings)
    workflow: WorkflowSettings = Field(default_factory=WorkflowSettings)

    # ─────────────────────────────────────────────────────────────────────────
    # Validators
    # ─────────────────────────────────────────────────────────────────────────
    @field_validator(
        "UNITY_TERMINAL_LOG",
        "UNITY_ASYNCIO_DEBUG",
        "EVENTBUS_PUBLISHING_ENABLED",
        "EVENTBUS_PUBSUB_STREAMING",
        "PYTEST_LOG_TO_FILE",
        "UNITY_READONLY_ASK_GUARD",
        "FIRST_ASK_TOOL_IS_SEARCH",
        "FIRST_MUTATION_TOOL_IS_ASK",
        "TEST",
        "UNITY_VALIDATE_LLM_PROVIDERS",
        "UNITY_OTEL",
        mode="before",
    )
    @classmethod
    def parse_bool_fields(cls, v: Any) -> bool:
        return _parse_bool(v)

    @field_validator("DEPLOY_ENV", mode="before")
    @classmethod
    def parse_deploy_env_field(cls, v: Any) -> str:
        return _parse_deploy_env(v)

    @field_validator("EVENTBUS_ORCHESTRA_PERSIST_MODE", mode="before")
    @classmethod
    def parse_orchestra_persist_mode(cls, v: Any) -> str:
        if v is None or (isinstance(v, str) and not v.strip()):
            return "all"
        normalized = str(v).strip().lower()
        if normalized not in {"all", "allowlist"}:
            raise ValueError(
                "EVENTBUS_ORCHESTRA_PERSIST_MODE must be 'all' or 'allowlist'",
            )
        return normalized

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore",
    )

    @property
    def ENV_SUFFIX(self) -> str:
        """Return the environment suffix used in shared resource names."""
        return "" if self.DEPLOY_ENV == "production" else f"-{self.DEPLOY_ENV}"

    def validate_llm_providers(self) -> None:
        """Validate that at least one LLM provider credential is set.

        Raises:
            RuntimeError: If no LLM provider credentials are set.
        """
        if not self.UNITY_VALIDATE_LLM_PROVIDERS:
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
