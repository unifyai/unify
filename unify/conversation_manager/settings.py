"""
ConversationManager-specific settings.

These settings are composed into the global ProductionSettings.
Environment variables use the prefix UNITY_CONVERSATION_.
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConversationSettings(BaseSettings):
    """ConversationManager settings.

    Attributes:
        IMPL: Implementation type - "real" or "simulated".
        COMMS_URL: URL for the communications service (reads from UNITY_COMMS_URL).
        JOB_NAME: Job name for the ConversationManager session.
        CONTACT_ID: Default contact ID for simulated ConversationManager.
        FAST_BRAIN_MODEL: LLM model for the voice fast brain (TTS mode).
            Override via UNITY_CONVERSATION_FAST_BRAIN_MODEL.
        SLOW_BRAIN_MODEL: Shared ConversationManager slow-brain model. Empty
            falls back to the global shared model (UNIFY_MODEL / assistant
            default resolution). Override via
            UNITY_CONVERSATION_SLOW_BRAIN_MODEL.
        SLOW_BRAIN_REASONING_EFFORT: Reasoning effort paired with
            SLOW_BRAIN_MODEL when that setting is non-empty. Empty leaves
            call-site effort intact. Override via
            UNITY_CONVERSATION_SLOW_BRAIN_REASONING_EFFORT.
        FAST_BRAIN_CONTEXT_WINDOW: Maximum number of conversation items
            (utterances, notifications, etc.) the fast brain keeps in its
            rolling context window. Also used as the limit when hydrating
            historical events at call start.
        FAST_BRAIN_MOOD_CLASSIFICATION_ENABLED: Enable structured mood
            classification after voice user and assistant turns.
        FAST_BRAIN_MOOD_CLASSIFICATION_MODEL: LLM model used for voice avatar
            mood classification.
        BLACKLIST_CHECKS_ENABLED: Enable blacklist filtering and unknown contact
            creation for inbound messages. Default False for fast inbound path.
            When False, no BlackListManager or ContactManager initialization
            occurs during message handling.
        INGRESS_TRANSPORT: Selector for the inbound transport
            (``unify.gateway.IngressTransport`` implementation) that
            CommsManager consumes. ``""`` (default) and ``"legacy"`` both
            keep the existing inline ``subscribe_to_topic`` Pub/Sub
            subscriber active. ``"inmemory"`` selects
            ``InMemoryIngressTransport`` (tests / single-process self-hosted
            Unity). ``"pubsub"`` selects ``PubSubIngressTransport`` and is
            the value the hosted deployment will set once Phase C cuts
            over. Override via ``UNITY_CONVERSATION_INGRESS_TRANSPORT``.
        OUTBOUND_TRANSPORT: Selector for the outbound transport
            (``unify.gateway.OutboundTransport`` implementation) that
            the comms_utils publish helpers use. Same value semantics
            as INGRESS_TRANSPORT. Override via
            ``UNITY_CONVERSATION_OUTBOUND_TRANSPORT``.
        LOCAL_COMMS_PUBLIC_URL_FILE: File containing the current public URL
            for local comms callbacks. The file is read for each URL
            construction so rotating quick tunnels take effect immediately.
    """

    FAST_BRAIN_MODEL: str = "gpt-5.4-mini@openai"
    SLOW_BRAIN_MODEL: str = "gpt-5.6-terra@openai"
    SLOW_BRAIN_REASONING_EFFORT: str = "high"
    FAST_BRAIN_CONTEXT_WINDOW: int = 50
    FAST_BRAIN_MOOD_CLASSIFICATION_ENABLED: bool = False
    FAST_BRAIN_MOOD_CLASSIFICATION_MODEL: str = "gpt-5.4-mini@openai"
    IMPL: str = "real"
    COMMS_URL: str = Field(default="", validation_alias="UNITY_COMMS_URL")
    ADAPTERS_URL: str = Field(default="", validation_alias="UNITY_ADAPTERS_URL")
    JOB_NAME: str = ""
    CONTACT_ID: str = "1"
    BLACKLIST_CHECKS_ENABLED: bool = False
    ASSISTANT_SESSION_GROUP: str = "infra.unify.ai"
    ASSISTANT_SESSION_VERSION: str = "v1alpha1"
    ASSISTANT_SESSION_PLURAL: str = "assistantsessions"
    ASSISTANT_SESSION_PROTOCOL_VERSION: str = "v1"
    ASSIGNMENT_POLL_INTERVAL: float = 0.5
    LOCAL_COMMS_ENABLED: bool = False
    LOCAL_COMMS_MODE: str = "hosted"
    LOCAL_COMMS_HOST: str = "127.0.0.1"
    LOCAL_COMMS_PORT: int = 8787
    LOCAL_COMMS_PUBLIC_URL: str = ""
    LOCAL_COMMS_PUBLIC_URL_FILE: str = "/runtime/call-tunnel-url"
    LOCAL_EMAIL_POLL_INTERVAL: float = 15.0
    INGRESS_TRANSPORT: str = ""
    OUTBOUND_TRANSPORT: str = ""

    model_config = SettingsConfigDict(
        env_prefix="UNITY_CONVERSATION_",
        case_sensitive=True,
        extra="ignore",
    )


def local_comms_public_url(settings: ConversationSettings) -> str:
    """Resolve the current externally reachable local comms URL.

    A non-empty URL from the configured runtime file takes precedence. Missing,
    unreadable, or empty files fall back to the environment-backed setting.
    """
    path = settings.LOCAL_COMMS_PUBLIC_URL_FILE.strip()
    if path:
        try:
            public_url = Path(path).read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError):
            public_url = ""
        if public_url:
            return public_url.rstrip("/")
    return settings.LOCAL_COMMS_PUBLIC_URL.strip().rstrip("/")


def local_comms_listener_url(settings: ConversationSettings) -> str:
    """Resolve the internal URL of the local comms listener."""
    return f"http://{settings.LOCAL_COMMS_HOST}:{settings.LOCAL_COMMS_PORT}"


def local_comms_callback_base_url(settings: ConversationSettings) -> str:
    """Resolve the public callback URL or its local listener fallback."""
    public_url = local_comms_public_url(settings)
    if public_url:
        return public_url
    return local_comms_listener_url(settings)
