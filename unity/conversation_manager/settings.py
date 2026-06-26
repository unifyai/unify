"""
ConversationManager-specific settings.

These settings are composed into the global ProductionSettings.
Environment variables use the prefix UNITY_CONVERSATION_.
"""

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
        PROACTIVE_SPEECH_MODEL: LLM model that decides when and what to say to
            break a silence. Decoupled from the fast brain: this path is not
            latency-critical (it sits behind a debounce and a chosen delay), so
            it runs on a stronger model to reliably honour the no-repeat
            constraint. Override via UNITY_CONVERSATION_PROACTIVE_SPEECH_MODEL.
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
        SPEECH_URGENCY_PREEMPT_ENABLED: Enable the concurrent fast-brain urgency
            evaluator for voice mode. When a user speaks while the slow brain is
            mid-run, a sidecar LLM call classifies the utterance as urgent
            (preempt) or not (let the queue proceed). Default False.
        SPEECH_DEDUP_ENABLED: Enable pre-speak deduplication gate in the fast
            brain subprocess during voice calls. Before playing queued slow brain
            speech, a lightweight LLM check compares the proposed speech against
            recent fast brain utterances and suppresses it when the information
            has already been communicated. Default True.
        INGRESS_TRANSPORT: Selector for the inbound transport
            (``unity.gateway.IngressTransport`` implementation) that
            CommsManager consumes. ``""`` (default) and ``"legacy"`` both
            keep the existing inline ``subscribe_to_topic`` Pub/Sub
            subscriber active. ``"inmemory"`` selects
            ``InMemoryIngressTransport`` (tests / single-process self-hosted
            Unity). ``"pubsub"`` selects ``PubSubIngressTransport`` and is
            the value the hosted deployment will set once Phase C cuts
            over. Override via ``UNITY_CONVERSATION_INGRESS_TRANSPORT``.
        OUTBOUND_TRANSPORT: Selector for the outbound transport
            (``unity.gateway.OutboundTransport`` implementation) that
            the comms_utils publish helpers use. Same value semantics
            as INGRESS_TRANSPORT. Override via
            ``UNITY_CONVERSATION_OUTBOUND_TRANSPORT``.
    """

    FAST_BRAIN_MODEL: str = "gpt-5.4-mini@openai"
    PROACTIVE_SPEECH_MODEL: str = "gpt-5.5@openai"
    FAST_BRAIN_CONTEXT_WINDOW: int = 50
    FAST_BRAIN_MOOD_CLASSIFICATION_ENABLED: bool = False
    FAST_BRAIN_MOOD_CLASSIFICATION_MODEL: str = "gpt-5.5-mini@openai"
    IMPL: str = "real"
    COMMS_URL: str = Field(default="", validation_alias="UNITY_COMMS_URL")
    ADAPTERS_URL: str = Field(default="", validation_alias="UNITY_ADAPTERS_URL")
    JOB_NAME: str = ""
    CONTACT_ID: str = "1"
    BLACKLIST_CHECKS_ENABLED: bool = False
    SPEECH_URGENCY_PREEMPT_ENABLED: bool = True
    SPEECH_DEDUP_ENABLED: bool = True
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
    LOCAL_EMAIL_POLL_INTERVAL: float = 15.0
    INGRESS_TRANSPORT: str = ""
    OUTBOUND_TRANSPORT: str = ""

    model_config = SettingsConfigDict(
        env_prefix="UNITY_CONVERSATION_",
        case_sensitive=True,
        extra="ignore",
    )
