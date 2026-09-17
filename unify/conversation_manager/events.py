import json
import uuid
from typing import Any, ClassVar
from datetime import datetime
from dataclasses import dataclass, asdict, field

from pydantic import BaseModel

from unify.common.prompt_helpers import now as prompt_now


def custom_dict_factory(kv):
    d = {}
    for k, v in kv:
        if isinstance(v, datetime):
            d[k] = v.isoformat()
        elif isinstance(v, BaseModel):
            d[k] = v.model_dump()
        else:
            d[k] = v
    return d


class _TruncatedReprMixin:
    """Mixin for events that need truncated repr (to avoid logging huge payloads)."""

    def __str__(self) -> str:
        return self._repr_truncated()

    def __repr__(self) -> str:
        return self._repr_truncated()

    def _repr_truncated(self) -> str:
        raise NotImplementedError


def _now_datetime() -> datetime:
    """Wrapper for prompt_now that returns datetime for dataclass default_factory."""
    return prompt_now(as_string=False)


@dataclass(kw_only=True)
class Event:
    timestamp: datetime = field(default_factory=_now_datetime)
    suppress_slow_brain_wake: bool = False

    _registry: ClassVar[dict[str, "Event"]] = {}
    loggable: ClassVar[bool] = True
    content_logged: ClassVar[bool] = False
    prominent: ClassVar[bool] = False
    topic: ClassVar[str | None] = None

    def to_json(self):
        return json.dumps(self.to_dict())

    def to_dict(self):
        return {
            "event_name": self.__class__.__name__,
            "payload": asdict(self, dict_factory=custom_dict_factory),
        }

    def to_bus_event(self):
        from unify.events.event_bus import Event as BusEvent

        payload = self.to_dict()["payload"]
        return BusEvent(
            calling_id="",
            type="Comms",
            timestamp=self.timestamp.isoformat(),
            payload=payload,
            payload_cls=self.__class__.__name__,
        )

    @classmethod
    def from_dict(cls, data) -> "Event":
        import dataclasses

        target_cls = cls._registry.get(data["event_name"])
        if not target_cls:
            raise Exception(f"Class {data['event_name']} is not registered.")
        kwargs = data["payload"].copy()
        timestamp = kwargs.pop("timestamp")

        # Filter to only fields the target dataclass accepts
        valid_fields = {f.name for f in dataclasses.fields(target_cls)}
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in valid_fields}

        return target_cls(
            **filtered_kwargs,
            timestamp=datetime.fromisoformat(timestamp),
        )

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls.from_dict(data)

    @classmethod
    def from_bus_event(cls, event):
        # Use mode="json" to ensure datetime objects are serialized to ISO strings,
        # which from_dict() expects for the timestamp field
        event_dump = event.model_dump(mode="json")
        data = {
            "event_name": event_dump["payload_cls"],
            "payload": event_dump["payload"],
        }
        return cls.from_dict(data)

    def __init_subclass__(cls):
        if cls.__name__ not in Event._registry:
            Event._registry[cls.__name__] = cls
        return cls


@dataclass
class UnifyMessageReceived(Event):
    """A message was received via the in-app chat.

    Each attachment is a dict with keys: filename, filepath, content_type,
    size_bytes. ``filepath`` is the local path the file was saved to and can
    be accessed via FileManager.
    """

    topic: ClassVar[str | None] = "app:comms:unify_message_message"
    content_logged: ClassVar[bool] = True

    contact: dict
    content: str
    attachments: list[dict] = field(default_factory=list)


@dataclass
class UnifyMessageSent(Event):
    """A message was sent via the in-app chat.

    Each attachment is a dict with keys: filename, filepath, content_type,
    size_bytes.
    """

    topic: ClassVar[str | None] = "app:comms:unify_message_sent"
    content_logged: ClassVar[bool] = True

    contact: dict
    content: str
    attachments: list[dict] = field(default_factory=list)


@dataclass
class ActionStopRequested(Event):
    """The user requested stop of an in-flight act by calling_id."""

    topic: ClassVar[str | None] = "app:comms:action_stop"

    calling_id: str
    reason: str = ""
    source: str = ""


@dataclass
class InitializationComplete(Event):
    """Published when ConversationManager has fully initialized all managers."""

    loggable: ClassVar[bool] = False


@dataclass
class OpenSlowBrainTurn(Event):
    """Follow-on slow-brain turn because the prior turn did not call wait."""

    loggable: ClassVar[bool] = False
    origin_run_id: str = ""
    previous_tools: list[str] = field(default_factory=list)


@dataclass
class Ping(Event):
    loggable: ClassVar[bool] = False
    kind: str


@dataclass
class Error(Event):
    prominent: ClassVar[bool] = True

    message: str


@dataclass
class LogMessageResponse(Event):
    medium: str
    exchange_id: int
    # Root the exchange was authored under. Exchange ids are root-local, so a
    # consumer that caches the id for a later write needs this alongside it.
    destination: str | None = None


@dataclass
class ContactInfoResponse(Event):
    contact_details: dict[str, Any]


@dataclass(repr=False)
class StoreChatHistory(_TruncatedReprMixin, Event):
    chat_history: list[dict]

    def _repr_truncated(self) -> str:
        return f"{self.__class__.__name__}(chat_history_len={len(self.chat_history)})"


@dataclass(repr=False)
class GetChatHistory(_TruncatedReprMixin, Event):
    loggable: ClassVar[bool] = False
    chat_history: list[dict]

    def _repr_truncated(self) -> str:
        return f"{self.__class__.__name__}(chat_history_len={len(self.chat_history)})"


@dataclass(repr=False)
class GetBusEventsResponse(_TruncatedReprMixin, Event):
    loggable: ClassVar[bool] = False
    events: list[dict[str, Any]]

    def _repr_truncated(self) -> str:
        return f"{self.__class__.__name__}(events_len={len(self.events)})"


# --------------------------------------------------------------------------- #
# LLM inference events
# --------------------------------------------------------------------------- #
@dataclass
class LLMInput(Event):
    chat_history: list[dict]


@dataclass
class UpdateContactRollingSummaryResponse(Event):
    rolling_summaries: list[tuple[int, str]]


@dataclass
class NotificationInjectedEvent(Event):
    """Event to inject a notification into the ConversationManager."""

    content: str
    source: str
    target_conversation_id: str
    interjection_id: str = field(default_factory=lambda: str(uuid.uuid4().hex[:12]))
    pinned: bool = False


@dataclass
class NotificationUnpinnedEvent(Event):
    """Event to unpin a previously pinned interjection."""

    interjection_id: str
    target_conversation_id: str


@dataclass(repr=False)
class ActorRequest(_TruncatedReprMixin, Event):
    """Event to ask or request the Actor to perform a task."""

    action_name: str
    query: str
    parent_chat_context: list[dict]

    def _repr_truncated(self) -> str:
        return (
            f"{self.__class__.__name__}(action_name={self.action_name}, "
            f"query={self.query}, "
            f"parent_chat_context_len={len(self.parent_chat_context)})"
        )


@dataclass
class ActorResponse(Event):
    """Event to respond to an Actor request."""

    handle_id: int
    action_name: str
    query: str
    response: str


@dataclass(repr=False)
class ActorHandleRequest(_TruncatedReprMixin, Event):
    """Event to any action on an existing Actor handle."""

    handle_id: int
    action_name: str
    query: str
    parent_chat_context: list[dict]

    def _repr_truncated(self) -> str:
        return (
            f"{self.__class__.__name__}(handle_id={self.handle_id}, "
            f"action_name={self.action_name}, "
            f"query={self.query}, "
            f"parent_chat_context_len={len(self.parent_chat_context)})"
        )


@dataclass
class ActorHandleResponse(Event):
    """Event to respond to an Actor handle request."""

    handle_id: int
    action_name: str
    query: str
    response: str
    call_id: str


@dataclass
class ActorResult(Event):
    """Event to the result of an Actor task."""

    handle_id: int
    success: bool
    result: dict | str | None = None
    error: str | None = None
    action_type: str = ""


@dataclass
class ActorClarificationRequest(Event):
    """Event to request clarification from the Actor."""

    handle_id: int
    query: str
    call_id: str


@dataclass
class ActorClarificationResponse(Event):
    """Event to respond to an Actor clarification request."""

    handle_id: int
    response: str
    call_id: str


@dataclass
class ActorNotification(Event):
    """Event to forward a notification from an Actor handle.

    Notifications carry status updates emitted explicitly by the actor via
    ``send_notification``.  The ``completed`` flag
    distinguishes a completion announcement from an in-progress update so
    downstream consumers can render them with unambiguous prefixes.
    """

    handle_id: int
    response: str
    completed: bool = False
    kind: str = ""


@dataclass
class ActorSessionResponse(Event):
    """Event signalling that a persistent actor session has completed a turn.

    Unlike ``ActorNotification``, a session response means the actor has
    finished its current work and is **waiting for the next instruction**
    (via ``interject``).  The ``content`` field carries the actor's output
    for this turn.
    """

    handle_id: int
    content: str


@dataclass
class ActorHandleStarted(Event):
    action_name: str
    handle_id: id
    query: str
    response_format: dict | None = None


@dataclass
class SyncContacts(Event):
    """Signal to re-sync the system contacts (assistant and user)."""

    reason: str = ""


@dataclass
class BackupContactsEvent(Event):
    """
    Fallback contacts from inbound messages for use before ContactManager initializes.

    When an inbound message arrives before the ContactManager is ready, this event
    carries the contacts list so they can be cached locally in ContactIndex. Once
    ContactManager is initialized, this local cache is cleared and all contact
    lookups go through ContactManager.
    """

    loggable: ClassVar[bool] = False
    contacts: list[dict[str, Any]]


@dataclass
class LLMUserMessage(Event):
    content: str


@dataclass
class LLMAssistantMessage(Event):
    content: str


@dataclass
class SummarizeContext(Event):
    pass


@dataclass
class DirectMessageEvent(Event):
    """
    Send a message directly to the user via the current medium,
    bypassing the Main CM Brain's decision-making.

    Used by ConversationManagerHandle.ask for questions and acknowledgments
    that should be delivered verbatim without LLM processing.
    """

    content: str
    source: str = "system"
