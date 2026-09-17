import json
import uuid
from collections.abc import Mapping as _Mapping
from typing import Any, ClassVar
from datetime import datetime
from dataclasses import dataclass, asdict, field

from pydantic import BaseModel

from unify.common.context_registry import ContextRegistry
from unify.common.prompt_helpers import now as prompt_now
from unify.task_scheduler.types.run_source import RunSource


def _coerce_int(value: Any) -> int | None:
    """Best-effort integer coercion for untyped JSON-derived task payloads.

    Returns ``None`` for empty values or anything that won't parse cleanly
    as an int.
    """

    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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
class TaskDue(Event):
    """A scheduled task activation became due.

    The in-process activation scheduler publishes this event from an asyncio
    timer. The activation identity fields let the slow brain reject stale
    deliveries before executing the task, and the packet carries compact
    human-facing wake context so the brain does not have to infer meaning
    from a bare task id alone.
    """

    topic: ClassVar[str | None] = "app:comms:task_due"

    task_id: int
    source_task_log_id: int
    revision: str
    scheduled_for: str
    destination: str | None = None
    wake: str = RunSource.scheduled.value
    task_label: str = ""
    task_summary: str = ""
    visibility_policy: str = "silent_by_default"
    recurrence_hint: str = "one_off"
    reason: str = ""

    @classmethod
    def from_dict(
        cls,
        payload: Any,
        *,
        reason: str = "",
    ) -> "TaskDue | None":
        """Build a `TaskDue` from a dict-shaped payload, or return ``None``.

        The activation scheduler converts a projected
        `TaskExecutionSnapshot` into a dict before calling this method.
        Returns ``None`` when any required identity field (``task_id``,
        ``source_task_log_id``, ``revision``, ``scheduled_for``) is missing
        or malformed; the caller decides how to log / drop the event.
        """

        if not isinstance(payload, _Mapping):
            return None
        task_id = _coerce_int(payload.get("task_id"))
        source_task_log_id = _coerce_int(payload.get("source_task_log_id"))
        revision = str(payload.get("revision") or "")
        scheduled_for = str(payload.get("scheduled_for") or "")
        wake = str(payload.get("wake") or RunSource.scheduled.value)
        if task_id is None or source_task_log_id is None:
            return None
        if not revision:
            return None
        if wake == RunSource.scheduled.value and not scheduled_for:
            return None
        try:
            destination = ContextRegistry.canonical_destination(
                payload.get("destination"),
            )
        except ValueError:
            return None
        task_label = str(payload.get("task_label") or "")
        resolved_reason = reason or (
            f"Scheduled task '{task_label}' became due."
            if task_label
            else f"Scheduled task {task_id} became due."
        )
        return cls(
            task_id=task_id,
            source_task_log_id=source_task_log_id,
            revision=revision,
            scheduled_for=scheduled_for,
            destination=destination,
            wake=wake,
            task_label=task_label,
            task_summary=str(payload.get("task_summary") or ""),
            visibility_policy=str(
                payload.get("visibility_policy") or "silent_by_default",
            ),
            recurrence_hint=str(payload.get("recurrence_hint") or "one_off"),
            reason=resolved_reason,
        )


@dataclass
class TaskTriggerRequested(Event):
    """A request to start a task immediately, outside its schedule."""

    topic: ClassVar[str | None] = "app:comms:task_trigger"

    task_id: int
    source_task_log_id: int | None = None
    destination: str | None = None
    source_ref: str = ""
    task_label: str = ""
    task_summary: str = ""
    reason: str = ""

    @classmethod
    def from_dict(
        cls,
        payload: Any,
        *,
        reason: str = "",
    ) -> "TaskTriggerRequested | None":
        """Build a task-trigger event from a dict-shaped payload."""

        if not isinstance(payload, _Mapping):
            return None
        task_id = _coerce_int(payload.get("task_id"))
        if task_id is None:
            return None
        try:
            destination = ContextRegistry.canonical_destination(
                payload.get("destination"),
            )
        except ValueError:
            return None
        task_label = str(payload.get("task_label") or "")
        resolved_reason = reason or (
            f"Task '{task_label}' was triggered."
            if task_label
            else f"Task {task_id} was triggered."
        )
        return cls(
            task_id=task_id,
            source_task_log_id=_coerce_int(payload.get("source_task_log_id")),
            destination=destination,
            source_ref=str(payload.get("source_ref") or ""),
            task_label=task_label,
            task_summary=str(payload.get("task_summary") or ""),
            reason=resolved_reason,
        )


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
