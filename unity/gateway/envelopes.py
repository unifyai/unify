"""Canonical inbound webhook envelope schemas.

Every inbound event that crosses the gateway -> Unity boundary is wrapped
in a ``{thread, publish_timestamp, event}`` envelope. The ``thread``
string identifies the kind of event (and therefore the shape of
``event``); ``publish_timestamp`` is the float Unix time when the
gateway-side publisher accepted the event.

Today the on-wire shape is established by:

* the gateway side -- ``communication/{phone,gmail,outlook,whatsapp,
  discord,teams,...}/views.py`` plus ``adapters/main.py``
* the assistant-side ingress -- ``unity.conversation_manager.local_ingress``
  (single-process self-hosted) and the Pub/Sub consumer inside
  ``unity.conversation_manager.comms_manager.CommsManager`` (hosted)

This module makes that contract explicit and machine-checkable. It is
deliberately Pydantic-based so envelopes can be validated at both the
publisher and consumer boundaries without bespoke parsing.

Coverage status
===============

Phase A (this commit) ships:

* the base envelope and base event shapes
* concrete models for four representative ``thread`` values --
  ``msg`` (SMS), ``email``, ``unify_message``, ``unity_system_event`` --
  chosen to cover the SMS, mail, app-message, and system-event patterns
* a ``GenericEnvelope`` escape hatch for every ``thread`` value not yet
  modelled, and a ``parse_envelope`` dispatcher that falls back to it

The full ``thread`` catalogue, recovered from the current code paths,
is documented inline below. Each remaining thread value gets a concrete
model when the corresponding channel router is migrated in Phase B.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, ValidationError

# ---------------------------------------------------------------------------
# Thread catalogue
# ---------------------------------------------------------------------------
#
# Every value below appears as a ``thread`` string in the current code paths.
# Sources cited at the time of authoring (Phase A):
#
# From unity/conversation_manager/local_ingress.py (single-process ingress):
#   msg, whatsapp, email, unify_message, api_message, unify_meet,
#   unity_system_event, log_pre_hire_chats, call, call_answered,
#   call_not_answered, whatsapp_call, whatsapp_call_answered,
#   whatsapp_call_not_answered
#
# From communication/discord/gateway.py: discord
# From communication/teams/views.py:    teams_chat, teams_channel
#
# A concrete schema for each is added when its channel router is migrated
# into unity/gateway/channels/ in Phase B.

KNOWN_THREADS: frozenset[str] = frozenset(
    {
        "msg",
        "whatsapp",
        "email",
        "unify_message",
        "api_message",
        "unify_meet",
        "unity_system_event",
        "log_pre_hire_chats",
        "call",
        "call_answered",
        "call_not_answered",
        "whatsapp_call",
        "whatsapp_call_answered",
        "whatsapp_call_not_answered",
        "discord",
        "teams_chat",
        "teams_channel",
    },
)


# ---------------------------------------------------------------------------
# Base shapes
# ---------------------------------------------------------------------------


class BaseInboundEvent(BaseModel):
    """Common fields on every inbound ``event`` payload."""

    model_config = ConfigDict(extra="allow")

    assistant_id: str = ""
    contacts: list[dict[str, Any]] = Field(default_factory=list)


class BaseEnvelope(BaseModel):
    """Common fields on every inbound envelope."""

    model_config = ConfigDict(extra="forbid")

    publish_timestamp: float
    thread: str
    event: BaseInboundEvent


# ---------------------------------------------------------------------------
# Concrete thread-specific schemas (representative subset for Phase A)
# ---------------------------------------------------------------------------


class SMSReceivedEvent(BaseInboundEvent):
    """Inbound SMS via Twilio (``thread: "msg"``)."""

    to_number: str = ""
    from_number: str = ""
    body: str = ""


class SMSEnvelope(BaseEnvelope):
    thread: Literal["msg"] = "msg"
    event: SMSReceivedEvent


class EmailAttachment(BaseModel):
    """An attachment carried inline on an inbound email envelope."""

    model_config = ConfigDict(extra="allow")

    id: str = ""
    filename: str = ""
    content_type: str = "application/octet-stream"
    size_bytes: int = 0
    content_base64: str | None = None


class EmailReceivedEvent(BaseInboundEvent):
    """Inbound email (``thread: "email"``)."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    from_: str = Field(default="", alias="from")
    subject: str = ""
    body: str = ""
    email_id: str = ""
    to: list[str] = Field(default_factory=list)
    cc: list[str] = Field(default_factory=list)
    bcc: list[str] = Field(default_factory=list)
    attachments: list[EmailAttachment] = Field(default_factory=list)


class EmailEnvelope(BaseEnvelope):
    thread: Literal["email"] = "email"
    event: EmailReceivedEvent


class UnifyMessageReceivedEvent(BaseInboundEvent):
    """Inbound app-to-assistant message (``thread: "unify_message"``)."""

    contact_id: int
    body: str = ""
    attachments: list[dict[str, Any]] = Field(default_factory=list)


class UnifyMessageEnvelope(BaseEnvelope):
    thread: Literal["unify_message"] = "unify_message"
    event: UnifyMessageReceivedEvent


class UnitySystemEvent(BaseInboundEvent):
    """Internal system signal (``thread: "unity_system_event"``).

    Carries scheduled-task hooks, desktop URLs, and binding lifecycle
    information that the hosted control plane needs to forward to a
    Unity session worker.
    """

    event_type: str
    message: str = ""
    task_id: int | None = None
    source_task_log_id: int | None = None
    activation_revision: str = ""
    scheduled_for: str = ""
    execution_mode: str = ""
    source_type: str = ""
    binding_id: str = ""
    desktop_url: str = ""
    vm_type: str = ""


class SystemEventEnvelope(BaseEnvelope):
    thread: Literal["unity_system_event"] = "unity_system_event"
    event: UnitySystemEvent


# ---------------------------------------------------------------------------
# Generic fallback + discriminated union
# ---------------------------------------------------------------------------


class GenericEnvelope(BaseEnvelope):
    """Fallback envelope for ``thread`` values not yet modelled concretely.

    Validates the envelope shape but accepts any ``event`` payload. Used by
    ``parse_envelope`` when the incoming ``thread`` is unknown to the
    concrete catalogue. Channel-migration PRs in Phase B replace fallback
    uses with concrete models per ``thread``.
    """

    thread: str
    event: BaseInboundEvent


Envelope = Annotated[
    Union[
        SMSEnvelope,
        EmailEnvelope,
        UnifyMessageEnvelope,
        SystemEventEnvelope,
    ],
    Field(discriminator="thread"),
]


def parse_envelope(payload: dict[str, Any]) -> BaseEnvelope:
    """Parse a raw envelope dict into the most-specific known model.

    Falls back to ``GenericEnvelope`` for ``thread`` values not yet
    covered by a concrete schema, so that unmigrated channels keep
    flowing through the seam unchanged.

    Raises ``pydantic.ValidationError`` only when the *envelope* shape
    itself (``thread``, ``publish_timestamp``, ``event``) is malformed.
    """

    thread = payload.get("thread")
    if thread == "msg":
        return SMSEnvelope.model_validate(payload)
    if thread == "email":
        return EmailEnvelope.model_validate(payload)
    if thread == "unify_message":
        return UnifyMessageEnvelope.model_validate(payload)
    if thread == "unity_system_event":
        return SystemEventEnvelope.model_validate(payload)
    return GenericEnvelope.model_validate(payload)


__all__ = [
    "BaseEnvelope",
    "BaseInboundEvent",
    "EmailAttachment",
    "EmailEnvelope",
    "EmailReceivedEvent",
    "Envelope",
    "GenericEnvelope",
    "KNOWN_THREADS",
    "SMSEnvelope",
    "SMSReceivedEvent",
    "SystemEventEnvelope",
    "UnifyMessageEnvelope",
    "UnifyMessageReceivedEvent",
    "UnitySystemEvent",
    "ValidationError",
    "parse_envelope",
]
