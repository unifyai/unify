"""Recall's wire shapes: one participant, and one realtime event frame.

Two sources describe the same participant. The REST bot payload
(``meeting_participants``) is what the roster poll reads; the realtime websocket
sends a frame per in-call event, relayed into the assistant's LiveKit room. Both
are parsed here so the two can never disagree about what a participant is, and
so the frame nesting lives in exactly one place.

That nesting is the whole reason this module exists. A realtime frame wraps its
payload twice -- the participant is at ``data.data.participant`` -- and a chat
message wraps its body a third time, at ``data.data.data.text``. Reading a level
short yields an empty participant and empty text, which downstream is
indistinguishable from a relay that is down: no speaker attribution, no inbound
chat, and nothing in the logs to say so.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

# Recall's realtime participant events. ``chat_message`` is the only one that
# carries a body of its own; for the rest the innermost ``data`` is null.
EVENT_JOIN = "participant_events.join"
EVENT_LEAVE = "participant_events.leave"
EVENT_UPDATE = "participant_events.update"
EVENT_SPEECH_ON = "participant_events.speech_on"
EVENT_SPEECH_OFF = "participant_events.speech_off"
EVENT_CHAT_MESSAGE = "participant_events.chat_message"
EVENT_SCREENSHARE_ON = "participant_events.screenshare_on"
EVENT_SCREENSHARE_OFF = "participant_events.screenshare_off"

# Recall's per-participant video frames. Screenshare and webcam frames arrive
# interleaved on this one event, told apart only by ``type`` -- there is no
# screenshare-specific stream -- so a consumer that does not check it will
# treat somebody's face as their shared screen.
EVENT_VIDEO_FRAME = "video_separate_png.data"
VIDEO_FRAME_TYPE_SCREENSHARE = "screenshare"

# Events that change who is in the meeting. ``update`` is here because a rename
# arrives as an update against an id already on the roster.
ROSTER_EVENTS = frozenset({EVENT_JOIN, EVENT_LEAVE, EVENT_UPDATE})

# Events that change who is presenting. Frames are useless without these: they
# are what says a share has ended, and a frame slot left unattended keeps
# showing the last thing somebody shared for the rest of the call.
SCREENSHARE_EVENTS = frozenset({EVENT_SCREENSHARE_ON, EVENT_SCREENSHARE_OFF})

# What a bot subscribes to at creation, in the order Recall is told them.
#
# MIRRORED: ``_RELAYED_EVENTS`` in unity-deploy's ``communication/meet_events.py``
# drops anything absent from its own copy, so an event added here alone is
# subscribed and then silently discarded in transit. Change both together.
SUBSCRIBED_EVENTS = (
    EVENT_JOIN,
    EVENT_LEAVE,
    EVENT_UPDATE,
    EVENT_SPEECH_ON,
    EVENT_SPEECH_OFF,
    EVENT_CHAT_MESSAGE,
    EVENT_SCREENSHARE_ON,
    EVENT_SCREENSHARE_OFF,
)


@dataclass(frozen=True)
class RecallParticipant:
    """One meeting participant as the platform reports it.

    This is the payload that makes Recall worth adopting for attribution: the
    platform's own name and email for a speaker, rather than a label scraped
    out of the meeting UI.

    ``email`` is frequently absent. Platforms disclose it to a bot created
    through the Create Bot API only sometimes, so a null address is the normal
    case rather than a fault, and nothing may depend on having one.
    """

    id: str
    name: str
    email: str | None = None
    is_host: bool = False
    platform: str | None = None


@dataclass(frozen=True)
class RelayedEvent:
    """One realtime frame, unwrapped.

    ``chat_text`` is set only for a chat message. ``participant`` is None when
    the frame carried no identifiable participant, which is not worth acting on
    for any event we subscribe to.
    """

    name: str
    participant: RecallParticipant | None = None
    chat_text: str | None = None
    chat_to: str | None = None


def participant_from_payload(entry: Any) -> RecallParticipant | None:
    """One participant from either the REST roster or a realtime frame.

    ``id`` arrives as an int over the websocket and as a string over REST, so it
    is normalised to a string here: roster upserts match on it, and 7 and "7"
    would otherwise be two different people in the same meeting.
    """

    if not isinstance(entry, Mapping):
        return None
    participant_id = entry.get("id")
    if participant_id is None:
        return None
    email = entry.get("email")
    extra = entry.get("extra_data")
    if not email and isinstance(extra, Mapping):
        email = extra.get("email")
    platform = entry.get("platform")
    return RecallParticipant(
        id=str(participant_id),
        name=str(entry.get("name") or ""),
        email=str(email) if email else None,
        is_host=bool(entry.get("is_host")),
        platform=str(platform) if platform else None,
    )


def parse_relayed_event(message: Any) -> RelayedEvent | None:
    """Unwrap one relayed frame, or None when it carries nothing usable.

    The relay forwards Recall's ``data`` verbatim alongside the event name, so
    what arrives here is ``{"event": ..., "data": {"data": {...}, ...}}`` -- the
    outer object also carrying bot/recording/endpoint artifacts nothing reads.
    """

    if not isinstance(message, Mapping):
        return None
    name = message.get("event")
    if not isinstance(name, str) or not name:
        return None

    outer = message.get("data")
    inner = outer.get("data") if isinstance(outer, Mapping) else None
    if not isinstance(inner, Mapping):
        return None

    body = inner.get("data")
    text = body.get("text") if isinstance(body, Mapping) else None
    to = body.get("to") if isinstance(body, Mapping) else None
    return RelayedEvent(
        name=name,
        participant=participant_from_payload(inner.get("participant")),
        chat_text=str(text) if text else None,
        chat_to=str(to) if to else None,
    )
