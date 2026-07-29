"""The seam between the call manager and whatever joins a browser meeting.

One backend satisfies this contract today: a hosted Recall.ai bot rendering our
bridge page, which reaches the fast brain over LiveKit like every other voice
channel. The seam is kept rather than inlined because Recall's own roadmap
(Meeting Direct Connect, once Google's Meet Media API leaves preview) is a
second implementation of exactly these operations.

Only meeting-backend operations live behind it. Room creation, fast-brain
dispatch, IPC notification and speaker bookkeeping are backend-independent and
stay in the call manager.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol

LOGGER = logging.getLogger(__name__)

RECALL_PROVIDER = "recall"


@dataclass(frozen=True)
class MeetJoinResult:
    """Outcome of asking a backend to join one meeting.

    ``lobby`` distinguishes the two successful shapes: in the meeting, versus
    admitted-pending in the waiting room. Both mean the join worked and only
    the wording downstream differs.
    """

    ok: bool
    session_id: str | None = None
    lobby: bool = False
    failure_reason: str | None = None


@dataclass(frozen=True)
class MeetParticipantView:
    """One participant, as the backend can see them."""

    id: str
    name: str
    email: str | None = None
    is_host: bool = False


@dataclass(frozen=True)
class MeetState:
    """Observed state of a joined meeting."""

    status: str
    ended: bool = False
    lobby: bool = False
    participants: tuple[MeetParticipantView, ...] = ()
    active_speaker: str | None = None
    failure_reason: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)


class MeetProvider(Protocol):
    """One backend that can put the assistant into a browser meeting."""

    name: str

    async def preflight(self) -> str | None:
        """Return a failure reason when this backend cannot accept a join.

        Called before the fast brain is dispatched, so a backend that is known
        to be unavailable costs nothing: dispatching a voice worker into a room
        no browser will ever reach leaves it talking to itself until it times
        out. Returns None when the backend is ready.
        """

    async def join(
        self,
        *,
        channel: str,
        meeting_url: str,
        display_name: str,
        room_name: str,
    ) -> MeetJoinResult:
        """Put the assistant into the meeting and report the outcome."""

    async def leave(self, *, channel: str, session_id: str) -> None:
        """Leave the meeting. Must reach its end state even on failure."""

    async def state(self, *, channel: str, session_id: str) -> MeetState | None:
        """Current meeting state, or None when the session is unknown."""

    async def send_chat(
        self,
        *,
        channel: str,
        session_id: str,
        text: str,
        to: str | None = None,
    ) -> bool:
        """Post into the meeting chat, optionally to one participant.

        Returns False rather than raising when the platform will not carry it:
        Teams channel meetings have no bot-writable chat at all, so a refusal is
        an ordinary outcome the assistant should be told about, not a fault.
        """


def build_meet_provider(call_manager: Any) -> MeetProvider:
    """Construct the backend that joins browser meetings.

    Recall is the only backend. There is deliberately no fallback: the local
    Playwright path it replaced is gone, so an unconfigured pod cannot join a
    meeting by any route and should say so at construction rather than fail
    somewhere less legible mid-join.

    The import is function-local because ``recall.provider`` imports the types
    above, so a module-scope import would be circular.
    """

    from unify.conversation_manager.domains.recall.provider import RecallMeetProvider

    return RecallMeetProvider(assistant_id=call_manager.assistant_id)
