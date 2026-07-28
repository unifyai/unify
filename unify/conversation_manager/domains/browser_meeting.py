"""The seam between the call manager and whatever joins a browser meeting.

Two backends satisfy this contract:

* ``agent_service`` -- the pod-local Playwright browser, driven through
  agent-service, with a PulseAudio bridge to the fast brain.
* ``recall`` -- a hosted Recall.ai bot rendering our bridge page, which
  reaches the fast brain over LiveKit like every other voice channel.

Only meeting-backend operations live behind the seam. Room creation, fast-brain
dispatch, IPC notification and speaker bookkeeping are identical either way and
stay in the call manager.

The provider is chosen per pod by ``MEET_PROVIDER``, so a cutover and a
rollback are the same one-value change rather than a deploy.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol

LOGGER = logging.getLogger(__name__)

AGENT_SERVICE_PROVIDER = "agent_service"
RECALL_PROVIDER = "recall"

# Absent or unrecognised means the browser we already run. A typo must not
# silently move customer meetings onto a different backend.
DEFAULT_MEET_PROVIDER = AGENT_SERVICE_PROVIDER


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

    async def present(self, *, channel: str, session_id: str, view_url: str) -> bool:
        """Begin sharing ``view_url`` with the meeting."""

    async def stop_present(self, *, channel: str, session_id: str) -> bool:
        """Stop sharing."""


def configured_meet_provider() -> str:
    """The provider name this pod is configured to use."""

    name = (os.environ.get("MEET_PROVIDER") or "").strip().lower()
    if name in (AGENT_SERVICE_PROVIDER, RECALL_PROVIDER):
        return name
    return DEFAULT_MEET_PROVIDER


def build_meet_provider(call_manager: Any) -> MeetProvider:
    """Construct the backend this pod is configured for.

    A pod asking for ``recall`` without credentials falls back to the local
    browser rather than refusing to join: during the transition there is still
    something to fall back to, and a loud log line is more useful than a
    meeting nobody attends. Once Recall is the only backend there will be
    nothing to fall back to and this should fail instead.

    Imports are function-local because both backends import this module.
    """

    # Imported here rather than at module scope: agent_service_meeting and
    # recall.provider both import the types above, so a top-level import would
    # be circular.
    from unify.conversation_manager.domains.agent_service_meeting import (
        AgentServiceMeetProvider,
    )

    if configured_meet_provider() == RECALL_PROVIDER:
        from unify.conversation_manager.domains.recall.client import recall_configured

        if recall_configured():
            from unify.conversation_manager.domains.recall.provider import (
                RecallMeetProvider,
            )

            LOGGER.info("browser meetings via Recall.ai")
            return RecallMeetProvider(assistant_id=call_manager.assistant_id)

        LOGGER.error(
            "MEET_PROVIDER=recall but RECALL_API_KEY is unset; "
            "falling back to the local browser",
        )

    return AgentServiceMeetProvider(call_manager)
