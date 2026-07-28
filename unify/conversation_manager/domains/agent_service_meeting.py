"""Browser-meeting backend that drives the Playwright browser in this pod.

This is the behaviour that predates the seam. The HTTP calls stay on the call
manager rather than moving here, for two reasons: the point of the seam is to
make the Recall path selectable and reversible, not to rewrite a working join
flow at the same time; and the whole path is deleted once Recall is the only
backend, so anything moved would only be moved to be thrown away.

This module and the ``_agent_service_*`` methods it calls go away together.
"""

from __future__ import annotations

from typing import Any

from unify.conversation_manager.domains.browser_meeting import (
    AGENT_SERVICE_PROVIDER,
    MeetJoinResult,
    MeetState,
)


class AgentServiceMeetProvider:
    """Join Google Meet / Teams with the browser running in this pod."""

    name = AGENT_SERVICE_PROVIDER

    def __init__(self, call_manager: Any) -> None:
        self._cm = call_manager

    async def preflight(self) -> str | None:
        return await self._cm._agent_service_preflight()

    async def join(
        self,
        *,
        channel: str,
        meeting_url: str,
        display_name: str,
        room_name: str,
    ) -> MeetJoinResult:
        # The browser reaches LiveKit through pod-local PulseAudio rather than
        # by joining the room itself, so it has no use for the room name.
        _ = room_name
        return await self._cm._agent_service_join(
            channel=channel,
            meet_url=meeting_url,
            display_name=display_name,
        )

    async def leave(self, *, channel: str, session_id: str) -> None:
        await self._cm._agent_service_leave(channel=channel, session_id=session_id)

    async def state(self, *, channel: str, session_id: str) -> MeetState | None:
        raw = await self._cm._agent_service_state(
            channel=channel,
            session_id=session_id,
        )
        if raw is None:
            return None
        status = str(raw.get("status") or "")
        return MeetState(
            status=status,
            # agent-service knows only these two live states; anything else
            # (including a session it has forgotten) means the meeting is over.
            ended=status not in ("active", "lobby"),
            lobby=status == "lobby",
            active_speaker=raw.get("activeSpeaker") or None,
            raw=raw,
        )

    async def present(self, *, channel: str, session_id: str, view_url: str) -> bool:
        return await self._cm._agent_service_present(
            channel=channel,
            session_id=session_id,
            view_url=view_url,
        )

    async def stop_present(self, *, channel: str, session_id: str) -> bool:
        return await self._cm._agent_service_stop_present(
            channel=channel,
            session_id=session_id,
        )
