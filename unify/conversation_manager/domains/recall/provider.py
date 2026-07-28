"""Meet backend that dispatches a hosted Recall.ai bot.

The bot renders our bridge page as its media surface. That page is an ordinary
LiveKit browser client, so it needs a room token -- which this provider mints
here, because the pod already holds the LiveKit credentials and the bot can be
handed a credential no other way than in the URL it loads.

The token is deliberately narrow: join rights on one room, no publish of data,
short lifetime. It reaches Recall's browser and nowhere else.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from urllib.parse import urlencode

from livekit.api import AccessToken, VideoGrants

from unify.conversation_manager.domains.browser_meeting import (
    RECALL_PROVIDER,
    MeetJoinResult,
    MeetParticipantView,
    MeetState,
)
from unify.conversation_manager.domains.recall.client import (
    RecallClient,
    RecallError,
    RecallBotState,
)

LOGGER = logging.getLogger(__name__)

# The bridge only has to outlive the join handshake -- once connected, LiveKit
# keeps the session alive independently of the token's remaining life. Long
# enough to absorb a slow bot start and one page reload, short enough that a
# leaked URL is worthless.
_BRIDGE_TOKEN_TTL = timedelta(hours=4)

_BRIDGE_IDENTITY_PREFIX = "recall-bridge"

# Recall's own sub_codes are stable strings, but they are not phrased for a
# person. Map the ones a user can act on; anything else passes through so an
# operator still sees the real reason in logs and stored state.
_FAILURE_REASONS = {
    "bot_denied_entry": "meet_join_denied",
    "meeting_not_started": "meet_not_started",
    "bot_kicked_from_call": "meet_removed",
    "meeting_full": "meet_full",
    "invalid_meeting_url": "meet_url_invalid",
}


class RecallMeetProvider:
    """Join Google Meet / Teams through a hosted Recall bot."""

    name = RECALL_PROVIDER

    def __init__(
        self,
        *,
        client: RecallClient | None = None,
        bridge_page_url: str | None = None,
        relay_url: str | None = None,
        assistant_id: str | int | None = None,
    ) -> None:
        self._client = client or RecallClient()
        self._bridge_page_url = (
            bridge_page_url
            if bridge_page_url is not None
            else (os.environ.get("MEET_BRIDGE_PAGE_URL") or "")
        ).strip()
        self._relay_url = relay_url if relay_url is not None else _default_relay_url()
        self._assistant_id = str(assistant_id) if assistant_id is not None else ""

    async def preflight(self) -> str | None:
        # Nothing to wait for -- Recall is a hosted API, not a process that
        # cold-starts alongside us. Only the local configuration can be wrong,
        # and catching that here saves dispatching a worker into a dead room.
        if not self._bridge_page_url:
            LOGGER.error("[recall] MEET_BRIDGE_PAGE_URL is unset; cannot join")
            return "bridge_page_unconfigured"
        return None

    async def join(
        self,
        *,
        channel: str,
        meeting_url: str,
        display_name: str,
        room_name: str,
    ) -> MeetJoinResult:
        if not self._bridge_page_url:
            LOGGER.error("[recall] MEET_BRIDGE_PAGE_URL is unset; cannot join")
            return MeetJoinResult(ok=False, failure_reason="bridge_page_unconfigured")

        try:
            bridge_url = self._bridge_url(room_name, display_name)
        except RuntimeError as exc:
            LOGGER.error("[recall] could not mint a bridge token: %s", exc)
            return MeetJoinResult(ok=False, failure_reason="livekit_unconfigured")

        try:
            state = await self._client.create_bot(
                meeting_url=meeting_url,
                bot_name=display_name,
                bridge_page_url=bridge_url,
                realtime_events_url=self._relay_url_for(room_name),
                metadata={
                    "unify_channel": channel,
                    "unify_room": room_name,
                    "unify_assistant_id": self._assistant_id,
                },
            )
        except RecallError as exc:
            LOGGER.error("[recall] %s join failed: %s", channel, exc)
            return MeetJoinResult(ok=False, failure_reason="recall_dispatch_failed")

        # A freshly created bot has not reached the call yet, so neither
        # "in the meeting" nor "in the lobby" is knowable here. The join
        # succeeded in the sense that matters -- a bot exists and is on its way
        # -- and the call manager's poll resolves which of the two it becomes.
        LOGGER.info(
            "[recall] %s bot %s dispatched (status=%s)",
            channel,
            state.bot_id,
            state.status,
        )
        return MeetJoinResult(
            ok=True,
            session_id=state.bot_id,
            lobby=state.in_waiting_room,
        )

    async def leave(self, *, channel: str, session_id: str) -> None:
        _ = channel
        await self._client.leave_call(session_id)

    async def state(self, *, channel: str, session_id: str) -> MeetState | None:
        _ = channel
        try:
            bot = await self._client.get_bot(session_id)
        except RecallError as exc:
            LOGGER.warning("[recall] state for %s failed: %s", session_id, exc)
            return None
        return _to_meet_state(bot)

    async def present(self, *, channel: str, session_id: str, view_url: str) -> bool:
        # The bridge page is already the bot's media surface, so presenting is
        # a mode change on that surface rather than a second stream: the page
        # renders the desktop view and Recall promotes it to a screenshare.
        _ = channel, view_url
        try:
            await self._client.start_screenshare(session_id)
            return True
        except RecallError as exc:
            LOGGER.warning("[recall] present failed for %s: %s", session_id, exc)
            return False

    async def stop_present(self, *, channel: str, session_id: str) -> bool:
        _ = channel
        try:
            await self._client.stop_screenshare(session_id)
            return True
        except RecallError as exc:
            LOGGER.warning("[recall] stop-present failed for %s: %s", session_id, exc)
            return False

    async def send_chat(
        self,
        *,
        session_id: str,
        text: str,
        to: str | None = None,
    ) -> bool:
        """Post into the meeting chat.

        Teams channel meetings do not support this at all, so a refusal is an
        expected outcome rather than a fault.
        """

        try:
            await self._client.send_chat_message(session_id, text, to=to)
            return True
        except RecallError as exc:
            LOGGER.warning("[recall] chat send failed for %s: %s", session_id, exc)
            return False

    def _bridge_url(self, room_name: str, display_name: str) -> str:
        server_url = (os.environ.get("LIVEKIT_URL") or "").strip()
        api_key = (os.environ.get("LIVEKIT_API_KEY") or "").strip()
        api_secret = (os.environ.get("LIVEKIT_API_SECRET") or "").strip()
        if not server_url or not api_key or not api_secret:
            raise RuntimeError("LIVEKIT_URL / _API_KEY / _API_SECRET must all be set")

        token = (
            AccessToken(api_key, api_secret)
            .with_identity(f"{_BRIDGE_IDENTITY_PREFIX}-{room_name}")
            .with_name(display_name or "Unify")
            .with_grants(
                # Publish carries the meeting audio in; subscribe carries the
                # assistant's voice out. Nothing else is granted: the bridge
                # never administers the room and never sends data messages.
                VideoGrants(
                    room_join=True,
                    room=room_name,
                    can_publish=True,
                    can_subscribe=True,
                    can_publish_data=False,
                ),
            )
            .with_ttl(_BRIDGE_TOKEN_TTL)
            .to_jwt()
        )
        query = urlencode(
            {"url": server_url, "token": token, "label": display_name or "Unify"},
        )
        separator = "&" if "?" in self._bridge_page_url else "?"
        return f"{self._bridge_page_url}{separator}{query}"

    def _relay_url_for(self, room_name: str) -> str | None:
        if not self._relay_url:
            return None
        secret = (os.environ.get("RECALL_RELAY_SECRET") or "").strip()
        if not secret:
            # Registering an endpoint the relay will reject burns 30 reconnect
            # attempts per bot and then gets the endpoint disabled workspace
            # wide, breaking the relay for later bots too. Skip it instead.
            LOGGER.info("[recall] no RECALL_RELAY_SECRET; skipping realtime relay")
            return None
        query = urlencode({"room": room_name, "token": secret})
        separator = "&" if "?" in self._relay_url else "?"
        return f"{self._relay_url}{separator}{query}"


def _default_relay_url() -> str:
    """Derive the relay websocket from the bridge page's own host.

    Both are served by comms, so one configured URL is enough and the two can
    never drift onto different hosts.
    """

    page = (os.environ.get("MEET_BRIDGE_PAGE_URL") or "").strip()
    if not page:
        return ""
    base = page.split("?", 1)[0]
    if base.endswith("/bridge"):
        base = base[: -len("/bridge")]
    base = base.rstrip("/")
    if base.startswith("https://"):
        return "wss://" + base[len("https://") :] + "/events"
    if base.startswith("http://"):
        return "ws://" + base[len("http://") :] + "/events"
    return ""


def _to_meet_state(bot: RecallBotState) -> MeetState:
    reason = None
    if bot.sub_code:
        reason = _FAILURE_REASONS.get(bot.sub_code, bot.sub_code)
    return MeetState(
        status=bot.status,
        ended=bot.terminal,
        lobby=bot.in_waiting_room,
        participants=tuple(
            MeetParticipantView(
                id=p.id,
                name=p.name,
                email=p.email,
                is_host=p.is_host,
            )
            for p in bot.participants
        ),
        failure_reason=reason,
        raw=bot.raw,
    )
