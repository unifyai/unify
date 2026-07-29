"""Client for the Recall.ai meeting-bot API.

Recall runs the browser that sits in a Google Meet / Teams call. One bot is
created per meeting, joins within seconds, and streams the meeting into a
webpage we host -- the bridge page, which relays audio over LiveKit to the
fast brain. This module is only the control plane: create, observe, leave,
and the in-call side channels (chat, screenshare).

Regions are separate deployments. An API key issued in one region is not
valid in another and resources are region-local, so ``RECALL_REGION`` and
``RECALL_API_KEY`` must always be set together -- a mismatch surfaces as an
authentication failure rather than as anything routing-shaped.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Mapping

import aiohttp

LOGGER = logging.getLogger(__name__)

# Recall deployments. us-west-2 is the pay-as-you-go region and where
# self-serve signup lands; the others are separate accounts entirely.
RECALL_REGIONS = ("us-west-2", "us-east-1", "eu-central-1", "ap-northeast-1")
DEFAULT_RECALL_REGION = "us-west-2"

# A create/leave round trip is a control-plane call against Recall, not the
# meeting itself, so it should either answer quickly or be retried by the
# caller's own join flow.
_REQUEST_TIMEOUT_S = 30.0

# Seconds the bot waits after the last other participant leaves before it exits.
# Recall's default is 2s; 5 rides out someone dropping and rejoining without
# leaving the bot sitting in an empty meeting on the clock. The assistant does
# not speak a closing line, so nothing needs protecting beyond that.
EVERYONE_LEFT_TIMEOUT_S = 5

# Seconds the bot waits in an empty meeting for anyone at all to arrive. Recall
# defaults to 1200, which bills twenty minutes of bot time for a meeting nobody
# attends; five minutes is long enough for a late host.
NOONE_JOINED_TIMEOUT_S = 300

# LiveKit data topic the relay republishes participant events on.
#
# MIRRORED: the publisher is ``RECALL_EVENT_TOPIC`` in unity-deploy's
# ``communication/meet_events.py``. Two repos, one string, and a mismatch is
# silent -- the fast brain simply never sees a participant event and speaker
# attribution quietly falls back to voice embeddings. Change both together.
RECALL_EVENT_TOPIC = "recall_meeting_events"

# Recall's own lifecycle vocabulary, kept verbatim so log lines and stored
# failure reasons match what their dashboard shows an operator.
STATUS_JOINING = "joining_call"
STATUS_IN_WAITING_ROOM = "in_waiting_room"
STATUS_IN_CALL_NOT_RECORDING = "in_call_not_recording"
STATUS_IN_CALL_RECORDING = "in_call_recording"
STATUS_CALL_ENDED = "call_ended"
STATUS_DONE = "done"
STATUS_FATAL = "fatal"

# The bot is present and usable from the assistant's point of view. Recording
# state is irrelevant to us: we transcribe locally off the LiveKit track, so a
# bot that is in the call but not recording is still a working bot.
IN_CALL_STATUSES = frozenset({STATUS_IN_CALL_NOT_RECORDING, STATUS_IN_CALL_RECORDING})

# The bot will never be in the call. Distinguished from the in-call set so a
# caller can stop polling rather than waiting out its own ceiling.
TERMINAL_STATUSES = frozenset({STATUS_CALL_ENDED, STATUS_DONE, STATUS_FATAL})


class RecallError(RuntimeError):
    """A Recall API call failed in a way the caller cannot paper over."""


class RecallNotConfigured(RecallError):
    """No API key is present, so this environment cannot dispatch bots."""


@dataclass(frozen=True)
class RecallParticipant:
    """One meeting participant as the platform reports it.

    This is the payload that makes Recall worth adopting for attribution: the
    platform's own name and email for a speaker, rather than a label scraped
    out of the meeting UI.
    """

    id: str
    name: str
    email: str | None = None
    is_host: bool = False
    platform: str | None = None


@dataclass(frozen=True)
class RecallBotState:
    """Observed state of one bot.

    ``status`` is Recall's latest lifecycle code; ``sub_code`` carries their
    reason on a failure (denied entry, meeting not started, and so on) and is
    what a user-facing "could not join because..." line should be built from.
    """

    bot_id: str
    status: str
    sub_code: str | None = None
    participants: tuple[RecallParticipant, ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict)

    @property
    def in_call(self) -> bool:
        return self.status in IN_CALL_STATUSES

    @property
    def in_waiting_room(self) -> bool:
        return self.status == STATUS_IN_WAITING_ROOM

    @property
    def terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES


def recall_region() -> str:
    """The configured Recall deployment."""

    region = (os.environ.get("RECALL_REGION") or "").strip() or DEFAULT_RECALL_REGION
    if region not in RECALL_REGIONS:
        raise RecallError(
            f"RECALL_REGION={region!r} is not a Recall deployment; "
            f"expected one of {', '.join(RECALL_REGIONS)}",
        )
    return region


def recall_base_url() -> str:
    """Base URL for the configured region."""

    return f"https://{recall_region()}.recall.ai/api/v1"


def recall_configured() -> bool:
    """Whether this environment can dispatch Recall bots at all.

    ``RECALL_API_KEY`` is an optional pod secret, so an environment with no
    Recall workspace provisioned boots normally and simply cannot use this
    provider.
    """

    return bool((os.environ.get("RECALL_API_KEY") or "").strip())


class RecallClient:
    """Thin async client over the Recall bot API.

    Deliberately without a retry loop: Orchestra and Unify already absorb
    transient provider failures for integration calls, and a join that needs
    retrying is retried by the caller's join flow, which owns the user-facing
    timeout budget.
    """

    def __init__(self, *, api_key: str | None = None, base_url: str | None = None):
        key = api_key if api_key is not None else os.environ.get("RECALL_API_KEY")
        key = (key or "").strip()
        if not key:
            raise RecallNotConfigured(
                "RECALL_API_KEY is not set; this environment cannot dispatch "
                "Recall bots",
            )
        self._api_key = key
        self._base_url = (base_url or recall_base_url()).rstrip("/")

    async def create_bot(
        self,
        *,
        meeting_url: str,
        bot_name: str,
        bridge_page_url: str,
        realtime_events_url: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        as_screenshare: bool = False,
        capture_participant_video: bool = False,
    ) -> RecallBotState:
        """Dispatch a bot to one meeting and return its initial state.

        ``bridge_page_url`` is rendered by the bot as its camera (or its
        screenshare when ``as_screenshare``) and supplies both directions of
        audio. It must already carry the LiveKit room token: the bot loads it
        verbatim and cannot be handed credentials any other way.
        """

        surface = "screenshare" if as_screenshare else "camera"
        payload: dict[str, Any] = {
            "meeting_url": meeting_url,
            "bot_name": bot_name,
            "output_media": {
                surface: {
                    "kind": "webpage",
                    "config": {"url": bridge_page_url},
                },
            },
            # Retention is pinned on every create, never left to the default.
            # Recall retains indefinitely for new accounts, so an unset value
            # silently accrues customer meeting media at a subprocessor. We
            # transcribe locally off the LiveKit track and never read their
            # recording, so nothing should be stored at all.
            "recording_config": {"retention": None},
            # Let Recall own the departure. The bot leaving is what the fast
            # brain detects (its bridge page drops out of the LiveKit room), so
            # these timeouts are the actual end-of-meeting policy rather than a
            # backstop behind one of ours.
            "automatic_leave": {
                "everyone_left_timeout": EVERYONE_LEFT_TIMEOUT_S,
                "noone_joined_timeout": NOONE_JOINED_TIMEOUT_S,
            },
        }
        if capture_participant_video:
            # Separate per-participant video only arrives in gallery layout;
            # without this the platform sends one composited stream and there is
            # no screenshare track to pick out.
            payload["recording_config"]["video_mixed_layout"] = "gallery_view_v2"
        if metadata:
            payload["metadata"] = dict(metadata)
        if realtime_events_url:
            payload["realtime_endpoints"] = [
                {
                    "type": "websocket",
                    "url": realtime_events_url,
                    # Only what a consumer reads. Recall bills nothing extra
                    # for breadth, but every unread event is traffic on the
                    # room's data channel for the length of the call.
                    "events": [
                        "participant_events.join",
                        "participant_events.leave",
                        "participant_events.update",
                        "participant_events.speech_on",
                        "participant_events.speech_off",
                        "participant_events.chat_message",
                    ],
                },
            ]
            if capture_participant_video:
                # PNG rather than H264: 360p at 2fps needs no decoder and no
                # web_4_core variant (+$0.10/hr), and H264 only reaches
                # 200-1000px anyway -- too little extra to read a shared screen
                # that PNG cannot, for materially more cost and complexity.
                payload["realtime_endpoints"][0]["events"].append(
                    "video_separate_png.data",
                )

        body = await self._request("POST", "/bot", json=payload)
        return _parse_bot_state(body)

    async def get_bot(self, bot_id: str) -> RecallBotState:
        """Current lifecycle state and roster for one bot."""

        return _parse_bot_state(await self._request("GET", f"/bot/{bot_id}"))

    async def leave_call(self, bot_id: str) -> None:
        """Ask the bot to leave. Idempotent from the caller's point of view.

        A bot that already left is not an error worth propagating into
        teardown -- the desired end state is reached either way.
        """

        try:
            await self._request("POST", f"/bot/{bot_id}/leave_call")
        except RecallError as exc:
            LOGGER.warning(
                "[recall] leave_call for %s failed (already gone?): %s",
                bot_id,
                exc,
            )

    async def send_chat_message(
        self,
        bot_id: str,
        text: str,
        to: str | None = None,
    ) -> None:
        """Post into the meeting chat, optionally as a direct message.

        Unsupported in Teams channel meetings; Recall answers with an error
        there and the caller should degrade rather than treat it as a fault.
        """

        payload: dict[str, Any] = {"message": text}
        if to:
            payload["to"] = to
        await self._request("POST", f"/bot/{bot_id}/send_chat_message", json=payload)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        headers = {
            "authorization": f"Token {self._api_key}",
            "accept": "application/json",
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method,
                    url,
                    json=dict(json) if json is not None else None,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT_S),
                ) as resp:
                    text = await resp.text()
                    if resp.status >= 400:
                        raise RecallError(
                            f"{method} {path} -> HTTP {resp.status}: {text[:400]}",
                        )
                    if not text:
                        return {}
                    return await _decode_json(resp, text, method, path)
        except aiohttp.ClientError as exc:
            raise RecallError(f"{method} {path} failed: {exc!r}") from exc
        except TimeoutError as exc:
            raise RecallError(f"{method} {path} timed out") from exc


async def _decode_json(
    resp: aiohttp.ClientResponse,
    text: str,
    method: str,
    path: str,
) -> dict[str, Any]:
    import json as _json

    try:
        decoded = _json.loads(text)
    except ValueError as exc:
        raise RecallError(f"{method} {path} returned non-JSON body") from exc
    if not isinstance(decoded, dict):
        raise RecallError(
            f"{method} {path} returned {type(decoded).__name__}, not an object",
        )
    return decoded


def _parse_bot_state(body: Mapping[str, Any]) -> RecallBotState:
    """Read a bot payload into the state the call manager acts on.

    Recall reports lifecycle as an append-only ``status_changes`` list rather
    than a single field, so the latest entry is the current status. An empty
    list means the bot was accepted but has not transitioned yet, which is
    ``joining_call`` in everything but name.
    """

    bot_id = str(body.get("id") or "")
    if not bot_id:
        raise RecallError("Recall bot payload has no id")

    status = STATUS_JOINING
    sub_code: str | None = None
    changes = body.get("status_changes")
    if isinstance(changes, list) and changes:
        latest = changes[-1]
        if isinstance(latest, Mapping):
            status = str(latest.get("code") or STATUS_JOINING)
            raw_sub = latest.get("sub_code")
            sub_code = str(raw_sub) if raw_sub else None

    return RecallBotState(
        bot_id=bot_id,
        status=status,
        sub_code=sub_code,
        participants=_parse_participants(body.get("meeting_participants")),
        raw=dict(body),
    )


def _parse_participants(value: Any) -> tuple[RecallParticipant, ...]:
    if not isinstance(value, list):
        return ()
    participants = []
    for entry in value:
        if not isinstance(entry, Mapping):
            continue
        participant_id = entry.get("id")
        if participant_id is None:
            continue
        extra = entry.get("extra_data")
        email = entry.get("email")
        if not email and isinstance(extra, Mapping):
            email = extra.get("email")
        participants.append(
            RecallParticipant(
                id=str(participant_id),
                name=str(entry.get("name") or ""),
                email=str(email) if email else None,
                is_host=bool(entry.get("is_host")),
                platform=(
                    str(entry.get("platform")) if entry.get("platform") else None
                ),
            ),
        )
    return tuple(participants)
