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

from unify.common.broker import broker_origin
from unify.conversation_manager.domains.recall.events import (
    EVENT_VIDEO_FRAME,
    SUBSCRIBED_EVENTS,
    RecallParticipant,
    participant_from_payload,
)

LOGGER = logging.getLogger(__name__)

# Recall deployments. Each is a wholly separate installation: an API key issued
# in one is invalid in the others, and resources do not cross between them.
RECALL_REGIONS = ("us-west-2", "us-east-1", "eu-central-1", "ap-northeast-1")

# Our workspace lives in eu-central-1 (Frankfurt), so that is the default rather
# than the region self-serve signup happens to land in. This is not cosmetic:
# ``RECALL_REGION`` is not injected into assistant pods, so a wrong default
# sends every hosted join to a deployment our key is not valid in -- which
# surfaces as an authentication failure, not as anything region-shaped.
DEFAULT_RECALL_REGION = "eu-central-1"

# A create/leave round trip is a control-plane call against Recall, not the
# meeting itself, so it should either answer quickly or be retried by the
# caller's own join flow.
_REQUEST_TIMEOUT_S = 30.0

# Bot variant with enough CPU for per-participant video. It buys headroom only:
# camera and screenshare output stay 1280x720 at 15fps on every variant, and the
# 360p/2fps of separate PNG video is fixed too.
VARIANT_WEB_4_CORE = "web_4_core"

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
    """Base URL for dispatching bots.

    With a broker sidecar the pod holds no ``RECALL_API_KEY``; route through the
    sidecar's header-swap proxy, which holds the key and forwards to the region
    host. The ``/api/v1`` suffix matches the direct URL so callers building
    paths are unchanged. Self-host / local dev talks to the region directly.
    """

    origin = broker_origin()
    if origin:
        return f"{origin}/proxy/recall/api/v1"
    return f"https://{recall_region()}.recall.ai/api/v1"


def recall_configured() -> bool:
    """Whether this environment can dispatch Recall bots at all.

    ``RECALL_API_KEY`` is an optional pod secret, so an environment with no
    Recall workspace provisioned boots normally and simply cannot use this
    provider.
    """

    if (os.environ.get("RECALL_API_KEY") or "").strip():
        return True
    # The key moved to the broker sidecar. RECALL_RELAY_SECRET is provisioned
    # together with it (see the module docstring) and stays on the pod, so its
    # presence is the local signal that Recall is available through the broker.
    return bool(broker_origin()) and bool(
        (os.environ.get("RECALL_RELAY_SECRET") or "").strip(),
    )


def meet_bridge_base_url() -> str:
    """Root of the comms host serving the bridge page and its side channels.

    The bridge page, the realtime event relay and the shared-screen store are
    all served by comms, so one configured URL is enough and they can never
    drift onto different hosts. Empty when unconfigured.
    """

    page = (os.environ.get("MEET_BRIDGE_PAGE_URL") or "").strip()
    if not page:
        return ""
    base = page.split("?", 1)[0]
    if base.endswith("/bridge"):
        base = base[: -len("/bridge")]
    return base.rstrip("/")


class RecallClient:
    """Thin async client over the Recall bot API.

    Deliberately without a retry loop: Orchestra and Unify already absorb
    transient provider failures for integration calls, and a join that needs
    retrying is retried by the caller's join flow, which owns the user-facing
    timeout budget.
    """

    def __init__(self, *, api_key: str | None = None, base_url: str | None = None):
        if api_key is None:
            # With a broker the pod holds no RECALL_API_KEY; present the pod's
            # UNIFY_KEY as the nonce and let the sidecar swap in the real key.
            api_key = (
                os.environ.get("UNIFY_KEY")
                if broker_origin()
                else os.environ.get("RECALL_API_KEY")
            )
        key = (api_key or "").strip()
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
        may_screenshare: bool = False,
    ) -> RecallBotState:
        """Dispatch a bot to one meeting and return its initial state.

        ``bridge_page_url`` is rendered by the bot as its camera (or its
        screenshare when ``as_screenshare``) and supplies both directions of
        audio. It must already carry the LiveKit room token: the bot loads it
        verbatim and cannot be handed credentials any other way.

        ``may_screenshare`` says this call *might* later put a second page on the
        screenshare surface. It only affects the variant, and it has to be decided
        here because the variant is fixed for the bot's whole life -- there is no
        upgrading it at the moment somebody asks to share.
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
            # Three keys, all load-bearing, and every one of them fails silently
            # when omitted -- the bot joins and simply never sends a frame.
            #
            # Gallery layout: separate per-participant video only arrives in it;
            # otherwise the platform sends one composited stream with no
            # screenshare track to pick out.
            payload["recording_config"]["video_mixed_layout"] = "gallery_view_v2"
            # The artifact block itself. It carries no options -- resolution and
            # frame rate are fixed at 360p/2fps and cannot be requested -- but
            # subscribing the event without declaring the artifact yields
            # nothing.
            payload["recording_config"]["video_separate_png"] = {}
        if capture_participant_video or may_screenshare:
            # CPU headroom, wanted for either of two independent reasons, and the
            # variant cannot be changed later so both are decided here.
            #
            # Participant video: Recall's prose says separate per-participant
            # video requires the four-core variant while their own PNG example
            # omits it. Paying the surcharge (+$0.10/hr on plan) is the cheaper of
            # the two mistakes; the other is a meeting where the assistant is
            # silently blind.
            #
            # Screensharing: a second rendered page competes with the bridge for
            # the default variant's 250 millicores, which Recall names as the
            # cause of choppy output. Kept separate from the flag above so turning
            # participant video off does not quietly degrade sharing too.
            payload["variant"] = {
                "google_meet": VARIANT_WEB_4_CORE,
                "microsoft_teams": VARIANT_WEB_4_CORE,
            }
        if metadata:
            payload["metadata"] = dict(metadata)
        if realtime_events_url:
            # Nested under recording_config, not top level. Recall ignores
            # unknown top-level keys, so getting this wrong registers no
            # endpoint at all and the bot silently never sends an event --
            # indistinguishable from a relay that is down.
            events = list(SUBSCRIBED_EVENTS)
            if capture_participant_video:
                # PNG rather than H264: a still frame needs no decoder, and H264
                # only reaches 200-1000px anyway -- too little extra to read a
                # shared screen that PNG cannot, for materially more complexity.
                events.append(EVENT_VIDEO_FRAME)
            payload["recording_config"]["realtime_endpoints"] = [
                {
                    "type": "websocket",
                    "url": realtime_events_url,
                    # Only what a consumer reads. Recall bills nothing extra
                    # for breadth, but every unread event is traffic on the
                    # room's data channel for the length of the call.
                    "events": events,
                },
            ]

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

    async def start_screenshare(self, bot_id: str, page_url: str) -> None:
        """Put ``page_url`` on the bot's screenshare surface, mid-call.

        Sends the screenshare surface **only**. The camera is deliberately absent:
        it is the page bridging audio, and this endpoint's merge-vs-replace
        behaviour is undocumented, so naming the camera at all risks reloading or
        replacing the page that carries the assistant's voice. Recall states the
        camera cannot be turned off while output media is on, so there is nothing
        to preserve by re-sending it either.

        The previous implementation of this always re-sent the camera URL, and
        losing audio mid-meeting is the failure that got it withdrawn.
        """

        await self._request(
            "POST",
            f"/bot/{bot_id}/output_media/",
            json={
                "screenshare": {
                    "kind": "webpage",
                    "config": {"url": page_url},
                },
            },
        )

    async def stop_screenshare(self, bot_id: str) -> None:
        """Drop the screenshare surface, leaving the camera running.

        ``DELETE`` with the surface named is the documented stop. Clearing it by
        re-POSTing a camera-only body -- what the withdrawn implementation did --
        is not, and depends on the same undocumented merge behaviour.
        """

        await self._request(
            "DELETE",
            f"/bot/{bot_id}/output_media/",
            json={"screenshare": True},
        )

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
    """The roster from a REST bot payload's ``meeting_participants``.

    Shares its per-entry parsing with the realtime relay so a participant means
    the same thing whichever source reported them.
    """

    if not isinstance(value, list):
        return ()
    parsed = (participant_from_payload(entry) for entry in value)
    return tuple(p for p in parsed if p is not None)
