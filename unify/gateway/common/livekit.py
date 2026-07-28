"""Shared LiveKit SDK helpers for gateway channels.

Promoted from ``communication/common/livekit.py`` for the channels
in ``unify.gateway`` that bridge external calls into LiveKit rooms
(phone, whatsapp, teams). Functionally equivalent to the
communication-side helpers; only the credential resolution path
changes (env reads -> ``CredentialStore.get``) so the channels stay
decoupled from any deployment-specific config layer.

Helpers
=======

* ``get_livekit_api`` -- LiveKit ``LiveKitAPI`` client factory.
* ``make_sip_uri`` -- builds the SIP URI used to bridge a Twilio
  call into a LiveKit inbound trunk.
* ``ensure_phone_dispatch_rule`` -- idempotently maintains the
  per-trunk dispatch rule that routes inbound SIP calls into the
  correct ``unity_{id}_{medium}`` room.
* ``create_room_and_dispatch_agent`` -- creates a LiveKit room and
  dispatches the LiveKit agent that owns the call session.
* ``start_room_egress`` -- starts the audio-only Room Composite
  Egress that captures a live call to GCS.

Recording is deliberately *not* wired into agent dispatch. Egress must
be started once the room has a publishing participant, so it is driven
from the call-started path (``/phone/start-recording``) rather than from
room creation. Starting it at dispatch time races the SIP bridge and the
agent join, and LiveKit fails such a job with "Start signal not
received" after producing no file at all.

``unify.conversation_manager.local_providers.livekit`` carries a
parallel copy of the same helpers for the self-hosted single-process
path.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus, urlencode

from livekit.api import (
    CreateAgentDispatchRequest,
    CreateSIPDispatchRuleRequest,
    EncodedFileOutput,
    GCPUpload,
    LiveKitAPI,
    RoomCompositeEgressRequest,
    SIPDispatchRuleInfo,
    WebhookConfig,
)
from livekit.protocol.egress import ListEgressRequest
from livekit.protocol.sip import (
    DeleteSIPDispatchRuleRequest,
    ListSIPDispatchRuleRequest,
    ListSIPInboundTrunkRequest,
    SIPDispatchRule,
    SIPDispatchRuleDirect,
)

from unify.gateway.credentials import CredentialNotFoundError, CredentialStore
from unify.settings import SETTINGS

_log = logging.getLogger("unify.gateway.common.livekit")

# Room suffixes whose audio never reaches the LiveKit room. Browser meets
# (Google Meet / Teams) bridge caller audio through the agent-service
# PortAudio device, so the LiveKit room carries no remote track for the
# compositor to mix -- see ``Assistant.stt_node`` in
# ``conversation_manager/medium_scripts/call.py``. Room Composite Egress on
# these rooms cannot produce a file: it stays alive for as long as the room
# does and then fails with "Start signal not received". Recording these
# channels needs a capture point on the bridge, not LiveKit egress.
UNRECORDABLE_ROOM_SUFFIXES = ("_gmeet", "_teams")


def room_supports_egress(room_name: str) -> bool:
    """Whether a Room Composite Egress can ever capture audio for this room."""
    return not room_name.endswith(UNRECORDABLE_ROOM_SUFFIXES)


async def has_active_egress(livekit_api: LiveKitAPI, room_name: str) -> bool:
    """Whether an egress job is already running for *room_name*.

    Several call paths can converge on one room (SIP bridge setup, agent
    dispatch, a watchdog respawn). Without this check each one starts its own
    Room Composite Egress, so the room is recorded twice into two separate
    files and billed twice. Treated as "no active egress" when the lookup
    itself fails, so a LiveKit hiccup cannot silently disable recording.
    """
    try:
        listed = await livekit_api.egress.list_egress(
            ListEgressRequest(room_name=room_name, active=True),
        )
    except Exception as exc:
        _log.warning(
            "could not list active egress for room %r, starting anyway: %s",
            room_name,
            exc,
        )
        return False
    return bool(listed.items)


def get_livekit_api(credentials: CredentialStore) -> LiveKitAPI:
    """Construct a LiveKit API client from configured credentials.

    Reads ``LIVEKIT_URL``, ``LIVEKIT_API_KEY``, and
    ``LIVEKIT_API_SECRET`` from ``credentials``. Raises
    ``RuntimeError`` with a clear message naming all three required
    keys when any is missing.
    """
    try:
        url = credentials.get("LIVEKIT_URL")
        api_key = credentials.get("LIVEKIT_API_KEY")
        api_secret = credentials.get("LIVEKIT_API_SECRET")
    except CredentialNotFoundError as exc:
        raise RuntimeError(
            "LIVEKIT_URL, LIVEKIT_API_KEY, and LIVEKIT_API_SECRET must be set",
        ) from exc
    return LiveKitAPI(url=url, api_key=api_key, api_secret=api_secret)


def make_sip_uri(phone_number: str, credentials: CredentialStore) -> str:
    """Build the SIP URI for bridging a Twilio call into LiveKit.

    Uses the E.164 phone number as the user part so LiveKit can match
    it against the inbound SIP trunk's ``numbers`` field. A per-trunk
    dispatch rule (created by ``ensure_phone_dispatch_rule``) then
    routes the SIP participant into the correct
    ``unity_{id}_{medium}`` room.

    ``LIVEKIT_SIP_URI`` is the SIP domain (e.g.
    ``mytenant.sip.livekit.cloud``). Missing or empty domain returns
    a URI with an empty host -- preserving the legacy behaviour so
    test fixtures that don't configure the SIP domain still work.
    """
    sip_domain = credentials.get_optional("LIVEKIT_SIP_URI", "")
    normalized = phone_number if phone_number.startswith("+") else f"+{phone_number}"
    return f"sip:{normalized}@{sip_domain}"


def make_call_scoped_sip_uri(
    phone_number: str,
    call_id: str,
    credentials: CredentialStore,
    *,
    headers: dict[str, str] | None = None,
) -> tuple[str, str]:
    sip_domain = credentials.get_optional("LIVEKIT_SIP_URI", "")
    normalized = phone_number if phone_number.startswith("+") else f"+{phone_number}"
    safe_call_id = "".join(ch if ch.isalnum() else "-" for ch in call_id).strip("-")
    sip_user = f"{normalized[1:]}-{safe_call_id}"
    uri = f"sip:{sip_user}@{sip_domain}"
    if headers:
        sip_headers = {
            key if key.lower().startswith("x-") else f"X-{key}": value
            for key, value in headers.items()
        }
        uri = f"{uri}?{urlencode(sip_headers)}"
    return uri, sip_user


async def ensure_phone_dispatch_rule(
    phone_number: str,
    room_name: str,
    credentials: CredentialStore,
) -> None:
    """Idempotently maintain the SIP dispatch rule for ``phone_number``.

    Routes inbound SIP calls to ``phone_number`` into ``room_name``.
    Skips creation when a matching rule already exists; replaces stale
    rules when the room name has changed (number reassigned to a
    different assistant).

    Best-effort: logs and swallows transport errors so a missing or
    deleted inbound trunk doesn't break the outbound call setup.
    """
    livekit_api = get_livekit_api(credentials)
    try:
        normalized = (
            phone_number if phone_number.startswith("+") else f"+{phone_number}"
        )

        trunks = await livekit_api.sip.list_sip_inbound_trunk(
            ListSIPInboundTrunkRequest(),
        )
        trunk_id = None
        for trunk in trunks.items:
            if normalized in list(trunk.numbers):
                trunk_id = trunk.sip_trunk_id
                break
        if trunk_id is None:
            _log.info(
                "no inbound trunk for %s; skipping dispatch rule creation",
                normalized,
            )
            return

        rules = await livekit_api.sip.list_sip_dispatch_rule(
            ListSIPDispatchRuleRequest(),
        )
        for rule in rules.items:
            if trunk_id not in list(rule.trunk_ids):
                continue
            if (
                rule.rule.HasField("dispatch_rule_direct")
                and rule.rule.dispatch_rule_direct.room_name == room_name
            ):
                return
            await livekit_api.sip.delete_sip_dispatch_rule(
                DeleteSIPDispatchRuleRequest(
                    sip_dispatch_rule_id=rule.sip_dispatch_rule_id,
                ),
            )

        await livekit_api.sip.create_sip_dispatch_rule(
            CreateSIPDispatchRuleRequest(
                dispatch_rule=SIPDispatchRuleInfo(
                    rule=SIPDispatchRule(
                        dispatch_rule_direct=SIPDispatchRuleDirect(
                            room_name=room_name,
                        ),
                    ),
                    name=f"Unity_phone_{normalized}",
                    trunk_ids=[trunk_id],
                ),
            ),
        )
        _log.info("created dispatch rule: %s -> %s", normalized, room_name)
    except Exception as exc:
        _log.warning(
            "failed to ensure dispatch rule for %s: %s",
            phone_number,
            exc,
        )
    finally:
        await livekit_api.aclose()


async def ensure_call_scoped_dispatch_rule(
    *,
    base_phone_number: str,
    sip_target: str,
    room_name: str,
    call_id: str,
    assistant_id: str,
    credentials: CredentialStore,
) -> str | None:
    livekit_api = get_livekit_api(credentials)
    try:
        normalized = (
            base_phone_number
            if base_phone_number.startswith("+")
            else f"+{base_phone_number}"
        )
        trunks = await livekit_api.sip.list_sip_inbound_trunk(
            ListSIPInboundTrunkRequest(),
        )
        trunk_id = None
        for trunk in trunks.items:
            if normalized in list(trunk.numbers):
                trunk_id = trunk.sip_trunk_id
                break
        if trunk_id is None:
            _log.info(
                "no inbound trunk for %s; skipping call-scoped dispatch rule",
                normalized,
            )
            return None

        dispatch = await livekit_api.sip.create_sip_dispatch_rule(
            CreateSIPDispatchRuleRequest(
                dispatch_rule=SIPDispatchRuleInfo(
                    rule=SIPDispatchRule(
                        dispatch_rule_direct=SIPDispatchRuleDirect(
                            room_name=room_name,
                        ),
                    ),
                    name=f"Unity_call_{call_id}",
                    trunk_ids=[trunk_id],
                    numbers=[sip_target],
                    attributes={
                        "call.id": call_id,
                        "assistant.id": str(assistant_id),
                    },
                ),
            ),
        )
        _log.info(
            "created call-scoped dispatch rule: %s -> %s",
            sip_target,
            room_name,
        )
        return dispatch.sip_dispatch_rule_id
    except Exception as exc:
        _log.warning(
            "failed to ensure call-scoped dispatch rule for %s: %s",
            call_id,
            exc,
        )
        return None
    finally:
        await livekit_api.aclose()


async def delete_sip_dispatch_rule(
    dispatch_rule_id: str | None,
    credentials: CredentialStore,
) -> None:
    if not dispatch_rule_id:
        return
    livekit_api = get_livekit_api(credentials)
    try:
        await livekit_api.sip.delete_sip_dispatch_rule(
            DeleteSIPDispatchRuleRequest(sip_dispatch_rule_id=dispatch_rule_id),
        )
        _log.info("deleted dispatch rule %s", dispatch_rule_id)
    except Exception as exc:
        _log.warning("failed to delete dispatch rule %s: %s", dispatch_rule_id, exc)
    finally:
        await livekit_api.aclose()


async def _start_room_egress(
    livekit_api: LiveKitAPI,
    room_name: str,
    assistant_id: str,
    user_id: str,
    credentials: CredentialStore,
    *,
    call_session_id: str = "",
    provider_call_sid: str = "",
    conference_name: str = "",
) -> None:
    """Start an audio-only Room Composite Egress that writes MP3 to GCS.

    LiveKit uploads the recording directly to GCS and calls the adapters
    ``/livekit/recording-complete`` webhook on completion. That webhook
    back-links the ``recording_url`` onto the call session (when a
    ``provider_call_sid`` is present) and republishes a ``recording_ready``
    event so the assistant runtime can attach it to the transcript exchange.
    The linkage query params are threaded through the webhook URL so the
    completion handler can resolve the correct session / exchange.

    No-ops when the room cannot be captured, when the caller has no assistant
    to attribute the file to, or when a job is already recording the room.
    """
    if not room_supports_egress(room_name):
        _log.info(
            "skipping egress for room %r: channel audio does not reach the "
            "LiveKit room",
            room_name,
        )
        return
    if not str(assistant_id).strip():
        # The object path is {env}/{assistant_id}/{room}.mp3, so an empty id
        # collapses the per-assistant prefix and strands the file where no
        # lookup by assistant will find it.
        _log.error(
            "refusing egress for room %r: assistant_id is required to build "
            "the recording path",
            room_name,
        )
        return
    if await has_active_egress(livekit_api, room_name):
        _log.info(
            "skipping egress for room %r: a job is already recording it",
            room_name,
        )
        return

    gcs_credentials = credentials.get_optional("GCP_SA_KEY", "")
    gcs_bucket = credentials.get_optional(
        "LIVEKIT_EGRESS_GCS_BUCKET",
        "unity-call-recordings",
    )
    adapters_url = SETTINGS.conversation.ADAPTERS_URL
    api_key = credentials.get("LIVEKIT_API_KEY")

    prefix = SETTINGS.DEPLOY_ENV
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    filepath = f"{prefix}/{assistant_id}/{room_name}_{timestamp}.mp3"

    webhook_url = (
        f"{adapters_url}/livekit/recording-complete"
        f"?assistant_id={quote_plus(str(assistant_id))}"
        f"&user_id={quote_plus(str(user_id))}"
        f"&room_name={quote_plus(room_name)}"
    )
    if call_session_id:
        webhook_url += f"&call_session_id={quote_plus(call_session_id)}"
    if provider_call_sid:
        webhook_url += f"&provider_call_sid={quote_plus(provider_call_sid)}"
    if conference_name:
        webhook_url += f"&conference_name={quote_plus(conference_name)}"

    egress_request = RoomCompositeEgressRequest(
        room_name=room_name,
        audio_only=True,
        file_outputs=[
            EncodedFileOutput(
                file_type=3,  # MP3
                filepath=filepath,
                gcp=GCPUpload(credentials=gcs_credentials, bucket=gcs_bucket),
            ),
        ],
        webhooks=[WebhookConfig(url=webhook_url, signing_key=api_key)],
    )
    info = await livekit_api.egress.start_room_composite_egress(egress_request)
    _log.info(
        "started room composite egress %s for room %r -> gs://%s/%s",
        getattr(info, "egress_id", "?"),
        room_name,
        gcs_bucket,
        filepath,
    )


async def start_room_egress(
    room_name: str,
    assistant_id: str,
    credentials: CredentialStore,
    user_id: str = "",
    *,
    call_session_id: str = "",
    provider_call_sid: str = "",
    conference_name: str = "",
) -> None:
    """Start an audio-only Room Composite Egress on an existing room.

    Use this when the room was created externally (e.g. by a SIP trunk) and you
    only need to start recording. Egress failures are logged and swallowed so a
    recording problem never breaks call setup.
    """
    livekit_api = get_livekit_api(credentials)
    try:
        await _start_room_egress(
            livekit_api,
            room_name,
            assistant_id,
            user_id,
            credentials,
            call_session_id=call_session_id,
            provider_call_sid=provider_call_sid,
            conference_name=conference_name,
        )
    except Exception as exc:
        _log.error("failed to start egress for room %r: %s", room_name, exc)
    finally:
        await livekit_api.aclose()


async def create_room_and_dispatch_agent(
    room_name: str,
    agent_name: str,
    credentials: CredentialStore,
    metadata: dict | None = None,
) -> Any:
    """Create a LiveKit room and dispatch an agent into it.

    Dispatch only. Recording is started separately once the room is live (see
    ``start_room_egress``); binding it to dispatch starts the compositor before
    any participant publishes, which LiveKit fails with no file produced.

    Returns the LiveKit dispatch object. Re-raises dispatch failures after
    logging so the caller's error handler sees the exception.
    """
    livekit_api = get_livekit_api(credentials)
    try:
        dispatch_request = CreateAgentDispatchRequest(
            agent_name=agent_name,
            room=room_name,
            metadata=json.dumps(metadata) if metadata else None,
        )
        dispatch = await livekit_api.agent_dispatch.create_dispatch(
            dispatch_request,
        )
        _log.info(
            "created room %r and dispatched LiveKit agent %r (dispatch_id=%s)",
            room_name,
            agent_name,
            getattr(dispatch, "id", "?"),
        )
        return dispatch
    except Exception as exc:
        _log.error(
            "create_room_and_dispatch_agent failed for room %r: %s",
            room_name,
            exc,
        )
        raise
    finally:
        await livekit_api.aclose()


__all__ = [
    "UNRECORDABLE_ROOM_SUFFIXES",
    "create_room_and_dispatch_agent",
    "delete_sip_dispatch_rule",
    "ensure_call_scoped_dispatch_rule",
    "ensure_phone_dispatch_rule",
    "get_livekit_api",
    "has_active_egress",
    "make_call_scoped_sip_uri",
    "make_sip_uri",
    "room_supports_egress",
    "start_room_egress",
]
