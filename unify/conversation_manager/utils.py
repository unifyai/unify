import requests

from unify.logger import LOGGER
from unify.common.hierarchical_logger import DEFAULT_ICON
from unify.session_details import SESSION_DETAILS
from unify.settings import SETTINGS


def _post_to_comms(path: str, payload: dict, *, label: str, timeout: float) -> bool:
    """POST a fire-and-forget control message to the comms gateway.

    Resilient to a missing ``UNITY_COMMS_URL`` (common in local/test
    environments), timeouts, and connection errors: none of these may take a
    live call down. Returns True when the request was attempted and not
    rejected, False when it was skipped or refused.
    """
    unity_comms_url = SETTINGS.conversation.COMMS_URL
    if not unity_comms_url:
        LOGGER.debug(
            f"{DEFAULT_ICON} [{label}] Skipping: UNITY_COMMS_URL not configured.",
        )
        return False

    try:
        response = requests.post(
            f"{unity_comms_url}{path}",
            # Authenticate as this assistant; the gateway accepts either a
            # valid user API key or the platform admin key here.
            headers={"Authorization": f"Bearer {SESSION_DETAILS.unify_key}"},
            json=payload,
            timeout=timeout,
        )
        if response.status_code != 200:
            LOGGER.error(f"{DEFAULT_ICON} [{label}] Refused: {response.text}")
            return False
        LOGGER.debug(f"{DEFAULT_ICON} [{label}] Accepted")
    except requests.exceptions.Timeout:
        LOGGER.debug(f"{DEFAULT_ICON} [{label}] Sent (response timed out)")
    except requests.exceptions.RequestException as e:
        LOGGER.error(f"{DEFAULT_ICON} [{label}] Request failed (non-fatal): {e}")
        return False
    return True


# dispatch LiveKit agent
def dispatch_livekit_agent(
    room_name: str,
    *,
    agent_name: str | None = None,
    call_session_id: str = "",
):
    """
    Dispatch a LiveKit agent via the communication service.

    By default ``room_name`` is used as both the LiveKit room name and the
    agent worker registration name. Pass ``agent_name`` when they must differ
    (org multi-assistant rooms share one room but register distinct workers).

    Dispatch only: recording is started from the call-started path via
    :func:`start_call_recording`, once the room has a publishing participant.
    """
    return _post_to_comms(
        "/phone/dispatch-livekit-agent",
        {
            "livekit_agent_name": agent_name or room_name,
            "room_name": room_name,
            "call_session_id": call_session_id,
        },
        label="dispatch_livekit_agent",
        timeout=1,
    )


def start_call_recording(
    room_name: str,
    assistant_id: str,
    *,
    user_id: str = "",
    call_session_id: str = "",
    provider_call_sid: str = "",
    conference_name: str = "",
):
    """Ask the gateway to record the live LiveKit room backing this session.

    Called once the session is up, so the room exists and carries audio. The
    linkage IDs travel with the request and come back on the completion webhook,
    which is how the finished file is matched to its transcript exchange.

    The gateway skips rooms it cannot capture and rooms already being recorded,
    so calling this more than once for one session is harmless.
    """
    if not room_name or not str(assistant_id).strip():
        LOGGER.debug(
            f"{DEFAULT_ICON} [start_call_recording] Skipping: room_name and "
            f"assistant_id are both required (room={room_name!r}, "
            f"assistant_id={assistant_id!r}).",
        )
        return False
    return _post_to_comms(
        "/phone/start-recording",
        {
            "room_name": room_name,
            "assistant_id": str(assistant_id),
            "user_id": str(user_id or ""),
            "call_session_id": call_session_id,
            "provider_call_sid": provider_call_sid,
            "conference_name": conference_name,
        },
        label="start_call_recording",
        # Recording is off the critical path for call setup, but the gateway
        # does a LiveKit round-trip here, so allow more than the dispatch hop.
        timeout=3,
    )
