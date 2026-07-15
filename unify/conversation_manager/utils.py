import requests

from unify.logger import LOGGER
from unify.common.hierarchical_logger import DEFAULT_ICON
from unify.session_details import SESSION_DETAILS
from unify.settings import SETTINGS


# dispatch LiveKit agent
def dispatch_livekit_agent(
    room_name: str,
    *,
    record: bool = True,
    assistant_id: str = "",
    user_id: str = "",
):
    """
    Dispatch a LiveKit agent via the communication service.

    The room_name (from make_room_name()) is used as both the LiveKit room
    name and the agent worker registration name.

    This is a fire-and-forget operation - we dispatch and move on regardless of
    the result. The function is resilient to:
    - Missing UNITY_COMMS_URL (common in local/test environments)
    - Network timeouts (expected behavior)
    - Connection errors (service unavailable)

    Returns True if dispatch was attempted, False if skipped due to missing config.
    """
    unity_comms_url = SETTINGS.conversation.COMMS_URL
    if not unity_comms_url:
        LOGGER.debug(
            f"{DEFAULT_ICON} [dispatch_livekit_agent] Skipping: UNITY_COMMS_URL not configured. "
            "Set this to enable LiveKit agent dispatch.",
        )
        return False

    try:
        response = requests.post(
            f"{unity_comms_url}/phone/dispatch-livekit-agent",
            # Authenticate as this assistant; the gateway accepts either a
            # valid user API key or the platform admin key here.
            headers={"Authorization": f"Bearer {SESSION_DETAILS.unify_key}"},
            json={
                "livekit_agent_name": room_name,
                "room_name": room_name,
                "record": record,
                "assistant_id": assistant_id,
                "user_id": user_id,
            },
            timeout=1,
        )
        if response.status_code != 200:
            LOGGER.error(
                f"{DEFAULT_ICON} Failed to dispatch LiveKit agent. {response.text}",
            )
            return False
        else:
            LOGGER.debug(f"{DEFAULT_ICON} LiveKit agent dispatched")
    except requests.exceptions.Timeout:
        LOGGER.debug(f"{DEFAULT_ICON} LiveKit agent dispatched (timeout)")
    except requests.exceptions.RequestException as e:
        # Connection errors, DNS failures, etc. - don't crash, just log
        LOGGER.error(
            f"{DEFAULT_ICON} [dispatch_livekit_agent] Request failed (non-fatal): {e}",
        )
        return False
    return True
