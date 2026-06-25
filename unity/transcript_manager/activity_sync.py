"""Fire-and-forget activity sync from transcripts to orchestra.

The transcript hook calls :func:`touch_assistant_activity` after every
``log_messages`` invocation so orchestra's inactivity-followup routine
sees fresh ``last_correspondence_at`` and clears any pending
``last_followup_sent_at`` (allowing the next silence to re-arm a
re-engagement follow-up).

The brain-driven opt-out helpers
:func:`opt_out_of_inactivity_followups_via_orchestra` and
:func:`opt_in_to_inactivity_followups_via_orchestra` live alongside it:
they tell orchestra whether this Coordinator should keep receiving
inactivity follow-ups.

All calls are best-effort: any failure is swallowed and logged at WARN.
The transcript log path and the brain primitive path must never break
because of an orchestra HTTP hiccup.

Endpoints:
    POST /admin/assistant/{assistant_id}/touch-activity
    POST /admin/assistant/{assistant_id}/opt-out-followups
    POST /admin/assistant/{assistant_id}/opt-in-followups
"""

from __future__ import annotations

import logging
from typing import Optional

from ..settings import SETTINGS

_log = logging.getLogger(__name__)


def _base_url() -> Optional[str]:
    return SETTINGS.ORCHESTRA_URL or None


def _admin_key() -> Optional[str]:
    return SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value() or None


def touch_assistant_activity(assistant_id: int | str | None) -> bool:
    """Notify orchestra that the assistant just exchanged a message.

    Best-effort: returns True on 2xx, False otherwise (including any
    swallowed exception). Skips silently when ``assistant_id`` or the
    orchestra config is missing.

    :param assistant_id: The assistant's agent_id. Coerced to int.
    :return: True if the call succeeded, False otherwise.
    """
    if assistant_id is None:
        return False
    try:
        agent_id_int = int(assistant_id)
    except (TypeError, ValueError):
        _log.debug(
            "Skipping touch_assistant_activity: assistant_id %r is not int-coercible",
            assistant_id,
        )
        return False

    base_url = _base_url()
    admin_key = _admin_key()
    if not base_url or not admin_key:
        _log.debug(
            "Skipping touch_assistant_activity for %d: orchestra URL or admin key missing",
            agent_id_int,
        )
        return False

    try:
        from unify.utils import http

        url = f"{base_url.rstrip('/')}/admin/assistant/{agent_id_int}/touch-activity"
        headers = {"Authorization": f"Bearer {admin_key}"}
        resp = http.post(url, headers=headers, timeout=10)
        if 200 <= resp.status_code < 300:
            return True
        _log.warning(
            "touch_assistant_activity for %d returned %d: %s",
            agent_id_int,
            resp.status_code,
            getattr(resp, "text", ""),
        )
        return False
    except Exception as exc:
        _log.warning("touch_assistant_activity for %d failed: %s", agent_id_int, exc)
        return False


def _post_followup_admin_action(
    assistant_id: int | str | None,
    action_path: str,
    *,
    label: str,
) -> bool:
    """Shared helper for opt-out / opt-in inactivity-followup POSTs.

    :param assistant_id: Assistant agent_id (coerced to int).
    :param action_path: Trailing path segment, e.g. ``"opt-out-followups"``.
    :param label: Short label used in log messages.
    :return: True on 2xx, False otherwise (including swallowed exceptions).
    """
    if assistant_id is None:
        return False
    try:
        agent_id_int = int(assistant_id)
    except (TypeError, ValueError):
        _log.debug(
            "Skipping %s: assistant_id %r is not int-coercible",
            label,
            assistant_id,
        )
        return False

    base_url = _base_url()
    admin_key = _admin_key()
    if not base_url or not admin_key:
        _log.debug(
            "Skipping %s for %d: orchestra URL or admin key missing",
            label,
            agent_id_int,
        )
        return False

    try:
        from unify.utils import http

        url = f"{base_url.rstrip('/')}/admin/assistant/{agent_id_int}/{action_path}"
        headers = {"Authorization": f"Bearer {admin_key}"}
        resp = http.post(url, headers=headers, timeout=10)
        if 200 <= resp.status_code < 300:
            _log.info("%s succeeded for assistant %d", label, agent_id_int)
            return True
        _log.warning(
            "%s for %d returned %d: %s",
            label,
            agent_id_int,
            resp.status_code,
            getattr(resp, "text", ""),
        )
        return False
    except Exception as exc:
        _log.warning("%s for %d failed: %s", label, agent_id_int, exc)
        return False


def opt_out_of_inactivity_followups_via_orchestra(
    assistant_id: int | str | None,
) -> bool:
    """Stop orchestra from sending inactivity follow-ups via this Coordinator.

    Called by the brain when the boss explicitly declines further
    follow-ups (e.g. "no longer interested", "stop contacting me").
    Sets ``inactivity_followup_opted_out`` server-side; nothing is
    deleted — the user simply won't be followed up with again until they
    opt back in.

    Best-effort: returns True on 2xx, False otherwise. Swallows any
    exception so the brain's call site never breaks because of an HTTP
    hiccup.
    """
    return _post_followup_admin_action(
        assistant_id,
        "opt-out-followups",
        label="opt_out_of_inactivity_followups_via_orchestra",
    )


def opt_in_to_inactivity_followups_via_orchestra(
    assistant_id: int | str | None,
) -> bool:
    """Re-enable inactivity follow-ups via this Coordinator.

    Called by the brain when the boss re-engages after having previously
    opted out. Clears ``inactivity_followup_opted_out`` server-side.
    """
    return _post_followup_admin_action(
        assistant_id,
        "opt-in-followups",
        label="opt_in_to_inactivity_followups_via_orchestra",
    )


# Mediums that already have first-class Console surfaces (live chat + the call
# window), so they must NOT trigger the avatar's "working on a laptop" pose.
COMMS_ACTIVITY_EXCLUDED_MEDIA = ("unify_message", "unify_meet")


def comms_activity_payload(
    medium: object,
    sender_id: int | None,
    self_contact_id: int | None,
) -> Optional[dict]:
    """Build the Console ``comms_activity`` event for a transcript message, or
    ``None`` when the medium should not trigger the working pose.

    Pure (no I/O) so the medium gate and inbound/outbound direction logic stay
    unit-testable. ``unify_message`` / ``unify_meet`` are excluded — they already
    have dedicated Console surfaces.
    """
    medium_str = str(medium or "")
    if not medium_str or medium_str in COMMS_ACTIVITY_EXCLUDED_MEDIA:
        return None
    is_outbound = (
        sender_id is not None
        and self_contact_id is not None
        and int(sender_id) == int(self_contact_id)
    )
    return {"medium": medium_str, "direction": "outbound" if is_outbound else "inbound"}


def publish_comms_activity(message: object, agent_id: int | str | None) -> None:
    """Notify Console that the assistant just sent or received a non-unify comms
    message (email / SMS / WhatsApp / Slack / …) so its call-window avatar can
    rotate into the "working on a laptop" pose.

    Fully guarded, fire-and-forget: the publish runs on a daemon thread and any
    failure is swallowed so the transcript log path never breaks.
    """
    try:
        if agent_id is None:
            return
        from unity.session_details import SESSION_DETAILS

        event = comms_activity_payload(
            getattr(message, "medium", None),
            getattr(message, "sender_id", None),
            SESSION_DETAILS.self_contact_id,
        )
        if event is None:
            return

        def _publish() -> None:
            try:
                from unity.conversation_manager.domains.comms_utils import (
                    _publish_to_assistant_topic,
                )

                _publish_to_assistant_topic(
                    agent_id=agent_id,
                    thread="comms_activity",
                    event=event,
                    timeout=10,
                )
            except Exception as exc:  # noqa: BLE001 – best-effort presence side channel
                _log.debug("publish_comms_activity failed for %s: %s", agent_id, exc)

        import threading

        threading.Thread(
            target=_publish,
            daemon=True,
            name="comms_activity_pub",
        ).start()
    except Exception:
        # A presence side channel must never break transcript logging.
        pass
