"""Inactivity-followup domain helpers for conversation-manager handlers.

Symmetric with ``task_activation`` for ``TaskDue``: parses the wake
reason (cold start) and system event payload (hot path), and surfaces
the request to the brain as a notification so it composes and sends
the re-engagement message via existing comms primitives.

The brain decides between the two template variants
(never-spoke vs spoke-before) by inspecting transcript history when
the notification fires; we deliberately do not bake that choice into
the wake reason, so a single signal covers both paths.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from unify.conversation_manager.events import InactivityFollowup

if TYPE_CHECKING:
    from unify.conversation_manager.conversation_manager import ConversationManager


_NOTIFICATION_TYPE = "Inactivity"
_DEFAULT_REASON = (
    "It has been a while since you exchanged any messages with your contacts. "
    "Compose and send a re-engagement message to your boss now."
)


def _inactivity_followup_event_from_payload(
    payload: dict[str, Any],
    *,
    reason: str = "",
) -> InactivityFollowup | None:
    """Build an :class:`InactivityFollowup` event from a comms payload.

    The hot-path adapter publishes a ``unity_system_event`` with
    ``event_type == "inactivity_followup"`` and an
    ``extra_event_fields`` dict of shape ``{"type": "inactivity_followup"}``.
    Either form lands here; this helper is intentionally permissive
    because the payload is small and one-off.
    """

    if not isinstance(payload, dict):
        return None
    return InactivityFollowup(reason=reason or _DEFAULT_REASON)


def _inactivity_followup_event_from_wake_reason(
    reason: Any,
) -> InactivityFollowup | None:
    """Convert one startup wake-reason payload into an :class:`InactivityFollowup`."""

    if not isinstance(reason, dict) or reason.get("type") != "inactivity_followup":
        return None
    return InactivityFollowup(reason=_DEFAULT_REASON)


def _inactivity_notification_text(event: InactivityFollowup) -> str:
    """Return the slow-brain instruction for one inactivity follow-up."""

    body = (event.reason or _DEFAULT_REASON).strip()
    composition_guidance = (
        "Pick the variant by inspecting transcript history with your "
        "transcripts primitives: if you have never spoken before, frame "
        "the message around the hire date. If you have, follow up on the "
        "most recent topic. Send the message via email using your "
        "existing send_email primitive — the inactivity follow-up "
        "channel is email for now. If you have your own WhatsApp number "
        "assigned, include it in the email body so the boss has a "
        "callback option; if you do not, just send email-only."
    )
    optout_guidance = (
        "If the boss replies and explicitly asks not to be contacted "
        "again (any clear opt-out — 'no longer interested', 'take me off "
        "your list', 'stop contacting me'), call "
        "comms.stop_inactivity_followups() so we won't follow up again. "
        "If they had previously opted out and now re-engage and want to "
        "keep hearing from us, call comms.resume_inactivity_followups(). "
        "Casual chatter alone does NOT change the opt-out state — only an "
        "explicit request does. Nothing is ever deleted either way."
    )
    return f"{body} {composition_guidance} {optout_guidance}"


async def _handle_inactivity_followup_event(
    event: InactivityFollowup,
    cm: "ConversationManager",
) -> bool:
    """Surface one inactivity-followup event to the brain.

    Returns True so the caller can trigger an LLM run.
    """

    cm.notifications_bar.push_notif(
        _NOTIFICATION_TYPE,
        _inactivity_notification_text(event),
        event.timestamp,
    )
    cm._session_logger.info(
        "inactivity_followup",
        "Inactivity follow-up requested; brain will compose re-engagement message.",
    )
    return True
