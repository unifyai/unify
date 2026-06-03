"""Coordinator onboarding narration handlers.

Symmetric with ``inactivity`` for ``InactivityFollowup``: parses the
``unity_system_event`` payload that orchestra publishes whenever a
real user action lands during Coordinator onboarding (workspace
OAuth, integration connect, task create, action start, specialist
hire) and surfaces the request to the brain as a notification so it
composes a short acknowledgement turn.

We deliberately keep the handler dumb: the brain decides the wording
based on the system-prompt rule injected in
``prompt_builders.build_system_prompt`` for ``is_coordinator``
sessions. The notification we push here carries only the structured
event payload + a one-line system instruction; the brain owns the
phrasing.

Gating on ``Coordinator/State.mode == 'onboarding'`` happens on the
orchestra side before the event is ever published, so handlers here
can assume the user is currently in the onboarding flow. Outside of
onboarding the helper simply never runs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from unity.conversation_manager.events import CoordinatorOnboardingEvent

if TYPE_CHECKING:
    from unity.conversation_manager.conversation_manager import ConversationManager


_NOTIFICATION_TYPE = "CoordinatorOnboarding"

# Per-subtype default instruction used when the orchestra-side
# ``message`` field is missing or empty — the live emit path always
# fills ``message`` in but we keep these as a defensive fallback so a
# malformed payload still produces a useful brain prompt rather than
# a silent skip.
_SUBTYPE_DEFAULT_MESSAGES: dict[str, str] = {
    "workspace_connected": "The user just connected their workspace to you.",
    "integration_connected": "The user just connected a new integration to you.",
    "onboarding_session_started": (
        "The user just opened the onboarding session with you — they are "
        "waiting for you to open with one short turn."
    ),
}


# Subtype constants used in branch logic below — kept in sync with
# the orchestra-side ``SUBTYPE_*`` constants in
# ``orchestra/services/coordinator_service.py``.
_SUBTYPE_ONBOARDING_SESSION_STARTED = "onboarding_session_started"


def _coordinator_onboarding_event_from_payload(
    payload: dict[str, Any],
    *,
    message: str = "",
) -> CoordinatorOnboardingEvent | None:
    """Build a :class:`CoordinatorOnboardingEvent` from a comms payload.

    Mirrors :func:`_inactivity_followup_event_from_payload`: the
    adapter publishes ``unity_system_event`` with
    ``event_type == "coordinator_onboarding_event"`` and an
    ``extra_event_fields`` dict carrying ``subtype`` + optional
    ``details``. Either landing here. The helper is intentionally
    permissive — anything that doesn't parse drops to ``None`` and
    the dispatcher silently no-ops.
    """
    if not isinstance(payload, dict):
        return None
    subtype_raw = payload.get("subtype")
    if not isinstance(subtype_raw, str) or not subtype_raw:
        # Fall back to the nested ``extra_event_fields`` shape when
        # the publisher decided not to flatten the payload onto the
        # top-level event dict — keeps both wire-shapes valid.
        extra = payload.get("extra_event_fields")
        if isinstance(extra, dict):
            subtype_raw = extra.get("subtype")
    if not isinstance(subtype_raw, str) or not subtype_raw:
        return None

    details_raw = payload.get("details")
    if not isinstance(details_raw, dict):
        extra = payload.get("extra_event_fields")
        details_raw = extra.get("details") if isinstance(extra, dict) else None
    details = details_raw if isinstance(details_raw, dict) else {}

    resolved_message = (
        message
        or payload.get("message")
        or _SUBTYPE_DEFAULT_MESSAGES.get(subtype_raw, "")
    )

    return CoordinatorOnboardingEvent(
        subtype=subtype_raw,
        message=str(resolved_message),
        details=details,
    )


def _coordinator_onboarding_notification_text(
    event: CoordinatorOnboardingEvent,
) -> str:
    """Compose the brain-facing instruction for one onboarding event.

    Combines the orchestra-supplied human summary (``event.message``)
    with a short tail nudge so the brain reliably (a) acknowledges
    what just happened, (b) ties it back to the onboarding checklist,
    and (c) previews the next pending step. The wording stays
    deliberately terse — extended onboarding guidance lives in the
    system-prompt rule, not the notification.

    The ``onboarding_session_started`` subtype is special: it doesn't
    narrate a milestone, it asks for the *first* line of the session.
    The brain must decide intro-vs-recap by looking at whether prior
    Coordinator messages exist in the transcript — the wording below
    points it at that signal explicitly so the rule lives where the
    decision happens.
    """
    body = (event.message or _SUBTYPE_DEFAULT_MESSAGES.get(event.subtype, "")).strip()
    subtype_hint = f"[onboarding subtype: {event.subtype}]"
    if event.subtype == _SUBTYPE_ONBOARDING_SESSION_STARTED:
        medium = ""
        details = event.details if isinstance(event.details, dict) else {}
        medium_raw = details.get("medium")
        if isinstance(medium_raw, str) and medium_raw:
            medium = medium_raw
        completed_steps = details.get("completed_step_ids")
        completed_hint = ""
        if isinstance(completed_steps, list) and completed_steps:
            joined = ", ".join(str(item) for item in completed_steps if item)
            if joined:
                completed_hint = (
                    f" The console reports these onboarding steps as already done: "
                    f"{joined}."
                )
        guidance = (
            "Open the session with exactly one short message. If you have *no* "
            "prior assistant messages in the transcript history, introduce "
            "yourself briefly as the user's coordinator assistant, say you'll "
            "help them get set up, and invite them to start by connecting their "
            "workspace. If prior assistant messages exist, skip the intro and "
            "open with a one-sentence recap of what's been done so far plus the "
            "single next step — do NOT re-introduce yourself."
        )
        medium_note = ""
        if medium == "call":
            medium_note = (
                " (Voice call: this notification is informational; the call "
                "agent generates its own spoken opener. No chat reply needed.)"
            )
        elif medium == "chat":
            medium_note = " (Chat: send exactly one short chat message.)"
        composed = f"{subtype_hint} {body} {guidance}{completed_hint}{medium_note}"
        return composed.strip()

    guidance = (
        "Acknowledge this in one short sentence to the user, name the thing they "
        "just completed, and preview the next pending onboarding checklist step. "
        "Stay celebratory but brief — do not re-list every prior step. If a voice "
        "call is active you MUST speak it by calling "
        'guide_voice_agent(should_speak=True, response_text="...") — do not send a '
        "chat message during a call (it is silent to the caller). Otherwise send a "
        "single chat message."
    )
    if not body:
        return f"{subtype_hint} {guidance}"
    return f"{subtype_hint} {body} {guidance}"


async def _handle_coordinator_onboarding_event(
    event: CoordinatorOnboardingEvent,
    cm: "ConversationManager",
) -> bool:
    """Surface one onboarding event to the brain.

    Returns True when the caller should trigger an LLM run — the
    brain's next turn composes the acknowledgement and routes it
    through the existing chat / voice primitives.

    The ``onboarding_session_started`` event with ``medium == 'call'``
    is the one exception: the voice agent's own sidecar greeting
    already produces the opener for that branch, so triggering a
    slow-brain run on top would duplicate the opener (the brain
    would emit a chat message right as the call is connecting). We
    still push the notification — keeps the brain aware that the
    user just opened a session in case it matters for a later turn —
    but suppress the immediate run.
    """
    cm.notifications_bar.push_notif(
        _NOTIFICATION_TYPE,
        _coordinator_onboarding_notification_text(event),
        event.timestamp,
    )
    cm._session_logger.info(
        "coordinator_onboarding_event",
        "Coordinator onboarding narration requested; "
        "brain will acknowledge subtype=%s." % event.subtype,
    )
    if event.subtype == _SUBTYPE_ONBOARDING_SESSION_STARTED:
        details = event.details if isinstance(event.details, dict) else {}
        if details.get("medium") == "call":
            return False
    return True
