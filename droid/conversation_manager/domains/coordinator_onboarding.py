"""Coordinator onboarding narration handlers.

Symmetric with ``inactivity`` for ``InactivityFollowup``: parses the
``droid_system_event`` payload that orchestra publishes whenever a
real user action lands during Coordinator onboarding (workspace
OAuth, integration connect, or session start from the picker) and
surfaces the request to the brain as a notification so it
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

from droid.conversation_manager.events import CoordinatorOnboardingEvent

if TYPE_CHECKING:
    from droid.conversation_manager.conversation_manager import ConversationManager


_NOTIFICATION_TYPE = "CoordinatorOnboarding"

# Per-subtype default instruction used when the orchestra-side
# ``message`` field is missing or empty — the live emit path always
# fills ``message`` in but we keep these as a defensive fallback so a
# malformed payload still produces a useful brain prompt rather than
# a silent skip.
_SUBTYPE_DEFAULT_MESSAGES: dict[str, str] = {
    "workspace_connected": "The user just connected their workspace to you.",
    "integration_connected": "The user just connected a new integration to you.",
    "step_skipped": "The user just skipped an onboarding step.",
    "onboarding_step_started": "The user just started an onboarding step.",
    "onboarding_session_started": (
        "The user just opened the onboarding session with you — they are "
        "waiting for you to open with one short turn."
    ),
}


# Subtype constants used in branch logic below — kept in sync with
# the orchestra-side ``SUBTYPE_*`` constants in
# ``orchestra/services/coordinator_service.py``.
_SUBTYPE_ONBOARDING_SESSION_STARTED = "onboarding_session_started"
_SUBTYPE_STEP_SKIPPED = "step_skipped"
_SUBTYPE_STEP_STARTED = "onboarding_step_started"
_SUBTYPE_REFERENCE_QUIZ_CLUE_REQUESTED = "reference_quiz_clue_requested"


def _detail_string(details: dict[str, Any], key: str) -> str:
    value = details.get(key)
    return value.strip() if isinstance(value, str) else ""


def _coordinator_onboarding_event_from_payload(
    payload: dict[str, Any],
    *,
    message: str = "",
) -> CoordinatorOnboardingEvent | None:
    """Build a :class:`CoordinatorOnboardingEvent` from a comms payload.

    Mirrors :func:`_inactivity_followup_event_from_payload`: the
    adapter publishes ``droid_system_event`` with
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
    what just happened, (b) ties it back to the Onboarding tab steps,
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
    if event.subtype == _SUBTYPE_REFERENCE_QUIZ_CLUE_REQUESTED:
        details = event.details if isinstance(event.details, dict) else {}
        channel = _detail_string(details, "channel")
        clue = _detail_string(details, "clue")
        quote = _detail_string(details, "quote")
        answer = _detail_string(details, "answer")
        trigger_step_id = _detail_string(details, "trigger_step_id")
        reply_step_id = _detail_string(details, "reply_step_id")
        tool_name = _detail_string(details, "tool_name")
        framing = _detail_string(details, "framing")
        channel_note = f" Target outbound channel: `{channel}`." if channel else ""
        tool_note = (
            f" Use `{tool_name}` for this trigger."
            if tool_name
            else " Use the matching outbound comms tool for this trigger."
        )
        step_note = (
            f" Trigger step id: `{trigger_step_id}`. Reply step id now active in Console: `{reply_step_id}`."
            if trigger_step_id or reply_step_id
            else ""
        )
        clue_note = (
            f' Clue to send exactly as the user-facing clue: "{clue}".' if clue else ""
        )
        quote_note = f' Underlying quote: "{quote}".' if quote else ""
        answer_note = (
            f' Correct answer: "{answer}". Do not reveal it unless the user asks for the answer or is clearly stuck.'
            if answer
            else ""
        )
        framing_note = f" Section framing: {framing}" if framing else ""
        call_note = (
            " If the tool starts a call, put the briefing and framing in the call context "
            "so the spoken sidecar can run the interaction without needing this notification."
            if tool_name.startswith("make_") or "call" in channel
            else ""
        )
        return (
            f"{subtype_hint} {body}{channel_note}{step_note}{clue_note}{quote_note}"
            f"{answer_note}{tool_note}{framing_note}{call_note}"
        ).strip()

    if event.subtype == _SUBTYPE_ONBOARDING_SESSION_STARTED:
        medium = ""
        details = event.details if isinstance(event.details, dict) else {}
        medium_raw = details.get("medium")
        if isinstance(medium_raw, str) and medium_raw:
            medium = medium_raw
        completed_steps = details.get("completed_step_ids")
        skipped_steps = details.get("skipped_step_ids")
        joined = ""
        if isinstance(completed_steps, list) and completed_steps:
            joined = ", ".join(str(item) for item in completed_steps if item)
        skipped_joined = ""
        if isinstance(skipped_steps, list) and skipped_steps:
            skipped_joined = ", ".join(str(item) for item in skipped_steps if item)
        completed_hint = (
            f" These onboarding steps are already done (derived from the "
            f"user's account state — steps finished in earlier sessions are "
            f"included): {joined}."
            if joined
            else (
                " No onboarding steps are done yet — trying a quick email "
                "reply is the first checklist step after meeting."
            )
        )
        skipped_hint = (
            f" These onboarding steps were explicitly skipped by the user: "
            f"{skipped_joined}. Treat them as passed over for now, not done."
            if skipped_joined
            else ""
        )
        guidance = (
            "Open the session with exactly one short message. The next step "
            "to propose is always the FIRST onboarding step that is not "
            "listed as already done or explicitly skipped below — never suggest "
            "a step either list marks resolved (e.g. do not say 'connect your "
            "workspace' when `workspace` is already done or skipped). If you "
            "have *no* prior assistant "
            "messages in the transcript history, introduce yourself briefly "
            "as the user's coordinator assistant, say you'll help them get "
            "set up, and invite them to take that next pending step. If "
            "prior assistant messages exist, skip the intro and open with a "
            "one-sentence recap of what's been done so far plus the single "
            "next pending step — do NOT re-introduce yourself."
        )
        medium_note = ""
        if medium == "call":
            medium_note = (
                " (Voice call: this notification is informational; the call "
                "agent generates its own spoken opener. No chat reply needed.)"
            )
        elif medium == "chat":
            medium_note = " (Chat: send exactly one short chat message.)"
        composed = f"{subtype_hint} {body} {guidance}{completed_hint}{skipped_hint}{medium_note}"
        return composed.strip()

    if event.subtype == _SUBTYPE_STEP_SKIPPED:
        details = event.details if isinstance(event.details, dict) else {}
        step_id = details.get("step_id")
        step_note = (
            f" The skipped step id is `{step_id}`." if isinstance(step_id, str) else ""
        )
        skipped_steps = details.get("skipped_step_ids")
        skipped_joined = ""
        if isinstance(skipped_steps, list) and skipped_steps:
            skipped_joined = ", ".join(str(item) for item in skipped_steps if item)
        skipped_note = (
            f" Steps skipped so far: {skipped_joined}." if skipped_joined else ""
        )
        guidance = (
            "Acknowledge in one short sentence that you'll leave that step for "
            "now, then preview the single next onboarding step that is neither "
            "done nor skipped. Do not say the skipped step is complete."
        )
        return f"{subtype_hint} {body}{step_note}{skipped_note} {guidance}".strip()

    if event.subtype == _SUBTYPE_STEP_STARTED:
        details = event.details if isinstance(event.details, dict) else {}
        step_id = details.get("step_id")
        step_note = (
            f" The active step id is `{step_id}`." if isinstance(step_id, str) else ""
        )
        completed_steps = details.get("completed_step_ids")
        completed_joined = ""
        if isinstance(completed_steps, list) and completed_steps:
            completed_joined = ", ".join(str(item) for item in completed_steps if item)
        skipped_steps = details.get("skipped_step_ids")
        skipped_joined = ""
        if isinstance(skipped_steps, list) and skipped_steps:
            skipped_joined = ", ".join(str(item) for item in skipped_steps if item)
        progress_note = (
            f" Steps already done: {completed_joined}." if completed_joined else ""
        )
        skipped_note = f" Steps skipped: {skipped_joined}." if skipped_joined else ""
        guidance = (
            "Handle this active step according to the onboarding prompt rules. "
            "If the step requires me to initiate a message, send exactly that "
            "message through the required channel. If it requires the user to "
            "message or call me first, give only brief setup guidance and wait "
            "for the channel event. Do not skip ahead to Connect or Delegate "
            "until this step is done or skipped."
        )
        return f"{subtype_hint} {body}{step_note}{progress_note}{skipped_note} {guidance}".strip()

    guidance = (
        "Acknowledge this in one short sentence to the user, name the thing they "
        "just completed, and preview the next pending onboarding step. "
        "Stay celebratory but brief — do not re-list every prior step. If a voice "
        "call is active you MUST speak it by calling "
        'guide_voice_agent(message="...", should_speak=True) — do not send a '
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
    from droid.settings import SETTINGS

    # No Console front-end (public local install): the onboarding flow these
    # events narrate is not visible to the user, so we drop them entirely —
    # no notification, no LLM run — rather than nudge them toward UI steps
    # they cannot see.
    if not SETTINGS.DROID_CONSOLE_UI:
        return False
    # Refresh the standing onboarding progress model from the event's
    # attached render so the slow brain's next turn reflects this change
    # immediately, without waiting for the TTL state fetch.
    if isinstance(event.details, dict):
        cm.set_coordinator_onboarding_render(event.details.get("onboarding"))
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
