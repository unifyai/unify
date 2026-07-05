"""Coordinator onboarding narration handlers.

Symmetric with ``inactivity`` for ``InactivityFollowup``: parses the
``unity_system_event`` payload that orchestra publishes whenever a
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

Gating on ``Coordinator/State.onboarding_active`` happens on the
orchestra side before the event is ever published, so handlers here
can assume the user is currently in the onboarding flow. Outside of
onboarding the helper simply never runs.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from unify.conversation_manager.events import CoordinatorOnboardingEvent
from unify.conversation_manager.medium_scripts.call import (
    COORDINATOR_ONBOARDING_CHAT_INTRO,
)
from unify.session_details import SESSION_DETAILS

if TYPE_CHECKING:
    from unify.conversation_manager.conversation_manager import ConversationManager


_NOTIFICATION_TYPE = "CoordinatorOnboarding"

# Total wall-clock delay from ``chat_intro_armed_at`` until the scripted
# opener is sent. Pairs with Console's typing indicator timing.
_COORDINATOR_ONBOARDING_CHAT_INTRO_TOTAL_DELAY_SECONDS = 3.5

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
    "onboarding_step_reset": (
        "The user just reset a previously-completed onboarding step — it is "
        "no longer done."
    ),
    "onboarding_step_completed": (
        "An onboarding step you were working on is now marked complete."
    ),
    "onboarding_render_updated": (
        "The onboarding checklist progress updated from durable account state."
    ),
    "onboarding_session_started": (
        "The user just opened the onboarding session with you — they are "
        "waiting for you to open with one short turn."
    ),
    "task_beat_requested": (
        "The user wants to set up a task with you but hasn't said what yet — "
        "ask them in one short message what they'd like."
    ),
    "task_chip_requested": (
        "The user picked an example task — set it up now with your task tools "
        "and confirm it in one short message."
    ),
    "learning_beat_requested": (
        "The user clicked the Learning tutorial row — run the guided "
        "expenses-etl correction demo now."
    ),
}


# Subtype constants used in branch logic below — kept in sync with
# the orchestra-side ``SUBTYPE_*`` constants in
# ``orchestra/services/coordinator_service.py``.
_SUBTYPE_ONBOARDING_SESSION_STARTED = "onboarding_session_started"
_SUBTYPE_STEP_SKIPPED = "step_skipped"
_SUBTYPE_STEP_STARTED = "onboarding_step_started"
_SUBTYPE_STEP_RESET = "onboarding_step_reset"
_SUBTYPE_STEP_COMPLETED = "onboarding_step_completed"
_SUBTYPE_ONBOARDING_RENDER_UPDATED = "onboarding_render_updated"
_SUBTYPE_REFERENCE_QUIZ_CLUE_REQUESTED = "reference_quiz_clue_requested"
_SUBTYPE_WORKSPACE_DEMO_REQUESTED = "workspace_demo_requested"
_SUBTYPE_TASK_BEAT_REQUESTED = "task_beat_requested"
_SUBTYPE_TASK_CHIP_REQUESTED = "task_chip_requested"
_SUBTYPE_LEARNING_BEAT_REQUESTED = "learning_beat_requested"
_LEARNING_BEAT_CHANNEL = "learning_beat"
_ONBOARDING_STEP_LEARN_FROM_CORRECTION = "learn-from-correction"
_ONBOARDING_LEARNING_PHASE_FIRST = "first_attempt"

_IMMEDIATE_TRIGGER_ACK_GUIDANCE = (
    "Mandatory: the user's checklist click has no visible UI feedback until I "
    "speak. In this same LLM turn, send one brief acknowledgement on the active "
    "channel (e.g. 'Checking your mailbox now', 'On it — pulling that up'). "
    "If I also call act or another long-running tool, the acknowledgement and "
    "that tool call MUST be in the same response — never wait for act to finish. "
    "The main deliverable (summary, clue, demo output) is separate and may come "
    "on a later turn; the ack only confirms the click registered."
)

_SUBTYPES_WITHOUT_USER_ACK = frozenset(
    {
        _SUBTYPE_ONBOARDING_SESSION_STARTED,
        _SUBTYPE_STEP_RESET,
        _SUBTYPE_STEP_COMPLETED,
        _SUBTYPE_ONBOARDING_RENDER_UPDATED,
    },
)


def _append_onboarding_trigger_ack_guidance(text: str, subtype: str) -> str:
    """Append mandatory immediate-ack suffix for step-trigger notifications."""
    stripped = text.strip()
    if subtype in _SUBTYPES_WITHOUT_USER_ACK:
        return stripped
    return f"{stripped} {_IMMEDIATE_TRIGGER_ACK_GUIDANCE}".strip()


def _boss_thread_has_assistant_unify_message(cm: "ConversationManager") -> bool:
    from unify.comms.medium import Medium
    from unify.conversation_manager.domains.contact_index import UnifyMessage

    boss_id = int(
        getattr(cm, "boss_contact_id", None) or SESSION_DETAILS.boss_contact_id or 1,
    )
    contact_index = getattr(cm, "contact_index", None)
    if contact_index is None:
        return False
    for msg in contact_index.get_messages_for_contact(boss_id, Medium.UNIFY_MESSAGE):
        if isinstance(msg, UnifyMessage) and msg.role == "assistant":
            return True
    return False


def _should_deliver_coordinator_chat_intro(cm: "ConversationManager") -> bool:
    return (
        cm.coordinator_onboarding_active
        and cm.coordinator_intro_watched
        and cm.coordinator_pending_chat_intro
        and not _boss_thread_has_assistant_unify_message(cm)
    )


def _remaining_chat_intro_delay_seconds(cm: "ConversationManager") -> float:
    armed_at = cm.coordinator_chat_intro_armed_at
    if not isinstance(armed_at, str) or not armed_at.strip():
        return _COORDINATOR_ONBOARDING_CHAT_INTRO_TOTAL_DELAY_SECONDS
    try:
        armed = datetime.fromisoformat(armed_at.replace("Z", "+00:00"))
    except ValueError:
        return _COORDINATOR_ONBOARDING_CHAT_INTRO_TOTAL_DELAY_SECONDS
    elapsed = (datetime.now(timezone.utc) - armed).total_seconds()
    return max(0.0, _COORDINATOR_ONBOARDING_CHAT_INTRO_TOTAL_DELAY_SECONDS - elapsed)


async def _deliver_coordinator_chat_intro_task(cm: "ConversationManager") -> None:
    from unify.conversation_manager.domains.brain_action_tools import (
        ConversationManagerBrainActionTools,
    )

    delay = _remaining_chat_intro_delay_seconds(cm)
    if delay > 0:
        await asyncio.sleep(delay)
    if not _should_deliver_coordinator_chat_intro(cm):
        return
    tools = ConversationManagerBrainActionTools(cm)
    await tools.send_unify_message_to_boss(content=COORDINATOR_ONBOARDING_CHAT_INTRO)
    await cm._patch_coordinator_pending_chat_intro(pending=False)
    cm._session_logger.info(
        "coordinator_onboarding_event",
        "Coordinator onboarding chat intro delivered.",
    )


def schedule_coordinator_chat_intro_delivery(cm: "ConversationManager") -> bool:
    """Arm the scripted chat opener when durable state says it is due."""
    if not _should_deliver_coordinator_chat_intro(cm):
        return False
    task = getattr(cm, "_coordinator_chat_intro_delivery_task", None)
    if task is not None and not task.done():
        return True
    cm._coordinator_chat_intro_delivery_task = asyncio.create_task(
        _deliver_coordinator_chat_intro_task(cm),
    )
    return True


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
        trigger_step_id = _detail_string(details, "trigger_step_id")
        reply_step_id = _detail_string(details, "reply_step_id")
        tool_name = _detail_string(details, "tool_name")
        framing = _detail_string(details, "framing")
        interaction = details.get("interaction")
        channel_note = f" Target channel: `{channel}`." if channel else ""
        tool_note = (
            f" The outbound tool for this channel is `{tool_name}`."
            if tool_name
            else " Use the matching outbound comms tool for this channel."
        )
        step_note = (
            f" Trigger step id: `{trigger_step_id}`. Reply step id now active in Console: `{reply_step_id}`."
            if trigger_step_id or reply_step_id
            else ""
        )
        # The click unlocks the outbound tool; verbal consent on a call does not.
        poll_note = (
            " This notification means the user clicked the trigger row — that "
            "click unlocked my outbound tool. A verbal ask or 'go ahead' on a "
            "call before they clicked does NOT count; if that happened, tell them "
            "to click the matching row in the Onboarding checklist. If I have "
            "already sent a clue on this channel for this step, I do NOT send "
            "another — I confirm it's on its way. I send a clue now only if none "
            "has gone out yet after their click."
        )
        clue_note = (
            " I invent my own short sci-fi quote clue on the spot — there is "
            "no supplied clue or answer, and I keep the answer to myself. "
            "User-facing lines stay minimal (one sentence that we're testing "
            "the channel with a quick sci-fi quiz); I never list genres or "
            "franchises."
        )
        framing_note = f" Section framing: {framing}" if framing else ""
        interaction_note = (
            " Structured interaction: reference_quiz. One short sentence of "
            "context before the clue — we're testing the channel with a quick "
            "sci-fi quiz — not a genre list or franchise rundown."
            if isinstance(interaction, dict)
            and interaction.get("type") == "reference_quiz"
            else ""
        )
        call_note = (
            " If the tool starts a call, put the full spoken line in the required "
            "`opener` argument — verbatim at call start — so the voice agent does "
            "not need this notification repeated."
            if tool_name.startswith("make_") or "call" in channel
            else ""
        )
        text = (
            f"{subtype_hint} {body}{channel_note}{step_note}{poll_note}{clue_note}"
            f"{tool_note}{framing_note}{interaction_note}{call_note}"
        ).strip()
        return _append_onboarding_trigger_ack_guidance(text, event.subtype)

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
                " No onboarding steps are done yet — propose the first valid "
                "next target from the live progress block."
            )
        )
        skipped_hint = (
            f" These onboarding steps were explicitly skipped by the user: "
            f"{skipped_joined}. Treat them as passed over for now, not done."
            if skipped_joined
            else ""
        )
        guidance = (
            "Open the session with exactly one message. The next step to "
            "propose is the first entry in the valid next-steps list in the "
            "'My onboarding progress (live)' section (that list is "
            "priority-ordered and already excludes done, skipped, and locked "
            "steps) — never suggest a step that isn't a valid next target "
            "(e.g. do not say 'connect your workspace' when `workspace` is "
            "already done or skipped). Frame that next step as clicking its row "
            "in the Onboarding checklist before mentioning any destination tab, "
            "dialog, or settings page — for communication trigger steps (email, "
            "SMS, WhatsApp, phone, etc.), the click is required before I can "
            "send; verbal consent on the call does not substitute. If there is no evidence that the user "
            "has already had a meaningful onboarding orientation from you, "
            "introduce yourself as T-W1N, frame yourself as their digital twin "
            "or stand-in, explain that onboarding is a shared walkthrough from "
            "communication basics into workspace access, integrations, "
            "recurring tasks, computer use, and other sections, invite them "
            "to take that first valid next step, and mention they can pause "
            "onboarding and resume later. If prior assistant messages, "
            "completed/skipped steps, or the user's current section show that "
            "orientation already happened, skip the intro and open with a "
            "one-sentence recap of what's been done so far plus that first "
            "valid next step — do NOT re-introduce yourself."
        )
        if medium == "chat":
            guidance = (
                "The onboarding picker resolved to chat and the fixed opener "
                "was already delivered verbatim in the transcript. Stay silent "
                "until the user sends their next message — do not send another "
                "opening turn or replay the intro/overview."
            )
        medium_note = ""
        if medium == "call":
            medium_note = (
                " (Voice call: this notification is informational; the call "
                "agent generates its own spoken opener. No chat reply needed.)"
            )
        elif medium == "chat":
            medium_note = (
                " (Chat: the scripted opener is already in the transcript — "
                "no chat reply on this event.)"
            )
        text = f"{subtype_hint} {body} {guidance}{completed_hint}{skipped_hint}{medium_note}"
        return _append_onboarding_trigger_ack_guidance(text.strip(), event.subtype)

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
            "now, then preview the first valid next target from the 'My "
            "onboarding progress (live)' section (the ordered next-steps list). "
            "Frame the next target as clicking its row in the Onboarding "
            "checklist. Do not say the skipped step is complete."
        )
        text = f"{subtype_hint} {body}{step_note}{skipped_note} {guidance}".strip()
        return _append_onboarding_trigger_ack_guidance(text, event.subtype)

    if event.subtype == _SUBTYPE_STEP_RESET:
        details = event.details if isinstance(event.details, dict) else {}
        step_id = details.get("step_id")
        step_note = (
            f" The reset step id is `{step_id}`." if isinstance(step_id, str) else ""
        )
        guidance = (
            "Do NOT message the user about this and do NOT run anything — this "
            "is a silent state update. Just note that this step is no longer "
            "complete: treat it as available again, never claim it is done, and "
            "do not re-send any summary or clue for it unless the step is "
            "triggered again. The 'My onboarding progress (live)' block is the "
            "only source of truth for what is done — ignore any earlier "
            "transcript memory of having finished this step."
        )
        text = f"{subtype_hint} {body}{step_note} {guidance}".strip()
        return _append_onboarding_trigger_ack_guidance(text, event.subtype)

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
        text = (
            f"{subtype_hint} {body}{step_note}{progress_note}{skipped_note} {guidance}"
        ).strip()
        return _append_onboarding_trigger_ack_guidance(text, event.subtype)

    if event.subtype in (_SUBTYPE_TASK_BEAT_REQUESTED, _SUBTYPE_TASK_CHIP_REQUESTED):
        # The orchestra-supplied ``body`` already carries the full directive
        # (ask-first for a beat row, create-now for a chip). We only append the
        # task-kind context and the medium-safety rule the message can't know.
        details = event.details if isinstance(event.details, dict) else {}
        task_kind = _detail_string(details, "task_kind")
        kind_note = (
            " (event-triggered 'triggerable task' work)"
            if task_kind == "triggered"
            else " (scheduled task work)" if task_kind == "scheduled" else ""
        )
        medium_note = (
            " Keep it to one short message and don't re-list onboarding steps. "
            "If a voice call is active you MUST speak via "
            'guide_voice_agent(message="...") rather than sending a chat '
            "message; otherwise send a single chat message."
        )
        text = f"{subtype_hint}{kind_note} {body}{medium_note}".strip()
        return _append_onboarding_trigger_ack_guidance(text, event.subtype)

    if event.subtype == _SUBTYPE_LEARNING_BEAT_REQUESTED:
        from unify.conversation_manager.domains.learning_expenses_fixtures import (
            learning_expenses_scenario_prompt_lines,
            learning_expenses_stop_act_for_storage_rule,
            learning_expenses_storage_check_nudge,
            learning_expenses_user_facing_voice,
        )

        details = event.details if isinstance(event.details, dict) else {}
        framing = _detail_string(details, "framing")
        scenario_id = _detail_string(details, "scenario_id")
        replay_hint = _detail_string(details, "replay_hint")
        framing_note = f" Section framing: {framing}" if framing else ""
        scenario_note = f" Scenario id: `{scenario_id}`." if scenario_id else ""
        replay_note = f" Replay hint: {replay_hint}" if replay_hint else ""
        fixture_note = " ".join(learning_expenses_scenario_prompt_lines())
        medium_note = (
            " This is an openly narrated tutorial demo — say so up front. "
            f"{learning_expenses_user_facing_voice()} "
            f"{fixture_note} "
            "Before the first attempt, send the month-N bank export CSVs as "
            "unify_message attachments (one attachment per message). Run a "
            "deliberately naive first pass via act(persist=True) with genuinely "
            "computed numbers (sum every outflow, add abs(Amount) again for each "
            "INTERNAL XFER row on either file including card-side credits, ignore "
            "refunds), state the naive total and explain the mistake in plain "
            "language (no tables or row-by-row breakdowns), suggest the exact "
            "correction text, and WAIT — never send the correction or proceed "
            "on the user's behalf. After their correction, interject_* into the "
            "running persist act with the corrected algorithm and include this "
            f"StorageCheck memoization request verbatim: "
            f"{learning_expenses_storage_check_nudge()} "
            "Then tag the improved deliverable with "
            "onboarding_learning_phase=improved. "
            f"{learning_expenses_stop_act_for_storage_rule()} "
            "The doing loop must not call "
            "GuidanceManager or FunctionManager store tools — StorageCheck "
            "persists after completion; tell the user to open the Brain rail "
            "Guidance and Functions sections and cite what StorageCheck stored — "
            "I have no tool to navigate the Console for them — invite them to ask "
            "for next month's report, and WAIT again "
            "before the replay act. Each phase "
            "deliverable (first attempt, improved version, replay) must be sent "
            "with send_unify_message using onboarding_learning_phase "
            "(first_attempt, improved, replay). Brain nudges and attachment "
            "intro messages are not phase deliverables. Tell the user to open "
            "the Actions tab themselves before/during each act run — I have no "
            "tool to navigate the Console for them. "
            "On a live in-app Unify Meet call: narrate spoken beats via "
            "guide_voice_agent, but CSV attachments and all three phase "
            "deliverables MUST still be sent as tagged unify_message chat "
            "messages — a report is a document, not a spoken line. "
            "On off-console channels (plain phone call, WhatsApp call): do not "
            "run the tutorial; say it is a Console exercise and offer to start "
            "when the user is back in the app."
        )
        text = (
            f"{subtype_hint} {body}{framing_note}{scenario_note}{replay_note}"
            f"{medium_note}"
        ).strip()
        return _append_onboarding_trigger_ack_guidance(text, event.subtype)

    if event.subtype == _SUBTYPE_WORKSPACE_DEMO_REQUESTED:
        details = event.details if isinstance(event.details, dict) else {}
        step_id = _detail_string(details, "step_id")
        step_note = f" The demo step id is `{step_id}`." if step_id else ""
        guidance = (
            "The user clicked a workspace demo row: after acking that I am on it, "
            "do the demo task now with my own tools. Read the relevant part of "
            "their connected workspace and deliver one short unify_message summary. "
            "The checklist does NOT "
            "auto-detect that summary, so handling this demo is not finished until "
            "I mark it done by calling set_onboarding_task_state(step_id, "
            "completed=True) after sending the summary. Any reply, tidy-up, or "
            "flag is an optional follow-up I only act on if the user says yes; it "
            "never gates completion. This notification is a poll, not a request to "
            "repeat finished work: if the task is already done I just confirm it "
            "and do not redo it or send a duplicate. If a voice call is active I "
            'speak via guide_voice_agent(message="...") instead of a chat message.'
        )
        text = f"{subtype_hint} {body}{step_note} {guidance}".strip()
        return _append_onboarding_trigger_ack_guidance(text, event.subtype)

    if event.subtype == _SUBTYPE_STEP_COMPLETED:
        # This event confirms a completion the brain itself just made via
        # set_onboarding_task_state. It exists to refresh the render and log the
        # milestone; the handler suppresses the follow-up run so the brain does
        # not acknowledge its own tool call in a second turn. The text is only
        # standing context for a later turn.
        details = event.details if isinstance(event.details, dict) else {}
        step_id = _detail_string(details, "step_id")
        step_note = f" Completed step id: `{step_id}`." if step_id else ""
        text = (
            f"{subtype_hint} {body}{step_note} No action needed now — this "
            "confirms a step I just marked complete; the live progress block is "
            "already updated."
        ).strip()
        return _append_onboarding_trigger_ack_guidance(text, event.subtype)

    guidance = (
        "Acknowledge this in one short sentence to the user, name the thing they "
        "just completed, and preview the next pending onboarding step. "
        "Frame that next step as clicking its row in the Onboarding checklist "
        "before mentioning any destination tab, dialog, or settings page. "
        "Stay celebratory but brief — do not re-list every prior step. If a voice "
        "call is active you MUST speak it by calling "
        'guide_voice_agent(message="...") — do not send a '
        "chat message during a call (it is silent to the caller). Otherwise send a "
        "single chat message."
    )
    if not body:
        text = f"{subtype_hint} {guidance}"
    else:
        text = f"{subtype_hint} {body} {guidance}"
    return _append_onboarding_trigger_ack_guidance(text, event.subtype)


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

    The ``medium == 'chat'`` branch is similar: the handler delivers
    the fixed onboarding opener as a unify_message and suppresses
    the immediate run so the slow brain cannot race a second intro.
    """
    from unify.settings import SETTINGS

    # No Console front-end (public local install): the onboarding flow these
    # events narrate is not visible to the user, so we drop them entirely —
    # no notification, no LLM run — rather than nudge them toward UI steps
    # they cannot see.
    if not SETTINGS.UNITY_CONSOLE_UI:
        return False
    if not cm.coordinator_onboarding_active:
        return False
    # Refresh the standing onboarding progress model from the event's
    # attached render so the slow brain's next turn reflects this change
    # immediately, without waiting for the TTL state fetch.
    if isinstance(event.details, dict):
        cm.set_coordinator_onboarding_render(event.details.get("onboarding"))
        if event.subtype == _SUBTYPE_ONBOARDING_SESSION_STARTED:
            # New session boundary: forget any prior in-session clicks so a
            # stale click can't keep a re-gated channel's tool unlocked.
            cm.clear_onboarding_clicked_trigger_steps()
            cm.clear_active_learning_beat()
        if event.subtype == _SUBTYPE_REFERENCE_QUIZ_CLUE_REQUESTED:
            trace = getattr(cm, "_current_event_trace", None) or {}
            # Unlock this channel's send tool for the session (the click is
            # what tags the outbound so the step can auto-complete).
            cm.record_onboarding_trigger_clicked(event.details.get("trigger_step_id"))
            cm.set_pending_onboarding_outbound(
                event.details,
                origin_event_id=trace.get("event_id", ""),
            )
        if event.subtype == _SUBTYPE_LEARNING_BEAT_REQUESTED:
            from unify.conversation_manager.domains.learning_expenses_fixtures import (
                provision_learning_expenses_fixtures,
            )
            from unify.file_manager.settings import get_local_root

            trace = getattr(cm, "_current_event_trace", None) or {}
            details = event.details if isinstance(event.details, dict) else {}
            provision_learning_expenses_fixtures(get_local_root())
            cm.set_active_learning_beat(details)
            cm.set_pending_onboarding_outbound(
                {
                    **details,
                    "channel": _LEARNING_BEAT_CHANNEL,
                    "onboarding_learning_phase": _ONBOARDING_LEARNING_PHASE_FIRST,
                },
                origin_event_id=trace.get("event_id", ""),
            )
        if event.subtype == _SUBTYPE_STEP_RESET:
            reset_details = event.details if isinstance(event.details, dict) else {}
            reset_step_id = reset_details.get("step_id")
            if reset_step_id == _ONBOARDING_STEP_LEARN_FROM_CORRECTION:
                cm.clear_active_learning_beat(
                    _ONBOARDING_STEP_LEARN_FROM_CORRECTION,
                )
    if event.subtype == _SUBTYPE_ONBOARDING_RENDER_UPDATED:
        return False
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
        medium = details.get("medium")
        if medium == "chat":
            cm._coordinator_state_checked_at = 0.0
            await cm._refresh_coordinator_onboarding_state(force=True)
            schedule_coordinator_chat_intro_delivery(cm)
            return False
        if medium == "call":
            return False
    if event.subtype == _SUBTYPE_STEP_RESET:
        # The render refresh + standing notification above are the whole
        # point of this event; a reset must not make the brain spontaneously
        # message the user, so suppress the immediate run.
        return False
    if event.subtype == _SUBTYPE_STEP_COMPLETED:
        # The completion originated from the brain's own set_onboarding_task_state
        # call, so it already told the user (or will on its current turn). The
        # event only exists to push the freshly-derived render; triggering a run
        # here would make the brain acknowledge its own action in a redundant
        # second turn. Refresh only, no run.
        return False
    return True
