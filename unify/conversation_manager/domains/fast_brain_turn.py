"""Unified fast-brain turn selection for live voice calls.

On each user turn the fast brain emits one structured decision:
classification + optional content. The slow brain composes substantive answers;
this layer covers silence, brief fillers, pure social replies, and interrupted
line resumption.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any, Literal, Sequence

from pydantic import BaseModel, Field

from unify.common.llm_client import new_llm_client
from unify.conversation_manager.events import (
    FAST_BRAIN_TURN_CONTINUATION,
    FAST_BRAIN_TURN_DEFER,
    FAST_BRAIN_TURN_HANG_UP,
    FAST_BRAIN_TURN_SILENCE,
    FAST_BRAIN_TURN_SMALLTALK,
)
from unify.logger import LOGGER
from unify.settings import SETTINGS

_DEFAULT_PHRASE = "One moment."
_MAX_DEFER_CHARS = 160
_MAX_SMALLTALK_CHARS = 300
# Briefed replies conduct a pre-scripted interaction (confirmations, wrap-up
# lines), so they get more room than ordinary small talk before being coerced
# to a defer.
_MAX_BRIEFED_SMALLTALK_CHARS = 600
_MAX_FAREWELL_CHARS = 200
_DEFAULT_FAREWELL = "Alright — bye for now!"

_RESUME_LEAD_INS = (
    "Sorry — as I was saying,",
    "Right, where was I —",
    "Okay, picking up where I left off —",
    "So, to finish that thought —",
    "Anyway, as I was saying —",
    "Right, continuing —",
    "Sorry about that — so,",
)

_IDLE_STATUS_SMALLTALK_GUARDRAIL = (
    "[system] Idle status small-talk is available for this turn. The runtime has "
    "confirmed that no action is in flight, no assistant message was sent "
    "recently, and no spoken line is pending. If the caller's WHOLE turn is a "
    "casual idle-status question like 'what are you doing?', 'what are you up "
    "to?', or 'why are you on your laptop?', you may answer with a playful "
    "non-work aside. The assistant is often visually rendered as working on a "
    "laptop, so make it feel like you are passing time there: 'Nothing "
    "important, just playing Snake for a minute', 'Nothing important, just "
    "stuck on a Sudoku', 'Nothing important, just losing at Mario Kart', or "
    "'Nothing important, just playing Tetris'. Vary the game naturally. Do NOT "
    "claim to be doing real work, checking anything, sending anything, waiting "
    "on a tool, or monitoring an action. If the turn asks for real status or "
    "mentions any actual task, action, message, call, file, data, or result, "
    "use classification defer."
)

_ALREADY_DEFERRED_NOTE = """\
You have already deferred once and the full answer still has not landed. Do NOT
repeat the same deferral wording or fall back on hollow "still on it" status lines.

If they asked a direct yes/no again, answer with Yes / No / I think so / I don't
think so / I'm not sure first, then a varied defer ("Still checking — yes, it
should be through soon." / "Not yet — almost there, let me confirm.").

If they asked timing or why again, vary the hedge ("Shouldn't be much longer —
let me see." / "I'm not sure yet — checking now.").

If they gave you space ("take your time", "thanks"), simply thank them — do not
start another lookup line."""

_GUIDANCE_NOTE = """\
The smarter system has handed you a short note to help with THIS moment:

{guidance}

Use it ONLY to directly respond to what the caller JUST said (e.g. confirm or
answer the specific thing they just asked). This is the one case where you may
give that piece of real information. Follow any instruction in the note exactly —
especially any "do not reveal / only confirm if…" constraint. NEVER volunteer it,
bring it up unprompted, or use it for anything they did not just ask about. If
their message is unrelated to the note, ignore the note and reply as normal. Keep
it to one short line."""

_INTERRUPTED_CONTEXT = """\
[system] You were speaking on a live call and the caller cut you off mid-sentence.
The EXACT words you still have left to say are provided below. You do NOT write
those words in content — the runtime resumes them verbatim.

Words still left to say: {resume_text}

Choose classification continuation to resume now, unless the caller EXPLICITLY
redirected (changed subject, told you to stop, asked something your remainder
does not answer, or said they already did what your remainder instructs). In those
cases pick defer, smalltalk, or silence as appropriate and put a brief line in
content if not silence.

continuation is the strong default for greetings ("Hello?"), "go on", agreeing,
partial overlap, or asking why you are calling — lean hard toward continuation."""

_CALL_BRIEFING_CONTEXT = """\
[system] Active call briefing — context, not script. You are on this call for
the reason below. NEVER read the briefing aloud or quote it verbatim; speak
naturally in your own words.

{briefing}

You fully own every interaction the briefing covers: use classification
smalltalk to conduct it yourself — give the real confirmations, answers, and
wrap-up lines it describes. Do NOT defer things the briefing already equips
you to handle. Use defer only for requests clearly outside the briefing's
scope. If the briefing says the interaction concludes after some point,
deliver its concluding line and then stop volunteering new topics — the
system handles whatever follows (ending the call, follow-up messages)."""

_HELD_OPENER_CONTEXT = """\
[system] You just placed this call and have NOT spoken yet — the other person
answered and spoke first, at some length. The EXACT planned opening line for
this call is provided below. You do NOT write those words in content — the
runtime speaks them verbatim.

Planned opening line: {resume_text}

Choose classification continuation to deliver the planned line now if it still
works as a natural reply to what they said — greetings, "who's this?", small
talk, or a long answer that doesn't redirect all favour continuation. If what
they said makes the planned line inappropriate as-is (they raised something
urgent, asked you not to speak, or clearly need something else addressed
first), pick defer, smalltalk, or silence as appropriate and put a brief line
in content if not silence."""

_HANG_UP_GATE_CONTEXT = """\
[system] Ending this call is now sanctioned. The reason it is appropriate to
wrap up: {reason}

An extra classification is available for THIS turn:

**hang_up** — content is a brief, warm closing line (1 short sentence). The
runtime speaks it and then ends the call. Choose hang_up ONLY when the
caller's whole turn is a close — "bye", "thanks, that's all", "talk later",
"sounds good, bye" — and nothing substantive is left owed to them. This should
land at the natural end of the conversation, ideally as your reply to their
goodbye.

Rules:
- Substance first: if their turn asks anything, raises anything new, or an
  interrupted line still needs resuming, handle that normally (defer /
  smalltalk / continuation) — do NOT force the goodbye.
- Never pick hang_up mid-topic just because ending is sanctioned; wait for the
  actual close.
- content must never be empty for hang_up — always say a short goodbye before
  the line drops. Never mention that the call is being ended by a system."""

FAST_BRAIN_TURN_PROMPT = """\
You are the fast, in-the-moment voice on a live call. A slower, smarter version
of you will answer substantive turns moments later. Your job THIS turn: pick ONE
classification and optional content, as JSON.

STAY ONE PERSON — never mention another system, model, agent, or "smarter"
component. Never call yourself "filler".

## Classifications and content rules

**silence** — content MUST be empty (""). Use ONLY when the WHOLE turn is a bare
acknowledgement that the caller heard you or is ready to continue — 'okay', 'ok',
'yeah', 'yep', 'sure', 'right', 'cool', 'mm-hm', 'got it', 'fine', a bare
'thanks' — AND you are NOT waiting on an answer or decision from them. NEVER echo
their acknowledgement back.

CRITICAL — NOT silence when:
- Your last assistant line asked a question (including an interrupted mid-sentence
  question) and their reply agrees, answers, or authorises an action → use defer
  with a brief ack in content (e.g. agreeing to proceed with something you offered → defer).
- They are responding to a choice you offered ("option A or option B").
- When unsure between silence and defer, choose defer.

**defer** — content is ONE short contextual line (max ~160 chars). The slow brain
composes the real answer next. Use defer for anything needing data, tools, actions,
real-world facts, status of work you control, or when unsure. Content may:
- Lead with Yes/No/I think so/I don't think so/I'm not sure on direct questions.
- Give timing/why hedges then defer.
- Acknowledge an action ("Got it — I'll check on that email now.").
- Thank them for patience without starting a new lookup.
NEVER: substantive facts, instructions, hollow "still on it" lines, or bare
"Got it." / "One moment." without naming their topic.

**smalltalk** — content is 1–2 short sentences you fully own without lookups:
social pleasantries, who you are from persona, simple self-context you actually
know, or repeat/clarify your immediately preceding line. Never invent facts.

**continuation** — ONLY when the interrupted-context block is present. content
MUST be empty (""). The runtime resumes your unheard words; do NOT write them.

## Anti-repeat
If an assistant line is shown as your previous line, do not repeat it in content;
say something different or choose continuation/silence as appropriate."""


class FastBrainTurnDecision(BaseModel):
    classification: Literal["silence", "defer", "smalltalk"]
    content: str = Field(
        default="",
        description="Spoken line for defer/smalltalk; empty for silence.",
    )


class FastBrainInterruptedTurnDecision(BaseModel):
    classification: Literal["silence", "defer", "smalltalk", "continuation"]
    content: str = Field(
        default="",
        description="Spoken line for defer/smalltalk; empty for silence/continuation.",
    )


class FastBrainGatedTurnDecision(BaseModel):
    classification: Literal["silence", "defer", "smalltalk", "hang_up"]
    content: str = Field(
        default="",
        description=(
            "Spoken line for defer/smalltalk/hang_up; empty for silence. For "
            "hang_up this is the farewell spoken before the call ends."
        ),
    )


class FastBrainInterruptedGatedTurnDecision(BaseModel):
    classification: Literal[
        "silence",
        "defer",
        "smalltalk",
        "continuation",
        "hang_up",
    ]
    content: str = Field(
        default="",
        description=(
            "Spoken line for defer/smalltalk/hang_up; empty for "
            "silence/continuation. For hang_up this is the farewell spoken "
            "before the call ends."
        ),
    )


def _response_model(
    *,
    interrupted: bool,
    hang_up_gated: bool,
) -> type[BaseModel]:
    if interrupted and hang_up_gated:
        return FastBrainInterruptedGatedTurnDecision
    if interrupted:
        return FastBrainInterruptedTurnDecision
    if hang_up_gated:
        return FastBrainGatedTurnDecision
    return FastBrainTurnDecision


@dataclass(frozen=True)
class PendingContinuation:
    """A substantive line the caller has not heard (fully or partially).

    ``spoken_prefix`` empty means nothing of the line was ever heard — either a
    barge-in landed before any audio, or this is a held call opener that was
    never spoken. Such lines resume verbatim with no "as I was saying" lead-in.
    """

    resume_text: str
    remainder: str
    spoken_prefix: str

    @property
    def heard_prefix(self) -> bool:
        return bool(self.spoken_prefix.strip())


@dataclass(frozen=True)
class ResolvedFastBrainTurn:
    classification: str
    intended_speech: str
    declined_continuation: bool = False


def pick_resume_lead_in() -> str:
    """Fixed bridge phrase prepended to a resumed line (never model-authored)."""
    return random.choice(_RESUME_LEAD_INS)


def compute_resume_text(full: str, spoken: str) -> str:
    """Return the unheard tail of ``full``, backed up to a clean resume point."""
    full = (full or "").strip()
    spoken = (spoken or "").strip()
    if not full:
        return ""
    if not spoken or not full.startswith(spoken):
        return full
    boundary = max(spoken.rfind(c) for c in ".!?")
    if boundary == -1:
        return full[len(spoken) :].strip()
    return full[boundary + 1 :].strip()


def build_fast_brain_turn_messages(
    *,
    system_prompt: str,
    history_messages: Sequence[dict[str, Any]],
    user_text: str,
    pending_continuation: PendingContinuation | None,
    already_deferred: bool,
    guidance: str,
    idle_status_smalltalk: bool,
    recent_assistant_text: str,
    hang_up_gate_reason: str | None = None,
    briefing: str = "",
) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
    ]
    messages.extend(dict(message) for message in history_messages)
    messages.append({"role": "system", "content": FAST_BRAIN_TURN_PROMPT})
    briefing = (briefing or "").strip()
    if briefing:
        messages.append(
            {
                "role": "system",
                "content": _CALL_BRIEFING_CONTEXT.format(briefing=briefing),
            },
        )
    if pending_continuation is not None:
        template = (
            _INTERRUPTED_CONTEXT
            if pending_continuation.heard_prefix
            else _HELD_OPENER_CONTEXT
        )
        messages.append(
            {
                "role": "system",
                "content": template.format(
                    resume_text=pending_continuation.resume_text.strip(),
                ),
            },
        )
    if hang_up_gate_reason is not None:
        messages.append(
            {
                "role": "system",
                "content": _HANG_UP_GATE_CONTEXT.format(
                    reason=hang_up_gate_reason.strip()
                    or "the conversation " "has reached its natural end",
                ),
            },
        )
    if idle_status_smalltalk:
        messages.append(
            {"role": "system", "content": _IDLE_STATUS_SMALLTALK_GUARDRAIL},
        )
    note = (guidance or "").strip()
    if note:
        messages.append(
            {"role": "system", "content": _GUIDANCE_NOTE.format(guidance=note)},
        )
    if already_deferred:
        messages.append({"role": "system", "content": _ALREADY_DEFERRED_NOTE})
    prev = (recent_assistant_text or "").strip()
    if prev:
        messages.append({"role": "assistant", "content": prev})
        messages.append(
            {
                "role": "system",
                "content": "That was your previous line — do not repeat it; "
                "say something different.",
            },
        )
    messages.append({"role": "user", "content": user_text.strip()})
    return messages


def _resolve_content(
    classification: str,
    content: str,
    *,
    pending_continuation: PendingContinuation | None,
    hang_up_gated: bool = False,
    briefed: bool = False,
) -> ResolvedFastBrainTurn:
    text = " ".join((content or "").split()).strip()

    if classification == FAST_BRAIN_TURN_HANG_UP:
        if not hang_up_gated:
            LOGGER.warning(
                "Fast brain returned hang_up without an armed gate; deferring",
            )
            return ResolvedFastBrainTurn(
                classification=FAST_BRAIN_TURN_DEFER,
                intended_speech=text[:_MAX_DEFER_CHARS] or _DEFAULT_PHRASE,
                declined_continuation=pending_continuation is not None,
            )
        if not text or len(text) > _MAX_FAREWELL_CHARS:
            text = _DEFAULT_FAREWELL
        return ResolvedFastBrainTurn(
            classification=FAST_BRAIN_TURN_HANG_UP,
            intended_speech=text,
            declined_continuation=pending_continuation is not None,
        )

    if classification == FAST_BRAIN_TURN_CONTINUATION:
        if pending_continuation is None:
            LOGGER.warning(
                "Fast brain returned continuation without pending context; deferring",
            )
            return ResolvedFastBrainTurn(
                classification=FAST_BRAIN_TURN_DEFER,
                intended_speech=_DEFAULT_PHRASE,
                declined_continuation=False,
            )
        resume = pending_continuation.resume_text.strip()
        # A lead-in only makes sense when the caller actually heard the start
        # of the line; a held/unheard line is delivered verbatim.
        if pending_continuation.heard_prefix:
            speech = f"{pick_resume_lead_in()} {resume}".strip()
        else:
            speech = resume
        return ResolvedFastBrainTurn(
            classification=FAST_BRAIN_TURN_CONTINUATION,
            intended_speech=speech,
            declined_continuation=False,
        )

    if classification == FAST_BRAIN_TURN_SILENCE:
        if text:
            LOGGER.warning(
                "Fast brain silence with non-empty content; coercing to defer",
            )
            if len(text) > _MAX_DEFER_CHARS:
                text = _DEFAULT_PHRASE
            return ResolvedFastBrainTurn(
                classification=FAST_BRAIN_TURN_DEFER,
                intended_speech=text,
                declined_continuation=pending_continuation is not None,
            )
        return ResolvedFastBrainTurn(
            classification=FAST_BRAIN_TURN_SILENCE,
            intended_speech="",
            declined_continuation=pending_continuation is not None,
        )

    if classification == FAST_BRAIN_TURN_SMALLTALK:
        smalltalk_cap = (
            _MAX_BRIEFED_SMALLTALK_CHARS if briefed else _MAX_SMALLTALK_CHARS
        )
        if not text or len(text) > smalltalk_cap:
            return ResolvedFastBrainTurn(
                classification=FAST_BRAIN_TURN_DEFER,
                intended_speech=_DEFAULT_PHRASE,
                declined_continuation=pending_continuation is not None,
            )
        return ResolvedFastBrainTurn(
            classification=FAST_BRAIN_TURN_SMALLTALK,
            intended_speech=text,
            declined_continuation=pending_continuation is not None,
        )

    # defer
    if not text or len(text) > _MAX_DEFER_CHARS:
        text = _DEFAULT_PHRASE
    return ResolvedFastBrainTurn(
        classification=FAST_BRAIN_TURN_DEFER,
        intended_speech=text,
        declined_continuation=pending_continuation is not None,
    )


def _wire_classification(raw: str) -> str:
    key = (raw or "").strip().lower()
    if key == "silence":
        return FAST_BRAIN_TURN_SILENCE
    if key == "smalltalk":
        return FAST_BRAIN_TURN_SMALLTALK
    if key == "continuation":
        return FAST_BRAIN_TURN_CONTINUATION
    if key == "hang_up":
        return FAST_BRAIN_TURN_HANG_UP
    return FAST_BRAIN_TURN_DEFER


async def select_fast_brain_turn(
    *,
    user_text: str,
    system_prompt: str,
    history_messages: Sequence[dict[str, Any]],
    pending_continuation: PendingContinuation | None,
    already_deferred: bool,
    guidance: str,
    idle_status_smalltalk: bool,
    recent_assistant_text: str = "",
    hang_up_gate_reason: str | None = None,
    briefing: str = "",
) -> ResolvedFastBrainTurn:
    """Select classification and spoken content for one fast-brain user turn."""
    if not (user_text or "").strip():
        return ResolvedFastBrainTurn(
            classification=FAST_BRAIN_TURN_DEFER,
            intended_speech=_DEFAULT_PHRASE,
            declined_continuation=False,
        )

    hang_up_gated = hang_up_gate_reason is not None
    response_model = _response_model(
        interrupted=pending_continuation is not None,
        hang_up_gated=hang_up_gated,
    )
    messages = build_fast_brain_turn_messages(
        system_prompt=system_prompt,
        history_messages=history_messages,
        user_text=user_text,
        pending_continuation=pending_continuation,
        already_deferred=already_deferred,
        guidance=guidance,
        idle_status_smalltalk=idle_status_smalltalk,
        recent_assistant_text=recent_assistant_text,
        hang_up_gate_reason=hang_up_gate_reason,
        briefing=briefing,
    )

    try:
        client = new_llm_client(
            SETTINGS.conversation.FAST_BRAIN_MODEL,
            origin="FastBrain.turn",
            reasoning_effort="low",
        )
        client.set_response_format(response_model)
        raw = await client.generate(messages=messages)
        decision = response_model.model_validate_json(raw)
        classification = _wire_classification(decision.classification)
        return _resolve_content(
            classification,
            decision.content,
            pending_continuation=pending_continuation,
            hang_up_gated=hang_up_gated,
            briefed=bool(briefing.strip()),
        )
    except Exception as exc:
        LOGGER.warning(f"Fast brain turn selection failed; deferring: {exc}")
        return ResolvedFastBrainTurn(
            classification=FAST_BRAIN_TURN_DEFER,
            intended_speech=_DEFAULT_PHRASE,
            declined_continuation=pending_continuation is not None,
        )
