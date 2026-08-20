"""Unified fast-brain turn selection for live voice calls.

On each user turn the fast brain emits one structured decision:
classification + optional content. The slow brain composes substantive answers;
this layer covers silence, brief fillers, pure social replies, and interrupted
line resumption.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, replace
from typing import Any, Literal, Sequence

from pydantic import BaseModel, Field

from unify.common.llm_client import new_llm_client
from unify.conversation_manager.events import (
    FAST_BRAIN_TURN_CONTINUATION,
    FAST_BRAIN_TURN_DEFER,
    FAST_BRAIN_TURN_HANG_UP,
    FAST_BRAIN_TURN_SILENCE,
    FAST_BRAIN_TURN_SMALLTALK,
    FAST_BRAIN_TURN_UNDECIDED,
    GROUP_CALL_MIN_PARTICIPANTS,
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

_PEER_ASSISTANTS_CONTEXT = """\
[system] Multi-assistant call. You are {own_name}. Other AI teammates are on
this call with you: {peers}. Exactly one assistant should respond to each human
turn. Decide whether THIS turn is yours before speaking:

- If the speaker addressed a teammate by name, or a teammate was clearly asked
  to handle it, choose classification silence — even for substantive turns.
- If the speaker said "{own_name}", or a teammate handed the turn to you, it is
  yours: respond normally.
- If nobody was addressed by name, take the turn only when it is plainly about
  work you own or directed at you by context; otherwise choose silence and let
  the better-placed teammate answer.
- Never answer on a teammate's behalf and never speak over them. If you have
  nothing of your own to add, silence is the correct choice.
- To pass a turn that suits a teammate better, use smalltalk with ONE short
  hand-off line naming them (e.g. "Ada, that one's yours.").

Two rules above this block are replaced while a teammate is on the call:

- **silence is not limited to bare acknowledgements here.** A substantive
  question that was put to a teammate takes classification silence with empty
  content. That is the correct output for this turn, not a rule violation.
- **unsure resolves to silence, not defer.** When you genuinely cannot tell
  whose turn it was, stay quiet: a teammate is on this call and can answer, and
  two assistants answering the same question is the failure these rules exist to
  prevent. A `defer` here is not a neutral holding move — it speaks aloud and
  commits you to answering.

That tiebreak covers the unclear case only. If you were plainly the one
addressed — named, or handed the turn — answering is not optional and staying
quiet is the worse failure of the two."""

_STANDING_ADDRESSEE_CONTEXT = """\
[system] The conversation is currently with **{addressee}** — that is who the
last turn anyone could attribute was put to, and it stands until something
changes it.

- A line that continues that exchange is still {addressee}'s, however it is
  phrased and whoever it names. Choose silence.
- What changes it: your own name, a hand-off to you, or a plainly new subject
  put to you. Then the turn is yours and you answer it normally.
- Do not treat "{addressee} has not replied in my transcript" as the exchange
  being over. You cannot hear their replies."""

_UNANSWERED_TURNS_CONTEXT = """\
[system] Lines on this call you did not answer, oldest first:

{lines}

Standing down on a line does not end who it was for. A name is said once and
then governs the exchange that follows it: "Ada, what's the pricing?" makes the
next line — "and when does it expire?" — Ada's too, though it names nobody.

- If the current line continues one of the above, it belongs to whoever that one
  belonged to. Still not you. Choose silence again.
- A follow-up with no name in it is the normal shape of a conversation, not an
  opening for you. Two unnamed lines in a row do not become yours by repetition.
- What ends it is the exchange actually turning to you: your name, a hand-off, or
  a plainly new subject put to you. Until one of those, leave it where it was.

You will not have heard the replies to these, so their going unanswered in your
transcript is not evidence nobody answered them."""

_PEER_TURNS_CONTEXT = """\
[system] What your teammates have said on this call.
{sections}
You did not hear any of that — it reached you over the assistants' own channel,
not your microphone. Everyone on the call heard it, so treat it as already said.

- If what the speaker just said is already answered by one of those lines, the
  turn is handled: choose silence. Do not repeat it, confirm it, or tack
  something on unless you genuinely have what a teammate did not cover.
- A teammate having just taken a turn makes the speaker's next line most likely
  a reply to THEM, not a new question for you. Read it that way unless it names
  you or plainly turns to you.
- These lines are theirs, not yours. Never present one as something you said."""

_PEER_TURNS_SINCE_SECTION = """
Said since the previous line in the conversation — so what the speaker just said
is most likely a response to this, not a new question for you:

{lines}
"""

_PEER_TURNS_EARLIER_SECTION = """
Said earlier on the call, before the previous line. Context, not a live exchange:

{lines}
"""

_GROUP_CALL_CONTEXT = """\
[system] Group call. You are {own_name}. The other people on this call are:
{participants}.

Most turns on a call like this are those people talking to EACH OTHER, not to
you. Someone sitting in a meeting does not answer every sentence they hear, and
neither do you. Decide whether THIS turn is yours before speaking:

- Participants talking between themselves, thinking aloud, or working something
  out together → choose classification silence, even for substantive turns you
  could have answered well. Their line is still transcribed and attributed to
  them, so staying out of it loses nothing.
- Someone said "{own_name}", or the turn is plainly put to you — a question or
  request aimed at you, or a hand-off like "let's ask {own_name}" → it is yours.
  Respond normally, and do NOT stand down merely because another person present
  could also have answered.
- Not hearing your name is NOT evidence the turn was not yours. People address
  you without naming you ("can you pull that up?", "what do we know about this
  account?"). Read who the line is aimed at rather than only whether it names
  you.
- Never answer on a participant's behalf and never speak over one. When someone
  else in the room was the one asked, silence is correct.

One rule above this block is replaced on a call like this: **silence is not
limited to bare acknowledgements here.** A substantive exchange between two
other people takes classification silence with empty content. That is the
correct output for those turns, not a rule violation. A `defer` is not a neutral
holding move — it speaks aloud into their conversation.

The unsure-choose-defer tiebreak still stands, because if nobody else here can
answer for you, silence leaves the turn dropped. Silence is the safe default
only when the turn clearly belonged to someone else. When you were the one
addressed, answering is not optional and staying quiet is the worse failure of
the two."""

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


_ADDRESSED_TO_DESCRIPTION = (
    "Who THIS turn was aimed at, by name. Your own name when it was put to you; "
    "a teammate's or a person's name when it was put to them; an empty string "
    "when you genuinely cannot tell. An unnamed line continuing an exchange "
    "already addressed to somebody names that somebody, not nobody — this is "
    "how the next turn knows who the conversation is still with."
)


class _AddressedTo(BaseModel):
    """Who a turn was aimed at, asked only on a multi-party call.

    Kept off the 1:1 models on purpose. There, every turn is necessarily the
    assistant's, so the field would be a question with one answer — and it would
    add a field to the structured output of every phone call to buy nothing.
    """

    addressed_to: str = Field(default="", description=_ADDRESSED_TO_DESCRIPTION)


class FastBrainMultiPartyTurnDecision(FastBrainTurnDecision, _AddressedTo):
    pass


class FastBrainMultiPartyInterruptedTurnDecision(
    FastBrainInterruptedTurnDecision,
    _AddressedTo,
):
    pass


class FastBrainMultiPartyGatedTurnDecision(FastBrainGatedTurnDecision, _AddressedTo):
    pass


class FastBrainMultiPartyInterruptedGatedTurnDecision(
    FastBrainInterruptedGatedTurnDecision,
    _AddressedTo,
):
    pass


def _response_model(
    *,
    interrupted: bool,
    hang_up_gated: bool,
    multi_party: bool = False,
) -> type[BaseModel]:
    if multi_party:
        if interrupted and hang_up_gated:
            return FastBrainMultiPartyInterruptedGatedTurnDecision
        if interrupted:
            return FastBrainMultiPartyInterruptedTurnDecision
        if hang_up_gated:
            return FastBrainMultiPartyGatedTurnDecision
        return FastBrainMultiPartyTurnDecision
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
    # Who the model judged this turn was aimed at, on a multi-party call. Empty
    # when it could not tell, or when the call is 1:1 and the question does not
    # arise. The runtime carries it into the next turn so an unnamed follow-up
    # does not have to be re-derived from the exchange every time.
    addressed_to: str = ""


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
    peer_assistants: Sequence[str] = (),
    other_participants: Sequence[str] = (),
    peer_turns: Sequence[str] = (),
    peer_turns_earlier: Sequence[str] = (),
    unanswered_turns: Sequence[str] = (),
    standing_addressee: str = "",
    own_name: str = "Assistant",
) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
    ]
    messages.extend(dict(message) for message in history_messages)
    messages.append({"role": "system", "content": FAST_BRAIN_TURN_PROMPT})
    own = (own_name or "").strip() or "Assistant"
    # A group call is the frame; peer assistants refine it, so the broader block
    # lands first. Both can apply at once on an org meet carrying several humans
    # and another assistant, and they do not contradict: one is about who among
    # the people was addressed, the other about which assistant takes the turn.
    participants = [name.strip() for name in other_participants if (name or "").strip()]
    multi_party = len(participants) >= GROUP_CALL_MIN_PARTICIPANTS or bool(
        [name for name in peer_assistants if (name or "").strip()],
    )
    if len(participants) >= GROUP_CALL_MIN_PARTICIPANTS:
        messages.append(
            {
                "role": "system",
                "content": _GROUP_CALL_CONTEXT.format(
                    own_name=own,
                    participants=", ".join(participants),
                ),
            },
        )
    peers = [name.strip() for name in peer_assistants if (name or "").strip()]
    if peers:
        messages.append(
            {
                "role": "system",
                "content": _PEER_ASSISTANTS_CONTEXT.format(
                    own_name=own,
                    peers=", ".join(peers),
                ),
            },
        )
    # What those teammates actually said, when the channel has carried anything.
    # Lands after the etiquette so the rule is in place before the evidence it
    # applies to, and only alongside it: lines with no peer block to interpret
    # them read as unattributed speech the assistant might mistake for its own.
    # Split around the previous line in the conversation rather than listed
    # flat: whether a teammate's answer landed before or after it is the
    # difference between "they already handled this" and "they answered
    # something else earlier", and the model cannot recover that from an
    # undifferentiated list.
    spoken = [line.strip() for line in peer_turns if (line or "").strip()]
    earlier = [line.strip() for line in peer_turns_earlier if (line or "").strip()]
    if peers and (spoken or earlier):
        sections = ""
        if spoken:
            sections += _PEER_TURNS_SINCE_SECTION.format(
                lines="\n".join(f"- {line}" for line in spoken),
            )
        if earlier:
            sections += _PEER_TURNS_EARLIER_SECTION.format(
                lines="\n".join(f"- {line}" for line in earlier),
            )
        messages.append(
            {
                "role": "system",
                "content": _PEER_TURNS_CONTEXT.format(sections=sections),
            },
        )
    # Turns already declined. Gated on the call being multi-party rather than on
    # peers alone: a name governing the lines after it is how people talk to each
    # other, teammate present or not. Off a multi-party call there is nothing to
    # decline, so the buffer feeding this is empty anyway.
    declined = [line.strip() for line in unanswered_turns if (line or "").strip()]
    if multi_party and declined:
        messages.append(
            {
                "role": "system",
                "content": _UNANSWERED_TURNS_CONTEXT.format(
                    lines="\n".join(f'- "{line}"' for line in declined),
                ),
            },
        )
    # The resolved state, last: whom the conversation is with, which the lines
    # above are only the evidence for. Named rather than inferred, so a
    # follow-up does not depend on re-reading the exchange every turn. Never
    # ourselves — an exchange that turned to us is one we answered, and the
    # runtime drops the addressee at that point.
    addressee = (standing_addressee or "").strip()
    if multi_party and addressee and addressee.casefold() != own.casefold():
        messages.append(
            {
                "role": "system",
                "content": _STANDING_ADDRESSEE_CONTEXT.format(addressee=addressee),
            },
        )
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
    peers_present: bool = False,
) -> ResolvedFastBrainTurn:
    """Turn one raw decision into the turn the runtime will act on.

    ``peers_present`` says another assistant is on this call, which changes where
    an unusable decision lands. Everywhere else a malformed or missing decision
    falls back to ``defer``: it speaks a short filler and schedules the slow
    brain, which is right on a 1:1 where staying quiet leaves the caller with
    nothing. With a teammate in the room that same fallback is how an assistant
    answers a question addressed to somebody else — so a decision that expressed
    no usable intent falls back to ``undecided``: nothing is spoken, and the slow
    brain gets the turn and decides whose it was.

    Two things are never converted:

    - a ``defer`` the model actually chose — that is a decision that the turn is
      ours, and the filler is the lead-in to answering it;
    - a ``silence`` the model actually chose — that is a decision to let the turn
      go, and waking the slow brain would relitigate it.
    """
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
        if text and not peers_present:
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
        if text:
            # The stated classification is what the model decided; the stray
            # content is the malformed part. Speaking it would answer over a
            # teammate on the strength of a formatting slip.
            LOGGER.warning(
                "Fast brain silence with non-empty content on a multi-assistant "
                "call; dropping the content and staying silent",
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
            if peers_present:
                LOGGER.warning(
                    "Fast brain smalltalk unusable on a multi-assistant call; "
                    "saying nothing and handing the turn to the slow brain",
                )
                return ResolvedFastBrainTurn(
                    classification=FAST_BRAIN_TURN_UNDECIDED,
                    intended_speech="",
                    declined_continuation=pending_continuation is not None,
                )
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
    peer_assistants: Sequence[str] = (),
    other_participants: Sequence[str] = (),
    peer_turns: Sequence[str] = (),
    peer_turns_earlier: Sequence[str] = (),
    unanswered_turns: Sequence[str] = (),
    standing_addressee: str = "",
    own_name: str = "Assistant",
) -> ResolvedFastBrainTurn:
    """Select classification and spoken content for one fast-brain user turn."""
    peers = [name.strip() for name in peer_assistants if (name or "").strip()]
    peers_present = bool(peers)
    others = [name.strip() for name in other_participants if (name or "").strip()]
    multi_party = peers_present or len(others) >= GROUP_CALL_MIN_PARTICIPANTS
    if not (user_text or "").strip():
        if peers_present:
            # Nothing was said that could have been addressed to anyone. On a
            # 1:1 the filler covers the gap; with a teammate in the room it is
            # just this assistant claiming a turn that does not exist.
            return ResolvedFastBrainTurn(
                classification=FAST_BRAIN_TURN_SILENCE,
                intended_speech="",
                declined_continuation=False,
            )
        return ResolvedFastBrainTurn(
            classification=FAST_BRAIN_TURN_DEFER,
            intended_speech=_DEFAULT_PHRASE,
            declined_continuation=False,
        )

    hang_up_gated = hang_up_gate_reason is not None
    response_model = _response_model(
        interrupted=pending_continuation is not None,
        hang_up_gated=hang_up_gated,
        multi_party=multi_party,
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
        peer_assistants=peers,
        other_participants=other_participants,
        peer_turns=peer_turns,
        peer_turns_earlier=peer_turns_earlier,
        unanswered_turns=unanswered_turns,
        standing_addressee=standing_addressee,
        own_name=own_name,
    )

    # Whose turn this was — and whether an unnamed follow-up still belongs to
    # whoever was named a turn ago — is a harder call than any 1:1 turn asks
    # for, and answering over somebody costs more than the added latency. Paid
    # only on a multi-party call.
    effort = (
        SETTINGS.conversation.FAST_BRAIN_MULTI_PARTY_REASONING_EFFORT
        if multi_party
        else SETTINGS.conversation.FAST_BRAIN_REASONING_EFFORT
    )

    try:
        client = new_llm_client(
            SETTINGS.conversation.FAST_BRAIN_MODEL,
            origin="FastBrain.turn",
            reasoning_effort=effort,
        )
        client.set_response_format(response_model)
        raw = await client.generate(messages=messages)
        decision = response_model.model_validate_json(raw)
        classification = _wire_classification(decision.classification)
        resolved = _resolve_content(
            classification,
            decision.content,
            pending_continuation=pending_continuation,
            hang_up_gated=hang_up_gated,
            briefed=bool(briefing.strip()),
            peers_present=peers_present,
        )
        # Attached after resolution rather than inside it: who a turn was for is
        # orthogonal to what to say about it, and threading it through every
        # branch of that function would only repeat it.
        return replace(
            resolved,
            addressed_to=str(getattr(decision, "addressed_to", "") or "").strip(),
        )
    except Exception as exc:
        if peers_present:
            # No decision was reached, so there is no basis for speaking first
            # and answering second — on a call with a teammate that is how a
            # question meant for them gets answered twice. Say nothing and let
            # the slow brain, which can tell whose turn it was, decide.
            LOGGER.warning(
                f"Fast brain turn selection failed on a multi-assistant call; "
                f"handing the turn to the slow brain: {exc}",
            )
            return ResolvedFastBrainTurn(
                classification=FAST_BRAIN_TURN_UNDECIDED,
                intended_speech="",
                declined_continuation=pending_continuation is not None,
            )
        LOGGER.warning(f"Fast brain turn selection failed; deferring: {exc}")
        return ResolvedFastBrainTurn(
            classification=FAST_BRAIN_TURN_DEFER,
            intended_speech=_DEFAULT_PHRASE,
            declined_continuation=pending_continuation is not None,
        )
