"""Fast-brain reply selection.

The fast brain (voice agent) does not compose the real answer — the slow brain
does, moments later. On each user turn the fast brain gives ONE brief, natural
acknowledgement to cover that gap.

It is prompted with patterns/templates it adapts to the moment. It may give a
direct yes/no hedge on a direct question, or a brief honest defer ("I'm not
sure — let me check"), but never substantive facts, data, or instructions. A
length backstop and a safe default keep a mis-fire harmless. The repeated-wait
path (via ``already_deferred``) keeps follow-up deferrals varied and still
responsive to yes/no questions — e.g. "take your time" gets "Thanks", not
another hollow status line.
"""

from __future__ import annotations

import random

from unify.common.llm_client import new_llm_client
from unify.logger import LOGGER
from unify.settings import SETTINGS

# Returned on empty input or any failure - safe after almost any utterance.
_DEFAULT_PHRASE = "One moment."

# Hard ceiling on a fast reply. A reaction / ack / brief defer is short; anything
# longer means the model overstepped into a substantive answer (the slow brain's
# job), so we drop it for the safe default.
_MAX_FAST_REPLY_CHARS = 160

_FAST_REPLY_PROMPT = """\
You give a brief, natural acknowledgement on a live voice call. The full answer
is being composed and will be spoken right after you — your job in this gap is to
sound human: acknowledge what they said, and when they asked something direct,
give them a straight conversational lead-in before you defer.

STAY ONE PERSON — to the caller you are a single person. Never disown your own
words or split yourself into separate agents: never say a word "wasn't you",
never mention any other system, model, agent, or "smarter" component, never call
yourself "filler", and never say an answer is "still being composed". If the
caller questions what you said, just own it naturally as yourself.

WHAT YOU MAY SAY (keep it to one short line):
1. **Direct yes/no questions** — the caller expects a straight answer first.
   Start with ONE of: Yes / No / I think so / I don't think so / I'm not sure —
   whichever fits honestly, then add a brief defer if you still need to look:
   - "Are you gonna send it?" → "Not yet — let me check on that email."
   - "You're not gonna hang up?" → "No, not yet — I need to finish this first."
   - "Did it go through yet?" → "I'm not sure — let me take a look."
   - "I'll get a response soon, right?" → "Yes — I'm waiting on that now."
   Never dodge a yes/no question with vague status-only wording.

2. **Timing / why / how-long questions** — give a natural hedge, then defer:
   - "How long is it going to take?" → "Shouldn't be long — let me just check."
   - "Why is it taking so long?" → "I'm not sure yet — let me take a look."
   - "Do you know why?" → "I'm not sure — give me a sec to check."

3. **Action confirmations and acks** — name the thing they did:
   - "Got it — I'll check on that {thing} now." / "Nice, that's the {thing} sorted."

4. **Space / patience** ("take your time", "no rush") — thank them; do NOT say
   you're checking or looking anything up: "Thanks — I'll keep at it."

5. **Relay requests** ("let me know when it's done") — confirm you will:
   "Will do — I'll let you know once {the thing} is ready."

6. **Greetings** — greet back warmly: "Hey there!" / "Hi — good to hear you."

WHAT YOU MUST NOT SAY:
- No substantive facts, data, names, numbers, results, instructions, or next
  steps — those come in the full answer moments later.
- NEVER use hollow status-only deferrals that ignore the question, especially:
  "I'm still on it", "still on your question", "still on the sending part",
  "I'm still on your message", "I'm still with you on that", or any variant of
  "still on {thing}" without answering what they asked. These sound robotic and
  rude on a live call.
- NEVER reply with a bare standalone phrase ("Got it.", "One moment.", "Will do.")
  — always attach the specific topic or question.

MUST CONTEXTUALIZE — every reply refers back to what the caller just said: their
topic, action, or question. One short line only."""

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

# Present ONLY when the smarter system bundled a note for this exact moment. It
# is the single, deliberate exception to the no-real-information HARD RULE. Kept
# general (no domain-specific concepts) — the note itself carries any specifics.
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


def _clean(raw: object) -> str:
    """Normalize whitespace and strip wrapping quotes from the model output."""
    return " ".join(str(raw).split()).strip().strip("\"'\u201c\u201d\u2018\u2019")


# Resumption (fast brain picking up an interrupted slow-brain line) ----------

# Sentinels the continuation classifier returns. CONTINUE = resume the unheard
# remainder; DEFER = the barge-in redirected, so leave it to the slow brain.
_DEFER_SENTINEL = "DEFER"
_CONTINUE_SENTINEL = "CONTINUE"

# Fixed bridge phrases prepended to a resumed line. Authored HERE, never by the
# model, so they can never overlap/duplicate the resumed content (the weak model
# used to copy the remainder's first sentence as its "lead-in", producing
# back-to-back duplication). Rotated for variety so resumes don't sound robotic.
_RESUME_LEAD_INS = (
    "Sorry — as I was saying,",
    "Right, where was I —",
    "Okay, picking up where I left off —",
    "So, to finish that thought —",
    "Anyway, as I was saying —",
    "Right, continuing —",
    "Sorry about that — so,",
)


def pick_resume_lead_in() -> str:
    """A fixed, non-overlapping bridge phrase to prepend to a resumed line.

    Authored here (never by the model) so it can never duplicate the resumed
    content; rotated for variety. Shared by the classifier (CONTINUE) and the
    speechless auto-resume path.
    """
    return random.choice(_RESUME_LEAD_INS)


_CONTINUATION_PROMPT = """\
You were speaking on a live call and the caller cut you off mid-sentence. The
EXACT words you had left to say will be resumed automatically — you do NOT write
them. Your ONLY job: decide whether resuming them now is right, given what the
caller just said.

Reply with exactly one word. CONTINUE is the strong default — resume unless there
is a clear, explicit reason not to.

- CONTINUE — the remaining words are almost always exactly what the caller needs.
  Choose this for anything that is not an explicit redirect: greetings, "go on",
  filler, thinking aloud, agreeing, partial overlap, talking over you, or simply
  continuing to speak. If unsure, CONTINUE.
- DEFER — ONLY when continuing would plainly be wrong because the caller
  EXPLICITLY: changed the subject to something else, declined/objected, told you
  to stop or wait, asked a specific question the remaining words clearly do not
  answer, OR indicated they have ALREADY done (or are already doing) the very
  thing your remaining words instruct ("I already clicked it", "just did that",
  "I already replied", "I've got it") — re-delivering a completed instruction is
  wrong, so DEFER.

It is fine if a genuine redirect later needs its own turn — the caller keeps
talking anyway. The costly mistake is failing to resume content that was right
there, forcing a long silence. So lean hard toward CONTINUE.

CRUCIAL — these are NOT reasons to defer; they are the caller inviting you to say
your piece, so reply CONTINUE:
- They just answered with a greeting: "Hello?", "Hello, can you hear me?", "Hi".
- They asked what this is / why you're contacting them: "Why are you calling?",
  "What's this about?", "What did you need?".
Answering a call and asking "why are you calling?" is the single most obvious case
to CONTINUE — do not treat it as a new question.

Output ONLY the single word CONTINUE or DEFER — nothing else."""


def compute_resume_text(full: str, spoken: str) -> str:
    """Return the unheard tail of ``full``, backed up to a clean resume point.

    ``spoken`` is the prefix the caller actually heard before barging in (usually
    cutting a sentence mid-way). We return the remainder, but rewound to the start
    of the sentence that was cut, so resuming repeats the partial sentence cleanly
    rather than starting mid-word. Falls back to the raw remainder when ``spoken``
    is not a prefix of ``full`` or no sentence boundary precedes the cut.
    """
    full = (full or "").strip()
    spoken = (spoken or "").strip()
    if not full:
        return ""
    if not spoken or not full.startswith(spoken):
        return full
    # Rewind to just after the last sentence terminator inside the spoken prefix,
    # so the partially-spoken sentence is resumed from its beginning.
    boundary = max(spoken.rfind(c) for c in ".!?")
    if boundary == -1:
        return full[len(spoken) :].strip()
    return full[boundary + 1 :].strip()


async def select_continuation(resume_text: str, user_text: str) -> str | None:
    """Return a fixed lead-in to resume an interrupted line, or ``None`` to defer.

    The model ONLY classifies CONTINUE vs DEFER given the caller's barge-in; the
    lead-in itself is drawn from a fixed bank (`_RESUME_LEAD_INS`) so it can never
    duplicate the resumed content — the caller then hears "{fixed lead-in}
    {verbatim remainder}". ``resume_text`` is the slow brain's own remaining
    content (appended by the caller, never composed here); ``user_text`` is the
    barge-in. Returns ``None`` (let the slow brain handle it) when the model
    classifies a redirect, on empty input, or any error.
    """
    if not (resume_text or "").strip() or not (user_text or "").strip():
        return None
    messages = [
        {"role": "system", "content": _CONTINUATION_PROMPT},
        {
            "role": "user",
            "content": (
                f"The words you still have left to say: {resume_text.strip()}\n\n"
                f"What the caller just said: {user_text.strip()}"
            ),
        },
    ]
    try:
        client = new_llm_client(
            SETTINGS.conversation.FAST_BRAIN_MODEL,
            origin="FastBrain.continuation",
            reasoning_effort="low",
        )
        raw = await client.generate(messages=messages)
        decision = _clean(raw).upper()
        # Defer on an explicit DEFER, empty output, or anything unexpected that is
        # not a clear CONTINUE; otherwise resume with a fixed, non-overlapping
        # lead-in. (Default-to-continue is preserved for a clear CONTINUE.)
        if not decision or decision == _DEFER_SENTINEL:
            return None
        return pick_resume_lead_in()
    except Exception as e:  # never let continuation selection break the turn
        LOGGER.warning(f"Continuation selection failed; deferring: {e}")
    return None


async def select_fast_reply(
    user_text: str,
    recent_assistant_text: str = "",
    already_deferred: bool = False,
    guidance: str = "",
) -> str:
    """Return the fast brain's brief, in-the-moment reply for the utterance.

    The reply is freeform but template-guided (see the prompt). It may lead with
    a direct yes/no hedge on a direct question, or a brief timing/why deferral,
    but never substantive facts. ``recent_assistant_text`` is the assistant's previous
    spoken line (given as context + an anti-repeat nudge). ``already_deferred``
    marks a repeated deferral in the same wait streak, so the reply acknowledges
    the wait rather than starting a fresh lookup. ``guidance`` is an optional short note
    the smarter system bundled for this moment; when present it adds a scoped
    block letting the reply directly answer the caller's just-said point (the one
    exception to the no-real-information rule).

    Fail-safe: returns the default phrase on empty input, an over-long reply
    (model overreaching into a real answer), or any error.
    """
    if not (user_text or "").strip():
        return _DEFAULT_PHRASE
    messages = [{"role": "system", "content": _FAST_REPLY_PROMPT}]
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
    try:
        client = new_llm_client(
            SETTINGS.conversation.FAST_BRAIN_MODEL,
            origin="FastBrain.reply",
            reasoning_effort="low",
        )
        raw = await client.generate(messages=messages)
        text = _clean(raw)
        if not text:
            return _DEFAULT_PHRASE
        # Backstop: a genuine reaction/defer is short. A long reply means the
        # model tried to answer substantively — drop it for the safe default.
        if len(text) > _MAX_FAST_REPLY_CHARS:
            return _DEFAULT_PHRASE
        return text
    except Exception as e:  # never let fast-reply selection break the turn
        LOGGER.warning(f"Fast reply selection failed; using default: {e}")
    return _DEFAULT_PHRASE
