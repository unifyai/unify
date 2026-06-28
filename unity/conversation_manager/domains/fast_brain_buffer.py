"""Fast-brain reply selection.

The fast brain (voice agent) does not compose the real answer — the slow brain
does, moments later. On each user turn the fast brain gives ONE brief, natural,
in-the-moment reaction to cover that gap and sound present rather than robotic.

It is no longer a fixed phrase bank: it is prompted with patterns/templates that
it adapts and fills in (e.g. "Yes, I'll let you know once {X}"), held to a hard
rule that it never actually answers or gives real information. A length backstop
and a safe default keep a mis-fire harmless. The deterministic "still waiting"
path is folded in here too (via ``already_deferred``) so repeated deferrals stay
context-aware — e.g. answering "take your time" with "Thanks", not a lookup.
"""

from __future__ import annotations

from unity.common.llm_client import new_llm_client
from unity.logger import LOGGER
from unity.settings import SETTINGS

# Returned on empty input or any failure - safe after almost any utterance.
_DEFAULT_PHRASE = "One moment."

# Hard ceiling on a fast reply. A reaction / ack / brief defer is short; anything
# longer means the model overstepped into a substantive answer (the slow brain's
# job), so we drop it for the safe default.
_MAX_FAST_REPLY_CHARS = 160

_FAST_REPLY_PROMPT = """\
You give a brief, natural, in-the-moment reply on a live voice call. The real
answer is being composed and will be spoken right after you — your only job is to
sound present and human in that gap, never robotic.

NEVER BREAK THE FOURTH WALL — to the caller you are one single person. Never
mention or hint that any other system, model, agent, or "smarter" component
exists, that you are "filler", or that an answer is "still being composed". If
the caller questions what you said, just own it naturally as yourself; never
explain how your speech is produced.

HARD RULE — you NEVER actually answer or give real information: no facts, data,
names, numbers, results, instructions, or next steps. That is handled moments
from now. You only react, acknowledge, agree, reassure, or say you're getting
something. If a real reply would need anything you were not just told, you do NOT
attempt it — you briefly defer instead.

MUST CONTEXTUALIZE — your reply ALWAYS refers back to what the caller just said:
their topic, the action they took, or what they asked for. NEVER reply with a
bare, standalone phrase like "Got it.", "One moment.", "Nice.", "Perfect.", or
"Will do." on its own — those sound robotic and ambiguous (a bare "Got it" sounds
like you received a thing). Always attach the specific thing, e.g. "Got it —
looking into your email now." instead of "Got it."

Reply as a present, attentive person would to what they just said. Adapt these
patterns to the moment, fill in the specifics in {braces}, keep it to a few
words, and never recite them verbatim:
- They tell you something or confirm an action → a quick contextual ack: "Got it — I'll check on that now." / "Nice, that's the {thing} sorted."
- They give you space ("take your time", "no rush", "whenever you're ready") → thank them and take the pause, anchored to it: "Thanks — I'll keep at it." / "No problem, I'll stay on it." NEVER say you're checking or looking anything up here.
- They ask you to do or relay something ("let me know when it's done") → confirm you will, naming it: "Will do — I'll let you know once {the thing} is ready."
- They ask a question or for something you'd have to look up → a brief, honest, anchored defer: "One sec — pulling up your {thing} now." / "Let me check on {the thing}."
- You're acknowledging what you're fetching or clarifying → name it: "Sure — getting your {reply / number / details} now."
- A greeting → greet back warmly: "Hey there!" / "Hi — good to hear you."

One short line only, always tied to what they just said."""

_ALREADY_DEFERRED_NOTE = """\
You have already deferred to this caller and are STILL waiting for the real reply
to land. Do not start a fresh lookup or re-explain why — just briefly, warmly
reassure them it's coming, anchored to what they're waiting on ("Bear with me,
almost there with your {thing}.") or, if they just gave you space, simply thank
them."""


def _clean(raw: object) -> str:
    """Normalize whitespace and strip wrapping quotes from the model output."""
    return " ".join(str(raw).split()).strip().strip("\"'\u201c\u201d\u2018\u2019")


# Resumption (fast brain picking up an interrupted slow-brain line) ----------

# Sentinel the continuation model returns when the caller's barge-in redirected
# or asked something new, so resuming the old line would be wrong.
_DEFER_SENTINEL = "DEFER"

# A resume lead-in is a few words ("Sorry - as I was saying,"). Anything longer
# means the model tried to re-compose the remainder itself; we defer instead.
_MAX_LEAD_IN_CHARS = 120

_CONTINUATION_PROMPT = """\
You were speaking on a live call and the caller cut you off mid-sentence. You are
about to resume the EXACT words you had left to say (they are appended verbatim
by the system — you do NOT write them). Your only job is a SHORT, natural lead-in
that bridges back into them, reacting to whatever the caller just said.

Default to RESUMING. The words you have left are usually exactly what the caller
needs, so bridge back into them. Only reply with exactly:
DEFER
when continuing would clearly be wrong — the caller changed the subject, declined
or objected, told you to stop or wait, or asked something the remaining words do
NOT address.

CRUCIAL — these are NOT reasons to defer; they are the caller inviting you to say
your piece, so you MUST resume (the words you have left are precisely the answer):
- They just answered the call/message with a greeting: "Hello?", "Hello, can you
  hear me?", "Hi", "Hey".
- They asked what this is / why you're contacting them: "Why are you calling?",
  "Hello, why are you calling?", "What's this about?", "What did you need?",
  "What's up?".
Answering a call and asking "why are you calling?" is the single most obvious case
where you resume — do not treat it as a new question.

When you resume, reply with ONLY a brief lead-in to glide back in, e.g.:
- "Sorry — as I was saying,"
- "Right, let me finish that thought —"
- "Yeah — so, continuing,"
- "Hey — glad you picked up. So,"

A few words only. Never include the remaining content itself, never answer
anything, never add new information."""


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
    """Return a short lead-in to resume an interrupted line, or ``None`` to defer.

    ``resume_text`` is the slow brain's own verbatim remaining content (appended
    by the caller, never composed here); ``user_text`` is the caller's barge-in.
    Returns ``None`` (let the slow brain handle it) when the model judges the
    interjection a redirect, on empty input, an over-long lead-in (the model
    overreached), or any error.
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
        text = _clean(raw)
        if not text or text.upper() == _DEFER_SENTINEL:
            return None
        # A genuine lead-in is short; an over-long reply means the model tried to
        # re-say the remainder itself — defer to be safe.
        if len(text) > _MAX_LEAD_IN_CHARS:
            return None
        return text
    except Exception as e:  # never let continuation selection break the turn
        LOGGER.warning(f"Continuation selection failed; deferring: {e}")
    return None


async def select_fast_reply(
    user_text: str,
    recent_assistant_text: str = "",
    already_deferred: bool = False,
) -> str:
    """Return the fast brain's brief, in-the-moment reply for the utterance.

    The reply is freeform but template-guided (see the prompt) and must never be
    a substantive answer. ``recent_assistant_text`` is the assistant's previous
    spoken line (given as context + an anti-repeat nudge). ``already_deferred``
    marks a repeated deferral in the same wait streak, so the reply reassures
    rather than starting a fresh lookup.

    Fail-safe: returns the default phrase on empty input, an over-long reply
    (model overreaching into a real answer), or any error.
    """
    if not (user_text or "").strip():
        return _DEFAULT_PHRASE
    messages = [{"role": "system", "content": _FAST_REPLY_PROMPT}]
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
