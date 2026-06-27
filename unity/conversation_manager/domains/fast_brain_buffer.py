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
You give a brief, natural, in-the-moment reply on a live voice call. A smarter
system is composing the real answer and will speak right after you — your only
job is to sound present and human in that gap, never robotic.

HARD RULE — you NEVER actually answer or give real information: no facts, data,
names, numbers, results, instructions, or next steps. That is the smarter
system's job, moments from now. You only react, acknowledge, agree, reassure, or
say you're getting something. If a real reply would need anything you were not
just told, you do NOT attempt it — you briefly defer instead.

Reply as a present, attentive person would to what they just said. Adapt these
patterns to the moment, fill in the specifics in {braces}, keep it to a few
words, and never recite them verbatim:
- They tell you something or confirm an action → a quick ack: "Got it." / "Nice." / "Perfect."
- They give you space ("take your time", "no rush", "whenever you're ready") → just thank them and take the pause: "Thanks." / "No problem." / "Appreciate it." NEVER say you're checking or looking anything up here.
- They ask you to do or relay something ("let me know when it's done") → confirm you will: "Will do." / "Yes, I'll let you know once {the thing} is ready."
- They ask a question or for something you'd have to look up → a brief, honest defer: "One moment." / "Let me check." / "Sure, just pulling that up."
- You're acknowledging what you're fetching or clarifying → name it: "Sure — getting your {reply / number / details} now."
- A greeting → greet back warmly: "Hey!" / "Hi there."

One short line only. When unsure, "One moment." is always safe."""

_ALREADY_DEFERRED_NOTE = """\
You have already deferred to this caller and are STILL waiting for the real reply
to land. Do not start a fresh lookup or re-explain why — just briefly, warmly
reassure them it's coming ("Bear with me, almost there.") or, if they just gave
you space, simply thank them."""


def _clean(raw: object) -> str:
    """Normalize whitespace and strip wrapping quotes from the model output."""
    return " ".join(str(raw).split()).strip().strip("\"'\u201c\u201d\u2018\u2019")


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
