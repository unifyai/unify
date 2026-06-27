"""Fast-brain buffer-phrase selection.

The fast brain (voice agent) no longer free-generates substantive replies. On
each user turn it emits one short, safe filler phrase (optionally a natural
combination of two) to cover the latency until the slow brain - which owns all
substantive speech - responds.

The selector LLM is asked to copy one or two phrases verbatim from a fixed set.
Crucially, we never speak the model's raw text: its output is validated against
the allowed set and re-emitted as our canonical phrases, so only vetted phrases
ever reach TTS. Every phrase (and pairing) is acceptable regardless of what the
slow brain says next, so a mis-pick is harmless; on empty input or any failure
we return a safe default and never free-generate text.
"""

from __future__ import annotations

import random
import re

from unity.common.llm_client import new_llm_client
from unity.logger import LOGGER
from unity.settings import SETTINGS

# Fixed, universally-safe filler phrases, each with a short "when to use" hint.
# This list is the SINGLE SOURCE OF TRUTH: the prompt menu and the validation
# lookup are both derived from it, so extending the set is a one-line addition
# here. Each phrase works standalone, and any two read naturally when spoken
# together - the model decides what pairs well.
BUFFER_OPTIONS: list[tuple[str, str]] = [
    ("Got it.", "acknowledging what they just told you"),
    ("Okay.", "neutral acknowledgement"),
    ("Sure.", "agreeing to something simple"),
    ("Sounds good.", "you're happy with what they told you"),
    ("Nice.", "a warm reaction to good news"),
    ("On it.", "you'll do the action now"),
    ("I'll give it a try.", "agreeing, tentatively"),
    ("I don't think so.", "a confident no"),
    ("I'm not sure.", "a tentative, uncertain no"),
    ("Let me check on that.", "you'll look something up"),
    ("Let me take a look.", "you'll inspect or look into it"),
    ("Let me have a think.", "you need a beat to consider it"),
    ("One moment.", "a short pause"),
    ("Hang on.", "asking them to wait a beat"),
]

# Spoken text the selector may return.
BUFFER_PHRASES: list[str] = [phrase for phrase, _hint in BUFFER_OPTIONS]

# "Still here, reply coming" fillers for the 2nd+ consecutive buffer in a row
# without a slow-brain reply yet. Unlike the first-reaction set above, these
# never imply a fresh lookup ("let me check") — after the assistant has already
# deferred once, a lookup-flavored line sounds like it's re-reading its own
# speech. These just own the lag honestly without explaining why.
WAIT_PHRASES: list[str] = [
    "Bear with me, I'll reply in a moment.",
    "Sorry, just a moment — I'll be with you in a second.",
    "Sorry, hang on — reply's coming.",
    "One sec, I'll be right with you.",
    "Still with you — won't be long.",
    "Give me just a beat, I'll reply shortly.",
    "Almost there — thanks for your patience.",
    "Hang tight, I'll be right back to you.",
]

# Returned on empty input or any failure - safe after almost any utterance.
_DEFAULT_PHRASE = "One moment."

# Most phrases we will ever stitch together for one turn (keeps fillers short).
_MAX_COMBINED = 2

# Hard ceiling on a fast-brain reply. A filler / acknowledgement / brief echo is
# short; anything longer is the model overstepping into a substantive answer
# (the slow brain's job), so we fall back to a safe reference phrase.
_MAX_FAST_REPLY_CHARS = 160

_PHRASE_MENU = "\n".join(f'- "{phrase}" ({hint})' for phrase, hint in BUFFER_OPTIONS)

_SELECTOR_PROMPT = f"""\
You give a brief, immediate reply on a live voice call to cover the moment while
a smarter system composes the real answer. Keep it SHORT — a filler, a quick
acknowledgement, or a brief natural reaction. You must NEVER actually answer a
question or give real information (facts, data, instructions, next steps); that
is the smarter system's job and it will follow right after you.

In MOST cases, use one of the reference phrases below — they are always safe. You
can combine two that feel natural, e.g. "Got it. One moment."

You MAY ad-lib slightly, but ONLY when the reply is very obvious and trivial —
for example naturally acknowledging a comment ("Ha, yeah — fair point.") or
briefly echoing something just said. Keep any ad-lib to a few words,
conversational, and never an answer. When in any doubt, just use a reference
phrase.

Reference phrases:
{_PHRASE_MENU}

Reply with the single short line to say — nothing else."""


def _normalize(text: str) -> str:
    """Lowercase, de-quote, collapse spaces, and drop trailing punctuation."""
    t = text.strip().strip("\"'\u201c\u201d\u2018\u2019").lower()
    t = re.sub(r"\s+", " ", t).strip()
    return t.rstrip(".!?,;: ").strip()


_CANONICAL_BY_NORM = {_normalize(phrase): phrase for phrase in BUFFER_PHRASES}


def resolve_buffer_phrases(raw: str) -> str | None:
    """Map a raw model reply to a verbatim canonical phrase (or pair).

    Splits on terminal punctuation, normalizes each chunk, and keeps the ones
    that match a known phrase (in order, de-duplicated, capped at
    ``_MAX_COMBINED``). Returns ``None`` when nothing matches so the caller can
    fall back to the default - novel text is never echoed.
    """
    matched: list[str] = []
    for chunk in re.split(r"[.!?]+", str(raw)):
        norm = _normalize(chunk)
        if not norm:
            continue
        canonical = _CANONICAL_BY_NORM.get(norm)
        if canonical is None or (matched and matched[-1] == canonical):
            continue
        matched.append(canonical)
        if len(matched) >= _MAX_COMBINED:
            break
    return " ".join(matched) if matched else None


async def select_buffer_phrase(user_text: str, recent_assistant_text: str = "") -> str:
    """Return the fast brain's brief reply for the caller's utterance.

    Defaults to one (or a natural pair) of the reference phrases, but allows a
    slight, short ad-lib for obvious/trivial replies (acks, brief echoes). A
    length backstop catches the model overstepping into a substantive answer and
    falls back to a safe reference phrase.

    ``recent_assistant_text`` is the assistant's previous spoken line; when it
    was itself a filler, we nudge the model to say something different so it does
    not repeat itself verbatim.

    Fail-safe: returns the default phrase on empty input or any error.
    """
    if not (user_text or "").strip():
        return _DEFAULT_PHRASE
    messages = [{"role": "system", "content": _SELECTOR_PROMPT}]
    # Only nudge when the previous line was itself a filler (resolves to a known
    # phrase); slow-brain prose resolves to None and needs no anti-repeat.
    previous_filler = resolve_buffer_phrases(recent_assistant_text or "")
    if previous_filler:
        messages.append(
            {
                "role": "system",
                "content": (
                    f'Your previous line was "{previous_filler}". Say something '
                    "different this time."
                ),
            },
        )
    messages.append({"role": "user", "content": user_text.strip()})
    try:
        client = new_llm_client(
            SETTINGS.conversation.FAST_BRAIN_MODEL,
            origin="FastBrain.buffer",
            reasoning_effort="low",
        )
        raw = await client.generate(messages=messages)
        text = " ".join(str(raw).split()).strip().strip("\"'\u201c\u201d")
        if not text:
            return _DEFAULT_PHRASE
        # Backstop: a real filler/ack/echo is short. A long reply means the model
        # tried to answer substantively (the slow brain's job) — drop it and fall
        # back to a safe reference phrase.
        if len(text) > _MAX_FAST_REPLY_CHARS:
            return resolve_buffer_phrases(text) or _DEFAULT_PHRASE
        return text
    except Exception as e:  # never let buffer selection break the turn
        LOGGER.warning(f"Buffer phrase selection failed; using default: {e}")
    return _DEFAULT_PHRASE


def select_wait_phrase(exclude: list[str] | None = None) -> str:
    """Pick a 'still here, reply coming' filler for a 2nd+ consecutive buffer.

    Deterministic (no LLM) so a repeated filler is near-instant — exactly when the
    caller is visibly waiting. ``exclude`` is the set of wait phrases already used
    in the current wait streak, so a phrase is never repeated within a streak;
    once the whole set is exhausted it falls back to the full set.
    """
    excluded = set(exclude or ())
    options = [p for p in WAIT_PHRASES if p not in excluded] or WAIT_PHRASES
    return random.choice(options)
