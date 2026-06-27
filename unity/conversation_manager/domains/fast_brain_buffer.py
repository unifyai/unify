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

# Returned on empty input or any failure - safe after almost any utterance.
_DEFAULT_PHRASE = "One moment."

# Most phrases we will ever stitch together for one turn (keeps fillers short).
_MAX_COMBINED = 2

_PHRASE_MENU = "\n".join(f'- "{phrase}" ({hint})' for phrase, hint in BUFFER_OPTIONS)

_SELECTOR_PROMPT = f"""\
You say a brief filler on a live voice call to cover the moment while a smarter
system composes the real reply. Build your reply ONLY from the exact phrases
below, copied verbatim. Never say anything else, and never actually answer.

You should usually combine two phrases that feel natural together, separated by
a space (for example "Got it. One moment."). Use a single phrase only when
combining would sound off.

Phrases:
{_PHRASE_MENU}

Reply with only the phrase or phrases, exactly as written."""


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
    """Return one (or a natural pair of) canonical phrase(s) for the utterance.

    ``recent_assistant_text`` is the assistant's previous spoken line; when it
    was itself a buffer filler, we nudge the model to pick something different so
    it does not say the exact same filler twice in a row.

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
                    f'Your previous filler was "{previous_filler}". Do not say the '
                    "same thing again - pick different phrasing this time."
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
        resolved = resolve_buffer_phrases(str(raw))
        if resolved:
            return resolved
    except Exception as e:  # never let buffer selection break the turn
        LOGGER.warning(f"Buffer phrase selection failed; using default: {e}")
    return _DEFAULT_PHRASE
