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

# Fixed, universally-safe filler phrases, each with a "when to use" hint and a
# group. This list is the SINGLE SOURCE OF TRUTH: the prompt's phrase menu and
# the validation lookup are both derived from it, so extending the set is a
# one-line addition here. Each phrase works standalone, and - crucially - a
# "reaction" pairs naturally with a "moment" phrase to form one spoken reply
# ("Sure. One moment.", "I don't think so. Let me have a think.").
#
# Groups:
#   "reaction" - affirm or decline, confident or tentative
#   "moment"   - buy a beat while the real answer is composed
BUFFER_OPTIONS: list[tuple[str, str, str]] = [
    ("Got it.", "acknowledging what the caller just said", "reaction"),
    ("Okay.", "neutral acknowledgement", "reaction"),
    ("Sure.", "agreeing to do something simple", "reaction"),
    ("On it.", "confidently agreeing to perform the action", "reaction"),
    ("I'll give it a try.", "agreeing, but tentatively", "reaction"),
    ("I don't think so.", "a confident no", "reaction"),
    ("I'm not sure.", "a tentative / uncertain no", "reaction"),
    ("Let me check on that.", "you'll look something up", "moment"),
    ("Let me take a look.", "you'll inspect or look into it", "moment"),
    ("Let me have a think.", "you need a beat to consider it", "moment"),
    ("One moment.", "a short pause for something that needs a beat", "moment"),
    ("Hang on.", "asking them to wait a beat", "moment"),
]

# Spoken text the selector may return.
BUFFER_PHRASES: list[str] = [phrase for phrase, _hint, _group in BUFFER_OPTIONS]

# Returned on empty input or any failure - safe after almost any utterance.
_DEFAULT_PHRASE = "One moment."

# Most phrases we will ever stitch together for one turn (keeps fillers short).
_MAX_COMBINED = 2


def _menu_for(group: str) -> str:
    return "\n".join(
        f'- "{phrase}" ({hint})' for phrase, hint, grp in BUFFER_OPTIONS if grp == group
    )


_SELECTOR_PROMPT = f"""\
You say a brief filler on a live voice call to cover the moment while a smarter
system composes the real reply. Your reply MUST be built only from the exact
phrases below, copied verbatim (including the trailing period). Never say
anything that is not one of these phrases, and never answer the caller.

Combining two phrases is usually best - it sounds the most natural. The natural
pattern is one REACTION followed by one BUYING-A-MOMENT phrase, for example:
  "Sure. One moment."
  "Got it. Let me take a look."
  "I'm not sure. Let me check on that."
  "I don't think so. Let me have a think."
Use a single phrase only when a combination would sound unnatural.

REACTIONS (affirm or decline; confident or tentative):
{_menu_for("reaction")}

BUYING A MOMENT:
{_menu_for("moment")}

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


async def select_buffer_phrase(user_text: str) -> str:
    """Return one (or a natural pair of) canonical phrase(s) for the utterance.

    Fail-safe: returns the default phrase on empty input or any error.
    """
    if not (user_text or "").strip():
        return _DEFAULT_PHRASE
    try:
        client = new_llm_client(
            SETTINGS.conversation.FAST_BRAIN_MODEL,
            origin="FastBrain.buffer",
            reasoning_effort="low",
        )
        raw = await client.generate(
            messages=[
                {"role": "system", "content": _SELECTOR_PROMPT},
                {"role": "user", "content": user_text.strip()},
            ],
        )
        resolved = resolve_buffer_phrases(str(raw))
        if resolved:
            return resolved
    except Exception as e:  # never let buffer selection break the turn
        LOGGER.warning(f"Buffer phrase selection failed; using default: {e}")
    return _DEFAULT_PHRASE
