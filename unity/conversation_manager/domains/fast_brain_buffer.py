"""Fast-brain buffer-phrase selection.

The fast brain (voice agent) no longer free-generates substantive replies. On
each user turn it emits exactly one short, safe filler phrase to cover the
latency until the slow brain (which owns all substantive speech) responds.

Selection is a constrained classification: a lightweight LLM picks the most
fitting phrase from a fixed set. Crucially, every phrase is acceptable
regardless of what the slow brain says next, so a mis-pick is harmless. On any
error the selector returns a safe default; it never free-generates text.
"""

from __future__ import annotations

from unity.common.llm_client import new_llm_client
from unity.logger import LOGGER
from unity.settings import SETTINGS

# Fixed, universally-safe filler phrases. Index order is the selector's contract.
# Each works both as a standalone reply (if the slow brain then stays silent) and
# as a lead-in (if the slow brain follows with the real content).
BUFFER_PHRASES: list[str] = [
    "Got it.",  # 0 - acknowledging a statement
    "Okay.",  # 1 - neutral acknowledgement
    "Sure.",  # 2 - agreeing to a request
    "Let me check on that.",  # 3 - a question / lookup
    "One moment.",  # 4 - an action is being taken
    "On it.",  # 5 - a request to do something
]

_DEFAULT_INDEX = 4  # "One moment." - safe for almost any input

_SELECTOR_PROMPT = """\
You pick a single short filler phrase for a live voice call. The phrase only
buys a moment while a smarter system composes the real reply; it must NOT answer
anything. Choose the phrase that sounds most natural as an immediate reaction to
the caller's last message.

Options (reply with ONLY the number):
0. "Got it." - the caller stated/confirmed something
1. "Okay." - neutral acknowledgement
2. "Sure." - the caller asked you to do something simple and you're agreeing
3. "Let me check on that." - the caller asked a question or for information
4. "One moment." - the caller asked for something that needs a beat
5. "On it." - the caller asked you to perform an action

Output the single digit 0-5 and nothing else."""


async def select_buffer_phrase(user_text: str) -> str:
    """Return one phrase from :data:`BUFFER_PHRASES` for the caller's utterance.

    Fail-safe: returns the default phrase on empty input or any error.
    """
    if not (user_text or "").strip():
        return BUFFER_PHRASES[_DEFAULT_INDEX]
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
        digits = [c for c in str(raw) if c.isdigit()]
        if digits:
            idx = int(digits[0])
            if 0 <= idx < len(BUFFER_PHRASES):
                return BUFFER_PHRASES[idx]
    except Exception as e:  # never let buffer selection break the turn
        LOGGER.warning(f"Buffer phrase selection failed; using default: {e}")
    return BUFFER_PHRASES[_DEFAULT_INDEX]
