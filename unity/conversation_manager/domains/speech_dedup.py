from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import Enum

from unity.common.hierarchical_logger import DEFAULT_ICON
from unity.common.llm_client import new_llm_client
from unity.logger import LOGGER


class SpeechDecision(str, Enum):
    """Three-way verdict for slow-brain speech proposed on a live call."""

    SPEAK = "speak"
    SUPPRESS = "suppress"
    REWRITE = "rewrite"


@dataclass
class DedupOutcome:
    """Result of the pre-speak gate.

    ``text_stream`` is only populated for ``REWRITE``: it yields the rewritten
    speech token-by-token so TTS can start ingesting on the first token.
    """

    decision: SpeechDecision
    reasoning: str = ""
    text_stream: AsyncIterator[str] | None = None

    @property
    def should_suppress(self) -> bool:
        return self.decision is SpeechDecision.SUPPRESS


SPEECH_DEDUP_PROMPT = """\
You are a gate on a live voice call. The slow brain has proposed a line to say, but it \
is slow (10-25s), so by the time it is ready the fast brain may have already told the \
user the same thing. Your job: stop the user hearing the same point twice, without \
dropping anything they still need to hear.

## What the user has actually heard

These lines were spoken aloud - the only record of what the user has heard:

{recent_utterances}

System state - what is currently true in the system. Use it to judge whether the \
proposed line is still correct and still worth saying, but never as proof the user has \
already heard it (the slow brain routinely queues a line that also shows up here):

{recent_notifications}

## The proposed line

"{proposed_speech}"

## Decide

First work out the *point* of the proposed line - what it is actually trying to tell the \
user. Then weigh that point against what they have already heard, and pick one:

- **SPEAK** - the point is new; nothing spoken above covers it (a match in system state \
does not count). Say it unchanged.
- **SUPPRESS** - the user has already heard the point; or the line no longer fits reality \
- it offers or asks to do something the state shows is already done, repeats a step the \
conversation has moved past, or contradicts the latest utterance. Say nothing. Also \
suppress when the only thing still unsaid is an incidental scrap - a stray name, address, \
number, or half-sentence that is not itself a message worth speaking.
- **REWRITE** - the user has heard *part* of the point but a genuinely useful piece is \
still missing, AND that piece is a complete, natural thing to say on its own. Speak only \
that piece.

"New" means the user is missing meaning they need - not merely that some words have not \
been spoken yet. Leftover words are not automatically worth saying. If trimming away what \
was already heard leaves only a fragment or an incidental detail, that is SUPPRESS, not \
REWRITE.

A REWRITE must read as one natural line continuing the conversation: only the missing \
point, no opener already spoken ("Yes,", "Got it,"), and no fact, name, or number that \
was not in the proposed line.

## Output format (STRICT)

Machine-parsed; a REWRITE is streamed straight to text-to-speech. Follow exactly:

- SUPPRESS: one line - `SUPPRESS | <brief reason>`
- SPEAK: one line - `SPEAK | <brief reason>`
- REWRITE: first line exactly `REWRITE`, then a newline, then the line to speak and \
nothing else - no quotes, labels, or explanation.
"""


def _reason_after(header_line: str, marker: str) -> str:
    """Return the brief reason that follows a decision marker on the header line."""
    rest = header_line[len(marker) :].strip()
    return rest.lstrip("|:-").strip()


class SpeechDeduplicationChecker:
    """Pre-speak gate that decides whether slow brain speech is spoken as-is,
    suppressed, or rewritten to strip redundancy.

    Runs in the fast brain subprocess at speak time (inside ``maybe_speak_queued``).
    Compares the proposed ``response_text`` against recent assistant utterances
    and recent notifications in the fast brain's chat context. The gate streams
    its verdict so a rewritten utterance can begin playing on the first token.
    """

    def __init__(self, model: str | None = None) -> None:
        self._model = model

    async def evaluate(
        self,
        proposed_speech: str,
        recent_utterances: list[str],
        recent_notifications: list[str] | None = None,
    ) -> DedupOutcome:
        """Decide how *proposed_speech* should be delivered.

        Returns ``DedupOutcome(SPEAK)`` immediately when there is no recent
        context to compare against (no LLM call needed). On any error, fails
        open (``SPEAK`` - the original text is spoken).
        """
        # Drop the proposal's own copy from the notifications. The slow brain's
        # guide_voice_agent guidance is injected into the fast brain's context as a
        # ``[notification]`` system message using the very same text that is now
        # being proposed for speech. Without this exclusion the gate would compare
        # the proposal against itself and wrongly conclude it was "already covered".
        proposed_norm = proposed_speech.strip()
        recent_notifications = [
            n for n in (recent_notifications or []) if n.strip() != proposed_norm
        ]

        if not recent_utterances and not recent_notifications:
            return DedupOutcome(
                decision=SpeechDecision.SPEAK,
                reasoning="no recent context to compare against",
            )

        try:
            client = new_llm_client(
                self._model,
                origin="FastBrain.speech_dedup",
                reasoning_effort="low",
            )
            client.set_stream(True)
            formatted_utterances = (
                "\n".join(f'- "{u}"' for u in recent_utterances)
                if recent_utterances
                else "(none)"
            )
            formatted_notifications = (
                "\n".join(f"- {n}" for n in recent_notifications)
                if recent_notifications
                else "(none)"
            )
            system_content = SPEECH_DEDUP_PROMPT.format(
                recent_utterances=formatted_utterances,
                recent_notifications=formatted_notifications,
                proposed_speech=proposed_speech,
            )
            messages = [
                {"role": "system", "content": system_content},
                {
                    "role": "user",
                    "content": (
                        "Apply the instructions above and output your verdict in "
                        "the required format."
                    ),
                },
            ]
            response = await client.generate(messages=messages)
            return await self._parse_stream(response)
        except Exception as e:
            LOGGER.error(
                f"{DEFAULT_ICON} Error in SpeechDedup evaluation: {e}",
            )
            import traceback

            traceback.print_exc()
            return DedupOutcome(
                decision=SpeechDecision.SPEAK,
                reasoning="evaluation failed",
            )

    async def _parse_stream(self, response: AsyncIterator[str]) -> DedupOutcome:
        """Read the streamed verdict header, then expose the rewrite body (if any).

        Accumulates chunks until the header line is complete (first newline) or
        the stream ends, classifies the decision, and for ``REWRITE`` returns an
        async generator that yields the body seeded with whatever was buffered
        past the header newline.
        """
        buf = ""
        exhausted = False
        async for chunk in response:
            if not chunk:
                continue
            buf += chunk
            if "\n" in buf:
                break
        else:
            exhausted = True

        header_line, sep, remainder = buf.partition("\n")
        token = header_line.strip()
        upper = token.upper()

        if upper.startswith("REWRITE"):
            # Body normally starts after the newline. If the model jammed text
            # onto the REWRITE line without a newline, recover it after the marker.
            seed = remainder if sep else _reason_after(token, "REWRITE")

            async def _body() -> AsyncIterator[str]:
                if seed:
                    yield seed
                if not exhausted:
                    async for chunk in response:
                        if chunk:
                            yield chunk

            return DedupOutcome(
                decision=SpeechDecision.REWRITE,
                reasoning="rewrite",
                text_stream=_body(),
            )

        if upper.startswith("SUPPRESS"):
            return DedupOutcome(
                decision=SpeechDecision.SUPPRESS,
                reasoning=_reason_after(token, "SUPPRESS"),
            )

        if upper.startswith("SPEAK"):
            return DedupOutcome(
                decision=SpeechDecision.SPEAK,
                reasoning=_reason_after(token, "SPEAK"),
            )

        # Unrecognized header: fail open and speak the original text.
        return DedupOutcome(
            decision=SpeechDecision.SPEAK,
            reasoning=f"unrecognized gate header: {token[:40]}",
        )
