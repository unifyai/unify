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
You are deciding how proposed speech from the slow brain should be delivered on a \
live voice call.

The system has two brains running in parallel:
- **Fast brain**: handles real-time conversation, responds instantly to user speech.
- **Slow brain**: processes events and decides what to say, but takes 10-25 seconds.

During the slow brain's thinking time, the fast brain may have already answered, \
OR new notifications may have arrived that make the proposed speech stale or wrong.

## Recent assistant utterances (the ONLY record of what the user has actually heard)

These are the words that were genuinely spoken aloud on the call. This is the single \
source of truth for what the user has heard.

{recent_utterances}

## Recent notifications (authoritative system *state* - NOT speech the user has heard)

These describe what is true in the system. They are NOT things the user has been told. \
A notification matching the proposed speech does NOT mean the user has heard it - the \
slow brain routinely queues speech that also appears here as state. Never treat a \
notification as evidence that something was already said.

{recent_notifications}

## Proposed speech from slow brain

"{proposed_speech}"

## Your decision

Judge redundancy ONLY against the recent assistant utterances above - never against \
notifications.

Choose exactly one of three verdicts:

### SUPPRESS - do not speak at all

- The recent *utterances* already convey the same core information or conclusion, and \
the proposal adds nothing meaningfully new.
- The proposal contradicts the current state: it offers to walk through, set up, or \
redo steps that a recent notification or utterance confirms are already complete; or \
it claims something is needed when a notification says it is done; or it gives a next \
step for a workflow that has already progressed past it; or it contradicts the most \
recent assistant utterance.
- After stripping everything already said, nothing useful would remain.

### REWRITE - speak only the genuinely new part

Use this when the proposal *partially* overlaps with what was just said but still \
carries new or important information. The redundant portion would feel like a glitchy \
repeat if spoken in full (for example a second "Yes, ..." or restating a confirmation \
the user already heard).

Output a trimmed version that:
- Contains ONLY the new/important information not already in the recent utterances.
- Flows naturally as a continuation of the most recent assistant utterance (drop \
acknowledgement openers like "Yes,"/"Got it,"/"Done -" that were already spoken).
- NEVER introduces any fact, claim, name, number, or offer that is not present in the \
proposed speech. You are trimming and re-joining, not inventing.
- NEVER restates information already covered by the recent utterances.

### SPEAK - speak the proposal unchanged

- The proposal is meaningfully new and does not overlap with recent utterances.
- No recent utterances or notifications address the same topic at all.
- The overlap is only superficial (e.g., both mention the same person but discuss \
different things), or the proposal only matches a notification (system state) and has \
not been spoken in any recent utterance.

## Output format (STRICT)

Your output is parsed by a machine and the rewrite body is streamed straight to \
text-to-speech, so follow this exactly:

- For SUPPRESS: a single line `SUPPRESS | <brief reason>` and nothing else.
- For SPEAK: a single line `SPEAK | <brief reason>` and nothing else.
- For REWRITE: the first line must be exactly `REWRITE`, then a newline, then the \
rewritten speech itself (and nothing but the speech - no quotes, no labels, no \
explanation).
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
