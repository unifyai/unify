from pydantic import BaseModel, Field

from unity.common.hierarchical_logger import DEFAULT_ICON
from unity.common.llm_client import new_llm_client
from unity.logger import LOGGER


class SpeechDedup(BaseModel):
    already_covered: bool = Field(
        description=(
            "Whether the proposed speech has already been spoken aloud to the "
            "user, judged ONLY against recent assistant utterances (the spoken "
            "transcript). Notifications never count as 'already said' - they are "
            "system state, not speech the user has heard."
        ),
    )
    contradicts_current_state: bool = Field(
        default=False,
        description=(
            "Whether the proposed speech contradicts recent assistant "
            "utterances or recent notification state (e.g., offering setup "
            "steps when a notification confirmed setup is complete)."
        ),
    )
    reasoning: str = Field(
        default="",
        description="Brief explanation of the decision.",
    )

    @property
    def should_suppress(self) -> bool:
        return self.already_covered or self.contradicts_current_state


SPEECH_DEDUP_PROMPT = """\
You are deciding whether proposed speech from the slow brain should actually be \
spoken on a live voice call.

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

## Suppress (already_covered=True) when

Judge this ONLY against the recent assistant utterances above - never against \
notifications.

- The recent *utterances* already convey the same core information or conclusion
- The user has already been *told aloud* about the completion, result, or status that \
the proposed speech covers
- Speaking this would be noticeably redundant to the listener given what was *spoken*

If the only overlap is with a notification (and no spoken utterance covers it), \
already_covered MUST be False - the user has not heard it yet.

## Suppress (contradicts_current_state=True) when

- The proposed speech offers to walk through, set up, or redo steps that a recent \
notification or utterance confirms are already complete
- The proposed speech claims something is needed when a notification says it is done
- The proposed speech gives a next step for a workflow that notifications show has \
already progressed past that step
- The proposed speech contradicts the most recent assistant utterance (e.g., one says \
"everything is done" and the proposed speech says "want me to help set it up?")

## Allow (both False) when

- The proposed speech contains meaningfully new information not in recent utterances
- The proposed speech adds important detail beyond what was said
- No recent utterances or notifications address the same topic at all
- The overlap is only superficial (e.g., both mention the same person but discuss \
different things about them)
- The proposed speech only matches a notification (system state) and has not been \
spoken in any recent utterance - the user still needs to hear it

Output JSON matching the SpeechDedup schema.\
"""


class SpeechDeduplicationChecker:
    """Pre-speak gate that suppresses slow brain speech that is redundant or
    contradicts the current conversation state.

    Runs in the fast brain subprocess at speak time (inside ``maybe_speak_queued``).
    Compares the proposed ``response_text`` against recent assistant utterances
    and recent notifications in the fast brain's chat context. Suppresses speech
    that is redundant, stale, or contradictory.
    """

    def __init__(self, model: str | None = None) -> None:
        self._model = model

    async def evaluate(
        self,
        proposed_speech: str,
        recent_utterances: list[str],
        recent_notifications: list[str] | None = None,
    ) -> SpeechDedup:
        """Check whether *proposed_speech* should be suppressed.

        Returns ``SpeechDedup(already_covered=False)`` immediately when there
        are no recent utterances to compare against (no LLM call needed).
        On error, fails open (allows speech).
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
            return SpeechDedup(
                already_covered=False,
                reasoning="no recent context to compare against",
            )

        try:
            client = new_llm_client(
                self._model,
                origin="FastBrain.speech_dedup",
            )
            client.set_response_format(SpeechDedup)
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
                        "Apply the instructions above and output JSON matching "
                        "the SpeechDedup schema."
                    ),
                },
            ]
            response = await client.generate(messages=messages)
            return SpeechDedup.model_validate_json(response)
        except Exception as e:
            LOGGER.error(
                f"{DEFAULT_ICON} Error in SpeechDedup evaluation: {e}",
            )
            import traceback

            traceback.print_exc()
            return SpeechDedup(
                already_covered=False,
                reasoning="evaluation failed",
            )
