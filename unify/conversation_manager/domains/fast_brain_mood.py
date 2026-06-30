from enum import StrEnum

from pydantic import BaseModel, Field

from unify.common.hierarchical_logger import DEFAULT_ICON
from unify.common.llm_client import new_llm_client
from unify.logger import LOGGER


class FastBrainMood(StrEnum):
    NEUTRAL_HAPPY = "neutral/happy"
    APOLOGETIC_SAD = "apologetic/sad"
    FRUSTRATED_ANGRY = "frustrated/angry"
    BORED = "bored"


class FastBrainMoodClassification(BaseModel):
    mood: FastBrainMood = Field(
        description="The avatar facial mood that best fits the full voice transcript.",
    )

    @property
    def avatar_mood(self) -> str:
        return {
            FastBrainMood.NEUTRAL_HAPPY: "happy",
            FastBrainMood.APOLOGETIC_SAD: "apologetic",
            FastBrainMood.FRUSTRATED_ANGRY: "frustrated",
            FastBrainMood.BORED: "bored",
        }[self.mood]


FAST_BRAIN_MOOD_PROMPT = """\
You classify the facial mood for a live assistant avatar.

Use the full fast-brain transcript below, including the latest turn, to choose exactly \
one mood:

- neutral/happy: friendly, calm, helpful, upbeat, or ordinary conversational state.
- apologetic/sad: apologizing, regretful, disappointed, sympathetic, or softly sad.
- frustrated/angry: annoyed, irritated, firm, angry, or dealing with clear user frustration.
- bored: low-energy, disengaged, waiting through repetitive or stalled conversation.

Prefer neutral/happy unless the transcript gives a clear reason for another expression. \
The output controls only the avatar's face, not what the assistant says.

## Latest turn

Role: {trigger_role}
Text: {trigger_text}

## Full fast-brain transcript

{transcript}

Output JSON matching the FastBrainMoodClassification schema.\
"""


class FastBrainMoodClassifier:
    def __init__(self, model: str) -> None:
        self._model = model

    async def evaluate(
        self,
        transcript: str,
        trigger_role: str,
        trigger_text: str,
    ) -> FastBrainMoodClassification | None:
        """Classify the avatar mood for the current fast-brain transcript."""
        try:
            client = new_llm_client(
                self._model,
                origin="FastBrain.mood_classification",
            )
            client.set_response_format(FastBrainMoodClassification)
            system_content = FAST_BRAIN_MOOD_PROMPT.format(
                transcript=transcript or "(no transcript)",
                trigger_role=trigger_role,
                trigger_text=trigger_text,
            )
            messages = [
                {"role": "system", "content": system_content},
                {
                    "role": "user",
                    "content": (
                        "Apply the instructions above and output JSON matching "
                        "the FastBrainMoodClassification schema."
                    ),
                },
            ]
            response = await client.generate(messages=messages)
            return FastBrainMoodClassification.model_validate_json(response)
        except Exception as e:
            LOGGER.error(
                f"{DEFAULT_ICON} Error in fast-brain mood classification: {e}",
            )
            import traceback

            traceback.print_exc()
            return None
