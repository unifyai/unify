from enum import StrEnum

from pydantic import BaseModel, Field

from .mode import Mode


class MediumInfo(BaseModel):
    """Metadata describing a communication medium."""

    value: str = Field(
        description="The unique string identifier for this medium used in the database",
    )
    description: str = Field(
        description="A natural language description of what this medium represents",
    )
    mode: Mode = Field(
        description="The ConversationManager operational mode for this medium",
    )


class Medium(StrEnum):
    """
    Enumeration of supported communication mediums.

    Medium serves as the single source of truth for communication channel
    types. Each medium value can be used directly as a conversation thread key.
    """

    UNIFY_MESSAGE = "unify_message"

    @property
    def info(self) -> MediumInfo:
        """Return the full Pydantic metadata model for this medium."""
        return MEDIUM_REGISTRY[self]

    @property
    def description(self) -> str:
        """Return the natural language description."""
        return self.info.description

    @property
    def mode(self) -> Mode:
        """Return the ConversationManager operational mode for this medium."""
        return self.info.mode


# Registry of metadata for each medium
MEDIUM_REGISTRY: dict[Medium, MediumInfo] = {
    Medium.UNIFY_MESSAGE: MediumInfo(
        value=Medium.UNIFY_MESSAGE,
        description="A text-based chat message sent within the unify chat interface.",
        mode=Mode.TEXT,
    ),
}

# Export valid values for validation/random selection
VALID_MEDIA: tuple[str, ...] = tuple(m.value for m in Medium)

# Contact field that identifies a counterpart on each external medium. The
# chat medium is internal and addresses contacts by id, so none is listed.
MEDIUM_TO_CONTACT_FIELD: dict[Medium, str] = {}
