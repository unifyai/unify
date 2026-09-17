from enum import StrEnum


class Mode(StrEnum):
    """
    Enumeration of ConversationManager operational modes.

    The local runtime speaks one medium, so there is one mode: asynchronous
    text-based conversation.
    """

    TEXT = "text"


# Export valid values for validation/random selection
VALID_MODES: tuple[str, ...] = tuple(m.value for m in Mode)
