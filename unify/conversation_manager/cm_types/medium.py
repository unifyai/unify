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
    Enumeration of all supported communication mediums.

    Medium serves as the single source of truth for communication channel types.
    Each medium value can be used directly as a conversation thread key.

    Naming convention uses _MESSAGE/_CALL suffixes to disambiguate related
    mediums (e.g., WHATSAPP_MESSAGE vs WHATSAPP_CALL).
    """

    UNIFY_MESSAGE = "unify_message"
    UNIFY_MEET = "unify_meet"
    EMAIL = "email"
    SMS_MESSAGE = "sms_message"
    WHATSAPP_MESSAGE = "whatsapp_message"
    WHATSAPP_CALL = "whatsapp_call"
    PHONE_CALL = "phone_call"
    GOOGLE_MEET = "google_meet"
    TEAMS_MEET = "teams_meet"
    API_MESSAGE = "api_message"
    DISCORD_MESSAGE = "discord_message"
    DISCORD_CHANNEL_MESSAGE = "discord_channel_message"
    SLACK_MESSAGE = "slack_message"
    SLACK_CHANNEL_MESSAGE = "slack_channel_message"
    TEAMS_MESSAGE = "teams_message"
    TEAMS_CHANNEL_MESSAGE = "teams_channel_message"
    MS_TEAMS_BOT_MESSAGE = "ms_teams_bot_message"
    MS_TEAMS_BOT_CHANNEL_MESSAGE = "ms_teams_bot_channel_message"
    UNIFY_REACTION = "unify_reaction"
    WHATSAPP_REACTION = "whatsapp_reaction"

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
        description="A text-based chat message sent within the internal Unify assistant interface.",
        mode=Mode.TEXT,
    ),
    Medium.UNIFY_MEET: MediumInfo(
        value=Medium.UNIFY_MEET,
        description="A live voice or video call conducted directly through the Unify platform.",
        mode=Mode.MEET,
    ),
    Medium.EMAIL: MediumInfo(
        value=Medium.EMAIL,
        description="An asynchronous email message.",
        mode=Mode.TEXT,
    ),
    Medium.SMS_MESSAGE: MediumInfo(
        value=Medium.SMS_MESSAGE,
        description="A standard SMS text message sent via cellular network.",
        mode=Mode.TEXT,
    ),
    Medium.WHATSAPP_MESSAGE: MediumInfo(
        value=Medium.WHATSAPP_MESSAGE,
        description="A WhatsApp message sent via Twilio WhatsApp Business API.",
        mode=Mode.TEXT,
    ),
    Medium.WHATSAPP_CALL: MediumInfo(
        value=Medium.WHATSAPP_CALL,
        description="A voice call initiated via WhatsApp Business calling.",
        mode=Mode.CALL,
    ),
    Medium.PHONE_CALL: MediumInfo(
        value=Medium.PHONE_CALL,
        description="A standard telephonic voice call.",
        mode=Mode.CALL,
    ),
    Medium.GOOGLE_MEET: MediumInfo(
        value=Medium.GOOGLE_MEET,
        description="A voice/video meeting conducted via Google Meet, joined by browser.",
        mode=Mode.MEET,
    ),
    Medium.TEAMS_MEET: MediumInfo(
        value=Medium.TEAMS_MEET,
        description="A voice/video meeting conducted via Microsoft Teams, joined by browser.",
        mode=Mode.MEET,
    ),
    Medium.API_MESSAGE: MediumInfo(
        value=Medium.API_MESSAGE,
        description="A programmatic message sent via the REST API.",
        mode=Mode.TEXT,
    ),
    Medium.DISCORD_MESSAGE: MediumInfo(
        value=Medium.DISCORD_MESSAGE,
        description="A direct message sent via a Discord bot.",
        mode=Mode.TEXT,
    ),
    Medium.DISCORD_CHANNEL_MESSAGE: MediumInfo(
        value=Medium.DISCORD_CHANNEL_MESSAGE,
        description="A message in a Discord guild channel, triggered by @mentioning the bot.",
        mode=Mode.TEXT,
    ),
    Medium.SLACK_MESSAGE: MediumInfo(
        value=Medium.SLACK_MESSAGE,
        description="A direct message sent via a Slack app.",
        mode=Mode.TEXT,
    ),
    Medium.SLACK_CHANNEL_MESSAGE: MediumInfo(
        value=Medium.SLACK_CHANNEL_MESSAGE,
        description="A message in a Slack channel, triggered by @mentioning the app.",
        mode=Mode.TEXT,
    ),
    Medium.TEAMS_MESSAGE: MediumInfo(
        value=Medium.TEAMS_MESSAGE,
        description="A message in a Microsoft Teams chat (1:1, group, or meeting chat).",
        mode=Mode.TEXT,
    ),
    Medium.TEAMS_CHANNEL_MESSAGE: MediumInfo(
        value=Medium.TEAMS_CHANNEL_MESSAGE,
        description="A message in a Microsoft Teams channel.",
        mode=Mode.TEXT,
    ),
    Medium.MS_TEAMS_BOT_MESSAGE: MediumInfo(
        value=Medium.MS_TEAMS_BOT_MESSAGE,
        description=(
            "A 1:1 direct message through the org-installed Unify Microsoft "
            "Teams bot app, routed via the Bot Framework rather than a user's "
            "delegated Microsoft account."
        ),
        mode=Mode.TEXT,
    ),
    Medium.MS_TEAMS_BOT_CHANNEL_MESSAGE: MediumInfo(
        value=Medium.MS_TEAMS_BOT_CHANNEL_MESSAGE,
        description=(
            "A message in a group chat or Microsoft Teams channel thread "
            "through the org-installed Unify Teams bot app, triggered by "
            "@mentioning the app and routed via the Bot Framework."
        ),
        mode=Mode.TEXT,
    ),
    Medium.UNIFY_REACTION: MediumInfo(
        value=Medium.UNIFY_REACTION,
        description="An emoji reaction on a Unify console chat message.",
        mode=Mode.TEXT,
    ),
    Medium.WHATSAPP_REACTION: MediumInfo(
        value=Medium.WHATSAPP_REACTION,
        description="An emoji reaction on a WhatsApp message.",
        mode=Mode.TEXT,
    ),
}

# Export valid values for validation/random selection
VALID_MEDIA: tuple[str, ...] = tuple(m.value for m in Medium)


# Maps each external Medium to the Contact field that uniquely identifies a
# sender on that medium. Used by inbound resolvers (e.g. CommsManager) to
# look up or create contacts from raw webhook details without re-deriving
# the mapping at every call site.
#
# Internal-only mediums (UNIFY_*, GOOGLE_MEET, API_MESSAGE) have no external
# contact identifier and are intentionally absent.
MEDIUM_TO_CONTACT_FIELD: dict[Medium, str] = {
    Medium.SMS_MESSAGE: "phone_number",
    Medium.PHONE_CALL: "phone_number",
    Medium.WHATSAPP_MESSAGE: "whatsapp_number",
    Medium.WHATSAPP_CALL: "whatsapp_number",
    Medium.EMAIL: "email_address",
    Medium.DISCORD_MESSAGE: "discord_id",
    Medium.DISCORD_CHANNEL_MESSAGE: "discord_id",
    Medium.SLACK_MESSAGE: "slack_user_id",
    Medium.SLACK_CHANNEL_MESSAGE: "slack_user_id",
    Medium.TEAMS_MESSAGE: "email_address",
    Medium.TEAMS_CHANNEL_MESSAGE: "email_address",
    Medium.TEAMS_MEET: "email_address",
    Medium.MS_TEAMS_BOT_MESSAGE: "email_address",
    Medium.MS_TEAMS_BOT_CHANNEL_MESSAGE: "email_address",
}
