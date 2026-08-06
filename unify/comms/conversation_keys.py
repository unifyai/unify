"""Conversation-thread exchange keys, shared by the live and headless paths.

A conversation key groups every message with one person (or in one provider
thread) under a single Transcripts exchange, whichever direction the message
travelled. The live ConversationManager writes the key when it logs inbound
and outbound traffic; the comms primitives read it back to recover routing
identifiers for a conversation nothing in the current process witnessed.

Both sides must derive byte-identical keys or the read silently misses, so
the format lives here rather than being rebuilt at each call site.
"""

from __future__ import annotations

from unify.conversation_manager.cm_types import Medium

# One-to-one direct-message channels. Each keys on ``contact_id`` and reuses a
# single exchange for the whole relationship with no inactivity window — the
# conversation with a person has one exchange id from start to end.
DM_MEDIA = frozenset(
    {
        Medium.SMS_MESSAGE,
        Medium.WHATSAPP_MESSAGE,
        Medium.DISCORD_MESSAGE,
        Medium.MS_TEAMS_BOT_MESSAGE,
        Medium.SLACK_MESSAGE,
    },
)
# Channels grouped by a native provider-supplied thread id. These reuse the
# exchange for the whole thread with no inactivity window (a reply days later
# still belongs to the thread). Unify Console chat keys on the unified
# chat-store thread id, so a team room and the 1:1 assistant DM never share
# an exchange.
PROVIDER_THREAD_MEDIA = frozenset(
    {
        Medium.DISCORD_CHANNEL_MESSAGE,
        Medium.MS_TEAMS_BOT_CHANNEL_MESSAGE,
        Medium.SLACK_CHANNEL_MESSAGE,
        Medium.EMAIL,
        Medium.UNIFY_MESSAGE,
    },
)
# Grouped mediums whose exchange is recovered from Exchanges metadata after the
# in-memory cache is lost (e.g. a CM restart), so the conversation keeps a single
# exchange id across restarts. DMs (keyed on the contact), email, and Unify
# Console chat (keyed on their thread) are durable; group channels stay
# in-memory only.
DURABLE_MEDIA = frozenset(DM_MEDIA | {Medium.EMAIL, Medium.UNIFY_MESSAGE})

# Exchange-metadata key the conversation key is stored under.
CONVERSATION_KEY_FIELD = "conversation_key"


def conversation_key(
    medium: Medium,
    *,
    contact_id: int | None = None,
    guild_id: str = "",
    channel_id: str = "",
    tenant_id: str = "",
    conversation_id: str = "",
    team_id: str = "",
    thread_ts: str = "",
    event_ts: str = "",
    thread_id: str = "",
) -> str | None:
    """Return the stable per-conversation key for one message.

    Both the inbound and outbound event for a conversation resolve to the
    same key, so an assistant reply lands in the same exchange as the
    message it answers. Keys use only fields carried by both directions:

    - DMs (SMS / WhatsApp / Discord / MS Teams bot / Slack) key on
      ``contact_id`` — the only identifier present on both the received and
      sent events.
    - Group channels key on the native provider thread: Discord on
      ``(guild_id, channel_id)``; MS Teams bot on
      ``(tenant_id, conversation_id)`` (the Bot Framework
      ``conversation_id`` already encodes the thread); Slack on
      ``(team_id, channel_id, thread_ts)`` where ``thread_ts`` falls back to
      the message's own ``event_ts`` for a top-level @mention (the value the
      reply threads under, so inbound and outbound resolve to the same
      thread).
    - Email keys on the provider ``thread_id`` (Gmail threadId / Outlook
      conversationId), carried on both received and sent events.

    Returns ``None`` when grouping should not apply (unknown medium or a
    required identifier is missing), so the caller falls back to a fresh
    exchange per message rather than grouping under a blank key.
    """
    if medium in DM_MEDIA:
        if contact_id is None:
            return None
        return f"{medium.value}:dm:{contact_id}"
    if medium == Medium.DISCORD_CHANNEL_MESSAGE:
        if not channel_id:
            return None
        return f"{medium.value}:{guild_id}:{channel_id}"
    if medium == Medium.MS_TEAMS_BOT_CHANNEL_MESSAGE:
        if not conversation_id:
            return None
        return f"{medium.value}:{tenant_id}:{conversation_id}"
    if medium == Medium.SLACK_CHANNEL_MESSAGE:
        thread = thread_ts or event_ts or ""
        if not channel_id or not thread:
            return None
        return f"{medium.value}:{team_id}:{channel_id}:{thread}"
    if medium in (Medium.EMAIL, Medium.UNIFY_MESSAGE):
        if not thread_id:
            return None
        return f"{medium.value}:thread:{thread_id}"
    return None


def conversation_key_for_event(
    event,
    medium: Medium,
    contact_id: int | None,
) -> str | None:
    """Derive the key from an event, reading whichever fields it carries."""
    return conversation_key(
        medium,
        contact_id=contact_id,
        guild_id=getattr(event, "guild_id", "") or "",
        channel_id=getattr(event, "channel_id", "") or "",
        tenant_id=getattr(event, "tenant_id", "") or "",
        conversation_id=getattr(event, "conversation_id", "") or "",
        team_id=getattr(event, "team_id", "") or "",
        thread_ts=getattr(event, "thread_ts", "") or "",
        event_ts=getattr(event, "event_ts", "") or "",
        thread_id=getattr(event, "thread_id", "") or "",
    )
