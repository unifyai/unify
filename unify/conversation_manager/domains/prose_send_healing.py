"""Heal prose-only slow-brain completions into outbound send tool calls."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

import unillm
from unillm.clients.completion_mutator import (
    CompletionMutator,
    CompletionMutatorContext,
)

from unify.conversation_manager.cm_types.medium import Medium
from unify.session_details import SESSION_DETAILS

if TYPE_CHECKING:
    from openai.types.chat import ChatCompletion

    from unify.conversation_manager.conversation_manager import ConversationManager

LOGGER = logging.getLogger("unify")

_PROSE_MAX_CHARS = 2000
_REASONING_PREFIXES = ("i need to", "let me think")
_XML_TOOL_MARKERS = ("<invoke", "<tool_call")

_SEND_TOOL_BY_MEDIUM: dict[Medium, tuple[str, str]] = {
    Medium.UNIFY_MESSAGE: ("send_unify_message", "send_unify_message_to_boss"),
    Medium.SMS_MESSAGE: ("send_sms", "send_sms_to_boss"),
    Medium.WHATSAPP_MESSAGE: ("send_whatsapp", "send_whatsapp_to_boss"),
    Medium.API_MESSAGE: ("send_api_response", "send_api_response_to_boss"),
    Medium.DISCORD_MESSAGE: ("send_discord_message", "send_discord_message_to_boss"),
    Medium.EMAIL: ("send_email", "send_email_to_boss"),
    Medium.SLACK_MESSAGE: ("send_slack_message", "send_slack_message_to_boss"),
    Medium.TEAMS_MESSAGE: ("send_teams_message", "send_teams_message_to_boss"),
}

_CHANNEL_SEND_TOOL_BY_MEDIUM: dict[Medium, str] = {
    Medium.DISCORD_CHANNEL_MESSAGE: "send_discord_channel_message",
    Medium.SLACK_CHANNEL_MESSAGE: "send_slack_channel_message",
    Medium.TEAMS_CHANNEL_MESSAGE: "send_teams_message",
}


def _forced_tool_choice(original_tool_choice: Any) -> bool:
    if original_tool_choice == "required":
        return True
    if isinstance(original_tool_choice, dict):
        return original_tool_choice.get("type") == "function"
    return False


def _message_content(completion: ChatCompletion) -> str | None:
    content = completion.choices[0].message.content
    if content is None:
        return None
    if isinstance(content, str):
        return content
    return str(content)


def _prose_is_healable(content: str) -> bool:
    stripped = content.strip()
    if not stripped or len(stripped) > _PROSE_MAX_CHARS:
        return False
    if stripped[0] in "{[":
        return False
    lower = stripped.lower()
    if lower.startswith(_REASONING_PREFIXES):
        return False
    if any(marker in lower for marker in _XML_TOOL_MARKERS):
        return False
    return True


def _email_subject_from_prose(prose: str) -> str:
    sentence_match = re.match(r"^(.+?[.!?])(?:\s|$)", prose.strip())
    candidate = sentence_match.group(1) if sentence_match else prose.strip()
    candidate = " ".join(candidate.split())
    if len(candidate) <= 80:
        return candidate
    trimmed = candidate[:80].rstrip()
    if " " in trimmed:
        trimmed = trimmed.rsplit(" ", 1)[0]
    return trimmed or "Reply"


def _resolve_send_tool_name(
    medium: Medium,
    *,
    is_coordinator: bool,
) -> str | None:
    channel_tool = _CHANNEL_SEND_TOOL_BY_MEDIUM.get(medium)
    if channel_tool is not None:
        return channel_tool
    pair = _SEND_TOOL_BY_MEDIUM.get(medium)
    if pair is None:
        return None
    return pair[1] if is_coordinator else pair[0]


def _build_send_arguments(
    *,
    medium: Medium,
    prose: str,
    reply_context: dict[str, Any],
) -> dict[str, Any] | None:
    contact_id = reply_context.get("contact_id")
    if medium == Medium.EMAIL:
        args: dict[str, Any] = {
            "body": prose,
            "subject": _email_subject_from_prose(prose),
        }
        email_id = reply_context.get("email_id")
        thread_id = reply_context.get("thread_id")
        if email_id:
            args["reply_all"] = True
            args["email_id_to_reply_to"] = email_id
            if thread_id:
                args["thread_id"] = thread_id
        elif contact_id is not None:
            args["to"] = [contact_id]
        else:
            return None
        return args

    if contact_id is None and medium != Medium.API_MESSAGE:
        return None

    args = {"content": prose, "contact_id": contact_id}

    if medium == Medium.API_MESSAGE:
        tags = reply_context.get("tags")
        if tags:
            args["tags"] = tags

    channel_id = reply_context.get("channel_id")
    team_id = reply_context.get("team_id")
    guild_id = reply_context.get("guild_id")
    chat_id = reply_context.get("chat_id")

    if medium == Medium.DISCORD_CHANNEL_MESSAGE:
        if not channel_id:
            return None
        args["channel_id"] = channel_id
        if guild_id:
            args["guild_id"] = guild_id

    if medium in (Medium.SLACK_CHANNEL_MESSAGE, Medium.SLACK_MESSAGE):
        if team_id:
            args["team_id"] = team_id
        if medium == Medium.SLACK_CHANNEL_MESSAGE:
            if not channel_id:
                return None
            args["channel_id"] = channel_id
            thread_ts = reply_context.get("thread_ts")
            if thread_ts:
                args["thread_ts"] = thread_ts

    if medium in (Medium.TEAMS_MESSAGE, Medium.TEAMS_CHANNEL_MESSAGE):
        if chat_id:
            args["chat_id"] = chat_id
        if medium == Medium.TEAMS_CHANNEL_MESSAGE:
            if not channel_id:
                return None
            args["channel_id"] = channel_id
            if team_id:
                args["team_id"] = team_id

    return args


def build_slow_brain_completion_mutator(
    cm: ConversationManager,
    *,
    trace_meta: dict[str, str],
    available_tool_names: set[str],
) -> CompletionMutator:
    """Return a completion mutator that heals prose into outbound send tools."""

    def mutator(
        completion: ChatCompletion,
        context: CompletionMutatorContext,
    ) -> ChatCompletion:
        from unify.conversation_manager.conversation_manager import (
            COMMISSIONING_OUTBOUND_FOLLOWUP_EVENTS,
        )

        request_kw = context.request_kw
        tools = request_kw.get("tools") or []
        if not tools:
            return completion

        msg = completion.choices[0].message
        if msg.tool_calls:
            return completion

        prose = _message_content(completion)
        if prose is None or not _prose_is_healable(prose):
            return completion

        if not _forced_tool_choice(context.original_tool_choice):
            return completion

        origin_event_name = trace_meta.get("origin_event_name") or ""
        if origin_event_name in COMMISSIONING_OUTBOUND_FOLLOWUP_EVENTS:
            return completion

        if cm.mode.is_voice:
            return completion

        reply_context = cm._last_inbound_reply_context
        if not reply_context:
            return completion

        medium_value = reply_context.get("medium")
        if not medium_value:
            return completion

        medium = Medium(medium_value)
        tool_name = _resolve_send_tool_name(
            medium,
            is_coordinator=SESSION_DETAILS.is_coordinator,
        )
        if tool_name is None or tool_name not in available_tool_names:
            return completion

        arguments = _build_send_arguments(
            medium=medium,
            prose=prose.strip(),
            reply_context=reply_context,
        )
        if arguments is None:
            return completion

        LOGGER.info(
            "prose_send_heal tool=%s medium=%s contact_id=%s",
            tool_name,
            medium.value,
            reply_context.get("contact_id"),
        )
        return unillm.inject_tool_call(
            completion,
            tool_name=tool_name,
            arguments=arguments,
        )

    return mutator
