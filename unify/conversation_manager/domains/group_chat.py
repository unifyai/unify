"""Team group-chat participation.

Group chat runs outside the contact-keyed conversation pipeline: every
non-coordinator assistant on a team receives every human message
(``UnifyGroupMessageReceived``) and makes one policy-gated decision — reply
or stay silent — via a single structured LLM call. Replies are posted back
through Orchestra's admin endpoint, which persists them to the team's
``Teams/{team_id}/GroupChat`` context and publishes them to the Console
stream WITHOUT fanning out to other assistants, so AI replies can never
trigger further AI replies.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from unify.common.llm_client import new_llm_client
from unify.session_details import SESSION_DETAILS
from unify.settings import SETTINGS

if TYPE_CHECKING:
    from unify.conversation_manager.conversation_manager import ConversationManager
    from unify.conversation_manager.events import UnifyGroupMessageReceived

LOGGER = logging.getLogger("unify")

# Keep group-chat turns cheap: one low-effort decision per human message.
GROUP_CHAT_REASONING_EFFORT = "low"


class GroupChatDecision(BaseModel):
    """Structured outcome of one group-chat turn."""

    should_reply: bool = Field(
        description=(
            "True only when you are addressed/mentioned or have something "
            "materially useful to add that nobody else in the room is better "
            "placed to say."
        ),
    )
    reply: str = Field(
        default="",
        description="The message to post when should_reply is true; else empty.",
    )


def _is_mentioned(event: "UnifyGroupMessageReceived") -> bool:
    agent_id = SESSION_DETAILS.assistant.agent_id
    if agent_id is None:
        return False
    for mention in event.message.get("mentions") or []:
        if not isinstance(mention, dict):
            continue
        if mention.get("kind") == "assistant" and str(mention.get("id")) == str(
            agent_id,
        ):
            return True
    return False


def _render_message(message: dict) -> str:
    sender = message.get("sender_name") or "Unknown"
    kind = message.get("sender_kind") or "user"
    label = f"{sender} (AI)" if kind == "assistant" else sender
    return f"{label}: {message.get('content') or ''}"


def _build_system_prompt(event: "UnifyGroupMessageReceived") -> str:
    assistant = SESSION_DETAILS.assistant
    humans = [
        entry.get("name") or entry.get("user_id", "?")
        for entry in (event.participants.get("humans") or [])
    ]
    other_assistants = [
        entry.get("name") or f"Assistant {entry.get('assistant_id')}"
        for entry in (event.participants.get("assistants") or [])
        if str(entry.get("assistant_id")) != str(assistant.agent_id)
    ]
    persona_bits = [
        f"You are {assistant.name or 'an AI assistant'}, an AI assistant",
    ]
    if assistant.job_title:
        persona_bits.append(f"working as {assistant.job_title}")
    persona = " ".join(persona_bits) + "."
    if assistant.about:
        persona += f" About you: {assistant.about}"

    return (
        f"{persona}\n\n"
        f"You are a participant in the group chat of the team "
        f"'{event.team_name or event.team_id}'. "
        f"Humans in the room: {', '.join(humans) or 'none listed'}. "
        f"Other AI assistants in the room: "
        f"{', '.join(other_assistants) or 'none'}.\n\n"
        "Group-chat etiquette (strict):\n"
        "- Reply ONLY when you are directly addressed or @mentioned, or when "
        "you have something materially useful that nobody else in the room "
        "is better placed to say.\n"
        "- If another participant (human or AI) was named or is clearly "
        "better suited to answer, stay silent.\n"
        "- When in doubt, stay silent. Silence is the default in group "
        "chats; unnecessary replies are noise.\n"
        "- When you do reply, be concise and conversational; do not repeat "
        "the question or restate the thread.\n"
        "- Never speak on behalf of another participant."
    )


def _build_user_prompt(event: "UnifyGroupMessageReceived") -> str:
    lines = []
    if event.recent_messages:
        lines.append("Recent thread history (oldest first):")
        lines.extend(_render_message(msg) for msg in event.recent_messages)
        lines.append("")
    lines.append("New message just posted:")
    lines.append(_render_message(event.message))
    if _is_mentioned(event):
        lines.append("")
        lines.append(
            "You were @mentioned directly in this message, so you are "
            "expected to reply.",
        )
    lines.append("")
    lines.append("Decide whether to reply, and if so, compose your reply.")
    return "\n".join(lines)


def _post_reply_to_orchestra(team_id: int, content: str) -> bool:
    """Persist + publish one group-chat reply via Orchestra's admin API.

    Best-effort: returns False (and logs) on any failure so a delivery
    hiccup never crashes the event loop.
    """
    base_url = SETTINGS.ORCHESTRA_URL or ""
    admin_key = SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value() or ""
    agent_id = SESSION_DETAILS.assistant.agent_id
    if not base_url or not admin_key or agent_id is None:
        LOGGER.warning(
            "Skipping group-chat reply for team %s: orchestra config missing",
            team_id,
        )
        return False
    try:
        from unisdk.utils import http

        response = http.post(
            f"{base_url.rstrip('/')}/admin/teams/{team_id}/messages",
            headers={"Authorization": f"Bearer {admin_key}"},
            json={"assistant_id": int(agent_id), "content": content},
            timeout=15,
        )
        if 200 <= response.status_code < 300:
            return True
        LOGGER.warning(
            "Group-chat reply for team %s returned %d: %s",
            team_id,
            response.status_code,
            getattr(response, "text", ""),
        )
        return False
    except Exception as exc:
        LOGGER.warning("Group-chat reply for team %s failed: %s", team_id, exc)
        return False


async def handle_group_message(
    event: "UnifyGroupMessageReceived",
    cm: "ConversationManager",
) -> None:
    """One group-chat turn: decide via LLM, then post the reply (if any)."""
    client = new_llm_client(
        stateful=True,
        origin="group_chat",
        reasoning_effort=GROUP_CHAT_REASONING_EFFORT,
    )
    client.set_system_message(_build_system_prompt(event))

    from unify.common.single_shot import single_shot_tool_decision

    result = await single_shot_tool_decision(
        client,
        _build_user_prompt(event),
        tools={},
        tool_choice="none",
        response_format=GroupChatDecision,
    )
    decision = result.structured_output
    if decision is None or not decision.should_reply or not decision.reply.strip():
        cm._session_logger.info(
            "group_chat_silent",
            f"Stayed silent in team {event.team_name or event.team_id} group chat.",
        )
        return

    import asyncio

    posted = await asyncio.to_thread(
        _post_reply_to_orchestra,
        event.team_id,
        decision.reply.strip(),
    )
    cm._session_logger.info(
        "group_chat_reply",
        f"Replied in team {event.team_name or event.team_id} group chat"
        + ("" if posted else " (delivery failed)")
        + f": {decision.reply.strip()}",
    )
