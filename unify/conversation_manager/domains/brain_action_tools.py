"""
Brain action tools for ConversationManager.

All contact information is fetched from ContactManager (source of truth).
No local caching of contact data.

Context Propagation:
- When `act` is called, the current state snapshot is passed to Actor via _parent_chat_context
- For `interject` operations, only the incremental diff from the initial snapshot is sent
  via _parent_chat_context_cont, avoiding duplication of unchanged state
"""

from __future__ import annotations

import asyncio
from functools import wraps
import inspect
import re
from typing import TYPE_CHECKING, Any, Optional

from pydantic import BaseModel as _BaseModel
from pydantic import create_model as _create_model

from unify.common.prompt_helpers import now as prompt_now
from unify.logger import LOGGER
from unify.common.hierarchical_logger import ICONS
from unify.comms import CommsPrimitives
from unify.comms.outbound_origin import (
    mark_slow_brain_direct_outbound,
    reset_slow_brain_direct_outbound,
)
from unify.session_details import SESSION_DETAILS

from unify.conversation_manager.console_actions import catalogue_form
from unify.conversation_manager.cm_types import Mode
from unify.conversation_manager.domains.comms_utils import publish_console_script
from unify.conversation_manager.domains import managers_utils
from unify.conversation_manager.domains.onboarding_tool_gating import (
    masked_reference_quiz_tools,
)
from unify.conversation_manager.event_broker import get_event_broker
from unify.conversation_manager.events import (
    ActorHandleStarted,
    ActorHandleResponse,
    FastBrainNotification,
)
from unify.common._async_tool.dynamic_tools_factory import DynamicToolFactory
from unify.common._async_tool.utils import get_handle_paused_state
from unify.conversation_manager.task_actions import (
    OPERATION_MAP,
    safe_call_id_suffix,
)
from unify.conversation_manager.domains.renderer import (
    SnapshotState,
    compute_snapshot_diff,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from unify.conversation_manager.conversation_manager import ConversationManager


# ─────────────────────────────────────────────────────────────────────────────
# Schema dict → Pydantic model conversion
# ─────────────────────────────────────────────────────────────────────────────

_SCHEMA_TYPE_MAP: dict[str, type] = {
    "string": str,
    "str": str,
    "integer": int,
    "int": int,
    "number": float,
    "float": float,
    "boolean": bool,
    "bool": bool,
}


def _resolve_schema_type(schema: Any, name_hint: str) -> type:
    """Resolve a single schema value to a Python type.

    Handles:
    - String type names (``"string"``, ``"integer"``, …)
    - Nested dicts (recursively creates a child Pydantic model)
    - Lists where the first element defines the item schema
    """
    if isinstance(schema, str):
        return _SCHEMA_TYPE_MAP.get(schema.lower(), str)
    if isinstance(schema, dict):
        return schema_dict_to_pydantic(schema, name_hint)
    if isinstance(schema, list) and len(schema) > 0:
        item_type = _resolve_schema_type(schema[0], f"{name_hint}Item")
        return list[item_type]  # type: ignore[valid-type]
    return str  # fallback for unrecognised shapes


def schema_dict_to_pydantic(
    schema: dict,
    model_name: str = "ResponseFormat",
) -> type[_BaseModel]:
    """Convert a simplified schema dict to a dynamic Pydantic model.

    The schema uses a concise, LLM-friendly format:

    - **String values** are type names: ``"string"``, ``"integer"``,
      ``"number"``, ``"boolean"`` (shorthand ``"str"``, ``"int"``, etc.
      also accepted).
    - **Dict values** define nested object schemas (recursively converted).
    - **List values** define array types; the first element is the item
      schema.

    Examples::

        # Flat
        {"email": "string", "age": "integer"}

        # Nested with array
        {"contacts": [{"name": "string", "phone": "string"}], "total": "integer"}
    """
    fields: dict[str, tuple[type, ...]] = {}
    for field_name, field_schema in schema.items():
        field_type = _resolve_schema_type(
            field_schema,
            f"{model_name}{field_name.title()}",
        )
        fields[field_name] = (field_type, ...)
    return _create_model(model_name, **fields)


_next_handle_id = 0


# Pattern matching <in_flight_actions>...</in_flight_actions> sections.
# These contain CM-level steering tools that should not be exposed to the Actor.
_IN_FLIGHT_ACTIONS_PATTERN = re.compile(
    r"<in_flight_actions>.*?</in_flight_actions>\s*",
    re.DOTALL,
)

# Completed-action (and any other) <steering_tools> blocks list CM-only
# ask_/stop_/pause_/interject_* names that are not in the Actor's scope.
_STEERING_TOOLS_PATTERN = re.compile(
    r"<steering_tools>.*?</steering_tools>\s*",
    re.DOTALL,
)


def _strip_onboarding_completion_tool_lines(text: str) -> str:
    """Remove CM-only checklist completion instructions from Actor parent context."""
    return "\n".join(
        line for line in text.splitlines() if "set_onboarding_task_state" not in line
    )


def _strip_cm_only_tool_surface(text: str) -> str:
    """Remove CM brain tool listings that must not leak into Actor parent context."""
    text = _IN_FLIGHT_ACTIONS_PATTERN.sub("", text)
    text = _STEERING_TOOLS_PATTERN.sub("", text)
    return _strip_onboarding_completion_tool_lines(text)


def _filter_cm_state_for_actor(state_snapshot: dict) -> dict:
    """Filter CM state snapshot before passing to Actor as parent context.

    The CM state snapshot contains <in_flight_actions> with <steering_tools>
    listing CM-level tools (stop_, pause_, interject_, ask_) for each action.
    <completed_actions> keeps the same <steering_tools> surface for post-hoc
    ask_* tools. These are CM brain tools that exist only in the CM's tool
    surface.

    If passed verbatim to the Actor, the Actor LLM may interpret these tool
    names as callable functions and generate code like:
        await stop_search_the_web_for__1()
    This causes NameError since these tools don't exist in the Actor's scope.

    Onboarding narration that names ``set_onboarding_task_state`` is also
    stripped — checklist completion is owned by the parent CM brain, not act
    subtasks.

    This function strips the <in_flight_actions> section and any remaining
    <steering_tools> blocks while preserving other useful context
    (notifications, active_conversations, completed action results/history).

    Args:
        state_snapshot: The CM state snapshot dict with "content" key.

    Returns:
        A filtered copy of the snapshot with CM-only tool listings removed.
    """
    if not state_snapshot:
        return state_snapshot

    content = state_snapshot.get("content", "")
    if not content:
        return state_snapshot

    # When screenshots are attached, content is a list of multimodal parts
    # rather than a plain string. Apply the regex to each text part.
    if isinstance(content, list):
        filtered_parts = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                filtered_parts.append(
                    {**part, "text": _strip_cm_only_tool_surface(part["text"])},
                )
            else:
                filtered_parts.append(part)
        return {**state_snapshot, "content": filtered_parts}

    return {
        **state_snapshot,
        "content": _strip_cm_only_tool_surface(content),
    }


class _DesktopActionHandle:
    """Lightweight handle wrapping a single desktop primitive call.

    Provides the minimal interface consumed by the CM watcher infrastructure
    (``actor_watch_result``) so desktop fast-path actions participate in the
    same in-flight lifecycle as ``act`` and the contact/transcript fast paths.

    Steering operations (pause/resume/interject/ask) are no-ops because these
    are atomic single-step actions with no inner loop to steer.
    """

    def __init__(self, task: asyncio.Task):
        self._task = task
        self._notification_q: asyncio.Queue = asyncio.Queue()

    def done(self) -> bool:
        return self._task.done()

    async def result(self) -> str:
        return await self._task

    async def next_notification(self) -> dict:
        while not self._task.done():
            try:
                return await asyncio.wait_for(self._notification_q.get(), timeout=30)
            except asyncio.TimeoutError:
                continue
        raise asyncio.CancelledError

    async def next_clarification(self) -> dict:
        while not self._task.done():
            await asyncio.sleep(30)
        raise asyncio.CancelledError

    async def stop(self, reason=None, **kwargs):
        self._task.cancel()

    async def interject(self, message, **kwargs):
        pass

    async def ask(self, question, **kwargs):
        pass

    async def pause(self):
        pass

    async def resume(self):
        pass


def slow_brain_direct_comms(method):
    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        token = mark_slow_brain_direct_outbound()
        try:
            return await method(self, *args, **kwargs)
        finally:
            reset_slow_brain_direct_outbound(token)

    return wrapper


class ConversationManagerBrainActionTools:
    """
    Side-effecting tools for the Main CM Brain.

    All contact data is fetched from ContactManager - no local caching.
    """

    def __init__(self, cm: "ConversationManager"):
        self._cm = cm
        self._event_broker = get_event_broker()
        self._comms = CommsPrimitives(
            conversation_manager=cm,
            event_broker=self._event_broker,
        )

    def _boss_contact_id(self) -> int:
        return int(SESSION_DETAILS.boss_contact_id)

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_sms)
    async def send_sms(
        self,
        *,
        contact_id: int | str,
        content: str,
        phone_number: str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.send_sms(
            contact_id=contact_id,
            content=content,
            phone_number=phone_number,
        )

    @slow_brain_direct_comms
    async def send_sms_to_boss(
        self,
        *,
        content: str,
    ) -> dict[str, Any]:
        """Send an SMS message directly to my boss only.

        This Coordinator direct communication tool is restricted to the boss
        contact (``contact_id==1`` in the normal runtime). It cannot be used
        to text anyone else. If my boss asks me to draft or send an SMS on
        their behalf, route that work through ``act`` instead.

        Parameters
        ----------
        content : str
            The full message body to send to my boss. Required on every call:
            I author the complete text myself and pass it here, and never
            invoke this tool without it. When the body is something I make up
            on the spot (e.g. an onboarding sci-fi quiz clue), I write it out
            in full in this argument rather than leaving it empty.
        """
        return await self._comms.send_sms(
            contact_id=self._boss_contact_id(),
            content=content,
        )

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_whatsapp)
    async def send_whatsapp(
        self,
        *,
        contact_id: int | str,
        content: str,
        whatsapp_number: str | None = None,
        attachment_filepath: str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.send_whatsapp(
            contact_id=contact_id,
            content=content,
            whatsapp_number=whatsapp_number,
            attachment_filepath=attachment_filepath,
        )

    @slow_brain_direct_comms
    async def send_whatsapp_to_boss(
        self,
        *,
        content: str,
        attachment_filepath: str | None = None,
    ) -> dict[str, Any]:
        """Send a WhatsApp message directly to my boss only.

        This Coordinator direct communication tool is restricted to the boss
        contact (``contact_id==1`` in the normal runtime). It cannot be used
        to WhatsApp anyone else. If my boss asks me to draft or send a
        WhatsApp message on their behalf, route that work through ``act``
        instead.

        Delivery is NOT confirmed in the turn I call this tool. If the
        24-hour WhatsApp window is closed, my boss receives only a generic
        template placeholder (not my verbatim text), and the real body is
        queued to resend after they reply. I will not know which happened until
        it surfaces as a ``[You WhatsApped <name>]`` row (delivered) or a
        ``[You WhatsApped <name> (not delivered directly)]`` row (placeholder
        only). So in the same turn I send, I say only that I am *sending* it —
        never that it arrived, is waiting, or is in their inbox — and I confirm
        receipt only once that proof row appears. If it is
        ``(not delivered directly)``, I tell them a placeholder went out and
        they must reply to it before I can deliver the real content.

        Parameters
        ----------
        content : str
            Message body to send to my boss.
        attachment_filepath : str | None, optional
            Workspace-relative path for one attachment.
        """
        return await self._comms.send_whatsapp(
            contact_id=self._boss_contact_id(),
            content=content,
            attachment_filepath=attachment_filepath,
        )

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_discord_message)
    async def send_discord_message(
        self,
        *,
        contact_id: int | str,
        content: str,
        discord_id: str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.send_discord_message(
            contact_id=contact_id,
            content=content,
            discord_id=discord_id,
        )

    @slow_brain_direct_comms
    async def send_discord_message_to_boss(
        self,
        *,
        content: str,
    ) -> dict[str, Any]:
        """Send a Discord direct message to my boss only.

        This Coordinator direct communication tool is restricted to the boss
        contact (``contact_id==1`` in the normal runtime). It cannot be used
        to DM anyone else. If my boss asks me to draft or send a Discord
        message on their behalf, route that work through ``act`` instead.

        Parameters
        ----------
        content : str
            Message body to send to my boss.
        """
        return await self._comms.send_discord_message(
            contact_id=self._boss_contact_id(),
            content=content,
        )

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_discord_channel_message)
    async def send_discord_channel_message(
        self,
        *,
        channel_id: str,
        content: str,
        guild_id: str = "",
        contact_id: int | str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.send_discord_channel_message(
            channel_id=channel_id,
            content=content,
            guild_id=guild_id,
            contact_id=contact_id,
        )

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_slack_message)
    async def send_slack_message(
        self,
        *,
        contact_id: int | str,
        content: str,
        team_id: str | None = None,
        slack_user_id: str | None = None,
        thread_ts: str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.send_slack_message(
            contact_id=contact_id,
            content=content,
            team_id=team_id,
            slack_user_id=slack_user_id,
            thread_ts=thread_ts,
        )

    @slow_brain_direct_comms
    async def send_slack_message_to_boss(
        self,
        *,
        content: str,
        team_id: str | None = None,
        thread_ts: str | None = None,
    ) -> dict[str, Any]:
        """Send a Slack direct message to my boss only.

        This Coordinator direct communication tool is restricted to the boss
        contact (``contact_id==1`` in the normal runtime). It cannot be used
        to DM anyone else or post in channels. If my boss asks me to draft or
        send Slack messages on their behalf, route that work through ``act``
        instead.

        Parameters
        ----------
        content : str
            Message body to send to my boss.
        team_id : str | None, optional
            Slack workspace/team ID (T...). Auto-resolved from my connected
            Slack workspace, so leave it unset for the normal single-
            workspace case; only pass it to override for a multi-workspace
            send.
        thread_ts : str | None, optional
            Existing boss DM thread timestamp to reply inside.
        """
        return await self._comms.send_slack_message(
            contact_id=self._boss_contact_id(),
            content=content,
            team_id=team_id,
            thread_ts=thread_ts,
        )

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_ms_teams_bot_message)
    async def send_ms_teams_bot_message(
        self,
        *,
        contact_id: int | str,
        content: str,
        tenant_id: str | None = None,
        conversation_id: str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.send_ms_teams_bot_message(
            contact_id=contact_id,
            content=content,
            tenant_id=tenant_id,
            conversation_id=conversation_id,
        )

    @slow_brain_direct_comms
    async def send_ms_teams_bot_message_to_boss(
        self,
        *,
        content: str,
        tenant_id: str,
        conversation_id: str,
    ) -> dict[str, Any]:
        """Reply to my boss only through the org-installed Unify Teams app.

        This Coordinator direct communication tool is restricted to the boss
        contact (``contact_id==1`` in the normal runtime). It cannot be used
        to DM anyone else or post in group chats or Teams channels. If my boss
        asks me to draft or send Teams messages on their behalf, route that
        work through ``act`` instead.

        The org-installed Unify Teams app (Bot Framework) replies into a
        conversation it was already addressed in, so this tool can only answer
        an inbound Teams message from my boss — pass the ``tenant_id`` and
        ``conversation_id`` surfaced on that inbound message. It is distinct
        from ``send_teams_message`` (my boss's own delegated Microsoft
        account).

        Parameters
        ----------
        content : str
            Message body to send to my boss.
        tenant_id : str
            Microsoft AAD tenant id from the inbound boss Teams message.
        conversation_id : str
            Bot Framework conversation id to reply into, from the inbound
            boss Teams message.
        """
        return await self._comms.send_ms_teams_bot_message(
            contact_id=self._boss_contact_id(),
            content=content,
            tenant_id=tenant_id,
            conversation_id=conversation_id,
        )

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_ms_teams_bot_channel_message)
    async def send_ms_teams_bot_channel_message(
        self,
        *,
        contact_id: int | str,
        content: str,
        tenant_id: str,
        conversation_id: str,
    ) -> dict[str, Any]:
        return await self._comms.send_ms_teams_bot_channel_message(
            contact_id=contact_id,
            content=content,
            tenant_id=tenant_id,
            conversation_id=conversation_id,
        )

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_slack_channel_message)
    async def send_slack_channel_message(
        self,
        *,
        channel_id: str,
        content: str,
        team_id: str | None = None,
        thread_ts: str | None = None,
        contact_id: int | str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.send_slack_channel_message(
            channel_id=channel_id,
            content=content,
            team_id=team_id,
            thread_ts=thread_ts,
            contact_id=contact_id,
        )

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_telegram_message)
    async def send_telegram_message(
        self,
        *,
        contact_id: int | str,
        content: str,
        telegram_id: str | None = None,
        chat_id: str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.send_telegram_message(
            contact_id=contact_id,
            content=content,
            telegram_id=telegram_id,
            chat_id=chat_id,
        )

    @slow_brain_direct_comms
    async def send_telegram_message_to_boss(
        self,
        *,
        content: str,
    ) -> dict[str, Any]:
        """Send a Telegram direct message to my boss only.

        This Coordinator direct communication tool is restricted to the boss
        contact. It cannot be used to message anyone else. If my boss asks me
        to draft or send a Telegram message on their behalf, route that work
        through ``act`` instead.

        Parameters
        ----------
        content : str
            Message body to send to my boss.
        """
        return await self._comms.send_telegram_message(
            contact_id=self._boss_contact_id(),
            content=content,
        )

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_telegram_group_message)
    async def send_telegram_group_message(
        self,
        *,
        chat_id: str,
        content: str,
        contact_id: int | str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.send_telegram_group_message(
            chat_id=chat_id,
            content=content,
            contact_id=contact_id,
        )

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_teams_message)
    async def send_teams_message(
        self,
        *,
        contact_id: int | str | list[int | str | dict],
        content: str,
        chat_id: str | None = None,
        channel_id: str | None = None,
        team_id: str | None = None,
        chat_topic: str | None = None,
        attachment_filepath: str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.send_teams_message(
            contact_id=contact_id,
            content=content,
            chat_id=chat_id,
            channel_id=channel_id,
            team_id=team_id,
            chat_topic=chat_topic,
            attachment_filepath=attachment_filepath,
        )

    @slow_brain_direct_comms
    async def send_teams_message_to_boss(
        self,
        *,
        content: str,
        chat_id: str | None = None,
        attachment_filepath: str | None = None,
    ) -> dict[str, Any]:
        """Send a Microsoft Teams message directly to my boss only.

        This Coordinator direct communication tool is restricted to the boss
        contact (``contact_id==1`` in the normal runtime). It cannot be used
        to start group chats or post in Teams channels. If my boss asks me to
        draft or send Teams messages on their behalf, route that work through
        ``act`` instead.

        Parameters
        ----------
        content : str
            Message body to send to my boss.
        chat_id : str | None, optional
            Existing boss Teams chat ID to reply inside. Omit to create or
            reuse a 1:1 chat with my boss.
        attachment_filepath : str | None, optional
            Workspace-relative path for one attachment.
        """
        return await self._comms.send_teams_message(
            contact_id=self._boss_contact_id(),
            content=content,
            chat_id=chat_id,
            attachment_filepath=attachment_filepath,
        )

    @wraps(CommsPrimitives.create_teams_channel)
    async def create_teams_channel(
        self,
        *,
        team_id: str,
        display_name: str,
        description: str | None = None,
        membership_type: str = "standard",
        owner_contact_ids: list[int | str | dict] | None = None,
    ) -> dict[str, Any]:
        return await self._comms.create_teams_channel(
            team_id=team_id,
            display_name=display_name,
            description=description,
            membership_type=membership_type,
            owner_contact_ids=owner_contact_ids,
        )

    @wraps(CommsPrimitives.create_teams_meet)
    async def create_teams_meet(
        self,
        *,
        mode: str = "scheduled",
        subject: str | None = None,
        start: str | None = None,
        duration_minutes: int = 30,
        timezone: str = "UTC",
        attendee_contact_ids: list[int | str | dict] | None = None,
        body_html: str | None = None,
        location: str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.create_teams_meet(
            mode=mode,
            subject=subject,
            start=start,
            duration_minutes=duration_minutes,
            timezone=timezone,
            attendee_contact_ids=attendee_contact_ids,
            body_html=body_html,
            location=location,
        )

    async def create_teams_meet_with_boss(
        self,
        *,
        mode: str = "scheduled",
        subject: str | None = None,
        start: str | None = None,
        duration_minutes: int = 30,
        timezone: str = "UTC",
        body_html: str | None = None,
        location: str | None = None,
    ) -> dict[str, Any]:
        """Create a Microsoft Teams meeting with my boss only.

        This Coordinator direct communication tool is restricted to the boss
        contact (``contact_id==1`` in the normal runtime). Scheduled meetings
        invite only my boss. If my boss asks me to schedule or host meetings
        with other people on their behalf, route that work through ``act``
        instead.

        Parameters
        ----------
        mode : str, optional
            ``"scheduled"`` (default) or ``"instant"``.
        subject : str | None, optional
            Meeting subject. Required for scheduled meetings.
        start : str | None, optional
            ISO-8601 start timestamp for scheduled meetings.
        duration_minutes : int, optional
            Meeting duration in minutes for scheduled meetings.
        timezone : str, optional
            Timezone name forwarded to Microsoft Graph.
        body_html : str | None, optional
            Meeting body, sent to Graph as HTML.
        location : str | None, optional
            Display-name location for the calendar event.
        """
        return await self._comms.create_teams_meet(
            mode=mode,
            subject=subject,
            start=start,
            duration_minutes=duration_minutes,
            timezone=timezone,
            attendee_contact_ids=(
                [self._boss_contact_id()] if mode == "scheduled" else None
            ),
            body_html=body_html,
            location=location,
        )

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_unify_message)
    async def send_unify_message(
        self,
        *,
        content: str,
        contact_id: int | str | None = None,
        attachment_filepath: str | None = None,
        team_id: int | None = None,
        group_id: int | None = None,
    ) -> dict[str, Any]:
        return await self._comms.send_unify_message(
            content=content,
            contact_id=contact_id,
            attachment_filepath=attachment_filepath,
            team_id=team_id,
            group_id=group_id,
        )

    @slow_brain_direct_comms
    async def send_unify_message_to_boss(
        self,
        *,
        content: str,
        attachment_filepath: str | None = None,
    ) -> dict[str, Any]:
        """Send a Unify inbox message directly to my boss only.

        This Coordinator direct communication tool is restricted to the boss
        contact (``contact_id==1`` in the normal runtime). It cannot be used
        to message anyone else. If my boss asks me to draft or send Unify
        messages on their behalf, route that work through ``act`` instead.

        Parameters
        ----------
        content : str
            Message body to send to my boss.
        attachment_filepath : str | None, optional
            Workspace-relative path for one attachment.
        """
        return await self._comms.send_unify_message(
            content=content,
            contact_id=self._boss_contact_id(),
            attachment_filepath=attachment_filepath,
        )

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_api_response)
    async def send_api_response(
        self,
        *,
        content: str,
        contact_id: int | str = 1,
        attachment_filepaths: list[str] | None = None,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        return await self._comms.send_api_response(
            content=content,
            contact_id=contact_id,
            attachment_filepaths=attachment_filepaths,
            tags=tags,
        )

    @slow_brain_direct_comms
    async def send_api_response_to_boss(
        self,
        *,
        content: str,
        attachment_filepaths: list[str] | None = None,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        """Reply to the pending API message with the boss as transcript anchor.

        This Coordinator direct communication tool never exposes a recipient
        choice. The response is anchored to the boss contact
        (``contact_id==1`` in the normal runtime). If my boss asks me to
        perform communication work on their behalf, route that work through
        ``act`` instead.

        Parameters
        ----------
        content : str
            Response text to send back to the waiting API caller.
        attachment_filepaths : list[str] | None, optional
            Workspace-local file paths to upload and attach to the API response.
        tags : list[str] | None, optional
            API routing tags to return; omit to echo inbound tags.
        """
        return await self._comms.send_api_response(
            content=content,
            contact_id=self._boss_contact_id(),
            attachment_filepaths=attachment_filepaths,
            tags=tags,
        )

    @slow_brain_direct_comms
    @wraps(CommsPrimitives.send_email)
    async def send_email(
        self,
        *,
        to: list[int | dict] | None = None,
        cc: list[int | dict] | None = None,
        bcc: list[int | dict] | None = None,
        subject: str,
        body: str,
        reply_all: bool = False,
        email_id_to_reply_to: str | None = None,
        thread_id: str | None = None,
        attachment_filepath: str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.send_email(
            to=to,
            cc=cc,
            bcc=bcc,
            subject=subject,
            body=body,
            reply_all=reply_all,
            email_id_to_reply_to=email_id_to_reply_to,
            thread_id=thread_id,
            attachment_filepath=attachment_filepath,
        )

    @slow_brain_direct_comms
    async def send_email_to_boss(
        self,
        *,
        subject: str,
        body: str,
        email_id_to_reply_to: str | None = None,
        thread_id: str | None = None,
        attachment_filepath: str | None = None,
    ) -> dict[str, Any]:
        """Send an email directly to my boss only.

        This Coordinator direct communication tool is restricted to the boss
        contact (``contact_id==1`` in the normal runtime). It cannot be used
        to email anyone else, cc anyone else, bcc anyone else, or reply-all to
        a thread. If my boss asks me to draft or send email on their behalf,
        route that work through ``act`` instead.

        Parameters
        ----------
        subject : str
            Subject line for the email to my boss.
        body : str
            Email body content to send to my boss.
        email_id_to_reply_to : str | None, optional
            Existing boss email ID to reply to for threading.
        thread_id : str | None, optional
            Provider thread identifier shown on the inbound email.
        attachment_filepath : str | None, optional
            Workspace-relative path for one attachment.
        """
        return await self._comms.send_email(
            to=[self._boss_contact_id()],
            subject=subject,
            body=body,
            email_id_to_reply_to=email_id_to_reply_to,
            thread_id=thread_id,
            attachment_filepath=attachment_filepath,
        )

    @wraps(CommsPrimitives.make_call)
    async def make_call(
        self,
        *,
        contact_id: int | str,
        opener: str,
        briefing: str | None = None,
        allow_hang_up: str | None = None,
        phone_number: str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.make_call(
            contact_id=contact_id,
            opener=opener,
            briefing=briefing,
            allow_hang_up=allow_hang_up,
            phone_number=phone_number,
        )

    async def make_call_to_boss(
        self,
        *,
        opener: str,
        briefing: str | None = None,
        allow_hang_up: str | None = None,
    ) -> dict[str, Any]:
        """Start an outbound phone call to my boss only.

        This Coordinator direct communication tool is restricted to the boss
        contact (``contact_id==1`` in the normal runtime). It cannot be used
        to call anyone else. If my boss asks me to call someone on their
        behalf, route that work through ``act`` instead.

        Parameters
        ----------
        opener : str
            Required. The exact words spoken to open the call — delivered
            verbatim right after my boss's brief "Hello?" (or a few seconds of
            silence). Write it so it reads naturally either way.
        briefing : str | None, optional
            Unspoken context for the live voice on the call — never read
            aloud. Describe the full task design (purpose, key facts, expected
            responses, how to confirm, and the wrap-up to give) so the voice
            runs the whole interaction itself.
        allow_hang_up : str | None, optional
            Pre-sanction ending the call, with a one-line reason. Set this
            whenever the call is expected to be short (a single question, a
            message delivery, a quick confirmation) so the live voice can end
            it at the natural close without waiting on me; revoke mid-call
            with ``withdraw_hang_up`` if it turns into a longer conversation.
        """
        return await self._comms.make_call(
            contact_id=self._boss_contact_id(),
            opener=opener,
            briefing=briefing,
            allow_hang_up=allow_hang_up,
        )

    @wraps(CommsPrimitives.make_whatsapp_call)
    async def make_whatsapp_call(
        self,
        *,
        contact_id: int | str,
        opener: str,
        briefing: str | None = None,
        allow_hang_up: str | None = None,
        whatsapp_number: str | None = None,
    ) -> dict[str, Any]:
        return await self._comms.make_whatsapp_call(
            contact_id=contact_id,
            opener=opener,
            briefing=briefing,
            allow_hang_up=allow_hang_up,
            whatsapp_number=whatsapp_number,
        )

    async def make_whatsapp_call_to_boss(
        self,
        *,
        opener: str,
        briefing: str | None = None,
        allow_hang_up: str | None = None,
    ) -> dict[str, Any]:
        """Start a WhatsApp voice call to my boss only.

        This Coordinator direct communication tool is restricted to the boss
        contact (``contact_id==1`` in the normal runtime). It cannot be used
        to call anyone else. If my boss asks me to call someone on their
        behalf, route that work through ``act`` instead.

        Parameters
        ----------
        opener : str
            Required. The exact words spoken to open the call — delivered
            verbatim right after my boss's brief "Hello?" (or a few seconds of
            silence). Write it so it reads naturally either way.
        briefing : str | None, optional
            Unspoken context for the live voice on the call — never read
            aloud. Describe the full task design (purpose, key facts, expected
            responses, how to confirm, and the wrap-up to give) so the voice
            runs the whole interaction itself.
        allow_hang_up : str | None, optional
            Pre-sanction ending the call, with a one-line reason. Set this
            whenever the call is expected to be short (a single question, a
            message delivery, a quick confirmation) so the live voice can end
            it at the natural close without waiting on me; revoke mid-call
            with ``withdraw_hang_up`` if it turns into a longer conversation.
        """
        return await self._comms.make_whatsapp_call(
            contact_id=self._boss_contact_id(),
            opener=opener,
            briefing=briefing,
            allow_hang_up=allow_hang_up,
        )

    async def join_google_meet(
        self,
        meet_url: str,
        opener: str,
    ) -> dict[str, Any]:
        """Join a Google Meet call.

        This is the **only** way to join a Google Meet — it configures audio
        devices, establishes the voice pipeline, and dispatches the voice
        agent so the assistant can hear and speak in the meeting.  Never
        attempt to join a Meet URL via ``act``.

        I do not create the meeting: the user hosts it and pastes the link.
        Wait for a real ``meet.google.com`` link before calling this rather
        than guessing one.

        The system automatically messages the user that I'm joining and then
        reports the outcome (in the meeting / waiting in the lobby / couldn't
        join) in chat on my behalf. Do NOT send a separate chat message
        announcing or narrating the join (no "Joining now", "I'm in", etc.) —
        just call this tool with the spoken ``opener``. Only send a chat
        message afterwards if the user asks something that needs a reply.

        Args:
            meet_url: The full Google Meet URL (e.g. https://meet.google.com/abc-defg-hij).
            opener: Verbatim spoken line once the meeting is live and someone
                is listening. Write the exact words to speak.
        """
        if (
            self._cm.call_manager.has_active_call
            or self._cm.call_manager.has_active_google_meet
            or self._cm.call_manager.has_active_teams_meet
        ):
            return {
                "status": "error",
                "message": "A call or meeting is already active.",
            }

        self._cm.call_manager.pending_opener = opener.strip()

        from unify.conversation_manager.events import GoogleMeetReceived

        boss = (
            self._cm.contact_index.get_contact(
                contact_id=SESSION_DETAILS.boss_contact_id,
            )
            or {}
        )
        event = GoogleMeetReceived(contact=boss, meet_url=meet_url)
        await self._event_broker.publish(event.topic, event.to_json())
        return {"status": "ok", "message": f"Joining Google Meet at {meet_url}"}

    async def send_meet_chat(self, message: str) -> dict[str, Any]:
        """Post a message into the meeting chat of the active call.

        Use this for anything a participant will want to read rather than hear:
        a link, an address, a spelling, a list of steps. Speech is gone the
        moment it is said; chat stays in the meeting for people to scroll back
        to and copy from.

        Not a substitute for speaking. Say what matters out loud and put the
        copyable detail in chat.
        """
        result = await self._cm.call_manager.send_meet_chat(message)
        if result:
            # Recorded only on success, so a failed post cannot leave a reply in
            # the transcript that no participant ever saw.
            from unify.conversation_manager.events import (
                GoogleMeetChatSent,
                TeamsMeetChatSent,
            )

            sent_cls = (
                TeamsMeetChatSent
                if self._cm.call_manager.has_active_teams_meet
                else GoogleMeetChatSent
            )
            boss = (
                self._cm.contact_index.get_contact(
                    contact_id=SESSION_DETAILS.boss_contact_id,
                )
                or {}
            )
            sent = sent_cls(contact=boss, content=message)
            await self._event_broker.publish(sent.topic, sent.to_json())
            return {"status": "ok", "message": "Posted to the meeting chat."}
        return {
            "status": "error",
            # Chat is genuinely unavailable in some meetings (Teams channel
            # meetings have no bot-writable chat), so tell the brain to say it
            # out loud rather than let it retry a send that cannot succeed.
            "message": (
                "Could not post to the meeting chat. This meeting may not "
                "support it -- say the information out loud instead."
            ),
        }

    async def start_meet_screenshare(self) -> dict[str, Any]:
        """Share my desktop with everyone in the meeting, so they can watch me work.

        Use this when showing is genuinely better than telling -- walking someone
        through where a setting lives, or letting them watch a task run. What
        participants see is my own machine, at a few frames a second: fine for
        reading a screen, not for anything that depends on smooth motion.

        Say what I am about to show before calling this. A screen appearing with
        no explanation makes people hunt for what changed.
        """
        ok = await self._cm.call_manager.start_meet_screenshare()
        if ok:
            return {
                "status": "ok",
                "message": "Sharing my desktop with the meeting.",
            }
        return {
            "status": "error",
            "message": (
                "Could not start sharing my desktop. Describe what I would have "
                "shown instead of saying a screen is up."
            ),
        }

    async def stop_meet_screenshare(self) -> dict[str, Any]:
        """Stop sharing my desktop with the meeting.

        Worth doing as soon as the thing I was showing is done: a stale screen
        left up reads as inattention, and participants keep watching it.
        """
        ok = await self._cm.call_manager.stop_meet_screenshare()
        if ok:
            return {"status": "ok", "message": "Stopped sharing my desktop."}
        return {
            "status": "error",
            "message": "Could not stop sharing -- my desktop may already be down.",
        }

    async def _leave_google_meet(self) -> dict[str, Any]:
        """Disconnect the assistant from the active Google Meet session.

        Internal helper routed through ``hang_up``; closes the browser and tears
        down the audio bridge.
        """
        if not self._cm.call_manager.has_active_google_meet:
            return {
                "status": "error",
                "message": "No active Google Meet session to leave.",
            }

        # Stop the browser agent immediately so the assistant disappears
        # from the Meet before the event handler's full cleanup pipeline.
        from unify.conversation_manager.events import GoogleMeetEnded

        contact = self._cm.call_manager._disconnect_contact or {}
        event = GoogleMeetEnded(contact=contact)
        await self._event_broker.publish(event.topic, event.to_json())
        return {"status": "ok", "message": "Leaving Google Meet"}

    async def join_teams_meet(
        self,
        meet_url: str,
        opener: str,
    ) -> dict[str, Any]:
        """Join a Microsoft Teams meeting.

        This is the **only** way to join a Teams meeting — it configures audio
        devices, establishes the voice pipeline, and dispatches the voice
        agent so the assistant can hear and speak in the meeting.  Never
        attempt to join a Teams meeting URL via ``act``.

        I do not create the meeting: the user hosts it and pastes the link.
        Wait for a real ``teams.microsoft.com`` / ``teams.live.com`` link
        before calling this rather than guessing one.

        The system automatically messages the user that I'm joining and then
        reports the outcome (in the meeting / waiting in the lobby / couldn't
        join) in chat on my behalf. Do NOT send a separate chat message
        announcing or narrating the join (no "Joining now", "I'm in", etc.) —
        just call this tool with the spoken ``opener``. Only send a chat
        message afterwards if the user asks something that needs a reply.

        Args:
            meet_url: The full Teams meeting URL (e.g.
                https://teams.microsoft.com/l/meetup-join/... or
                https://teams.live.com/meet/...).
            opener: Verbatim spoken line once the meeting is live and someone
                is listening. Write the exact words to speak.
        """
        if (
            self._cm.call_manager.has_active_call
            or self._cm.call_manager.has_active_google_meet
            or self._cm.call_manager.has_active_teams_meet
        ):
            return {
                "status": "error",
                "message": "A call or meeting is already active.",
            }

        self._cm.call_manager.pending_opener = opener.strip()

        from unify.conversation_manager.events import TeamsMeetReceived

        boss = (
            self._cm.contact_index.get_contact(
                contact_id=SESSION_DETAILS.boss_contact_id,
            )
            or {}
        )
        event = TeamsMeetReceived(contact=boss, meet_url=meet_url)
        await self._event_broker.publish(event.topic, event.to_json())
        return {"status": "ok", "message": f"Joining Teams meeting at {meet_url}"}

    async def _leave_teams_meet(self) -> dict[str, Any]:
        """Disconnect the assistant from the active Teams meeting session.

        Internal helper routed through ``hang_up``; closes the browser and tears
        down the audio bridge.
        """
        if not self._cm.call_manager.has_active_teams_meet:
            return {
                "status": "error",
                "message": "No active Teams meeting session to leave.",
            }

        from unify.conversation_manager.events import TeamsMeetEnded

        contact = self._cm.call_manager._disconnect_contact or {}
        event = TeamsMeetEnded(contact=contact)
        await self._event_broker.publish(event.topic, event.to_json())
        return {"status": "ok", "message": "Leaving Teams meeting"}

    async def start_unify_meet(
        self,
        opener: str,
        briefing: str | None = None,
        allow_hang_up: str | None = None,
    ) -> dict[str, Any]:
        """Ring my boss on Unify Meet (the in-app live call) and ask them to answer.

        Unify Meet is the in-app live call inside the Console - the canonical
        place to talk face to face (no phone credits, nothing to hold). Use this
        to move the conversation onto that call, e.g. to continue onboarding on
        the live call after verifying another channel.

        I cannot join my boss's browser for them: this rings them, and a pinned
        incoming-call window with an Answer button appears in their Console. When
        they answer, the call connects and I join automatically. If they do not
        answer shortly, I will be told to continue with them over text instead.

        Only one voice session can be active at a time, so if I am currently on a
        phone or WhatsApp call I must ``hang_up`` first, then ring the Meet.

        Args:
            opener: Verbatim spoken line once the call connects. Write the exact
                words to speak.
            briefing: Optional unspoken context for the live voice — never read
                aloud. Describe the full task design (purpose, key facts,
                expected responses, how to confirm, and the wrap-up to give)
                so the voice runs the whole interaction itself.
            allow_hang_up: Optional one-line reason that pre-sanctions ending
                the call. Set it whenever the call is expected to be short (a
                single question, a message delivery, a quick confirmation) so
                the live voice can end it at the natural close without waiting
                on me; revoke mid-call with ``withdraw_hang_up`` if it turns
                into a longer conversation.
        """
        if (
            self._cm.call_manager.has_active_call
            or self._cm.call_manager.has_active_google_meet
            or self._cm.call_manager.has_active_teams_meet
        ):
            return {
                "status": "error",
                "message": (
                    "A call or meeting is already active. Hang up first, then "
                    "ring the Unify Meet."
                ),
            }
        return await self._cm.ring_unify_meet(
            opener=opener,
            briefing=briefing,
            allow_hang_up=allow_hang_up,
        )

    async def allow_hang_up(self, reason: str) -> dict[str, Any]:
        """Sanction ending the current call; the live voice picks the moment.

        Arms the hang-up gate: the live voice on the call gains permission to
        end it, and will do so at the natural close — typically right after
        goodbyes are exchanged, or after a stretch of dead air. It will NOT cut
        anyone off mid-conversation, and it keeps handling substantive turns
        normally until the close actually arrives.

        This is the preferred way to end a call when the conversation is
        wrapping up: I decide THAT ending is right, the live voice decides
        WHEN. Use ``hang_up`` instead only when my boss explicitly tells me to
        end the call right now.

        If the conversation moves on to something new after I arm this, I use
        ``withdraw_hang_up`` to take the permission back.

        Parameters
        ----------
        reason : str
            One short line on why wrapping up is appropriate (e.g. "channel
            test complete — wrap up warmly"). The live voice sees this
            verbatim as its guidance for the close.
        """
        if not self._cm.in_voice_session:
            return {
                "status": "error",
                "message": "No active voice session to sanction ending.",
            }
        reason = " ".join((reason or "").split()).strip()
        if not reason:
            return {
                "status": "error",
                "message": (
                    "reason is required: state briefly why wrapping up is "
                    "appropriate."
                ),
            }
        await self._cm.call_manager.set_hang_up_gate(reason)
        return {
            "status": "ok",
            "message": (
                "Hang-up gate armed — the live voice will end the call at the "
                "natural close. Use withdraw_hang_up if the conversation "
                "moves on instead."
            ),
        }

    async def withdraw_hang_up(self) -> dict[str, Any]:
        """Withdraw a previously granted permission to end the current call.

        Disarms the hang-up gate set by ``allow_hang_up`` — for example when
        my boss changes their mind, raises something new, or the conversation
        clearly is not over after all. The live voice loses the ability to end
        the call until I arm the gate again.
        """
        if self._cm.call_manager.hang_up_gate_reason is None:
            return {
                "status": "ok",
                "message": "The hang-up gate was not armed; nothing to withdraw.",
            }
        await self._cm.call_manager.set_hang_up_gate(None)
        return {
            "status": "ok",
            "message": "Hang-up gate disarmed — the call continues normally.",
        }

    async def hang_up(self) -> dict[str, Any]:
        """End the current call or meeting immediately.

        Hangs up / leaves the live voice session — a phone call, WhatsApp call,
        Unify Meet, Google Meet, or Microsoft Teams meeting. Only one voice
        session can be active at a time, so this always targets that session.
        After it ends, the call-starting tools become available again.

        This ends the session NOW. Use it only when my boss explicitly asks me
        to hang up / end the call / leave the meeting, or when the session must
        be torn down (e.g. to recover from a broken line). When a conversation
        is simply wrapping up naturally, prefer ``allow_hang_up`` — it lets the
        live voice finish the goodbyes and end at the right moment instead of
        cutting anyone off.
        """
        channel = self._cm.call_manager._call_channel
        has_meet = (
            self._cm.call_manager.has_active_google_meet
            or self._cm.call_manager.has_active_teams_meet
        )
        if not has_meet and (
            not self._cm.in_voice_session
            or channel
            not in (
                "phone_call",
                "whatsapp_call",
                "unify_meet",
            )
        ):
            return {
                "status": "error",
                "message": "No active voice session to end.",
            }

        # Defer the actual teardown until any explanatory line this turn has been
        # spoken, so the session never ends mid-utterance. The orchestration lives
        # in ``_run_llm`` (after the spoken guidance is published and delivered).
        self._cm._pending_hang_up = True
        self._cm._pending_hang_up_teardown = self._perform_hang_up_teardown
        return {
            "status": "ok",
            "message": "Acknowledged — I'll end the call once I've finished speaking.",
        }

    async def _perform_hang_up_teardown(self) -> dict[str, Any]:
        """Tear down the active voice session (run after the spoken line lands).

        Internal helper invoked by ``_run_llm`` once any explanatory guidance for
        the hang-up turn has been delivered; never called directly as a tool.
        """
        if self._cm.call_manager.has_active_google_meet:
            return await self._leave_google_meet()
        if self._cm.call_manager.has_active_teams_meet:
            return await self._leave_teams_meet()

        await self._cm.call_manager.end_call()
        # Wait for the outbound-only resource gate so a follow-on dial only
        # proceeds once a new assistant-initiated call can open cleanly.
        ready = await self._cm.call_manager.await_ready_for_outbound_call()
        return {
            "status": "ok",
            "message": (
                "Call ended; the line is clear and ready for a new call."
                if ready
                else "Call ended, but the voice line is still being prepared."
            ),
            "ready_for_outbound_call": ready,
        }

    async def act(
        self,
        *,
        query: str,
        requesting_contact_id: int,
        response_format: Optional[dict] = None,
        persist: bool = False,
        include_conversation_context: bool = True,
        llm_profile: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Engage with knowledge, resources, and the world beyond immediate conversations.

        This is the all-purpose method for any work that requires searching, retrieving,
        manipulating, or acting on information. Use ``act`` liberally — if it cannot
        help, it will simply report back. There is no penalty for speculative delegation.

        **Capabilities include:**

        - **Retrieval**: Search contact records, query knowledge bases, look up past
          conversations, find calendar events, search the web, retrieve files
        - **Action**: Update records, modify spreadsheets, control the desktop/web interface,
          schedule tasks, create reminders
        - **Combined**: Find information and act on it (e.g., "find David's email")

        **Excluded:** Do not use ``act`` for Google Meet or Microsoft Teams
        meeting operations — use the dedicated tools instead:
        ``join_google_meet`` / ``join_teams_meet`` to join, ``hang_up`` to leave
        the current meeting or call, and ``send_meet_chat`` to post in the
        meeting chat.

        **When uncertain, call ``act``**: If you need information you don't have (like
        a contact's email address), call ``act`` to search for it. If ``act`` can't find
        it, it will tell you, and you can then ask the user.

        Args:
            query: Natural language request specifying what to do or find.
            requesting_contact_id: The contact_id of the person whose request or
                needs this action serves.  For responses to a contact's message,
                use that contact's ID.  For proactive actions benefiting a
                specific person, use their contact_id.  In ambiguous cases,
                choose the contact who most directly benefits from the action.
            response_format: An optional structured schema describing the shape of
                the result you need back.  When provided, the action is required to
                return a JSON object conforming to this schema (via a dedicated
                ``final_response`` tool) instead of free-form text.

                The schema uses a concise format where keys are field names and
                values describe their types:

                - Type strings: ``"string"``, ``"integer"``, ``"number"``,
                  ``"boolean"`` (shorthand ``"str"``, ``"int"``, etc. also work).
                - Nested objects: use a dict value, e.g.
                  ``{"address": {"city": "string", "zip": "string"}}``.
                - Arrays: use a single-element list whose element defines the item
                  schema, e.g. ``[{"name": "string", "email": "string"}]``.

                **Examples:**

                - Simple flat fields::

                      {"email": "string", "phone": "string"}

                - Nested with array::

                      {"contacts": [{"name": "string", "email": "string"}],
                       "total_count": "integer"}

                When omitted (the default), the action returns free-form text and
                the result is whatever the actor decides to report.
            persist: If True, the action runs as a **persistent session** that does
                not self-complete.  The actor stays alive after each response and
                waits for the next ``interject`` before continuing.  Use this for
                long-running interactive sessions (e.g. guided onboarding, live
                screen-sharing walkthroughs, multi-step procedures with a tight
                feedback loop between conversation and action).

                **Key differences from the default (persist=False):**

                - The action will **never** complete on its own.  You must
                  explicitly call ``stop_*`` to end the session.
                - Intermediate responses from the actor appear as **response**
                  events in the action's history (marked ``awaiting_input``).
                  Each response means the actor has finished its current turn
                  and is waiting for your next instruction via ``interject_*``.
                - Progress updates (notifications) may still arrive while the
                  actor is working, before it sends a response.

                The default (False) is a one-shot task: the actor works until
                done and the result arrives as an ``ActorResult``. Checklist
                completion for manual onboarding demo steps stays with the parent
                CM brain — act subtasks deliver the work only.
            include_conversation_context: Whether to pass the current conversation
                state to the action. When ``true`` (default), the action receives
                the full rendered conversation snapshot — messages, notifications,
                and in-flight actions — helping it understand the broader context.
                Set ``false`` when the action is self-contained and the query
                alone provides all necessary information (e.g. simple lookups,
                web searches, or factual questions). Subsequent steering calls
                (interject, ask) on this action will also skip context forwarding.
            llm_profile: Optional curated LLM profile for this action. Leave
                unset for the default actor profile, normally
                ``openai/gpt-5.6-sol@openrouter`` at high reasoning effort. Use
                ``gpt_5_5_low``, ``gpt_5_5_medium``, or ``gpt_5_5_high`` only
                when the task or the user's wording warrants the GPT-5.5
                family specifically. Requests to "use all of your thinking effort"
                or similar explicitly select ``gpt_5_5_high``.

                Escalate the profile when retrying an action that shows
                concrete evidence of model/tool-use struggle, rather than
                retrying the same default profile repeatedly. Good escalation
                signals include a previous ``ActorResult`` ending in a
                tool-schema or tool-call formatting error, the same failed
                step recurring after a retry, repeated execution mistakes
                without new information being gathered, clear user frustration,
                or the user's explicit request for stronger/premium reasoning.
                Do not escalate solely because an action has been running for
                a long time; long-running data, coding, or browser work can be
                normal. If restarting after one of the concrete failure
                signals above, preserve the user's task and set
                ``llm_profile`` to ``gpt_5_5_medium`` or ``gpt_5_5_high``
                depending on difficulty and urgency.
        """
        global _next_handle_id

        import time as _bat_time

        _bat_t0 = _bat_time.perf_counter()

        def _bat_ms() -> str:
            return f"{(_bat_time.perf_counter() - _bat_t0) * 1000:.0f}ms"

        import logging as _bat_logging

        _bat_log = _bat_logging.getLogger("unify")
        _bat_log.debug(f"⏱️ [CM.act tool +{_bat_ms()}] entered")
        cm = self._cm

        from unify.runtime.drain_gate import is_admission_blocked

        if is_admission_blocked():
            return {
                "status": "refused",
                "reason": "drain_in_progress",
                "detail": (
                    "Assistant drain/restart is in progress; new act calls "
                    "are refused until the pod recycles"
                ),
            }

        suppression = cm.suppress_duplicate_commissioning_tool(
            tool_name="act",
            tool_args={
                "query": query,
                "requesting_contact_id": requesting_contact_id,
                "response_format": response_format,
                "persist": persist,
                "include_conversation_context": include_conversation_context,
                "llm_profile": llm_profile,
            },
        )
        if suppression is not None:
            return suppression

        # Override cost attribution for all nested LLM calls in this action.
        from unify.events.cost_attribution import COST_ATTRIBUTION
        from unify.session_details import SESSION_DETAILS

        # Per-user cost attribution only matters in org context (personal
        # accounts have a single user). Resolve the acting user here.
        effective_user_id = SESSION_DETAILS.user.id
        if SESSION_DETAILS.org_id is not None:
            contact = self._cm.contact_index.get_contact(
                contact_id=requesting_contact_id,
            )
            # Only trust user_id from system contacts (boss + provisioned org
            # members).  A contact from another org could carry a platform
            # user_id that doesn't belong to this org.
            attributed_user_id = (
                contact.get("user_id") if contact and contact.get("is_system") else None
            )
            effective_user_id = attributed_user_id or SESSION_DETAILS.user.id
            COST_ATTRIBUTION.set(
                (
                    [attributed_user_id]
                    if attributed_user_id
                    else [SESSION_DETAILS.user.id]
                ),
            )

        # Use the first sentence (everything before the first ".") as a
        # concise action label, falling back to the whole query when there is
        # no period.
        action_summary = query.split(".", 1)[0].strip() if query else ""
        action_label = f"Action: {action_summary}" if action_summary else None

        # Bind the billing context so all nested LLM calls in this action are
        # recorded as tool-driven work (source="tool") with the action label.
        # This must run for personal workspaces too: otherwise the action's
        # LLM spend inherits the conversation turn's "chat" context and shows
        # in the usage ledger as generic chat work instead of the action.
        try:
            import unillm

            unillm.set_billing_context(
                assistant_id=SESSION_DETAILS.assistant.agent_id,
                user_id=effective_user_id,
                organization_id=SESSION_DETAILS.org_id,
                source="tool",
                label=action_label,
            )
        except (ImportError, Exception):
            pass

        # Pass the fresh rendered state snapshot as context for the Actor,
        # unless the LLM opted out.
        parent_context = None
        if include_conversation_context:
            parent_context = (
                [_filter_cm_state_for_actor(self._cm._current_state_snapshot)]
                if self._cm._current_state_snapshot
                else None
            )
        _bat_log.debug(f"⏱️ [CM.act tool +{_bat_ms()}] parent context built")

        # Convert the LLM-provided schema dict into a Pydantic model that the
        # Actor's async tool loop uses for structured output validation.
        pydantic_response_format = None
        if response_format is not None:
            pydantic_response_format = schema_dict_to_pydantic(response_format)

        handle_id = _next_handle_id
        _next_handle_id += 1

        _bat_log.debug(f"⏱️ [CM.act tool +{_bat_ms()}] calling cm.actor.act()")
        handle = await cm.actor.act(
            query,
            _parent_chat_context=parent_context,
            response_format=pydantic_response_format,
            persist=persist,
            llm_profile=llm_profile,
        )
        _bat_log.debug(
            f"⏱️ [CM.act tool +{_bat_ms()}] cm.actor.act() returned handle",
        )

        initial_snapshot_state: SnapshotState | None = None
        if hasattr(self._cm, "_current_snapshot_state"):
            initial_snapshot_state = self._cm._current_snapshot_state

        self._cm.in_flight_actions[handle_id] = {
            "handle": handle,
            "query": query,
            "persist": persist,
            "llm_profile": llm_profile,
            "action_type": "act",
            "calling_id": getattr(handle, "_manager_call_id", None),
            "handle_actions": [
                {
                    "action_name": "act_started",
                    "query": query,
                    "timestamp": prompt_now(),
                },
            ],
            "initial_snapshot_state": initial_snapshot_state,
            "context_opted_in": include_conversation_context,
        }
        asyncio.create_task(
            managers_utils.actor_watch_result(
                handle_id,
                handle,
                action_type="act",
            ),
        )
        asyncio.create_task(
            managers_utils.actor_watch_notifications(handle_id, handle),
        )
        asyncio.create_task(
            managers_utils.actor_watch_clarifications(handle_id, handle),
        )
        _bat_log.debug(f"⏱️ [CM.act tool +{_bat_ms()}] watchers started")

        _bat_log.debug(f"⏱️ [CM.act tool +{_bat_ms()}] publishing ActorHandleStarted")
        await self._event_broker.publish(
            f"app:actor:actor_started_handle_{handle_id}",
            ActorHandleStarted(
                handle_id=handle_id,
                action_name="act",
                query=query,
                response_format=response_format,
            ).to_json(),
        )
        _bat_log.debug(f"⏱️ [CM.act tool +{_bat_ms()}] done, returning")

        return {"status": "acting", "query": query}

    async def _invoke_manager_action(
        self,
        *,
        manager: Any,
        method_name: str,
        text: str,
        action_type: str,
        response_format: Optional[dict] = None,
        include_conversation_context: bool = True,
    ) -> dict[str, Any]:
        """Shared lifecycle for direct manager tools (contact and transcript actions).

        Follows the same pattern as ``act``: store handle in
        ``in_flight_actions``, spawn watcher tasks, publish started event.
        """
        global _next_handle_id
        LOGGER.info(
            f"{ICONS['fast_path']} [FastPath] {action_type}: {text}",
        )

        parent_context = None
        if include_conversation_context:
            parent_context = (
                [_filter_cm_state_for_actor(self._cm._current_state_snapshot)]
                if self._cm._current_state_snapshot
                else None
            )

        pydantic_response_format = None
        if response_format is not None:
            pydantic_response_format = schema_dict_to_pydantic(response_format)

        cm = self._cm

        handle_id = _next_handle_id
        _next_handle_id += 1

        method = getattr(manager, method_name)
        handle = await method(
            text,
            response_format=pydantic_response_format,
            _parent_chat_context=parent_context,
        )

        initial_snapshot_state: SnapshotState | None = None
        if hasattr(cm, "_current_snapshot_state"):
            initial_snapshot_state = cm._current_snapshot_state

        cm.in_flight_actions[handle_id] = {
            "handle": handle,
            "query": text,
            "persist": False,
            "action_type": action_type,
            "calling_id": getattr(handle, "_manager_call_id", None),
            "handle_actions": [
                {
                    "action_name": f"{action_type}_started",
                    "query": text,
                    "timestamp": prompt_now(),
                },
            ],
            "initial_snapshot_state": initial_snapshot_state,
            "context_opted_in": include_conversation_context,
        }
        asyncio.create_task(
            managers_utils.actor_watch_result(
                handle_id,
                handle,
                action_type=action_type,
            ),
        )
        asyncio.create_task(
            managers_utils.actor_watch_notifications(handle_id, handle),
        )
        asyncio.create_task(
            managers_utils.actor_watch_clarifications(handle_id, handle),
        )

        await self._event_broker.publish(
            f"app:actor:actor_started_handle_{handle_id}",
            ActorHandleStarted(
                handle_id=handle_id,
                action_name=action_type,
                query=text,
                response_format=response_format,
            ).to_json(),
        )

        return {"status": "acting", "query": text}

    async def ask_about_contacts(
        self,
        *,
        text: str,
        response_format: Optional[dict] = None,
    ) -> dict[str, Any]:
        """
        Query contact records directly — names, emails, phone numbers, roles,
        relationships, and any other stored contact attributes.

        This is a **direct channel** to the contact management system, bypassing
        the general ``act`` pathway. Use it for any purely contact-related
        questions:

        - Looking up a specific contact's details
        - Finding contacts by attribute (role, location, company, etc.)
        - Checking if a contact exists
        - Listing or filtering contacts
        - Comparing contact records

        **Route here instead of ``act`` when the question is purely about
        contact data** — including "find Alice's email/phone so I can
        message her." Looking up a contact to send them a message is still
        a contact query; after this action returns the contact_id and
        address, call ``send_email`` / ``send_sms`` / … yourself. If the
        question also involves non-contact information (tasks, knowledge,
        transcripts, web, files, etc.) or requires cross-domain reasoning,
        use ``act`` instead.

        Args:
            text: Natural language question about contacts
                (e.g. "What is Sarah's email address?",
                "Find Alice's contact_id and email so I can email her").
            response_format: Optional structured schema describing the shape of
                the result you need back. Same format as ``act``'s
                ``response_format`` — keys are field names, values are type
                strings (``"string"``, ``"integer"``, etc.), nested dicts, or
                single-element lists for arrays. When omitted, a free-form text
                answer is returned.
        """
        return await self._invoke_manager_action(
            manager=self._cm.contact_manager,
            method_name="ask",
            text=text,
            action_type="ask_about_contacts",
            response_format=response_format,
        )

    async def update_contacts(
        self,
        *,
        text: str,
        response_format: Optional[dict] = None,
    ) -> dict[str, Any]:
        """
        Create, edit, delete, or merge contact records directly.

        This is a **direct channel** to the contact management system, bypassing
        the general ``act`` pathway. Use it for any purely contact-related
        mutations:

        - Creating new contacts
        - Updating contact details (phone, email, address, role, bio, etc.)
        - Deleting contacts
        - Merging duplicate contacts

        **Route here instead of ``act`` when the request is purely about
        modifying contacts.** If the request also involves non-contact work
        or cross-domain operations, use ``act`` instead.

        Args:
            text: Natural language description of the contact change
                (e.g. "Add a new contact for John Smith, email john@acme.com").
            response_format: Optional structured schema describing the shape of
                the result you need back. Same format as ``act``'s
                ``response_format``. When omitted, a free-form text summary of
                the mutation is returned.
        """
        return await self._invoke_manager_action(
            manager=self._cm.contact_manager,
            method_name="update",
            text=text,
            action_type="update_contacts",
            response_format=response_format,
        )

    async def query_past_transcripts(
        self,
        *,
        text: str,
        response_format: Optional[dict] = None,
    ) -> dict[str, Any]:
        """
        Search and analyse past messages and conversation history directly.

        This is a **direct channel** to the transcript store, bypassing the
        general ``act`` pathway. Use it for any purely transcript-related
        questions:

        - Retrieving recent messages from a specific contact or channel
        - Searching past conversations for a keyword or topic
        - Summarising what was discussed in a previous exchange
        - Checking what someone said or when they last messaged
        - Comparing or filtering messages by date, medium, or sender

        **Route here instead of ``act`` when the question is purely about
        past messages or conversation history.** If the question also involves
        non-transcript information (contacts, knowledge, tasks, web, files,
        etc.) or requires cross-domain reasoning, use ``act`` instead.

        Args:
            text: Natural language question about past transcripts
                (e.g. "What did Bob say about the deadline yesterday?").
            response_format: Optional structured schema describing the shape of
                the result you need back. Same format as ``act``'s
                ``response_format`` — keys are field names, values are type
                strings (``"string"``, ``"integer"``, etc.), nested dicts, or
                single-element lists for arrays. When omitted, a free-form text
                answer is returned.
        """
        return await self._invoke_manager_action(
            manager=self._cm.transcript_manager,
            method_name="ask",
            text=text,
            action_type="query_past_transcripts",
            response_format=response_format,
        )

    # ── Computer fast-path tools ──────────────────────────────────────────

    async def _silent_interject_act_sessions(
        self,
        message: str,
    ) -> None:
        """Send a silent interjection to every in-flight ``act`` session,
        keeping the Actor informed without triggering an immediate LLM turn."""
        for hid, data in list(self._cm.in_flight_actions.items()):
            if data.get("action_type") != "act":
                continue
            handle = data.get("handle")
            if handle and not handle.done():
                try:
                    await handle.interject(
                        message,
                        trigger_immediate_llm_turn=False,
                        suppress_response_notification=True,
                    )
                except TypeError:
                    await handle.interject(message)

    async def _invoke_fast_path_action(
        self,
        *,
        coro,
        text: str,
        action_type: str,
    ) -> dict[str, Any]:
        """Shared lifecycle for desktop fast-path tools.

        Runs the desktop primitive call in the background and registers it as
        an in-flight action with the same lifecycle as ``act`` and the contact/
        transcript fast paths: ``ActorHandleStarted`` event, watcher tasks for
        result delivery, and automatic cleanup on completion.

        Interjects in-flight Actor desktop sessions twice:
        1. Immediately when the request is made (so the Actor knows what's happening)
        2. After the primitive completes with the result
        """
        global _next_handle_id
        LOGGER.info(
            f"{ICONS['fast_path']} [FastPath] {action_type}: {text}",
        )

        cm = self._cm
        handle_id = _next_handle_id
        _next_handle_id += 1

        async def _run():
            result = await coro
            summary = str(result) if result is not None else "done"
            LOGGER.info(
                f"{ICONS['fast_path']} [FastPath] {action_type} completed:\n"
                f"{summary}",
            )
            await self._silent_interject_act_sessions(
                f'[Fast-path result] {action_type}("{text}") completed. '
                f"Result: {result}\n\n"
                f"If this result looks wrong or incomplete — especially if "
                f"the task falls within your loaded guidance or requires "
                f"capabilities the fast path lacks (credentials, secrets, "
                f"multi-step procedures) — escalate by calling the "
                f"send_notification tool with a message starting with "
                f'"Escalation:" describing what you can do better.  '
                f"Otherwise, no action needed.",
            )
            return str(result) if result is not None else "done"

        handle = _DesktopActionHandle(asyncio.create_task(_run()))

        cm.in_flight_actions[handle_id] = {
            "handle": handle,
            "query": text,
            "persist": False,
            "action_type": action_type,
            "calling_id": getattr(handle, "_manager_call_id", None),
            "handle_actions": [
                {
                    "action_name": f"{action_type}_started",
                    "query": text,
                    "timestamp": prompt_now(),
                },
            ],
            "initial_snapshot_state": getattr(cm, "_current_snapshot_state", None),
            "context_opted_in": False,
        }
        asyncio.create_task(
            managers_utils.actor_watch_result(
                handle_id,
                handle,
                action_type=action_type,
            ),
        )

        await self._event_broker.publish(
            f"app:actor:actor_started_handle_{handle_id}",
            ActorHandleStarted(
                handle_id=handle_id,
                action_name=action_type,
                query=text,
            ).to_json(),
        )

        await self._silent_interject_act_sessions(
            f"[Fast-path request] The outer process is executing "
            f'{action_type}("{text}"). Do not replicate this action — it '
            f"is already in progress. You will see the result shortly.",
        )

        return {"status": "acting", "query": text}

    async def desktop_act(
        self,
        *,
        instruction: str,
    ) -> dict[str, Any]:
        """Execute a single desktop interaction on native UI (non-browser).

        Each call performs exactly **one** desktop interaction — one click,
        one keystroke, one window switch.

        **Only for interactions that cannot be done inside a web browser:**
        native application windows, terminal commands, file manager operations,
        system dialogs, or desktop UI elements outside any browser window.

        For any task involving a web browser — opening a browser, navigating,
        clicking web page elements, typing into web forms — use ``web_act``
        instead.

        Examples:

        - "Open the Terminal application"
        - "Switch to the File Manager window"
        - "Right-click the desktop background"
        - "Click the system tray notification"

        **Route through ``act`` or ``interject_*``** when the task requires
        more than one interaction, reasoning about *what* to do, or benefits
        from guidance and compositional functions.

        Args:
            instruction: A concrete single native desktop action to perform
                (e.g. "Open the Terminal application").
        """
        cp = self._cm.computer_primitives
        return await self._invoke_fast_path_action(
            coro=cp.desktop.act(instruction),
            text=instruction,
            action_type="desktop_act",
        )

    # ── Web fast-path tools ────────────────────────────────────────────

    def _resolve_or_create_web_session(self, session_id: int | None):
        """Return (handle, is_new) for an existing or freshly-created session."""
        cp = self._cm.computer_primitives

        async def _resolve():
            if session_id is not None:
                return cp.web.get_session(session_id), False
            handle = await cp.web.new_session(visible=True)
            return handle, True

        return _resolve()

    async def web_act(
        self,
        *,
        request: str,
        session_id: int | None = None,
    ) -> dict[str, Any]:
        """Execute a single browser interaction in a visible web session.

        Each call performs exactly **one** browser interaction — one click,
        one text entry, one scroll, or one navigation.  It bypasses the
        general ``act`` pathway and runs directly against a Chromium browser
        session visible on the desktop.

        A new browser session is created automatically when ``session_id``
        is omitted.  Pass a numeric ``session_id`` from
        ``<active_web_sessions>`` to continue working in an existing session.

        **Use ``desktop_act`` instead** only for native desktop interactions
        that cannot be done inside a browser (terminal, file manager, native
        app windows, system dialogs).

        **Use ``act`` or ``interject_*`` instead** when the task requires
        more than one interaction (e.g. click a button then type a value
        then click Save), or needs guidance / functions / knowledge.

        Args:
            request: Natural language description of a single browser action
                (e.g. "Navigate to example.com", "Click the Submit button").
            session_id: Optional numeric ID of an existing active web session
                to reuse.  When omitted a new visible session is created.
        """
        handle, is_new = await self._resolve_or_create_web_session(session_id)
        used_id = handle.session_id
        label = f"[session={used_id}, new={is_new}]"
        return await self._invoke_fast_path_action(
            coro=handle.act(request),
            text=f"{request} {label}",
            action_type="web_act",
        )

    async def close_web_session(
        self,
        *,
        session_id: int,
    ) -> dict[str, Any]:
        """Close a visible web browser session to free resources.

        Use after completing browser work to clean up.  Check
        ``<active_web_sessions>`` in the current state for valid IDs.

        Args:
            session_id: The numeric ID of the web session to close.
        """
        cp = self._cm.computer_primitives
        try:
            handle = cp.web.get_session(session_id)
        except ValueError:
            return {
                "status": "not_found",
                "session_id": session_id,
                "error": "No active web session with that ID.",
            }
        await handle.stop()
        return {"status": "closed", "session_id": session_id}

    async def deactivate_onboarding(self) -> dict[str, Any]:
        """Pause the global onboarding flow so my boss can use the platform normally.

        **Only call after my boss has verbally confirmed** they want to pause
        onboarding — not merely hinted, not while I am mid-explanation, and
        not when they are deferring a single checklist row (that is a per-row
        UI control, not a global pause).

        **Call when:** they clearly want to stop setup for now and have
        confirmed after I restate intent — e.g. "Yes, pause it", "Let's do
        that", "I'll finish setup later" in direct answer to my confirmation
        question.

        **Do not call when:** I am suggesting pause proactively; they asked
        what pause means; they said "skip this step" (row defer only); they
        have not answered a confirmation question; onboarding is already
        inactive; they only want help with a normal task while onboarding is
        still active (answer normally instead).

        After success: tell them onboarding is paused, they can ask for help
        normally, and they can resume later from the **Onboarding** tab in
        Assistant info or by asking me to continue setup.
        """
        return await self._cm._patch_coordinator_onboarding_active(
            active=False,
            clear_onboarding_step=True,
        )

    async def activate_onboarding(self) -> dict[str, Any]:
        """Resume the global onboarding checklist and nudges.

        **Only call after my boss has verbally confirmed** they want to return
        to setup — not when they are asking a one-off product question while
        onboarding is paused (answer normally without reactivating).

        **Call when:** they ask to resume or finish setup and confirm after I
        restate intent — e.g. "Yes, let's continue onboarding", "Take me back
        to the checklist".

        **Do not call when:** onboarding is already active; they have not
        confirmed; they only asked how to open the Onboarding tab (give
        directions instead); the Console **Return to onboarding** button is
        enough and they did not ask me to flip it.

        After success: confirm onboarding is live again and guide them to the
        first valid next step from my live onboarding progress block (if any).
        """
        return await self._cm._patch_coordinator_onboarding_active(active=True)

    async def set_onboarding_task_state(
        self,
        step_id: str,
        completed: bool,
    ) -> dict[str, Any]:
        """Mark one onboarding checklist step complete or incomplete.

        Use the ``step_id`` values from my live onboarding progress block
        (for example ``apps``, ``create-scheduled-task``, ``learn-from-correction``,
        ``my-computer-demo``, and the workspace demos ``workspace-mailbox`` /
        ``workspace-drive`` / ``workspace-calendar``). Communication rows still
        complete on their own when messages are sent and received — if this tool
        returns an error, relay that explanation rather than guessing which steps
        are settable.

        **Workspace demos, the Learning tutorial, and the My Computer live demo
        are completed here, explicitly.** A demo never auto-completes, so the
        checklist cannot detect it on its own. I do the demo task — read the
        relevant area and deliver one short summary as a single ``unify_message``
        (e.g. for ``workspace-mailbox`` I summarise the recent mail), for
        ``learn-from-correction`` I finish the full correction loop including the
        replay deliverable, or for ``my-computer-demo`` I run the call-anchored
        desktop errand and deliver the downloaded file as a chat attachment —
        and then call this with ``completed=True``; the step is not finished until
        I make that call. Any reply, tidy-up, or flag I offer afterwards is an
        optional follow-up and never gates completion.

        **Call when:** I have finished onboarding work the checklist cannot
        detect yet (a workspace demo task, semantic setup, or a guided
        walkthrough outside Communication) and it is genuinely done — or when I
        need to undo a manual completion I set earlier.

        **Do not call when:** onboarding is inactive; the step is in the
        Communication section; or I have not actually finished the work yet
        (e.g. I've not yet delivered the workspace demo summary).

        After success: confirm briefly and continue from the refreshed
        onboarding progress block.
        """
        return await self._cm._patch_coordinator_onboarding_step_state(
            step_id=step_id,
            completed=completed,
        )

    async def wait(
        self,
        delay: int | None = None,
    ) -> dict[str, Any]:
        """
        Wait for more input without taking any action.

        Call this tool when I have nothing left to say or do this turn:
        - After I already answered the user's latest message and they should
          have the last word
        - When there are no NEW inbound messages or completion events to handle
        - After starting a long-running action or outbound send whose outcome
          I will handle on the next event
        - To let a natural exchange end

        Do NOT call this tool when:
        - The user just sent a unify_message I have not answered yet
        - I sent something on another channel (email, SMS, WhatsApp, etc.) but
          have not yet told the user in chat where to look or what to do next
        - The user asks a question, expresses confusion, or checks whether I
          am still here ("hello?", "what next?", "are you ignoring me?")

        The user should usually have the last word after I answer — not while
        they are waiting on me. Do not send unprompted "anything else?" filler,
        but do not leave an unanswered unify_message on the Console chat thread.

        Parameters
        ----------
        delay : int | None
            Seconds to wait before automatically waking up for another thinking
            turn.  When ``None`` (the default), wait indefinitely until the next
            external event (new message, action completion, etc.).  When set to a
            positive integer, the system schedules a follow-up thinking turn after
            that many seconds — useful for probing a long-running action or
            revisiting a situation after a reasonable interval.
        """
        return {"status": "waiting", "delay": delay}

    async def guide_voice_agent(
        self,
        *,
        message: str,
        fast_brain_guidance: str = "",
    ) -> dict[str, Any]:
        """
        Speak a line to the caller during a live call.

        On each turn I SPEAK (call this tool with ``message``) or WAIT (omit it
        and call ``wait``). The Voice Agent otherwise only fills the latency gap
        with a brief filler; everything the caller actually hears is the
        ``message`` I speak here. I call this **in parallel** with my action tool
        (``wait``, ``act``, ``send_sms``, etc.).

        I may optionally attach ``fast_brain_guidance``: a short note the Voice
        Agent may use to give a basic, direct reply to the caller's *very next*
        message (e.g. confirm a fact I have just told it), without waiting for me.
        It is never spoken on its own, the Voice Agent never volunteers it, and it
        is ONLY honored when ``message`` is also set — I can never hand over
        guidance without also speaking. It applies to the next moment only; my
        next spoken turn replaces or clears it.

        Write ``message`` in the language currently spoken on the call.

        Args:
            message: The exact words to speak to the caller now, spoken verbatim
                via TTS. Use **spoken prose** (no numbered lists, bullets, or
                outline labels — TTS reads them literally).
            fast_brain_guidance: Optional short note for the Voice Agent to give a
                basic direct reply to the caller's next message. Never spoken;
                only honored alongside ``message``. Include any "do not reveal
                unless…" constraint explicitly if the note is sensitive.
        """
        return {"status": "guidance_noted"}

    async def show_in_console(
        self,
        *,
        targets: list[str],
    ) -> dict[str, Any]:
        """
        Take my boss to a page in the console while I talk them through it.

        Offered whenever my boss has the console open, whatever we are talking
        over — a Meet, a phone call, SMS, email, chat. If they are reading my
        reply with the console in another tab, they can watch it happen.

        On a **Unify Meet** I call this alongside ``guide_voice_agent`` and mark
        the moments in that spoken line with ``[[1]]``, ``[[2]]``, … — one marker
        per target, in order. The markers are removed before the line is spoken,
        and each move happens on its words rather than before or after them::

            guide_voice_agent(message="Sure — I'll open Integrations [[1]], and
                              your billing page is over here [[2]].")
            show_in_console(targets=["section:integrations", "route:/billing"])

        **Anywhere else** there is no spoken line to sit inside, so I just name
        the targets and the console walks them in order while my boss reads what
        I said. No markers are needed::

            show_in_console(targets=["section:integrations"])

        Every target must be one of the ids listed for me under console
        navigation targets. Those are the only places I can go; if what my boss
        wants is not among them I say where it lives and let them click, rather
        than moving them somewhere near it.

        This moves the page my boss is looking at, so I use it while I am
        actually showing them something — alongside a message that says what they
        are looking at, never on its own and never to jump them somewhere
        unasked. A few steps at a time. On a call, if they interrupt me, the
        moves I had not yet reached are dropped, which is what should happen.

        Args:
            targets: Navigation target ids, in the order they should happen. On a
                Meet, one per ``[[n]]`` marker in the spoken line.
        """
        catalogue = self._cm.console_action_catalogue()
        if not catalogue:
            return {
                "status": "error",
                "error": "The console is not open, so there is nothing to show.",
            }
        unknown = [t for t in targets if f"`{catalogue_form(t)}`" not in catalogue]
        if unknown:
            return {
                "status": "error",
                "error": (
                    f"Not console targets: {', '.join(unknown)}. "
                    "Use an id from the console navigation targets list."
                ),
            }
        # In a Meet the moves are held until the spoken line reaches their
        # markers, so they are published from the voice pipeline rather than
        # here. Every other medium has no line to sit inside, and no room
        # Console is a participant in, so they go out now over its event stream.
        if self._cm.mode == Mode.MEET:
            return {"status": "showing", "targets": targets}

        await publish_console_script(
            steps=[{"target": target} for target in targets],
        )
        return {"status": "showing", "targets": targets}

    def _whatsapp_contact_label(self, contact_id: int) -> str:
        """Human-friendly name for a contact in the window-status appendix."""
        contact = None
        try:
            contact = self._cm.contact_index.get_contact(contact_id)
        except Exception:
            contact = None
        first = (contact or {}).get("first_name") if contact else None
        if first:
            return first
        if SESSION_DETAILS.is_coordinator and contact_id == self._boss_contact_id():
            return "my boss"
        return f"contact {contact_id}"

    def _whatsapp_window_doc_suffix(self) -> str:
        """Window-status appendix for the ``send_whatsapp`` tool docstring.

        Lists, by name, every contact whose 24-hour WhatsApp free-form window is
        currently known to be CLOSED (a send delivers only a placeholder) or OPEN
        (delivered verbatim). Returns ``""`` when nothing is known so the
        window-agnostic docstring guidance stands on its own.
        """
        cm = self._cm
        relevant: set[int] = set()
        if SESSION_DETAILS.is_coordinator:
            relevant.add(self._boss_contact_id())
        relevant.update(cm._pending_whatsapp_resends.keys())
        relevant.update(cm._whatsapp_window_open.keys())

        closed: list[str] = []
        open_: list[str] = []
        for cid in relevant:
            state = cm.whatsapp_window_state(cid)
            if state is None:
                continue
            (open_ if state else closed).append(self._whatsapp_contact_label(cid))

        if not closed and not open_:
            return ""

        lines = [
            "",
            "Current WhatsApp free-form window (decides whether a send arrives "
            "verbatim or only as a placeholder):",
        ]
        if closed:
            lines.append(
                "- CLOSED for: "
                + ", ".join(sorted(closed))
                + ". A send now delivers only a generic placeholder; my real "
                "text is queued to resend after they reply. I tell them to reply "
                "to the placeholder first and I do NOT claim the actual "
                "message/clue arrived.",
            )
        if open_:
            lines.append(
                "- OPEN for: "
                + ", ".join(sorted(open_))
                + ". A send now is delivered verbatim.",
            )
        return "\n".join(lines)

    def _with_whatsapp_window_doc(
        self,
        base: "Callable[..., Any]",
    ) -> "Callable[..., Any]":
        """Return ``base`` with the live window status appended to its docstring.

        Rebuilt per turn (``as_tools`` runs each turn), so the status reflects
        the latest known window state. Returns ``base`` unchanged when nothing is
        known, to avoid an empty appendix.
        """
        suffix = self._whatsapp_window_doc_suffix()
        if not suffix:
            return base

        @wraps(base)
        async def _send_whatsapp_window_aware(**kwargs: Any) -> Any:
            return await base(**kwargs)

        # Pin the schema signature to the bound method's (which already excludes
        # ``self``); without this, ``inspect.signature`` would unwrap past the
        # bound method to the underlying primitive and re-expose ``self``.
        _send_whatsapp_window_aware.__signature__ = inspect.signature(base)
        base_doc = inspect.getdoc(base) or ""
        _send_whatsapp_window_aware.__doc__ = f"{base_doc}\n{suffix}"
        return _send_whatsapp_window_aware

    def as_tools(self) -> dict[str, "Callable[..., Any]"]:
        """Return the static tools dict for start_async_tool_loop.

        The comms surface is decided by ``is_private_coordinator``, not
        ``is_coordinator``: a single-player twin is boss-only on every
        channel and cannot enter browser meetings at all, while a
        multiplayer twin communicates like any hired teammate.
        """
        from unify.settings import SETTINGS

        is_coordinator = SESSION_DETAILS.is_private_coordinator
        tools: dict[str, Callable[..., Any]] = {
            "send_unify_message": (
                self.send_unify_message_to_boss
                if is_coordinator
                else self.send_unify_message
            ),
            "send_api_response": (
                self.send_api_response_to_boss
                if is_coordinator
                else self.send_api_response
            ),
            "wait": self.wait,
        }
        call_or_meet_in_progress = self._cm.in_voice_session
        # Assistant-initiated call tools are exposed only when a new outbound
        # call can actually open cleanly: no live session AND the worker has a
        # freshly prewarmed idle process ready. Inbound sessions bypass this
        # gate and are accepted by the runtime immediately.
        ready_to_start_call = (
            not call_or_meet_in_progress
            and self._cm.call_manager.is_ready_for_outbound_call
        )
        if not call_or_meet_in_progress:
            # Browser meetings are multi-party rooms, so a single-player
            # twin never gets the join tools — not even for a meeting its
            # boss hosts.
            if not is_coordinator:
                tools["join_google_meet"] = self.join_google_meet
                tools["join_teams_meet"] = self.join_teams_meet
            # Ringing the in-app Meet only signals the Console (the owner answers
            # in-browser), so unlike telephony call-start it needs no prewarmed
            # worker; expose it whenever no other voice session is live.
            tools["start_unify_meet"] = self.start_unify_meet
        else:
            # One voice session at a time; a single tool ends whichever is live
            # (phone, WhatsApp, Unify Meet, Google Meet, or Teams).
            tools["hang_up"] = self.hang_up
            # Preferred close: arm the gate and let the live voice pick the
            # natural cut-off point (withdrawable if the conversation moves on).
            if self._cm.call_manager.hang_up_gate_reason is None:
                tools["allow_hang_up"] = self.allow_hang_up
            else:
                tools["withdraw_hang_up"] = self.withdraw_hang_up
        if (
            self._cm.call_manager.has_active_google_meet
            or self._cm.call_manager.has_active_teams_meet
        ):
            tools["send_meet_chat"] = self.send_meet_chat
            # Only a managed desktop is ever shared. A user's own linked machine
            # is not the assistant's to put in front of a room, so the tool is
            # absent rather than failing when asked -- an assistant with no
            # desktop should not be offering to show one.
            if SESSION_DETAILS.assistant.has_managed_desktop:
                if self._cm.call_manager.is_presenting_to_meet:
                    tools["stop_meet_screenshare"] = self.stop_meet_screenshare
                else:
                    tools["start_meet_screenshare"] = self.start_meet_screenshare
        if self._cm.assistant_number:
            tools["send_sms"] = (
                self.send_sms_to_boss if is_coordinator else self.send_sms
            )
            if ready_to_start_call:
                tools["make_call"] = (
                    self.make_call_to_boss if is_coordinator else self.make_call
                )
        if self._cm.assistant_whatsapp_number:
            base_send_whatsapp = (
                self.send_whatsapp_to_boss if is_coordinator else self.send_whatsapp
            )
            tools["send_whatsapp"] = self._with_whatsapp_window_doc(base_send_whatsapp)
            if ready_to_start_call:
                tools["make_whatsapp_call"] = (
                    self.make_whatsapp_call_to_boss
                    if is_coordinator
                    else self.make_whatsapp_call
                )
        if self._cm.assistant_email:
            tools["send_email"] = (
                self.send_email_to_boss if is_coordinator else self.send_email
            )
        if self._cm.assistant_discord_bot_id:
            tools["send_discord_message"] = (
                self.send_discord_message_to_boss
                if is_coordinator
                else self.send_discord_message
            )
            if not is_coordinator:
                tools["send_discord_channel_message"] = (
                    self.send_discord_channel_message
                )
        if self._cm.assistant_slack_bot_user_id:
            tools["send_slack_message"] = (
                self.send_slack_message_to_boss
                if is_coordinator
                else self.send_slack_message
            )
            if not is_coordinator:
                tools["send_slack_channel_message"] = self.send_slack_channel_message
        if self._cm.assistant_has_ms_teams_bot:
            tools["send_ms_teams_bot_message"] = (
                self.send_ms_teams_bot_message_to_boss
                if is_coordinator
                else self.send_ms_teams_bot_message
            )
            if not is_coordinator:
                tools["send_ms_teams_bot_channel_message"] = (
                    self.send_ms_teams_bot_channel_message
                )
        if self._cm.assistant_has_teams:
            tools["send_teams_message"] = (
                self.send_teams_message_to_boss
                if is_coordinator
                else self.send_teams_message
            )
            if is_coordinator:
                tools["create_teams_meet"] = self.create_teams_meet_with_boss
            else:
                tools["create_teams_channel"] = self.create_teams_channel
                tools["create_teams_meet"] = self.create_teams_meet
        if getattr(self._cm.mode, "is_voice", False):
            tools["guide_voice_agent"] = self.guide_voice_agent
        # Offered whenever Console reports the boss present, whatever medium
        # this conversation is on. Someone reading a text reply with the Console
        # open can watch a page change exactly as well as someone on a call; the
        # only thing that makes it pointless is nobody being there to see it.
        if self._cm.console_action_catalogue():
            tools["show_in_console"] = self.show_in_console
        if self._cm.initialized:
            tools["act"] = self.act
            tools["ask_about_contacts"] = self.ask_about_contacts
            tools["update_contacts"] = self.update_contacts
            tools["query_past_transcripts"] = self.query_past_transcripts
        # During onboarding, withhold a reference-quiz channel's send tool until
        # the user clicks its trigger row (this session) or the step durably
        # completes — so T-W1N cannot send an untagged clue proactively.
        if self._cm.coordinator_onboarding_active:
            for name in masked_reference_quiz_tools(
                self._cm.coordinator_onboarding_render,
                self._cm.onboarding_clicked_trigger_steps,
            ):
                tools.pop(name, None)
        if is_coordinator and SETTINGS.UNIFY_CONSOLE_UI:
            if self._cm.coordinator_onboarding_active:
                tools["deactivate_onboarding"] = self.deactivate_onboarding
                tools["set_onboarding_task_state"] = self.set_onboarding_task_state
            else:
                tools["activate_onboarding"] = self.activate_onboarding
        return tools

    def build_action_steering_tools(self) -> dict[str, "Callable[..., Any]"]:
        """Fixed-name steering tools that address actions by ``handle_id``.

        The tool set must stay constant across brain runs: tool definitions
        precede messages in provider prompt-cache keys, so a schema that
        changes with the in-flight action set re-bills the entire static
        prompt on every action transition. Targets arrive as ``handle_id``
        (the ``<action id='N'>`` shown in the ``in_flight_actions`` pane)
        and are resolved at call time; a stale or unknown id gets a
        corrective error instead of a vanished tool.
        """
        cm = self._cm

        def _in_flight(handle_id: Any) -> tuple[Optional[int], Optional[dict]]:
            try:
                hid = int(handle_id)
            except (TypeError, ValueError):
                return None, None
            return hid, (cm.in_flight_actions or {}).get(hid)

        def _missing(handle_id: Any, operation: str) -> dict[str, Any]:
            ids = sorted((cm.in_flight_actions or {}).keys())
            return {
                "status": "error",
                "operation": operation,
                "message": (
                    f"No in-flight action with handle_id={handle_id!r}. "
                    f"In flight now: {ids if ids else 'none'} — use the id "
                    "from the in_flight_actions pane."
                ),
            }

        def _delegate(
            op_name: str,
            hid: int,
            handle_data: dict,
            call_id: str | None = None,
        ) -> "Callable[..., Any]":
            op = OPERATION_MAP[op_name]
            return self._make_steering_tool(
                hid,
                handle_data.get("handle"),
                op.name,
                op.param_name,
                op.get_docstring(),
                handle_data.get("query", ""),
                call_id,
            )

        async def interject_action(handle_id: int, message: str) -> dict[str, Any]:
            """Send guidance into a running action without stopping it.

            Args:
                handle_id: The action's id from the in_flight_actions pane.
                message: The guidance or correction to deliver.
            """
            hid, handle_data = _in_flight(handle_id)
            if handle_data is None:
                return _missing(handle_id, "interject")
            return await _delegate("interject", hid, handle_data)(message=message)

        async def stop_action(handle_id: int, reason: str = "") -> dict[str, Any]:
            """Terminate a running action immediately.

            Args:
                handle_id: The action's id from the in_flight_actions pane.
                reason: Optional reason recorded with the stop.
            """
            hid, handle_data = _in_flight(handle_id)
            if handle_data is None:
                return _missing(handle_id, "stop")
            return await _delegate("stop", hid, handle_data)(reason=reason)

        async def pause_action(handle_id: int) -> dict[str, Any]:
            """Pause a running action; resume it later with resume_action.

            Args:
                handle_id: The action's id from the in_flight_actions pane.
            """
            hid, handle_data = _in_flight(handle_id)
            if handle_data is None:
                return _missing(handle_id, "pause")
            if get_handle_paused_state(handle_data.get("handle")) is True:
                return {
                    "status": "ok",
                    "operation": "pause",
                    "message": f"Action {hid} is already paused.",
                }
            return await _delegate("pause", hid, handle_data)()

        async def resume_action(handle_id: int) -> dict[str, Any]:
            """Resume a paused action.

            Args:
                handle_id: The action's id from the in_flight_actions pane.
            """
            hid, handle_data = _in_flight(handle_id)
            if handle_data is None:
                return _missing(handle_id, "resume")
            if get_handle_paused_state(handle_data.get("handle")) is not True:
                return {
                    "status": "ok",
                    "operation": "resume",
                    "message": f"Action {hid} is not paused.",
                }
            return await _delegate("resume", hid, handle_data)()

        async def ask_action(handle_id: int, question: str) -> dict[str, Any]:
            """Ask a question of a running or completed action.

            Args:
                handle_id: The action's id from the in_flight_actions or
                    completed_actions pane.
                question: What to ask about the action's work or results.
            """
            hid, handle_data = _in_flight(handle_id)
            if handle_data is not None:
                return await _delegate("ask", hid, handle_data)(question=question)
            completed = (
                (cm.completed_actions or {}).get(hid) if hid is not None else None
            )
            if completed is not None:
                ask_op = OPERATION_MAP["ask"]
                tool_fn = self._make_completed_action_ask_tool(
                    hid,
                    completed.get("handle"),
                    ask_op.param_name,
                    ask_op.get_docstring(),
                    completed.get("query", ""),
                )
                return await tool_fn(question=question)
            return _missing(handle_id, "ask")

        async def answer_clarification_action(
            handle_id: int,
            answer: str,
            call_id: str = "",
        ) -> dict[str, Any]:
            """Answer a pending clarification a running action has raised.

            Args:
                handle_id: The action's id from the in_flight_actions pane.
                answer: The answer to give the action.
                call_id: The clarification's call id (shown in the pane).
                    May be omitted when exactly one clarification is pending.
            """
            hid, handle_data = _in_flight(handle_id)
            if handle_data is None:
                return _missing(handle_id, "answer_clarification")
            pending = [
                a
                for a in handle_data.get("handle_actions", [])
                if a.get("action_name") == "clarification_request"
                and not a.get("response")
            ]
            if not pending:
                return {
                    "status": "error",
                    "operation": "answer_clarification",
                    "message": f"Action {hid} has no pending clarification.",
                }
            chosen = None
            if call_id:
                for clar in pending:
                    cid = clar.get("call_id", "")
                    if cid == call_id or safe_call_id_suffix(cid) == call_id:
                        chosen = clar
                        break
                if chosen is None:
                    return {
                        "status": "error",
                        "operation": "answer_clarification",
                        "message": (
                            f"No pending clarification with call_id={call_id!r} "
                            f"on action {hid}; pending: "
                            f"{[c.get('call_id', '') for c in pending]}"
                        ),
                    }
            elif len(pending) == 1:
                chosen = pending[0]
            else:
                return {
                    "status": "error",
                    "operation": "answer_clarification",
                    "message": (
                        f"Action {hid} has {len(pending)} pending "
                        "clarifications; pass call_id to pick one: "
                        f"{[c.get('call_id', '') for c in pending]}"
                    ),
                }
            return await _delegate(
                "answer_clarification",
                hid,
                handle_data,
                chosen.get("call_id", ""),
            )(answer=answer)

        return {
            "interject_action": interject_action,
            "stop_action": stop_action,
            "pause_action": pause_action,
            "resume_action": resume_action,
            "ask_action": ask_action,
            "answer_clarification_action": answer_clarification_action,
        }

    def build_completed_action_tools(self) -> dict[str, "Callable[..., Any]"]:
        """Completed actions are served by ``ask_action`` (see
        ``build_action_steering_tools``); no per-action tools remain."""
        return {}

    @staticmethod
    def _extract_tool_param_value(
        *,
        kwargs: dict[str, Any],
        primary_name: str,
        aliases: tuple[str, ...] = (),
    ) -> Any:
        """Extract a tool parameter value from kwargs using primary name then aliases."""
        if not primary_name:
            return ""
        for name in (primary_name, *aliases):
            if name in kwargs:
                return kwargs.get(name, "")
        return ""

    def _make_completed_action_ask_tool(
        self,
        handle_id: int,
        handle: Any,
        param_name: str,
        docstring: str,
        query: str,
    ) -> "Callable[..., Any]":
        """Create an ask tool closure for a completed action."""

        cm = self._cm
        event_broker = cm.event_broker
        ask_param_aliases = tuple(
            name for name in ("question", "query") if name != param_name
        )

        async def ask_completed_action(
            **kwargs: Any,
        ) -> dict[str, Any]:
            param_value = self._extract_tool_param_value(
                kwargs=kwargs,
                primary_name=param_name,
                aliases=ask_param_aliases,
            )

            # Get handle_data from completed_actions
            handle_data = cm.completed_actions.get(handle_id)

            # Record action with pending status
            if handle_data:
                handle_data["handle_actions"].append(
                    {
                        "action_name": f"ask_{handle_id}",
                        "query": param_value,
                        "status": "pending",
                        "timestamp": prompt_now(),
                    },
                )

            _handle = handle
            _param_value = param_value
            _handle_id = handle_id
            _parent_context = (
                [cm._current_state_snapshot] if cm._current_state_snapshot else None
            )

            async def _perform_ask_and_emit():
                await event_broker.publish(
                    "app:call:notification",
                    FastBrainNotification(
                        message=f"Ask dispatched on action: {_param_value[:200]}",
                        source="system",
                        contact={},
                    ).to_json(),
                )
                try:
                    ask_handle = await _handle.ask(
                        _param_value,
                        _parent_chat_context=_parent_context,
                    )
                    ask_result = await ask_handle.result()
                except Exception as e:
                    ask_result = f"Error: {e}"
                await event_broker.publish(
                    f"app:actor:handle_response_{_handle_id}",
                    ActorHandleResponse(
                        handle_id=_handle_id,
                        action_name="ask",
                        query=_param_value,
                        response=ask_result,
                        call_id="",
                    ).to_json(),
                )

            task = asyncio.create_task(_perform_ask_and_emit())
            cm._pending_steering_tasks.add(task)
            task.add_done_callback(cm._pending_steering_tasks.discard)

            return {
                "status": "ok",
                "operation": "ask",
                "result": (
                    "Query submitted. You will receive another turn "
                    "when the answer is ready."
                ),
            }

        # Build signature with proper parameter name
        if param_name:
            params = [
                inspect.Parameter(
                    param_name,
                    inspect.Parameter.KEYWORD_ONLY,
                    annotation=str,
                ),
            ]
        else:
            params = []

        ask_completed_action.__signature__ = inspect.Signature(params)
        base_doc = docstring or "Ask about this completed action."
        ask_completed_action.__doc__ = f"{base_doc}\n\nFor action: {query}"
        return ask_completed_action

    def _make_steering_tool(
        self,
        handle_id: int,
        handle: Any,
        operation: str,
        param_name: str,
        docstring: str,
        query: str,
        call_id: str | None = None,
    ) -> "Callable[..., Any]":
        """Create a closure for an action steering operation."""

        cm = self._cm
        # Use cm.event_broker to ensure the same broker is used throughout
        # (important for test patching)
        event_broker = cm.event_broker

        async def steering_tool(
            **kwargs: Any,
        ) -> dict[str, Any]:
            param_aliases: tuple[str, ...] = ()
            if operation == "ask":
                param_aliases = tuple(
                    name for name in ("question", "query") if name != param_name
                )
            param_value = self._extract_tool_param_value(
                kwargs=kwargs,
                primary_name=param_name,
                aliases=param_aliases,
            )

            handle_data = cm.in_flight_actions.get(handle_id)

            result = ""
            try:
                match operation:
                    case "ask":
                        # Record action with pending status - result will arrive async
                        if handle_data:
                            handle_data["handle_actions"].append(
                                {
                                    "action_name": f"ask_{handle_id}",
                                    "query": param_value,
                                    "status": "pending",
                                    "timestamp": prompt_now(),
                                },
                            )

                        # Capture values for the closure.
                        # Use the fresh rendered state snapshot (set by _run_llm before tools execute).
                        # Only pass context if the original action opted in.
                        _handle = handle
                        _param_value = param_value
                        _handle_id = handle_id
                        _ctx_opted_in = (
                            handle_data.get("context_opted_in", True)
                            if handle_data
                            else True
                        )
                        _parent_context = None
                        if _ctx_opted_in and cm._current_state_snapshot:
                            _parent_context = [cm._current_state_snapshot]

                        # Spawn background task to perform ask and emit result
                        async def _perform_ask_and_emit():
                            await event_broker.publish(
                                "app:call:notification",
                                FastBrainNotification(
                                    message=f"Ask dispatched on action: {_param_value[:200]}",
                                    source="system",
                                    contact={},
                                ).to_json(),
                            )
                            try:
                                # Start the ask operation (does the LLM roundtrip)
                                ask_handle = await _handle.ask(
                                    _param_value,
                                    _parent_chat_context=_parent_context,
                                )
                                # Await the result
                                ask_result = await ask_handle.result()
                            except Exception as e:
                                ask_result = f"Error: {e}"
                            # Emit ActorHandleResponse event to wake brain
                            await event_broker.publish(
                                f"app:actor:handle_response_{_handle_id}",
                                ActorHandleResponse(
                                    handle_id=_handle_id,
                                    action_name="ask",
                                    query=_param_value,
                                    response=ask_result,
                                    call_id="",
                                ).to_json(),
                            )

                        task = asyncio.create_task(_perform_ask_and_emit())
                        cm._pending_steering_tasks.add(task)
                        task.add_done_callback(
                            cm._pending_steering_tasks.discard,
                        )

                        # Return immediately - brain will be woken when result arrives
                        return {
                            "status": "ok",
                            "operation": "ask",
                            "result": (
                                "Query submitted. You will receive another turn "
                                "when the answer is ready."
                            ),
                        }

                    case "interject":
                        if handle_data:
                            handle_data["handle_actions"].append(
                                {
                                    "action_name": f"{operation}_{handle_id}",
                                    "query": param_value,
                                    "timestamp": prompt_now(),
                                },
                            )

                        # Only compute and send context diffs if the original
                        # action opted into conversation context.
                        parent_context_cont = None
                        _interject_ctx_opted_in = (
                            handle_data.get("context_opted_in", True)
                            if handle_data
                            else True
                        )

                        if _interject_ctx_opted_in:
                            initial_snapshot = (
                                handle_data.get("initial_snapshot_state")
                                if handle_data
                                else None
                            )
                            current_snapshot = getattr(
                                cm,
                                "_current_snapshot_state",
                                None,
                            )

                            if current_snapshot is not None:
                                diff_content = compute_snapshot_diff(
                                    initial_snapshot,
                                    current_snapshot,
                                )
                                if diff_content:
                                    parent_context_cont = [
                                        {
                                            "role": "user",
                                            "content": diff_content,
                                            "_cm_context_diff": True,
                                        },
                                    ]
                            elif cm._current_state_snapshot:
                                parent_context_cont = [cm._current_state_snapshot]

                        await handle.interject(
                            param_value,
                            _parent_chat_context_cont=parent_context_cont,
                        )
                        result = "Interjected successfully"
                    case "stop":
                        if handle_data:
                            handle_data["handle_actions"].append(
                                {
                                    "action_name": f"{operation}_{handle_id}",
                                    "query": param_value,
                                    "timestamp": prompt_now(),
                                },
                            )
                        await handle.stop(reason=param_value or None)
                        stopped = cm.in_flight_actions.pop(handle_id, None)
                        if stopped:
                            cm.completed_actions[handle_id] = stopped
                        result = "Action stopped"
                    case "pause":
                        if handle_data:
                            handle_data["handle_actions"].append(
                                {
                                    "action_name": f"{operation}_{handle_id}",
                                    "query": param_value,
                                    "timestamp": prompt_now(),
                                },
                            )
                        await handle.pause()
                        result = "Action paused"
                    case "resume":
                        if handle_data:
                            handle_data["handle_actions"].append(
                                {
                                    "action_name": f"{operation}_{handle_id}",
                                    "query": param_value,
                                    "timestamp": prompt_now(),
                                },
                            )
                        await handle.resume()
                        result = "Action resumed"
                    case "answer_clarification":
                        if handle_data:
                            handle_data["handle_actions"].append(
                                {
                                    "action_name": f"{operation}_{handle_id}",
                                    "query": param_value,
                                    "timestamp": prompt_now(),
                                },
                            )
                        if call_id:
                            await handle.answer_clarification(call_id, param_value)
                            result = "Clarification answered"
                        else:
                            result = "No clarification call_id available"
                    case _:
                        if handle_data:
                            handle_data["handle_actions"].append(
                                {
                                    "action_name": f"{operation}_{handle_id}",
                                    "query": param_value,
                                    "timestamp": prompt_now(),
                                },
                            )
                        result = f"Unknown operation: {operation}"
            except Exception as e:
                result = f"Error: {e}"

            return {"status": "ok", "operation": operation, "result": result}

        # Copy signature + docstring from the handle's method. Parameters
        # starting with _ are automatically hidden by method_to_schema.
        if handle is not None and hasattr(handle, operation):
            DynamicToolFactory._adopt_signature_and_annotations(
                getattr(handle, operation),
                steering_tool,
            )

        # Append action context so the CM knows which action this tool steers.
        # Preserve the docstring set by _adopt_signature_and_annotations (or
        # fall back to the docstring passed in from SteeringOperation).
        base_doc = inspect.getdoc(steering_tool) or docstring
        steering_tool.__doc__ = f"{base_doc}\n\nFor action: {query}"

        return steering_tool
