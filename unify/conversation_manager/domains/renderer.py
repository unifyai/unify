"""
Renderer: Renders conversation state for the ConversationManager LLM.

Contact information is fetched from ContactManager (source of truth).
Conversation state (threads) is fetched from ContactIndex.

SnapshotState: Tracks constituent elements of a rendered snapshot with identity,
enabling incremental diff computation for context propagation to Actor interjections.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from time import perf_counter
from typing import Any

from unify.common._async_tool.utils import get_handle_paused_state
from unify.common.prompt_helpers import get_assistant_timezone
from unify.common.startup_timing import log_startup_timing
from unify.conversation_manager.domains.contact_index import (
    UnifyMessage,
    GuidanceMessage,
    ConversationState,
    ContactIndex,
    GlobalThreadEntry,
)
from unify.conversation_manager.domains.notifications import NotificationBar
from unify.conversation_manager.task_actions import (
    derive_short_name,
    iter_steering_tools_for_action,
    iter_steering_tools_for_completed_action,
)
from unify.logger import LOGGER
from unify.session_details import is_boss_contact

# =============================================================================
# Timezone Helpers for Participant Awareness
# =============================================================================


def _get_current_time_in_timezone(tz_name: str) -> str:
    """Get the current time formatted for a specific timezone.

    Reads the clock through ``prompt_helpers.now`` like every other prompt
    surface rather than calling ``datetime.now`` directly. That is the seam the
    test suite freezes, and this block renders into the transcript once per
    participant group, so a raw clock here alone is enough to make a prompt
    differ between runs and miss the LLM cache.

    Args:
        tz_name: IANA timezone identifier (e.g., "America/New_York")

    Returns:
        Formatted time string like "3:45 PM"
    """
    from zoneinfo import ZoneInfo

    from unify.common.prompt_helpers import now as prompt_now

    _timing_t0 = perf_counter()
    current_dt = prompt_now(as_string=False)
    _utc_now_ms = (perf_counter() - _timing_t0) * 1000
    _step_t0 = perf_counter()
    success = True
    try:
        tz_info = ZoneInfo(tz_name)
        _zoneinfo_ms = (perf_counter() - _step_t0) * 1000
        _step_t0 = perf_counter()
        local_dt = current_dt.astimezone(tz_info)
        _astimezone_ms = (perf_counter() - _step_t0) * 1000
        _step_t0 = perf_counter()
        result = local_dt.strftime("%I:%M %p").lstrip("0")
    except Exception:
        success = False
        _zoneinfo_ms = (perf_counter() - _step_t0) * 1000
        _astimezone_ms = 0.0
        _step_t0 = perf_counter()
        result = "unknown"
    _format_ms = (perf_counter() - _step_t0) * 1000
    log_startup_timing(
        LOGGER,
        (
            "⏱️ [StartupTiming] timezone.current_time.detail "
            "total=%.0fms utc_now=%.0fms zoneinfo=%.0fms astimezone=%.0fms "
            "format=%.0fms tz=%s success=%s"
        ),
        (perf_counter() - _timing_t0) * 1000,
        _utc_now_ms,
        _zoneinfo_ms,
        _astimezone_ms,
        _format_ms,
        tz_name,
        success,
    )
    return result


def _format_timezone_block(
    assistant_tz: str | None,
    participants: list[tuple[str, str | None]],
) -> str | None:
    """Format a timezone block showing current local times for all participants.

    Groups participants by timezone and avoids duplication.

    Format examples:
    - Same timezone: "[Now: You and Alice 2:00 PM (America/New_York)]"
    - Different: "[Now: You 2:00 PM (America/New_York) | Alice 11:00 AM (America/Los_Angeles)]"
    - Multiple same: "[Now: You, Alice, and Bob 2:00 PM (America/New_York)]"

    Args:
        assistant_tz: Assistant's timezone (IANA identifier) or None
        participants: List of (name, timezone) tuples for other participants

    Returns:
        Formatted timezone block string, or None if no timezone data
    """
    _timing_t0 = perf_counter()
    if not assistant_tz and not any(tz for _, tz in participants):
        log_startup_timing(
            LOGGER,
            (
                "⏱️ [StartupTiming] timezone.format_block.detail "
                "total=%.0fms early_return=True build_map=0ms current_times=0ms "
                "format_names=0ms join=0ms participants=%d timezones=0 unknown=%d"
            ),
            (perf_counter() - _timing_t0) * 1000,
            len(participants),
            len(participants) + 1,
        )
        return None

    # Build timezone -> list of names mapping
    # Include "You" (assistant) in the mapping
    tz_to_names: dict[str, list[str]] = {}
    unknown_names: list[str] = []
    _build_map_t0 = perf_counter()

    if assistant_tz:
        tz_to_names[assistant_tz] = ["You"]
    else:
        unknown_names.append("You")

    for name, tz in participants:
        if tz:
            if tz not in tz_to_names:
                tz_to_names[tz] = []
            tz_to_names[tz].append(name)
        else:
            unknown_names.append(name)

    if not tz_to_names and not unknown_names:
        _build_map_ms = (perf_counter() - _build_map_t0) * 1000
        log_startup_timing(
            LOGGER,
            (
                "⏱️ [StartupTiming] timezone.format_block.detail "
                "total=%.0fms early_return=True build_map=%.0fms "
                "current_times=0ms format_names=0ms join=0ms participants=%d "
                "timezones=0 unknown=0"
            ),
            (perf_counter() - _timing_t0) * 1000,
            _build_map_ms,
            len(participants),
        )
        return None
    _build_map_ms = (perf_counter() - _build_map_t0) * 1000

    # Format each timezone group
    parts: list[str] = []
    _current_times_ms = 0.0
    _format_names_ms = 0.0
    for tz_name in sorted(tz_to_names.keys()):
        names = tz_to_names[tz_name]
        _current_time_t0 = perf_counter()
        current_time = _get_current_time_in_timezone(tz_name)
        _current_times_ms += (perf_counter() - _current_time_t0) * 1000
        _format_names_t0 = perf_counter()
        # Format names: "You", "You and Alice", "You, Alice, and Bob"
        if len(names) == 1:
            names_str = names[0]
        elif len(names) == 2:
            names_str = f"{names[0]} and {names[1]}"
        else:
            names_str = ", ".join(names[:-1]) + f", and {names[-1]}"
        parts.append(f"{names_str} {current_time} ({tz_name})")
        _format_names_ms += (perf_counter() - _format_names_t0) * 1000

    _unknown_format_t0 = perf_counter()
    if unknown_names:
        if len(unknown_names) == 1:
            names_str = unknown_names[0]
        elif len(unknown_names) == 2:
            names_str = f"{unknown_names[0]} and {unknown_names[1]}"
        else:
            names_str = ", ".join(unknown_names[:-1]) + f", and {unknown_names[-1]}"
        parts.append(f"{names_str} (unknown timezone)")
    _format_names_ms += (perf_counter() - _unknown_format_t0) * 1000

    if not parts:
        log_startup_timing(
            LOGGER,
            (
                "⏱️ [StartupTiming] timezone.format_block.detail "
                "total=%.0fms early_return=True build_map=%.0fms "
                "current_times=%.0fms format_names=%.0fms join=0ms "
                "participants=%d timezones=%d unknown=%d"
            ),
            (perf_counter() - _timing_t0) * 1000,
            _build_map_ms,
            _current_times_ms,
            _format_names_ms,
            len(participants),
            len(tz_to_names),
            len(unknown_names),
        )
        return None

    _join_t0 = perf_counter()
    rendered = "[Now: " + " | ".join(parts) + "]"
    _join_ms = (perf_counter() - _join_t0) * 1000
    log_startup_timing(
        LOGGER,
        (
            "⏱️ [StartupTiming] timezone.format_block.detail "
            "total=%.0fms early_return=False build_map=%.0fms "
            "current_times=%.0fms format_names=%.0fms join=%.0fms "
            "participants=%d timezones=%d unknown=%d chars=%d"
        ),
        (perf_counter() - _timing_t0) * 1000,
        _build_map_ms,
        _current_times_ms,
        _format_names_ms,
        _join_ms,
        len(participants),
        len(tz_to_names),
        len(unknown_names),
        len(rendered),
    )
    return rendered


def _get_message_timezone_block(
    contact_name: str,
    contact_timezone: str | None,
    assistant_timezone: str | None,
) -> str | None:
    """Get the timezone block for a chat message.

    Args:
        contact_name: Name of the contact
        contact_timezone: Contact's timezone (IANA identifier) or None
        assistant_timezone: Assistant's timezone or None

    Returns:
        Formatted timezone block or None
    """
    return _format_timezone_block(
        assistant_tz=assistant_timezone,
        participants=[(contact_name, contact_timezone)],
    )


# =============================================================================
# Snapshot State Tracking for Incremental Context Propagation
# =============================================================================


@dataclass
class MessageElement:
    """A message element with identity for diff tracking.

    Identity is based on (contact_id, thread_name, index_in_thread, timestamp).
    """

    contact_id: int
    thread_name: str
    index_in_thread: int
    timestamp: datetime
    rendered: str


@dataclass
class NotificationElement:
    """A notification element with identity for diff tracking.

    Identity is based on (timestamp, content_hash, pinned).
    """

    timestamp: datetime
    content_hash: int
    pinned: bool
    rendered: str


@dataclass
class ActionElement:
    """An in-flight action element with identity for diff tracking.

    Identity is based on handle_id. State changes (new history events,
    status changes) are tracked via history_count and status.
    """

    handle_id: int
    query: str
    status: str
    history_count: int
    rendered: str


@dataclass
class SnapshotState:
    """Tracks the constituent elements of a rendered snapshot.

    This enables computing diffs between snapshots for incremental context
    propagation. Each element type has identity tracking:

    - Messages: (contact_id, thread_name, index, timestamp)
    - Notifications: (timestamp, content_hash, pinned)
    - Actions: (handle_id, with status/history tracking for state changes)

    The full_render contains the complete rendered snapshot string.
    """

    # The complete rendered snapshot
    full_render: str

    # Constituent elements with identity
    messages: list[MessageElement] = field(default_factory=list)
    notifications: list[NotificationElement] = field(default_factory=list)
    actions: list[ActionElement] = field(default_factory=list)

    # Snapshot metadata
    snapshot_time: datetime | None = None

    def message_ids(self) -> set[tuple[int, str, int, datetime]]:
        """Return set of message identity tuples for diff comparison."""
        return {
            (m.contact_id, m.thread_name, m.index_in_thread, m.timestamp)
            for m in self.messages
        }

    def notification_ids(self) -> set[tuple[datetime, int, bool]]:
        """Return set of notification identity tuples for diff comparison."""
        return {(n.timestamp, n.content_hash, n.pinned) for n in self.notifications}

    def action_states(self) -> dict[int, tuple[str, int]]:
        """Return dict of action_id -> (status, history_count) for diff comparison."""
        return {a.handle_id: (a.status, a.history_count) for a in self.actions}


def compute_snapshot_diff(
    old_snapshot: SnapshotState | None,
    new_snapshot: SnapshotState,
) -> str:
    """Compute the incremental diff between two snapshots.

    Returns a rendered string containing only the NEW or CHANGED elements:
    - New messages (not present in old snapshot)
    - New or changed notifications (new, or pinned state changed)
    - Action state changes (status changed, new history events)

    If old_snapshot is None, returns the full new snapshot (no diff possible).

    Args:
        old_snapshot: Previous snapshot state (or None for first snapshot)
        new_snapshot: Current snapshot state

    Returns:
        Rendered string containing only incremental updates, or empty string
        if nothing changed.
    """
    if old_snapshot is None:
        return new_snapshot.full_render

    diff_parts: list[str] = []

    # Find new messages
    old_msg_ids = old_snapshot.message_ids()
    new_messages = [
        m
        for m in new_snapshot.messages
        if (m.contact_id, m.thread_name, m.index_in_thread, m.timestamp)
        not in old_msg_ids
    ]
    if new_messages:
        msg_renders = [m.rendered for m in new_messages]
        diff_parts.append(
            "<new_messages>\n" + "\n".join(msg_renders) + "\n</new_messages>",
        )

    # Find new or changed notifications
    old_notif_ids = old_snapshot.notification_ids()
    new_notifications = [
        n
        for n in new_snapshot.notifications
        if (n.timestamp, n.content_hash, n.pinned) not in old_notif_ids
    ]
    if new_notifications:
        notif_renders = [n.rendered for n in new_notifications]
        diff_parts.append(
            "<new_notifications>\n"
            + "\n".join(notif_renders)
            + "\n</new_notifications>",
        )

    # Find action state changes
    old_action_states = old_snapshot.action_states()
    action_changes = []
    for action in new_snapshot.actions:
        old_state = old_action_states.get(action.handle_id)
        if old_state is None:
            # New action
            action_changes.append(action.rendered)
        elif old_state != (action.status, action.history_count):
            # Status or history changed
            action_changes.append(action.rendered)
    if action_changes:
        diff_parts.append(
            "<action_updates>\n" + "\n".join(action_changes) + "\n</action_updates>",
        )

    if not diff_parts:
        return ""

    return "\n\n".join(diff_parts)


def _attachment_detail(att: Any) -> str:
    """One attachment as ``filename (filepath)``.

    Attachments are local files whichever direction they travelled, so the
    path is what the Actor needs to open one.
    """
    if isinstance(att, dict):
        fname = att.get("filename") or "attachment"
        fpath = att.get("filepath")
        return f"{fname} ({fpath})" if fpath else str(fname)
    return str(att)


class Renderer:

    def render_state(
        self,
        contact_index: ContactIndex,
        notification_bar: NotificationBar = None,
        in_flight_actions: dict = None,
        completed_actions: dict = None,
        last_snapshot: datetime = None,
        recent_tool_executions: list[dict[str, Any]] | None = None,
        max_pinned_notifications: int = 50,
        max_contact_medium_messages: int = 25,
        max_action_history_events: int = 20,
        max_completed_actions: int = 20,
        max_completed_action_history_events: int = 5,
    ) -> SnapshotState:
        """Render the full conversation state.

        Returns a SnapshotState containing the rendered string and constituent
        element tracking for incremental diff computation.
        """
        from unify.common.prompt_helpers import now as prompt_now

        _render_t0 = perf_counter()
        _last_step = _render_t0

        def _mark_step() -> float:
            nonlocal _last_step
            now = perf_counter()
            elapsed_ms = (now - _last_step) * 1000
            _last_step = now
            return elapsed_ms

        message_elements: list[MessageElement] = []
        notification_elements: list[NotificationElement] = []
        action_elements: list[ActionElement] = []

        notif_render = self.render_notification_bar(
            notification_bar,
            last_snapshot=last_snapshot,
            max_pinned=max_pinned_notifications,
            elements_out=notification_elements,
        )
        _notifications_ms = _mark_step()
        actions_render = self.render_in_flight_actions(
            in_flight_actions,
            max_history=max_action_history_events,
            elements_out=action_elements,
        )
        _in_flight_ms = _mark_step()
        completed_render = self.render_completed_actions(
            completed_actions,
            max_completed=max_completed_actions,
            max_history=max_completed_action_history_events,
        )
        _completed_ms = _mark_step()
        recent_tools_render = self.render_recent_tool_executions(
            recent_tool_executions,
        )
        convs_render = self.render_active_conversations(
            contact_index,
            last_snapshot=last_snapshot,
            max_contact_medium_messages=max_contact_medium_messages,
            elements_out=message_elements,
        )
        _conversations_ms = _mark_step()

        # The wall clock closes the snapshot rather than living in the system
        # prompt: a minute rollover then only re-tokenizes the snapshot tail
        # instead of invalidating the provider's system+tools cache.
        time_render = f"Current time: {prompt_now()}."

        sections = [
            s
            for s in [
                notif_render,
                actions_render,
                completed_render,
                recent_tools_render,
                convs_render,
                time_render,
            ]
            if s
        ]
        full_render = "\n\n".join(sections)
        _join_ms = _mark_step()

        snapshot_state = SnapshotState(
            full_render=full_render,
            messages=message_elements,
            notifications=notification_elements,
            actions=action_elements,
            snapshot_time=prompt_now(as_string=False),
        )
        _snapshot_ms = _mark_step()

        log_startup_timing(
            LOGGER,
            (
                "⏱️ [StartupTiming] llm_preamble.render_state.detail "
                "total=%.0fms notifications=%.0fms in_flight=%.0fms "
                "completed=%.0fms conversations=%.0fms join=%.0fms "
                "snapshot=%.0fms chars=%d messages=%d notifications_count=%d "
                "actions=%d sections=%d"
            ),
            (perf_counter() - _render_t0) * 1000,
            _notifications_ms,
            _in_flight_ms,
            _completed_ms,
            _conversations_ms,
            _join_ms,
            _snapshot_ms,
            len(full_render),
            len(message_elements),
            len(notification_elements),
            len(action_elements),
            len(sections),
        )

        return snapshot_state

    def render_notification_bar(
        self,
        notification_bar: NotificationBar,
        last_snapshot: datetime = None,
        max_pinned: int = 50,
        elements_out: list[NotificationElement] | None = None,
    ) -> str:
        """Render the notification bar."""
        if notification_bar is None:
            return "<notifications>\n</notifications>"

        pinned_notifs = sorted(
            (n for n in notification_bar.notifications if n.pinned),
            key=lambda n: n.timestamp,
        )[-max_pinned:]
        new_notifs = [
            n
            for n in notification_bar.notifications
            if not n.pinned and n.timestamp > last_snapshot
        ]
        all_notifs = pinned_notifs + new_notifs

        rendered_lines = []
        for n in all_notifs:
            prefix = "[PINNED]" if n.pinned else ""
            line = f'{prefix}[{n.type.title()} Notification @ {n.timestamp.strftime("%A, %B %d, %Y at %I:%M %p")}] {n.content}'
            rendered_lines.append(line)

            if elements_out is not None:
                elements_out.append(
                    NotificationElement(
                        timestamp=n.timestamp,
                        content_hash=hash(n.content),
                        pinned=n.pinned,
                        rendered=line,
                    ),
                )

        return "<notifications>\n" + "\n".join(rendered_lines) + "\n</notifications>"

    @staticmethod
    def _render_action_history(
        handle_actions: list[dict],
        short_name: str,
        handle_id: int,
        max_history: int,
    ) -> str:
        """Render the event history for an action, capped to the most recent events."""
        displayed = handle_actions[-max_history:]
        if not displayed:
            return ""
        out = "<history>\n"
        for a in displayed:
            action_type = a.get("action_name", "")
            action_query = a.get("query", "")
            action_status = a.get("status", "")
            action_ts = a.get("timestamp", "")

            attrs = f"type='{action_type}'"
            if action_ts:
                attrs += f" timestamp='{action_ts}'"
            if action_status:
                attrs += f" status='{action_status}'"
            out += f"<event {attrs}>\n"

            if action_query:
                out += f"  <content>{action_query}</content>\n"
            if a_res := a.get("response"):
                out += f"  <response>{a_res}</response>\n"

            if action_status == "pending" and action_type.startswith("ask_"):
                out += (
                    "  <note>Result pending - you will receive another "
                    "turn when the answer is ready.</note>\n"
                )

            if action_type == "clarification_request" and not a.get("response"):
                call_id = a.get("call_id", "")
                out += (
                    "  <pending>Use answer_clarification_action("
                    f"handle_id={handle_id}, call_id='{call_id}', "
                    "answer=...) to respond</pending>\n"
                )
            out += "</event>\n"
        out += "</history>\n"
        return out

    def render_in_flight_actions(
        self,
        in_flight_actions: dict,
        max_history: int = 20,
        elements_out: list[ActionElement] | None = None,
    ) -> str:
        """Render in-flight actions with their status and history."""
        out = "<in_flight_actions>\n"
        if not in_flight_actions:
            out += "No actions currently executing.\n"
        else:
            for handle_id, handle_data in in_flight_actions.items():
                query = handle_data.get("query", "")
                short_name = derive_short_name(query)
                handle = handle_data.get("handle")
                handle_actions = handle_data.get("handle_actions", [])

                is_paused = get_handle_paused_state(handle)
                status = "paused" if is_paused else "executing"

                pending_clarifications = [
                    a
                    for a in handle_actions
                    if a.get("action_name") == "clarification_request"
                    and not a.get("response")
                ]

                is_persistent = handle_data.get("persist", False)
                mode_attr = " mode='persistent'" if is_persistent else ""
                action_type = handle_data.get("action_type", "act")
                type_attr = f" type='{action_type}'"
                action_render = f"<action id='{handle_id}' short_name='{short_name}' status='{status}'{type_attr}{mode_attr}>\n"
                action_render += f"<original_request>{query}</original_request>\n"
                if is_persistent:
                    action_render += (
                        "<note>Persistent session — will NOT self-complete. "
                        "Use stop_action(handle_id=...) to end it. Responses marked "
                        "'awaiting_input' "
                        "mean the actor finished its turn and needs your next "
                        "interject_action to continue.</note>\n"
                    )

                action_render += "<steering_tools>\n"
                for action_name, description in iter_steering_tools_for_action(
                    handle_id,
                    query,
                    pending_clarifications,
                    is_paused=is_paused,
                ):
                    action_render += f"  - {action_name}: {description}\n"
                action_render += "</steering_tools>\n"

                action_render += self._render_action_history(
                    handle_actions,
                    short_name,
                    handle_id,
                    max_history,
                )

                action_render += "</action>\n"
                out += action_render

                if elements_out is not None:
                    elements_out.append(
                        ActionElement(
                            handle_id=handle_id,
                            query=query,
                            status=status,
                            history_count=len(handle_actions),
                            rendered=action_render,
                        ),
                    )

        out += "</in_flight_actions>"
        return out

    def render_recent_tool_executions(
        self,
        recent_tool_executions: list[dict[str, Any]] | None,
        max_items: int = 12,
    ) -> str:
        """Render a bounded summary of recently executed tools."""
        out = "<recent_tool_executions>\n"
        if not recent_tool_executions:
            out += "No recent tool executions.\n"
            out += "</recent_tool_executions>"
            return out

        for entry in recent_tool_executions[-max_items:]:
            tool_name = str(entry.get("tool_name") or "unknown")
            generation = str(entry.get("generation") or "?")
            origin_event = str(entry.get("origin_event_name") or "-")
            args_preview = str(entry.get("args_preview") or "{}")
            result_preview = str(entry.get("result_preview") or "null")
            out += f"- generation={generation} origin={origin_event} tool={tool_name}\n"
            out += f"  args={args_preview}\n"
            out += f"  result={result_preview}\n"
        out += "</recent_tool_executions>"
        return out

    def render_active_conversations(
        self,
        contact_index: ContactIndex,
        last_snapshot: datetime = None,
        max_contact_medium_messages: int = 25,
        elements_out: list[MessageElement] | None = None,
    ) -> str:
        """Render active conversations derived from the shared global thread.

        Only contacts with messages in the global thread are rendered. Per-contact
        views are derived from the shared deque at render time.
        """
        _render_t0 = perf_counter()
        # Fetch assistant's timezone once for all contacts
        assistant_timezone = get_assistant_timezone()
        _timezone_ms = (perf_counter() - _render_t0) * 1000

        # Group global thread entries by contact_id
        _group_t0 = perf_counter()
        grouped = contact_index.get_messages_grouped_by_contact()
        _group_ms = (perf_counter() - _group_t0) * 1000

        contacts = []
        _contacts_t0 = perf_counter()
        for contact_id, entries in grouped.items():
            contact_info = contact_index.get_contact(contact_id) or {}
            conv_state = contact_index.get_or_create_conversation(contact_id)
            rendered = self.render_contact(
                contact_info=contact_info,
                conv_state=conv_state,
                entries=entries,
                max_contact_medium_messages=max_contact_medium_messages,
                last_snapshot=last_snapshot,
                elements_out=elements_out,
                assistant_timezone=assistant_timezone,
            )
            contacts.append(rendered)
        _contacts_ms = (perf_counter() - _contacts_t0) * 1000

        _join_t0 = perf_counter()
        contacts_str = "\n\n".join(contacts)
        rendered = f"<active_conversations>\n{contacts_str}\n</active_conversations>"
        _join_ms = (perf_counter() - _join_t0) * 1000

        log_startup_timing(
            LOGGER,
            (
                "⏱️ [StartupTiming] llm_preamble.render_state.conversations "
                "total=%.0fms timezone=%.0fms group=%.0fms contacts=%.0fms "
                "join=%.0fms contact_count=%d entry_count=%d chars=%d "
                "assistant_timezone_cached=%s"
            ),
            (perf_counter() - _render_t0) * 1000,
            _timezone_ms,
            _group_ms,
            _contacts_ms,
            _join_ms,
            len(grouped),
            sum(len(entries) for entries in grouped.values()),
            len(rendered),
            assistant_timezone is not None,
        )

        return rendered

    def render_contact(
        self,
        contact_info: dict,
        conv_state: ConversationState,
        entries: list[GlobalThreadEntry] | None = None,
        max_contact_medium_messages: int = 25,
        last_snapshot: datetime = None,
        elements_out: list[MessageElement] | None = None,
        assistant_timezone: str | None = None,
    ) -> str:
        """Render a single contact's conversation.

        Entries are grouped by medium and each medium's thread is capped, so
        the contact block reads as one thread per medium the contact used.
        """
        _contact_t0 = perf_counter()
        contact_id = conv_state.contact_id
        first_name = contact_info.get("first_name") or ""
        surname = contact_info.get("surname") or ""
        phone_number = contact_info.get("phone_number") or ""
        email_address = contact_info.get("email_address") or ""
        timezone = contact_info.get("timezone") or ""
        bio = contact_info.get("bio") or ""
        rolling_summary = contact_info.get("rolling_summary") or ""
        response_policy = contact_info.get("response_policy") or ""
        should_respond = contact_info.get("should_respond", True)
        is_boss = is_boss_contact(contact_id)

        # Compute contact name for timezone display
        contact_name = f"{first_name} {surname}".strip() or f"Contact #{contact_id}"
        contact_timezone = contact_info.get("timezone")

        if entries is None:
            entries = []
        _metadata_ms = (perf_counter() - _contact_t0) * 1000

        _medium_group_t0 = perf_counter()
        medium_messages: dict[str, list] = {}
        for entry in entries:
            medium_key = str(entry.medium)
            if medium_key not in medium_messages:
                medium_messages[medium_key] = []
            medium_messages[medium_key].append(entry.message)
        _medium_group_ms = (perf_counter() - _medium_group_t0) * 1000

        _medium_render_t0 = perf_counter()
        threads_content = "\n\n".join(
            self.render_thread(
                medium_name,
                msgs,
                contact_id=contact_id,
                max_messages=max_contact_medium_messages,
                last_snapshot=last_snapshot,
                elements_out=elements_out,
                contact_name=contact_name,
                contact_timezone=contact_timezone,
                assistant_timezone=assistant_timezone,
            )
            for medium_name, msgs in medium_messages.items()
            if msgs
        )
        _medium_render_ms = (perf_counter() - _medium_render_t0) * 1000
        _join_t0 = perf_counter()
        rendered = (
            f'<contact contact_id="{contact_id}" first_name="{first_name}" surname="{surname}" '
            f'is_boss="{is_boss}" phone_number="{phone_number}" email_address="{email_address}" '
            f'timezone="{timezone}" should_respond="{should_respond}">\n'
            f"<bio>{bio}</bio>\n"
            f"<rolling_summary>{rolling_summary}</rolling_summary>\n"
            f"<response_policy>{response_policy}</response_policy>\n"
            f"<threads>\n{threads_content}\n</threads>\n"
            f"</contact>"
        )
        _format_ms = (perf_counter() - _join_t0) * 1000

        log_startup_timing(
            LOGGER,
            (
                "⏱️ [StartupTiming] llm_preamble.render_state.contact "
                "contact_id=%s total=%.0fms metadata=%.0fms "
                "medium_group=%.0fms medium_render=%.0fms format=%.0fms "
                "entries=%d mediums=%d chars=%d"
            ),
            contact_id,
            (perf_counter() - _contact_t0) * 1000,
            _metadata_ms,
            _medium_group_ms,
            _medium_render_ms,
            _format_ms,
            len(entries),
            len(medium_messages),
            len(rendered),
        )

        return rendered

    def render_thread(
        self,
        thread_name: str,
        thread,
        contact_id: int = None,
        max_messages: int = 25,
        last_snapshot: datetime = None,
        elements_out: list[MessageElement] | None = None,
        contact_name: str | None = None,
        contact_timezone: str | None = None,
        assistant_timezone: str | None = None,
    ) -> str:
        """Render a thread."""
        thread_list = list(thread)
        displayed_messages = thread_list[-max_messages:]
        start_index = len(thread_list) - len(displayed_messages)

        rendered_messages = []
        for i, m in enumerate(displayed_messages):
            rendered = self.render_message(
                m,
                last_snapshot,
                contact_name=contact_name,
                contact_timezone=contact_timezone,
                assistant_timezone=assistant_timezone,
            )
            rendered_messages.append(rendered)

            if elements_out is not None:
                elements_out.append(
                    MessageElement(
                        contact_id=contact_id,
                        thread_name=thread_name,
                        index_in_thread=start_index + i,
                        timestamp=m.timestamp,
                        rendered=rendered,
                    ),
                )

        return (
            f"<{thread_name}>\n" + "\n".join(rendered_messages) + f"\n</{thread_name}>"
        )

    def render_message(
        self,
        message: UnifyMessage | GuidanceMessage,
        last_snapshot: datetime = None,
        contact_name: str | None = None,
        contact_timezone: str | None = None,
        assistant_timezone: str | None = None,
    ):
        # Mark all recent messages as NEW (both incoming and outbound)
        is_new = last_snapshot < message.timestamp
        new_marker = "**NEW** " if is_new else ""
        timestamp_str = message.timestamp.strftime("%A, %B %d, %Y at %I:%M %p")

        if isinstance(message, GuidanceMessage):
            # Silent awareness guidance the assistant issued to itself; already
            # delivered, so it carries no attachments or timezone block.
            return f"{new_marker}[{message.name} @ {timestamp_str}]: {message.content}"

        attachments_line = ""
        if message.attachments:
            attachment_details = [
                _attachment_detail(att) for att in message.attachments
            ]
            attachments_line = f" [Attachments: {', '.join(attachment_details)}]"

        # Show timezone info for the contact
        tz_block_line = ""
        if contact_name:
            tz_block = _get_message_timezone_block(
                contact_name,
                contact_timezone,
                assistant_timezone,
            )
            if tz_block:
                tz_block_line = f"\n{tz_block}"

        return f"{new_marker}[{message.name} @ {timestamp_str}]: {message.content}{attachments_line}{tz_block_line}"

    def render_completed_actions(
        self,
        completed_actions: dict,
        max_completed: int = 20,
        max_history: int = 5,
    ):
        """Render completed actions with their result and a brief history.

        Each entry is self-contained: original query, result, capped history,
        and steering tools for post-completion queries.
        """
        out = "<completed_actions>\n"
        if not completed_actions:
            out += "No completed actions.\n"
        else:
            # Cap to the most recent completed actions by handle_id (monotonic)
            items = list(completed_actions.items())[-max_completed:]

            for handle_id, handle_data in items:
                query = handle_data.get("query", "")
                short_name = derive_short_name(query)
                handle_actions = handle_data.get("handle_actions", [])

                # Extract terminal status from the most recent completion marker.
                terminal_event = None
                for a in reversed(handle_actions):
                    if a.get("action_name") in {"act_completed", "act_failed"}:
                        terminal_event = a
                        break

                action_type = handle_data.get("action_type", "act")
                action_status = (
                    "failed"
                    if terminal_event is not None
                    and terminal_event.get("success") is False
                    else "completed"
                )
                out += f"<action id='{handle_id}' short_name='{short_name}' status='{action_status}' type='{action_type}'>\n"
                out += f"<original_request>{query}</original_request>\n"
                task_description = handle_data.get("task_description")
                if task_description:
                    out += f"<task_description>{task_description}</task_description>\n"
                # The author's instruction about delivery, e.g. "Deliver the
                # briefing as one chat message". Rendered here because this is
                # the turn that can act on it: without it the decision to relay
                # a finished run or stay silent was taken with no statement of
                # intent in view, and went both ways on identical inputs.
                response_policy = handle_data.get("response_policy")
                if response_policy:
                    out += (
                        f"<task_response_policy>{response_policy}"
                        "</task_response_policy>\n"
                    )

                if terminal_event is not None:
                    if terminal_event.get("success") is False:
                        error_text = terminal_event.get("error") or terminal_event.get(
                            "query",
                            "",
                        )
                        if error_text:
                            out += f"<error>{error_text}</error>\n"
                    else:
                        result = terminal_event.get(
                            "result",
                            terminal_event.get("query", ""),
                        )
                        out += f"<result>{result}</result>\n"

                out += self._render_action_history(
                    handle_actions,
                    short_name,
                    handle_id,
                    max_history,
                )

                out += "<steering_tools>\n"
                for (
                    action_name,
                    description,
                ) in iter_steering_tools_for_completed_action(
                    handle_id,
                    query,
                ):
                    out += f"  - {action_name}: {description}\n"
                out += "</steering_tools>\n"

                out += "</action>\n"
        out += "</completed_actions>"
        return out
