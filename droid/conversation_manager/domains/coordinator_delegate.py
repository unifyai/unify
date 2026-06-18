"""Coordinator delegation wake-reason helpers.

The Coordinator can assign work to a colleague without writing directly into
that colleague's manager-owned contexts. These helpers turn the cold-start wake
reason or hot system event into a slow-brain notification so the colleague uses
its own primitives to perform the work.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from droid.conversation_manager.events import CoordinatorDelegate

if TYPE_CHECKING:
    from droid.conversation_manager.conversation_manager import ConversationManager


_WAKE_REASON_TYPE = "coordinator_delegate"
_NOTIFICATION_TYPE = "Coordinator"
_SEEN_DEDUPE_KEYS: set[tuple[str, str]] = set()


def _optional_text(value: Any) -> str | None:
    """Return stripped text when the payload supplied a meaningful string."""

    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _coordinator_delegate_event_from_payload(
    payload: dict[str, Any],
    *,
    reason: str = "",
) -> CoordinatorDelegate | None:
    """Build a Coordinator delegation event from a hot system-event payload."""

    if not isinstance(payload, dict):
        return None
    return _coordinator_delegate_event_from_mapping(payload, reason=reason)


def _coordinator_delegate_event_from_wake_reason(
    reason: Any,
) -> CoordinatorDelegate | None:
    """Build a Coordinator delegation event from one startup wake reason."""

    if not isinstance(reason, dict) or reason.get("type") != _WAKE_REASON_TYPE:
        return None
    return _coordinator_delegate_event_from_mapping(reason)


def _coordinator_delegate_event_from_mapping(
    payload: dict[str, Any],
    *,
    reason: str = "",
) -> CoordinatorDelegate | None:
    """Return an event when the mapping contains the required delegation fields."""

    requested_by = _optional_text(payload.get("requested_by_assistant_id"))
    instruction = _optional_text(payload.get("instruction"))
    if requested_by is None or instruction is None:
        return None

    intent = _optional_text(payload.get("intent")) or "general"
    dedupe_key = _optional_text(payload.get("dedupe_key"))
    related_context = payload.get("related_context")
    if not isinstance(related_context, dict):
        related_context = None

    return CoordinatorDelegate(
        requested_by_assistant_id=requested_by,
        instruction=instruction,
        intent=intent,
        dedupe_key=dedupe_key,
        related_context=related_context,
        reason=reason,
    )


def _coordinator_delegate_notification_text(event: CoordinatorDelegate) -> str:
    """Return the slow-brain instruction for one delegated assignment."""

    parts = [
        f"Coordinator {event.requested_by_assistant_id} assigned you work.",
        f"Intent: {event.intent}.",
        f"Assignment: {event.instruction}",
        (
            "Carry this out through your own available manager primitives "
            "instead of assuming any setup rows already exist."
        ),
        (
            "The Coordinator only received confirmation that the assignment "
            "was dispatched, so do not wait for a synchronous acknowledgement."
        ),
    ]
    if event.dedupe_key:
        parts.append(
            f"If you can tell dedupe key {event.dedupe_key} was already handled, avoid duplicating the work.",
        )
    if event.related_context:
        context_json = json.dumps(event.related_context, sort_keys=True, default=str)
        parts.append(f"Related context: {context_json}")
    return " ".join(parts)


async def _handle_coordinator_delegate_event(
    event: CoordinatorDelegate,
    cm: "ConversationManager",
) -> bool:
    """Surface one Coordinator delegation to the colleague brain."""

    if event.dedupe_key:
        dedupe_id = (event.requested_by_assistant_id, event.dedupe_key)
        if dedupe_id in _SEEN_DEDUPE_KEYS:
            cm._session_logger.info(
                "coordinator_delegate",
                f"Ignoring duplicate Coordinator delegation {event.dedupe_key}.",
            )
            return False
        _SEEN_DEDUPE_KEYS.add(dedupe_id)

    cm.notifications_bar.push_notif(
        _NOTIFICATION_TYPE,
        _coordinator_delegate_notification_text(event),
        event.timestamp,
    )
    cm._session_logger.info(
        "coordinator_delegate",
        (
            f"Coordinator {event.requested_by_assistant_id} delegated "
            f"{event.intent} work to this assistant."
        ),
    )
    return True
