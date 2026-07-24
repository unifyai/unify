"""
CommsManager: External communications handler for ConversationManager.

This module bridges external communication channels (GCP PubSub for SMS, email,
calls, etc.) to the internal event broker.

Threading Model:
----------------
GCP PubSub uses a thread pool for message callbacks. The `handle_message` method
is called from these background threads, NOT from the asyncio event loop. Therefore:

- `handle_message` uses `asyncio.run_coroutine_threadsafe()` to safely publish
  events to the async event broker from a sync callback context.
- `send_pings` and `start` are async methods that run on the event loop and can
  use direct `await` for async operations.

Testing:
--------
For testing, CommsManager is typically disabled (enable_comms_manager=False) since
there are no real external events to receive. Tests can publish events directly
to the event broker instead.
"""

from __future__ import annotations

import asyncio
from functools import partial
import html
import json
import re
import threading
import time
from typing import TYPE_CHECKING, Any, Callable

from dotenv import load_dotenv

try:
    from google.cloud import pubsub_v1
except ImportError:  # pragma: no cover - exercised in local-only installs
    pubsub_v1 = None

from unify.logger import LOGGER
from unify.common.hierarchical_logger import DEFAULT_ICON, ICONS
from unify.settings import SETTINGS
from unify.deploy_runtime import (
    mark_job_container_ready,
    read_assistant_session,
    read_job_assignment_record,
    read_session_bootstrap_secret_record,
    wait_for_assistant_session_name,
)
from unify.conversation_manager.domains.comms_utils import (
    add_email_attachments,
    add_unify_message_attachments,
    publish_system_error,
    resolve_slack_user_profile,
)
from unify.conversation_manager.domains.coordinator_onboarding import (
    _coordinator_onboarding_event_from_payload,
)
from unify.conversation_manager.domains.coordinator_delegate import (
    _coordinator_delegate_event_from_payload,
)
from unify.conversation_manager.domains.integration_sync import (
    _integration_tools_sync_completed_from_payload,
    _integration_tools_sync_failed_from_payload,
    _integration_tools_sync_requested_from_payload,
)
from unify.conversation_manager.events import *
from unify.conversation_manager.metrics import pubsub_e2e_latency
from unify.session_details import SESSION_DETAILS
from unify.contact_manager.types.contact import UNASSIGNED
from unify.contact_manager.ops import partition_create_kwargs
from unify.conversation_manager.cm_types import MEDIUM_TO_CONTACT_FIELD, Medium

load_dotenv()

# Lock for unknown contact creation to prevent duplicates
_unknown_contact_lock = threading.Lock()

# In-memory dedup for Discord message snowflake IDs.
# Keyed on message ID; values are insertion timestamps for TTL eviction.
_seen_discord_ids: dict[str, float] = {}
_DISCORD_DEDUP_TTL = 300.0


def _required_contact_id(event: dict, field_name: str) -> int:
    """Return a resolved contact id required by startup/update events."""
    value = event.get(field_name)
    if value is None:
        assistant_id = event.get("assistant_id")
        raise ValueError(f"Assistant {assistant_id} is missing required {field_name}")
    return int(value)


def _already_seen_discord(message_id: str) -> bool:
    """Return True if this Discord message_id was already processed recently."""
    now = time.time()
    cutoff = now - _DISCORD_DEDUP_TTL
    expired = [k for k, t in _seen_discord_ids.items() if t < cutoff]
    for k in expired:
        del _seen_discord_ids[k]
    if message_id in _seen_discord_ids:
        return True
    _seen_discord_ids[message_id] = now
    return False


# In-memory dedup for Microsoft Graph Teams message IDs.
# Graph change-notification subscriptions can redeliver when acks fall behind,
# so we guard against duplicate ingestion the same way Discord does.
_seen_teams_ids: dict[str, float] = {}
_TEAMS_DEDUP_TTL = 300.0


def _already_seen_teams(message_id: str) -> bool:
    """Return True if this Teams message_id was already processed recently."""
    now = time.time()
    cutoff = now - _TEAMS_DEDUP_TTL
    expired = [k for k, t in _seen_teams_ids.items() if t < cutoff]
    for k in expired:
        del _seen_teams_ids[k]
    if message_id in _seen_teams_ids:
        return True
    _seen_teams_ids[message_id] = now
    return False


# In-memory dedup for inbound Slack messages. Two redelivery sources exist:
# (1) the Events API replays an event when an ack is missed within ~3s, and
# (2) a single channel mention is delivered twice -- once as ``app_mention``
# and once as ``message`` -- with *distinct* ``event_id``s but the *same*
# ``client_msg_id``. Keying on the stable message identity (client_msg_id,
# falling back to ts) collapses both cases; keying on ``event_id`` would miss
# the app_mention/message pair.
_seen_slack_ids: dict[str, float] = {}
_SLACK_DEDUP_TTL = 300.0


def _already_seen_slack(message_key: str) -> bool:
    """Return True if this Slack message key was already processed recently."""
    now = time.time()
    cutoff = now - _SLACK_DEDUP_TTL
    expired = [k for k, t in _seen_slack_ids.items() if t < cutoff]
    for k in expired:
        del _seen_slack_ids[k]
    if message_key in _seen_slack_ids:
        return True
    _seen_slack_ids[message_key] = now
    return False


# In-memory dedup for inbound Unify Teams bot activities. The Bot Connector
# retries delivery when a webhook ack is slow, so we guard on the stable
# activity id the same way the Graph Teams and Slack paths do.
_seen_ms_teams_bot_ids: dict[str, float] = {}
_MS_TEAMS_BOT_DEDUP_TTL = 300.0


def _already_seen_ms_teams_bot(message_key: str) -> bool:
    """Return True if this Teams bot activity was already processed recently."""
    now = time.time()
    cutoff = now - _MS_TEAMS_BOT_DEDUP_TTL
    expired = [k for k, t in _seen_ms_teams_bot_ids.items() if t < cutoff]
    for k in expired:
        del _seen_ms_teams_bot_ids[k]
    if message_key in _seen_ms_teams_bot_ids:
        return True
    _seen_ms_teams_bot_ids[message_key] = now
    return False


if TYPE_CHECKING:
    from unify.conversation_manager.in_memory_event_broker import InMemoryEventBroker
    from unify.gateway.ingress import IngressTransport

    EventBroker = InMemoryEventBroker
    _ = IngressTransport  # silence unused-import on the TYPE_CHECKING branch


def _get_subscription_id() -> str:
    """Build subscription ID from current assistant context."""
    agent_id = SESSION_DETAILS.assistant.agent_id
    env_suffix = SETTINGS.ENV_SUFFIX if agent_id is not None else ""
    return f"unity-{agent_id}{env_suffix}-sub"


def _get_local_contact() -> dict:
    """Build local contact dict from current assistant context."""
    return {
        "contact_id": SESSION_DETAILS.boss_contact_id,
        "first_name": SESSION_DETAILS.user.first_name,
        "surname": SESSION_DETAILS.user.surname,
        "phone_number": SESSION_DETAILS.user.number,
        "email_address": SESSION_DETAILS.user.email,
        "whatsapp_number": SESSION_DETAILS.user.whatsapp_number,
    }


def _coerce_int(value: Any) -> int | None:
    """Best-effort integer coercion for webhook payloads."""

    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


_TEAMS_BR_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
_TEAMS_BLOCK_END_RE = re.compile(r"</(p|div|li|h[1-6]|tr)>", re.IGNORECASE)
_TEAMS_TAG_RE = re.compile(r"<[^>]+>")
_TEAMS_INLINE_WS_RE = re.compile(r"[ \t]+")
_TEAMS_BLANK_RUN_RE = re.compile(r"\n{3,}")


def _teams_html_to_text(raw: str) -> str:
    """Convert a Teams HTML message body into plain text.

    Microsoft Graph returns Teams chat and channel message bodies as HTML
    whenever the user authored them in the Teams UI (e.g. ``<p>hi</p>``).
    Storing the raw markup in transcripts is noisy and bloats downstream
    LLM prompts, so we normalize to plain text at ingress:

    - ``<br>`` and block-closing tags (``</p>``, ``</div>`` etc.) become newlines
    - all remaining tags are dropped
    - HTML entities (``&amp;``, ``&nbsp;`` etc.) are unescaped
    - inline whitespace is collapsed; leading/trailing empty lines stripped
    """
    if not raw:
        return ""
    s = _TEAMS_BR_RE.sub("\n", raw)
    s = _TEAMS_BLOCK_END_RE.sub("\n", s)
    s = _TEAMS_TAG_RE.sub("", s)
    s = html.unescape(s)
    lines = [_TEAMS_INLINE_WS_RE.sub(" ", line).strip() for line in s.splitlines()]
    joined = "\n".join(line for line in lines).strip()
    # An empty paragraph (e.g. ``<p><br></p>``) emits both a ``<br>`` newline and
    # a closing-tag newline on top of the surrounding paragraphs' own breaks,
    # which adds up to 3+ consecutive newlines. Collapse runs to a single blank
    # line so empty paragraphs render as one visual gap, matching how Teams
    # itself displays them.
    return _TEAMS_BLANK_RUN_RE.sub("\n\n", joined)


def _task_due_event_from_payload(
    payload: dict[str, Any],
    *,
    reason: str = "",
) -> TaskDue | None:
    """Build a `TaskDue` event from a comms Pub/Sub payload.

    Thin alias around :meth:`TaskDue.from_dict` kept here to preserve the
    call-site name `comms_manager` already imports.
    """

    return TaskDue.from_dict(payload, reason=reason)


def _task_trigger_event_from_payload(
    payload: dict[str, Any],
    *,
    reason: str = "",
) -> TaskTriggerRequested | None:
    """Build a REST task-trigger event from a comms Pub/Sub payload."""

    return TaskTriggerRequested.from_dict(payload, reason=reason)


def _provider_event_dispatch_event_from_payload(
    payload: dict[str, Any],
    *,
    reason: str = "",
) -> "ProviderEventDispatchRequested | None":
    """Build a live provider-event dispatch event from a comms Pub/Sub payload."""

    from unify.conversation_manager.events import ProviderEventDispatchRequested

    return ProviderEventDispatchRequested.from_dict(payload, reason=reason)


def _assistant_turn_injected_from_payload(
    payload: dict[str, Any],
    *,
    reason: str = "",
) -> AssistantTurnInjected | None:
    if not isinstance(payload, dict):
        return None
    extra = payload.get("extra_event_fields")
    fields = {**(extra if isinstance(extra, dict) else {}), **payload}
    contact_id = _coerce_int(fields.get("contact_id"))
    contact = {"contact_id": contact_id} if contact_id is not None else {}
    content = str(fields.get("content") or fields.get("message") or reason or "")
    if not content:
        return None
    return AssistantTurnInjected(
        contact=contact,
        content=content,
        source=str(fields.get("source") or "console"),
        schedule_proactive=bool(fields.get("schedule_proactive", False)),
    )


def _proactive_speech_control_from_payload(
    payload: dict[str, Any],
    *,
    reason: str = "",
) -> ProactiveSpeechControl | None:
    if not isinstance(payload, dict):
        return None
    extra = payload.get("extra_event_fields")
    fields = {**(extra if isinstance(extra, dict) else {}), **payload}
    enabled = fields.get("enabled")
    if not isinstance(enabled, bool):
        return None
    return ProactiveSpeechControl(
        enabled=enabled,
        source=str(fields.get("source") or "console"),
        reason=str(fields.get("reason") or reason or ""),
        schedule_now=bool(fields.get("schedule_now", False)),
    )


def _action_stop_from_payload(
    payload: dict[str, Any],
    *,
    reason: str = "",
) -> ActionStopRequested | None:
    if not isinstance(payload, dict):
        return None
    extra = payload.get("extra_event_fields")
    fields = {**(extra if isinstance(extra, dict) else {}), **payload}
    calling_id = fields.get("calling_id")
    if not isinstance(calling_id, str) or not calling_id.strip():
        return None
    return ActionStopRequested(
        calling_id=calling_id.strip(),
        reason=str(fields.get("reason") or reason or ""),
        source=str(fields.get("source") or "console"),
    )


# Map subscription IDs to their corresponding event types
events_map: dict[str, Event] = {
    "msg": SMSReceived,
    "whatsapp": WhatsAppReceived,
    "email": EmailReceived,
    "unify_message": UnifyMessageReceived,
    "api_message": ApiMessageReceived,
    "discord": DiscordMessageReceived,
    "slack": SlackMessageReceived,
    "teams_chat": TeamsMessageReceived,
    "teams_channel": TeamsChannelMessageReceived,
    "ms_teams_bot": MsTeamsBotMessageReceived,
}


def _is_blacklisted(medium: str, contact_detail: str | None) -> bool:
    """
    Check if a contact detail is blacklisted for a given medium.

    This is a fail-open check: returns False on any error to avoid
    blocking legitimate messages due to infrastructure issues.

    Gated by SETTINGS.conversation.BLACKLIST_CHECKS_ENABLED (default False).
    When disabled, returns False immediately without any manager initialization.

    Args:
        medium: The communication medium (e.g., "sms_message", "email", "phone_call")
        contact_detail: The phone number or email address to check

    Returns:
        True if the contact detail is blacklisted, False otherwise
    """
    # Fast path: skip all manager initialization when blacklist checks disabled
    if not SETTINGS.conversation.BLACKLIST_CHECKS_ENABLED:
        return False

    if not contact_detail:
        return False

    try:
        from unify.blacklist_manager import BlackListManager

        blm = BlackListManager()
        result = blm.filter_blacklist(
            filter=f"medium == '{medium}' and contact_detail == '{contact_detail}'",
            limit=1,
        )
        return len(result.get("entries", [])) > 0
    except Exception:
        # Fail-open: don't block messages if blacklist check fails
        return False


def _get_or_create_unknown_contact(
    medium: str,
    contact_detail: str,
) -> dict | None:
    """
    Get an existing contact or create a new unknown contact.

    When an inbound message arrives from an unknown sender (not in Contacts
    and not in BlackList), we create a minimal contact record with:
    - Only the medium field populated (phone_number or email_address)
    - should_respond=False to prevent automatic responses
    - A response_policy guiding the assistant to seek boss guidance

    Uses a lock to prevent duplicate contact creation when multiple
    messages arrive from the same unknown sender simultaneously.

    Gated by SETTINGS.conversation.BLACKLIST_CHECKS_ENABLED (default False).
    When disabled, returns None immediately without any manager initialization.

    Args:
        medium: The communication medium (determines which contact field to set)
        contact_detail: The phone number or email address

    Returns:
        The contact dict (existing or newly created), or None on error
    """
    # Fast path: skip all manager initialization when blacklist checks disabled
    if not SETTINGS.conversation.BLACKLIST_CHECKS_ENABLED:
        return None

    from unify.manager_registry import ManagerRegistry
    from unify.contact_manager.contact_manager import ContactManager

    with _unknown_contact_lock:
        try:
            cm = ManagerRegistry.get_contact_manager()

            try:
                medium_enum = Medium(medium)
            except ValueError:
                return None
            field_name = MEDIUM_TO_CONTACT_FIELD.get(medium_enum)
            if not field_name:
                return None

            # Check if contact already exists
            result = cm.filter_contacts(
                filter=f"{field_name} == '{contact_detail}'",
                limit=1,
            )
            existing = result.get("contacts", [])
            if existing:
                contact = existing[0]
                return (
                    contact.model_dump() if hasattr(contact, "model_dump") else contact
                )

            # Create new unknown contact
            create_kwargs = {
                field_name: contact_detail,
                "should_respond": False,
                "response_policy": ContactManager.UNKNOWN_INBOUND_RESPONSE_POLICY,
            }
            outcome = cm._create_contact(**partition_create_kwargs(create_kwargs))
            new_contact_id = outcome["details"]["contact_id"]

            # Fetch the newly created contact
            contact_info = cm.get_contact_info(new_contact_id)
            new_contact = contact_info.get(new_contact_id)
            return new_contact

        except Exception as e:
            LOGGER.error(f"{DEFAULT_ICON} Error in _get_or_create_unknown_contact: {e}")
            return None


def _filter_single_contact(cm, contact_filter: str) -> dict | None:
    """Return one contact matching a filter, or None (errors swallowed)."""
    try:
        result = cm.filter_contacts(filter=contact_filter, limit=1)
    except Exception:
        # Any backend filter error is treated as "not found".
        return None
    contacts = result.get("contacts", [])
    if not contacts:
        return None
    contact = contacts[0]
    return contact.model_dump() if hasattr(contact, "model_dump") else contact


def _get_or_create_team_chat_sender_contact(
    sender_name: str,
    sender_email: str,
    sender_assistant_id: int | str | None = None,
) -> dict | None:
    """Resolve a team/org-group chat sender to a contact, provisioning if needed.

    Team and org-group chat fan-out can carry messages from senders the
    receiving assistant has never talked to: another member's assistant, or
    an org member whose contact row was never provisioned. Assistant senders
    are keyed on their stable ``agent_id`` (provisioned into teammate contacts
    at startup), so resolution works even when the sender has no provisioned
    email; email is the fallback identity. Teammates are known, trusted
    collaborators — unlike arbitrary unknown inbound senders — so resolution
    is not gated by the blacklist-check flag and the created contact is a
    normal responsive system contact.
    """
    if not sender_email and sender_assistant_id is None:
        return None

    with _unknown_contact_lock:
        try:
            from unify.manager_registry import ManagerRegistry
            from unify.contact_manager.system_contacts import (
                TEAMMATE_ASSISTANT_RESPONSE_POLICY,
            )

            cm = ManagerRegistry.get_contact_manager()
            if cm is None:
                return None

            contact = None
            if sender_assistant_id is not None:
                contact = _filter_single_contact(
                    cm,
                    f"agent_id == '{sender_assistant_id}'",
                )
            if contact is None and sender_email:
                contact = _filter_single_contact(
                    cm,
                    f"email_address == '{sender_email}'",
                )
            if contact is not None:
                return contact

            name_parts = (sender_name or "").strip().split(" ", 1)
            create_kwargs = {
                "first_name": name_parts[0] or None,
                "surname": name_parts[1] if len(name_parts) > 1 else None,
                "email_address": sender_email or None,
                "should_respond": True,
                "response_policy": TEAMMATE_ASSISTANT_RESPONSE_POLICY,
                "is_system": True,
            }
            if sender_assistant_id is not None:
                create_kwargs["agent_id"] = str(sender_assistant_id)
            outcome = cm._create_contact(**partition_create_kwargs(create_kwargs))
            new_contact_id = outcome["details"]["contact_id"]
            contact_info = cm.get_contact_info(new_contact_id)
            return contact_info.get(new_contact_id)
        except Exception as e:
            LOGGER.error(
                f"{DEFAULT_ICON} Error resolving team chat sender "
                f"(email={sender_email!r}, assistant={sender_assistant_id!r}): {e}",
            )
            return None


def _lookup_known_contact(medium: str, contact_detail: str) -> dict | None:
    """Resolve a sender to an existing contact via the live ContactManager.

    The inbound resolver matches senders against adapter-supplied contacts and
    the session-snapshot local contact. Neither reflects later edits to the
    Contacts context (the single source of truth) — e.g. the owner adding their
    WhatsApp number on the account page after the session started. This queries
    ContactManager directly so such edits resolve immediately. It only reads
    known contacts: it never creates one and is not gated by blacklist checks
    (that gating governs unknown-contact creation, not lookup of known senders).
    """
    try:
        medium_enum = Medium(medium)
    except ValueError:
        return None
    field_name = MEDIUM_TO_CONTACT_FIELD.get(medium_enum)
    if not field_name:
        return None
    try:
        from unify.manager_registry import ManagerRegistry

        cm = ManagerRegistry.get_contact_manager()
        if cm is None:
            return None
        result = cm.filter_contacts(
            filter=f"{field_name} == '{contact_detail}'",
            limit=1,
        )
        contacts = result.get("contacts", [])
        if contacts:
            contact = contacts[0]
            return contact.model_dump() if hasattr(contact, "model_dump") else contact
    except Exception as e:
        LOGGER.error(f"{DEFAULT_ICON} Error in _lookup_known_contact: {e}")
    return None


def _normalize_name(name: str) -> str:
    """Lower-case + strip punctuation/diacritics for tolerant name compares."""
    if not name:
        return ""
    import unicodedata as _ud

    decomposed = _ud.normalize("NFKD", name)
    stripped = "".join(c for c in decomposed if not _ud.combining(c))
    cleaned = re.sub(r"[^\w\s]", " ", stripped).lower()
    return re.sub(r"\s+", " ", cleaned).strip()


def _match_contact_by_name(name: str, contacts: list[dict]) -> dict | None:
    """Return the unique contact whose name matches ``name``, else None.

    Tries both ``"First Surname"`` and ``"Surname First"`` orderings (locale
    differences). More than one match is treated as ambiguous — we refuse
    rather than risk routing to the wrong contact.
    """
    target = _normalize_name(name)
    if not target:
        return None
    matches = [
        c
        for c in contacts
        if target
        in (
            _normalize_name(
                f"{c.get('first_name', '') or ''} {c.get('surname', '') or ''}",
            ),
            _normalize_name(
                f"{c.get('surname', '') or ''} {c.get('first_name', '') or ''}",
            ),
        )
    ]
    return matches[0] if len(matches) == 1 else None


def _persist_slack_user_id(contact_id: int, slack_user_id: str) -> None:
    """Attach ``slack_user_id`` to an existing contact so future inbound
    Slack messages from this user match it directly."""
    from unify.manager_registry import ManagerRegistry

    try:
        ManagerRegistry.get_contact_manager().update_contact(
            contact_id=contact_id,
            slack_user_id=slack_user_id,
        )
    except Exception as e:
        LOGGER.error(
            f"{DEFAULT_ICON} Failed to persist slack_user_id on contact {contact_id}: {e}",
        )


def _create_slack_contact(slack_user_id: str, profile: dict) -> dict | None:
    """Create a respondable contact for an addressed Slack sender.

    Used when an explicit ``@app`` mention arrives from a user we can't
    match to an existing contact — the mention is a clear intent to
    converse, so the contact is created with ``should_respond=True``.
    """
    from unify.manager_registry import ManagerRegistry

    cm = ManagerRegistry.get_contact_manager()

    def _existing_by_slack_id() -> dict | None:
        result = cm.filter_contacts(
            filter=f"slack_user_id == '{slack_user_id}'",
            limit=1,
        )
        existing = result.get("contacts", [])
        if not existing:
            return None
        c = existing[0]
        return c.model_dump() if hasattr(c, "model_dump") else c

    # Another in-flight event (the app_mention/message pair) or a prior
    # session may already own this slack_user_id. Reuse it rather than
    # racing into a duplicate insert.
    found = _existing_by_slack_id()
    if found is not None:
        return found

    full_name = (profile.get("real_name") or profile.get("display_name") or "").strip()
    email = (profile.get("email") or "").strip()

    # Never mint a nameless, email-less orphan: it would capture this
    # slack_user_id and permanently shadow the real contact (the fast
    # slack_user_id match would resolve to the orphan and skip the
    # email/name match). Without any identifying detail, leave it
    # unresolved so a later attempt (once the profile lookup succeeds)
    # can bind the sender to the right existing contact by email/name.
    if not full_name and not email:
        return None

    try:
        first_name, _, surname = full_name.partition(" ")
        outcome = cm._create_contact(
            first_name=first_name or None,
            surname=surname or None,
            email_address=(email or None),
            should_respond=True,
            slack_user_id=slack_user_id,
        )
        new_id = outcome["details"]["contact_id"]
        return cm.get_contact_info(new_id).get(new_id)
    except Exception as e:
        # Lost a create race on the unique slack_user_id constraint — the
        # winner's row is now queryable, so resolve to it instead of dropping.
        found = _existing_by_slack_id()
        if found is not None:
            return found
        LOGGER.error(f"{DEFAULT_ICON} Error creating Slack contact: {e}")
        return None


def _create_ms_teams_bot_contact(display_name: str) -> dict | None:
    """Create a respondable contact for an addressed Teams bot sender.

    The Bot Framework only delivers an activity to the bot when it is a 1:1
    chat or an explicit @mention, so an inbound bot message is always intent
    to converse — the contact is created with ``should_respond=True``.

    Teams activities carry no email (roster/Graph lookup is deferred), so the
    sender's AAD display name is the only durable key available. A stable
    display name lets the next inbound message re-match this contact by name.
    A nameless sender is left unresolved rather than minting an orphan.
    """
    full_name = (display_name or "").strip()
    if not full_name:
        return None
    from unify.manager_registry import ManagerRegistry

    cm = ManagerRegistry.get_contact_manager()
    try:
        first_name, _, surname = full_name.partition(" ")
        outcome = cm._create_contact(
            first_name=first_name or None,
            surname=surname or None,
            should_respond=True,
        )
        new_id = outcome["details"]["contact_id"]
        return cm.get_contact_info(new_id).get(new_id)
    except Exception as e:
        LOGGER.error(f"{DEFAULT_ICON} Error creating Teams bot contact: {e}")
        return None


def _resolve_teams_participants(
    *,
    raw_participants: list[dict],
    sender_email: str,
    sender_contact_id: int | None,
    medium: str,
    unknown_contact_resolver,
) -> list[int]:
    """Collapse the adapter-supplied participants payload into contact IDs.

    Each entry is shaped as ``{"contact_id": int | None, "email": str | None,
    "display_name": str, "aad_user_id": str | None}``. Pre-resolved entries
    (``contact_id`` set) are used directly. Unresolved entries with a real
    email are finished via ``unknown_contact_resolver`` — the single
    canonical path for minting external contacts (gated by
    ``BLACKLIST_CHECKS_ENABLED``). Entries without a usable email (or with
    a synthetic ``@teams`` placeholder) are dropped rather than pollute the
    contact store.

    The sender's contact_id is seeded into the set because the ingress
    caller has already resolved them via the sender-specific branch;
    entries matching ``sender_email`` are skipped to avoid a redundant
    resolver call.
    """
    participant_contact_ids: set[int] = set()
    if sender_contact_id is not None:
        participant_contact_ids.add(sender_contact_id)

    for entry in raw_participants:
        entry_email = (entry.get("email") or "").strip()
        if entry_email and sender_email and entry_email == sender_email:
            continue
        entry_cid = entry.get("contact_id")
        if entry_cid is not None:
            participant_contact_ids.add(entry_cid)
            continue
        if not entry_email or entry_email.endswith("@teams"):
            continue
        resolved = unknown_contact_resolver(medium, entry_email)
        resolved_cid = (resolved or {}).get("contact_id")
        if resolved_cid is not None:
            participant_contact_ids.add(resolved_cid)

    return sorted(participant_contact_ids)


class CommsManager:
    """
    Handles external communications via GCP PubSub.

    Receives events from external channels (SMS, email, calls) and publishes
    them to the internal event broker for ConversationManager to process.
    """

    def __init__(
        self,
        event_broker: "EventBroker",
        ingress_transport: "IngressTransport | None" = None,
        ingress_transport_factory: "Callable[[], IngressTransport | None] | None" = None,
    ):
        self.subscribers: dict = {}
        self.call_proc = None
        self.credentials = None
        # Store reference to event loop for thread-safe publishing from callbacks
        self.loop = asyncio.get_event_loop()
        self.event_broker: "EventBroker" = event_broker
        # Optional pluggable inbound transport (unify.gateway.IngressTransport).
        # When provided, replaces the inline self.subscribe_to_topic Pub/Sub
        # path. When None (the default for every call site at the time of
        # this change), the existing inline Pub/Sub subscriber remains active
        # so behaviour for legacy callers is unchanged. The inline path will
        # be removed in a later phase once the injected path has soaked in
        # production. See unify/gateway/PHASES.md.
        self.ingress_transport: "IngressTransport | None" = ingress_transport
        # Optional factory for lazy transport construction. Resolved inside
        # ``_start_inbound_subscription``, which runs *after*
        # ``_poll_for_assignment`` has set the per-assistant ``agent_id``.
        # Required for the hosted Pub/Sub path because the subscription ID
        # is derived from ``agent_id`` and therefore unknown at the moment
        # ``CommsManager`` is constructed in an idle pod. Wins over
        # ``ingress_transport`` when both are supplied.
        self.ingress_transport_factory: (
            "Callable[[], IngressTransport | None] | None"
        ) = ingress_transport_factory

    def _publish_from_callback(self, channel: str, message: str) -> None:
        """
        Publish to event broker from a sync callback (thread-safe).

        This method is called from GCP PubSub callbacks which run in a thread pool,
        NOT from the asyncio event loop. We use run_coroutine_threadsafe to safely
        schedule the async publish on the main event loop.
        """
        asyncio.run_coroutine_threadsafe(
            self.event_broker.publish(channel, message),
            self.loop,
        )

    def _ack_with_latency(self, message, publish_timestamp, topic):
        """Ack the message and record end-to-end Pub/Sub latency if available."""
        if publish_timestamp is not None:
            latency = time.time() - publish_timestamp
            pubsub_e2e_latency.record(latency, {"topic": topic})
        message.ack()

    def _ack_callback(self, ack, publish_timestamp, topic):
        """Ack a message callback while preserving latency metrics."""
        if publish_timestamp is not None:
            latency = time.time() - publish_timestamp
            pubsub_e2e_latency.record(latency, {"topic": topic})
        ack()

    def _log_dispatch_future(self, future) -> None:
        """Log unexpected failures from background envelope dispatch tasks."""
        exc = future.exception()
        if exc is not None:
            LOGGER.error(f"{DEFAULT_ICON} Error processing message: {exc}")

    async def _handle_attachment_ingestion_complete(self, event: dict) -> None:
        """Apply a worker-reported attachment ingestion outcome to FileRecords.

        Invoked when a ``thread="attachment_ingestion_complete"`` envelope
        arrives on the per-assistant Pub/Sub topic.  The event payload must
        include ``display_name`` and ``status`` (``"success"`` or ``"error"``);
        optional ``error`` carries a failure message.
        """
        display_name = event.get("display_name")
        status = event.get("status", "success")
        error = event.get("error")
        if not display_name:
            LOGGER.warning(
                f"{DEFAULT_ICON} attachment_ingestion_complete missing display_name; "
                "ignoring event.",
            )
            return
        try:
            from unify.conversation_manager.domains.managers_utils import (
                ManagerRegistry,
            )

            file_manager = ManagerRegistry.get_file_manager()
        except Exception:
            file_manager = None
        if file_manager is None:
            LOGGER.warning(
                f"{DEFAULT_ICON} attachment_ingestion_complete received but "
                "FileManager is unavailable; cannot update FileRecords.",
            )
            return
        from unify.file_manager.managers.utils.attachment_ingestion import (
            apply_attachment_completion,
        )

        await asyncio.to_thread(
            apply_attachment_completion,
            file_manager,
            display_name=display_name,
            status=status,
            error=error,
        )

    async def dispatch_envelope_payload(
        self,
        payload: dict,
        *,
        source_topic: str = "",
        ack=None,
        nack=None,
    ) -> None:
        """Dispatch a normalized {thread, event} payload to the event broker."""
        await self.dispatch_inbound_envelope(
            thread=payload["thread"],
            event=payload["event"],
            publish_timestamp=payload.get("publish_timestamp"),
            source_topic=source_topic,
            ack=ack,
            nack=nack,
        )

    async def dispatch_inbound_envelope(
        self,
        *,
        thread: str,
        event: dict,
        publish_timestamp: float | None = None,
        source_topic: str = "",
        ack=None,
        nack=None,
    ) -> None:
        """Map a comms envelope onto the existing app:comms:* broker contract."""

        async def publish(channel: str, payload: str) -> None:
            await self.event_broker.publish(channel, payload)

        async def publish_blocking(channel: str, payload: str) -> None:
            await self.event_broker.publish(channel, payload)

        def schedule(coro) -> None:
            asyncio.create_task(coro)

        def ack_now() -> None:
            if ack is not None:
                self._ack_callback(ack, publish_timestamp, source_topic)

        def nack_now() -> None:
            if nack is not None:
                nack()

        try:
            if thread == "assistant_update":
                details = {
                    "api_key": event["api_key"],
                    "binding_id": event.get("binding_id", ""),
                    "medium": event.get("medium", "assistant_update"),
                    "assistant_id": event["assistant_id"],
                    "user_id": event["user_id"],
                    "assistant_first_name": event["assistant_first_name"],
                    "assistant_surname": event["assistant_surname"],
                    "assistant_age": event["assistant_age"],
                    "assistant_nationality": event["assistant_nationality"],
                    "assistant_timezone": event.get("assistant_timezone", ""),
                    "assistant_about": event["assistant_about"],
                    "assistant_job_title": event.get("assistant_job_title", ""),
                    "assistant_number": event["assistant_number"],
                    "assistant_email": event["assistant_email"],
                    "assistant_email_provider": event.get(
                        "assistant_email_provider",
                        "google_workspace",
                    ),
                    "self_contact_id": _required_contact_id(
                        event,
                        "self_contact_id",
                    ),
                    "boss_contact_id": _required_contact_id(
                        event,
                        "boss_contact_id",
                    ),
                    "assistant_whatsapp_number": event.get(
                        "assistant_whatsapp_number",
                        "",
                    ),
                    "assistant_discord_bot_id": event.get(
                        "assistant_discord_bot_id",
                        "",
                    ),
                    "assistant_slack_bot_user_id": event.get(
                        "assistant_slack_bot_user_id",
                        "",
                    ),
                    "assistant_slack_team_id": event.get(
                        "assistant_slack_team_id",
                        "",
                    ),
                    "assistant_has_ms_teams_bot": event.get(
                        "assistant_has_ms_teams_bot",
                        SESSION_DETAILS.assistant.has_ms_teams_bot,
                    ),
                    "user_first_name": event["user_first_name"],
                    "user_surname": event["user_surname"],
                    "user_number": event["user_number"],
                    "user_email": event["user_email"],
                    "user_whatsapp_number": event.get("user_whatsapp_number", ""),
                    "voice_provider": event["voice_provider"],
                    "voice_id": event["voice_id"],
                    "default_model": event.get("default_model", ""),
                    "default_reasoning_effort": event.get(
                        "default_reasoning_effort",
                        "",
                    ),
                    "slow_brain_model": event.get("slow_brain_model", ""),
                    "slow_brain_reasoning_effort": event.get(
                        "slow_brain_reasoning_effort",
                        "",
                    ),
                    "desktop_mode": event.get("desktop_mode", "ubuntu"),
                    "user_desktops": event.get("user_desktops") or [],
                    "org_id": event.get("org_id"),
                    "org_name": event.get("org_name", ""),
                    "team_ids": event.get("team_ids") or [],
                    "team_summaries": event.get("team_summaries") or [],
                    "is_coordinator": event.get("is_coordinator", False),
                    "update_kind": event.get("update_kind", "general"),
                }
                await publish(
                    "app:comms:assistant_update",
                    AssistantUpdateEvent(**details).to_json(),
                )
                ack_now()
                return

            if thread == "ping":
                await publish(
                    "app:comms:ping",
                    Ping(kind="keepalive").to_json(),
                )
                ack_now()
                return

            if thread == "attachment_ingestion_complete":
                await self._handle_attachment_ingestion_complete(event)
                ack_now()
                return

            if thread == "unity_system_event":
                system_event_type = event.get("event_type")
                system_message = event.get("message")
                reason = str(system_message) if system_message is not None else ""

                desktop_ready_ttl = 300
                if (
                    system_event_type == "assistant_desktop_ready"
                    and publish_timestamp is not None
                    and time.time() - publish_timestamp > desktop_ready_ttl
                ):
                    age = time.time() - publish_timestamp
                    LOGGER.warning(
                        f"{DEFAULT_ICON} Discarding stale assistant_desktop_ready "
                        f"(age={age:.0f}s, TTL={desktop_ready_ttl}s)",
                    )
                    ack_now()
                    return

                system_event_map = {
                    "sync_contacts": lambda r: SyncContacts(
                        reason=r or "Contact sync requested via system event.",
                    ),
                    "task_due": lambda r: _task_due_event_from_payload(
                        event,
                        reason=r,
                    ),
                    "task_trigger": lambda r: _task_trigger_event_from_payload(
                        event,
                        reason=r,
                    ),
                    "provider_event_dispatch": lambda r: (
                        _provider_event_dispatch_event_from_payload(
                            event,
                            reason=r,
                        )
                    ),
                    "coordinator_delegate": lambda r: _coordinator_delegate_event_from_payload(
                        event,
                        reason=r,
                    ),
                    "coordinator_onboarding_event": lambda r: _coordinator_onboarding_event_from_payload(
                        event,
                        message=r,
                    ),
                    "integration_tools_sync_requested": lambda r: _integration_tools_sync_requested_from_payload(
                        event,
                        message=r,
                    ),
                    "integration_tools_sync_completed": lambda r: _integration_tools_sync_completed_from_payload(
                        event,
                        message=r,
                    ),
                    "integration_tools_sync_failed": lambda r: _integration_tools_sync_failed_from_payload(
                        event,
                        message=r,
                    ),
                    "assistant_presence_observed": lambda r: AssistantPresenceObserved(
                        reason=str(
                            event.get("reason") or r or "User presence observed.",
                        ),
                        source=str(event.get("source") or "console"),
                        page_visibility=str(event.get("page_visibility") or ""),
                        occurred_at=str(event.get("occurred_at") or ""),
                    ),
                    "assistant_turn_injected": lambda r: _assistant_turn_injected_from_payload(
                        event,
                        reason=r,
                    ),
                    "proactive_speech_control": lambda r: _proactive_speech_control_from_payload(
                        event,
                        reason=r,
                    ),
                    "action_stop": lambda r: _action_stop_from_payload(
                        event,
                        reason=r,
                    ),
                    "assistant_screen_share_started": lambda r: AssistantScreenShareStarted(
                        reason=r or "User enabled assistant screen sharing.",
                    ),
                    "assistant_screen_share_stopped": lambda r: AssistantScreenShareStopped(
                        reason=r or "User disabled assistant screen sharing.",
                    ),
                    "user_screen_share_started": lambda r: UserScreenShareStarted(
                        reason=r or "User started sharing their screen.",
                    ),
                    "user_screen_share_stopped": lambda r: UserScreenShareStopped(
                        reason=r or "User stopped sharing their screen.",
                    ),
                    "user_webcam_started": lambda r: UserWebcamStarted(),
                    "user_webcam_stopped": lambda r: UserWebcamStopped(),
                    "user_remote_control_started": lambda r: UserRemoteControlStarted(
                        reason=r or "User took remote control of assistant desktop.",
                    ),
                    "user_remote_control_stopped": lambda r: UserRemoteControlStopped(
                        reason=r
                        or "User released remote control of assistant desktop.",
                    ),
                    "user_filesys_access_started": lambda r: UserFilesysAccessStarted(
                        user_id=str(event.get("user_id") or ""),
                        reason=r or "User enabled filesystem access.",
                    ),
                    "user_filesys_access_stopped": lambda r: UserFilesysAccessStopped(
                        user_id=str(event.get("user_id") or ""),
                        reason=r or "User disabled filesystem access.",
                    ),
                    "assistant_desktop_ready": lambda r: AssistantDesktopReady(
                        binding_id=event.get("binding_id") or "",
                        desktop_url=event.get("desktop_url")
                        or SESSION_DETAILS.assistant.desktop_url
                        or "",
                        vm_type=event.get("vm_type")
                        or SESSION_DETAILS.assistant.desktop_mode,
                    ),
                }

                factory = system_event_map.get(system_event_type)
                if factory is not None:
                    mapped_event = factory(reason)
                    if mapped_event is not None:
                        await publish(
                            f"app:comms:{system_event_type}",
                            mapped_event.to_json(),
                        )
                ack_now()
                return

            if thread in events_map:
                contacts = [*event.get("contacts", []), _get_local_contact()]
                await publish(
                    "app:comms:backup_contacts",
                    BackupContactsEvent(contacts=contacts).to_json(),
                )

                content = event["body"]

                if (
                    thread in ("teams_chat", "teams_channel")
                    and event.get("content_type") == "html"
                ):
                    content = _teams_html_to_text(content)

                if thread == "email":
                    content = "Subject: " + event["subject"] + "\n\n" + event["body"]
                    raw_from = event["from"]
                    contact_detail = (
                        raw_from.split("<")[1].rstrip(">")
                        if "<" in raw_from
                        else raw_from.strip()
                    )
                    medium_for_blacklist = Medium.EMAIL

                    if _is_blacklisted(medium_for_blacklist, contact_detail):
                        LOGGER.debug(
                            f"{DEFAULT_ICON} Ignoring blacklisted email from: {contact_detail}",
                        )
                        ack_now()
                        return

                    contact = next(
                        (c for c in contacts if c["email_address"] == contact_detail),
                        None,
                    )
                    is_new_unknown = False
                    if contact is None:
                        contact = _get_or_create_unknown_contact(
                            medium_for_blacklist,
                            contact_detail,
                        )
                        is_new_unknown = contact is not None

                    if contact is None:
                        LOGGER.error(
                            f"{DEFAULT_ICON} Failed to resolve contact for email from: {contact_detail}",
                        )
                        ack_now()
                        return

                    attachments = event.get("attachments") or []

                    def _normalize_recipients(value):
                        if not value:
                            return []
                        if isinstance(value, str):
                            return [value] if value else []
                        return list(value)

                    await publish(
                        f"app:comms:{thread}_message",
                        events_map[thread](
                            subject=event["subject"],
                            body=event["body"],
                            contact=contact,
                            email_id=event["email_id"],
                            thread_id=event.get("thread_id") or None,
                            attachments=attachments,
                            to=_normalize_recipients(event.get("to")),
                            cc=_normalize_recipients(event.get("cc")),
                            bcc=_normalize_recipients(event.get("bcc")),
                        ).to_json(),
                    )

                    if is_new_unknown:
                        await publish(
                            "app:comms:unknown_contact_created",
                            UnknownContactCreated(
                                contact=contact,
                                medium=medium_for_blacklist,
                                message_preview=(
                                    event["subject"][:100]
                                    if event.get("subject")
                                    else ""
                                ),
                            ).to_json(),
                        )

                    if attachments:
                        schedule(
                            add_email_attachments(
                                attachments,
                                SESSION_DETAILS.assistant.email,
                                event.get("message_id")
                                or event.get("gmail_message_id", ""),
                            ),
                        )

                    ack_now()
                    return

                if thread == "unify_message":
                    team_id = event.get("team_id")
                    target_contact_id = event.get("contact_id")
                    contact = None
                    if target_contact_id is not None:
                        contact = next(
                            (
                                c
                                for c in contacts
                                if c["contact_id"] == target_contact_id
                            ),
                            None,
                        )
                    group_id = event.get("group_id")
                    # Chat fan-out: the sender may be another org member or a
                    # fellow assistant rather than this assistant's owner (in
                    # rooms, or an org member's assistant DM), in which case
                    # the adapters layer cannot resolve a per-assistant
                    # contact id. Assistant senders resolve by their stable
                    # agent_id (teammate contacts are provisioned at
                    # startup); humans resolve by email — the identity
                    # org-member contacts are stored under. Either way a
                    # teammate contact is provisioned on first message if the
                    # startup sync has not covered the sender yet.
                    if contact is None and (
                        team_id or group_id or event.get("sender_email")
                    ):
                        contact = _get_or_create_team_chat_sender_contact(
                            str(event.get("sender_name") or ""),
                            str(event.get("sender_email") or ""),
                            event.get("sender_assistant_id"),
                        )
                    if contact is None:
                        LOGGER.error(
                            f"{DEFAULT_ICON} Error: could not resolve sender contact "
                            f"for unify_message (contact_id={target_contact_id}, "
                            f"team_id={team_id}, group_id={group_id}), skipping message",
                        )
                        ack_now()
                        return

                    attachments = event.get("attachments") or []
                    thread_id_value = event.get("thread_id")
                    chat_message_id_value = event.get("chat_message_id")
                    await publish(
                        f"app:comms:{thread}_message",
                        events_map[thread](
                            content=content,
                            contact=contact,
                            attachments=attachments,
                            thread_id=(
                                int(thread_id_value) if thread_id_value else None
                            ),
                            chat_message_id=(
                                int(chat_message_id_value)
                                if chat_message_id_value
                                else None
                            ),
                            team_id=int(team_id) if team_id else None,
                            team_name=str(event.get("team_name") or ""),
                            group_id=int(group_id) if group_id else None,
                        ).to_json(),
                    )

                    if attachments:
                        schedule(add_unify_message_attachments(attachments))

                    ack_now()
                    return

                if thread == "unify_message_reaction":
                    target_contact_id = event.get("contact_id")
                    target_message_id = event.get("target_message_id")
                    chat_message_id_value = event.get("chat_message_id")
                    if target_contact_id is None or (
                        target_message_id is None and chat_message_id_value is None
                    ):
                        LOGGER.error(
                            f"{DEFAULT_ICON} unify_message_reaction requires "
                            "contact_id and target_message_id or chat_message_id",
                        )
                        ack_now()
                        return
                    contact = next(
                        (c for c in contacts if c["contact_id"] == target_contact_id),
                        None,
                    )
                    if contact is None:
                        LOGGER.error(
                            f"{DEFAULT_ICON} unify_message_reaction contact_id "
                            f"{target_contact_id} not found",
                        )
                        ack_now()
                        return
                    emoji = event.get("emoji")
                    if emoji == "":
                        emoji = None
                    reaction_thread_id = event.get("thread_id")
                    await publish(
                        "app:comms:unify_message_reaction",
                        UnifyMessageReactionChanged(
                            contact=contact,
                            target_message_id=(
                                int(target_message_id) if target_message_id else 0
                            ),
                            chat_message_id=(
                                int(chat_message_id_value)
                                if chat_message_id_value
                                else None
                            ),
                            thread_id=(
                                int(reaction_thread_id) if reaction_thread_id else None
                            ),
                            emoji=emoji,
                        ).to_json(),
                    )
                    ack_now()
                    return

                if thread == "whatsapp_reaction":
                    raw_from = (event.get("from_number") or "").strip()
                    contact_detail = (
                        raw_from.replace("whatsapp:", "")
                        if raw_from.startswith("whatsapp:")
                        else raw_from
                    )
                    contact = next(
                        (
                            c
                            for c in contacts
                            if c.get("whatsapp_number") == contact_detail
                            or c.get("phone_number") == contact_detail
                        ),
                        None,
                    )
                    if contact is None:
                        contact = _lookup_known_contact(
                            Medium.WHATSAPP_MESSAGE,
                            contact_detail,
                        )
                    if contact is None:
                        LOGGER.error(
                            f"{DEFAULT_ICON} Failed to resolve contact for WhatsApp "
                            f"reaction from: {contact_detail}",
                        )
                        ack_now()
                        return
                    emoji = event.get("emoji")
                    if emoji == "":
                        emoji = None
                    await publish(
                        "app:comms:whatsapp_reaction",
                        WhatsAppReactionChanged(
                            contact=contact,
                            target_message_id=int(event.get("target_message_id") or 0),
                            provider_message_sid=str(
                                event.get("provider_message_sid")
                                or event.get("message_sid")
                                or "",
                            ),
                            emoji=emoji,
                        ).to_json(),
                    )
                    ack_now()
                    return

                if thread == "api_message":
                    target_contact_id = event.get("contact_id", 1)
                    contact = next(
                        (c for c in contacts if c["contact_id"] == target_contact_id),
                        contacts[0] if contacts else {},
                    )
                    api_message_id = event.get("api_message_id", "")
                    attachments = event.get("attachments") or []
                    tags = event.get("tags") or []

                    await publish(
                        f"app:comms:{thread}_message",
                        events_map[thread](
                            content=content,
                            contact=contact,
                            api_message_id=api_message_id,
                            attachments=attachments,
                            tags=tags,
                        ).to_json(),
                    )

                    if attachments:
                        schedule(add_unify_message_attachments(attachments))

                    ack_now()
                    return

                if thread == "whatsapp":
                    if event.get("type") == "call_permission_response":
                        raw_contact_number = (
                            event.get("contact_number")
                            or event.get("from_number")
                            or ""
                        )
                        contact_number = raw_contact_number.replace(
                            "whatsapp:",
                            "",
                        ).strip()
                        payload = event.get("payload")
                        if payload == "ACCEPTED":
                            permission_status = "accepted"
                        elif payload == "REJECTED":
                            permission_status = "rejected"
                        else:
                            permission_status = "unknown_interaction"
                        accepted = permission_status == "accepted"
                        contact = next(
                            (
                                c
                                for c in contacts
                                if c.get("whatsapp_number") == contact_number
                            ),
                            None,
                        )
                        if contact is None:
                            contact = next(
                                (
                                    c
                                    for c in contacts
                                    if c.get("phone_number") == contact_number
                                ),
                                None,
                            )
                        if contact is None:
                            LOGGER.error(
                                f"{DEFAULT_ICON} Failed to resolve contact for WhatsApp call permission from: {contact_number}",
                            )
                            ack_now()
                            return

                        await publish(
                            "app:comms:whatsapp_call_permission",
                            WhatsAppCallPermissionResponse(
                                contact=contact,
                                accepted=accepted,
                                status=permission_status,
                            ).to_json(),
                        )
                        ack_now()
                        return

                    raw_from = event["from_number"].strip()
                    contact_detail = (
                        raw_from.replace("whatsapp:", "")
                        if raw_from.startswith("whatsapp:")
                        else raw_from
                    )
                    medium_for_blacklist = Medium.WHATSAPP_MESSAGE

                    if _is_blacklisted(medium_for_blacklist, contact_detail):
                        LOGGER.debug(
                            f"{DEFAULT_ICON} Ignoring blacklisted WhatsApp from: {contact_detail}",
                        )
                        ack_now()
                        return

                    contact = next(
                        (
                            c
                            for c in contacts
                            if c.get("whatsapp_number") == contact_detail
                            or c["phone_number"] == contact_detail
                        ),
                        None,
                    )
                    is_new_unknown = False
                    if contact is None:
                        contact = _lookup_known_contact(
                            medium_for_blacklist,
                            contact_detail,
                        )
                    if contact is None:
                        contact = _get_or_create_unknown_contact(
                            medium_for_blacklist,
                            contact_detail,
                        )
                        is_new_unknown = contact is not None

                    if contact is None:
                        LOGGER.error(
                            f"{DEFAULT_ICON} Failed to resolve contact for WhatsApp from: {contact_detail}",
                        )
                        ack_now()
                        return

                    attachments = event.get("attachments") or []
                    provider_message_sid = str(
                        event.get("message_sid")
                        or event.get("MessageSid")
                        or event.get("provider_message_sid")
                        or "",
                    )
                    await publish(
                        f"app:comms:{thread}_message",
                        events_map[thread](
                            content=content,
                            contact=contact,
                            **({"attachments": attachments} if attachments else {}),
                            **(
                                {"provider_message_sid": provider_message_sid}
                                if provider_message_sid
                                else {}
                            ),
                        ).to_json(),
                    )

                    if attachments:
                        schedule(add_unify_message_attachments(attachments))

                    if is_new_unknown:
                        await publish(
                            "app:comms:unknown_contact_created",
                            UnknownContactCreated(
                                contact=contact,
                                medium=medium_for_blacklist,
                                message_preview=content[:100] if content else "",
                            ).to_json(),
                        )

                    ack_now()
                    return

                if thread == "discord":
                    sender_discord_id = event.get("sender_discord_id", "")
                    message_id = event.get("message_id", "")
                    is_channel = event.get("is_channel", False)
                    channel_id = event.get("channel_id", "")
                    guild_id = event.get("guild_id")
                    bot_id = event.get("bot_id", "")
                    attachments = event.get("attachments") or []

                    if message_id and _already_seen_discord(message_id):
                        LOGGER.debug(
                            f"{DEFAULT_ICON} Skipping duplicate Discord message {message_id}",
                        )
                        ack_now()
                        return

                    medium_for_blacklist = (
                        Medium.DISCORD_CHANNEL_MESSAGE
                        if is_channel
                        else Medium.DISCORD_MESSAGE
                    )

                    if _is_blacklisted(medium_for_blacklist, sender_discord_id):
                        LOGGER.debug(
                            f"{DEFAULT_ICON} Ignoring blacklisted Discord from: {sender_discord_id}",
                        )
                        ack_now()
                        return

                    contact = next(
                        (
                            c
                            for c in contacts
                            if c.get("discord_id") == sender_discord_id
                        ),
                        None,
                    )
                    is_new_unknown = False
                    if contact is None:
                        contact = _get_or_create_unknown_contact(
                            medium_for_blacklist,
                            sender_discord_id,
                        )
                        is_new_unknown = contact is not None

                    if contact is None:
                        LOGGER.error(
                            f"{DEFAULT_ICON} Failed to resolve contact for Discord from: {sender_discord_id}",
                        )
                        ack_now()
                        return

                    if is_channel:
                        discord_event = DiscordChannelMessageReceived(
                            contact=contact,
                            content=content,
                            channel_id=channel_id,
                            guild_id=guild_id or "",
                            bot_id=bot_id,
                            message_id=message_id,
                            attachments=attachments,
                        )
                        await publish(
                            "app:comms:discord_channel_message",
                            discord_event.to_json(),
                        )
                    else:
                        await publish(
                            "app:comms:discord_message",
                            events_map[thread](
                                content=content,
                                contact=contact,
                                channel_id=channel_id,
                                bot_id=bot_id,
                                message_id=message_id,
                                attachments=attachments,
                            ).to_json(),
                        )

                    if attachments:
                        schedule(add_unify_message_attachments(attachments))

                    if is_new_unknown:
                        await publish(
                            "app:comms:unknown_contact_created",
                            UnknownContactCreated(
                                contact=contact,
                                medium=medium_for_blacklist,
                                message_preview=content[:100] if content else "",
                            ).to_json(),
                        )

                    ack_now()
                    return

                if thread == "slack":
                    sender_slack_user_id = event.get("sender_slack_user_id", "")
                    is_channel = event.get("is_channel", False)
                    team_id = event.get("team_id", "")
                    channel_id = event.get("channel_id", "")
                    bot_user_id = event.get("bot_user_id", "")
                    event_ts = event.get("event_ts", "")
                    thread_ts = event.get("thread_ts", "")
                    message_id = event.get("message_id", "")
                    attachments = event.get("attachments") or []
                    routing_metadata = event.get("routing_metadata") or {}

                    # Dedup on the stable message identity (client_msg_id/ts,
                    # carried as ``message_id``) so the app_mention + message
                    # pair for one channel mention collapses to a single
                    # processed event. Fall back to ``event_id`` only when no
                    # message id is present.
                    dedup_key = message_id or event.get("event_id", "")
                    if dedup_key and _already_seen_slack(dedup_key):
                        LOGGER.debug(
                            f"{DEFAULT_ICON} Skipping duplicate Slack message {dedup_key}",
                        )
                        ack_now()
                        return

                    medium_for_blacklist = (
                        Medium.SLACK_CHANNEL_MESSAGE
                        if is_channel
                        else Medium.SLACK_MESSAGE
                    )

                    if _is_blacklisted(medium_for_blacklist, sender_slack_user_id):
                        LOGGER.debug(
                            f"{DEFAULT_ICON} Ignoring blacklisted Slack from: {sender_slack_user_id}",
                        )
                        ack_now()
                        return

                    contact = next(
                        (
                            c
                            for c in contacts
                            if c.get("slack_user_id") == sender_slack_user_id
                        ),
                        None,
                    )
                    is_new_unknown = False

                    # First inbound from this Slack user: look up their Slack
                    # profile and match an existing contact by email (then
                    # name), persisting slack_user_id so subsequent messages
                    # match directly. Breaks the bootstrap deadlock where a
                    # contact's slack_user_id is only ever set after a message
                    # is processed.
                    profile: dict = {}
                    if contact is None:
                        profile = await resolve_slack_user_profile(
                            team_id=team_id,
                            slack_user_id=sender_slack_user_id,
                        )
                        email = (profile.get("email") or "").strip().lower()
                        matched = None
                        if email:
                            matched = next(
                                (
                                    c
                                    for c in contacts
                                    if (c.get("email_address") or "").lower() == email
                                ),
                                None,
                            )
                        if matched is None:
                            for nm in (
                                profile.get("real_name"),
                                profile.get("display_name"),
                            ):
                                matched = _match_contact_by_name(nm or "", contacts)
                                if matched is not None:
                                    break
                        if matched is not None:
                            _persist_slack_user_id(
                                matched["contact_id"],
                                sender_slack_user_id,
                            )
                            matched["slack_user_id"] = sender_slack_user_id
                            contact = matched

                    # Still unresolved: an explicit ``@app`` mention is a clear
                    # intent to converse, so create a respondable contact keyed
                    # by the Slack user id. Otherwise fall back to the gated,
                    # silent unknown-contact policy.
                    if contact is None:
                        if routing_metadata.get("reason") == "token_addressed":
                            contact = _create_slack_contact(
                                sender_slack_user_id,
                                profile,
                            )
                        else:
                            contact = _get_or_create_unknown_contact(
                                medium_for_blacklist,
                                sender_slack_user_id,
                            )
                        is_new_unknown = contact is not None

                    if contact is None:
                        LOGGER.error(
                            f"{DEFAULT_ICON} Failed to resolve contact for Slack from: {sender_slack_user_id}",
                        )
                        ack_now()
                        return

                    common_kwargs = dict(
                        contact=contact,
                        content=content,
                        team_id=team_id,
                        channel_id=channel_id,
                        bot_user_id=bot_user_id,
                        event_ts=event_ts,
                        thread_ts=thread_ts,
                        message_id=message_id,
                        attachments=attachments,
                        routing_metadata=routing_metadata,
                    )

                    if is_channel:
                        slack_event = SlackChannelMessageReceived(**common_kwargs)
                        await publish(
                            "app:comms:slack_channel_message",
                            slack_event.to_json(),
                        )
                    else:
                        await publish(
                            "app:comms:slack_message",
                            events_map[thread](**common_kwargs).to_json(),
                        )

                    if attachments:
                        schedule(add_unify_message_attachments(attachments))

                    if is_new_unknown:
                        await publish(
                            "app:comms:unknown_contact_created",
                            UnknownContactCreated(
                                contact=contact,
                                medium=medium_for_blacklist,
                                message_preview=content[:100] if content else "",
                            ).to_json(),
                        )

                    ack_now()
                    return

                if thread in ("teams_chat", "teams_channel"):
                    sender_email = event.get("sender", "")
                    message_id = event.get("message_id", "")
                    is_channel = thread == "teams_channel"
                    attachments = event.get("attachments") or []

                    if message_id and _already_seen_teams(message_id):
                        LOGGER.debug(
                            f"{DEFAULT_ICON} Skipping duplicate Teams message {message_id}",
                        )
                        ack_now()
                        return

                    medium_for_blacklist = (
                        Medium.TEAMS_CHANNEL_MESSAGE
                        if is_channel
                        else Medium.TEAMS_MESSAGE
                    )

                    if _is_blacklisted(medium_for_blacklist, sender_email):
                        LOGGER.debug(
                            f"{DEFAULT_ICON} Ignoring blacklisted Teams from: {sender_email}",
                        )
                        ack_now()
                        return

                    contact = next(
                        (c for c in contacts if c.get("email_address") == sender_email),
                        None,
                    )
                    is_new_unknown = False
                    if contact is None:
                        contact = _get_or_create_unknown_contact(
                            medium_for_blacklist,
                            sender_email,
                        )
                        is_new_unknown = contact is not None

                    if contact is None:
                        LOGGER.error(
                            f"{DEFAULT_ICON} Failed to resolve contact for Teams from: {sender_email}",
                        )
                        ack_now()
                        return

                    if event.get("participants_incomplete"):
                        LOGGER.warning(
                            f"{DEFAULT_ICON} Teams participants incomplete "
                            f"(reason={event.get('participants_reason', 'ok')}, "
                            f"thread={thread})",
                        )

                    participants_list = _resolve_teams_participants(
                        raw_participants=event.get("participants") or [],
                        sender_email=sender_email,
                        sender_contact_id=contact.get("contact_id"),
                        medium=medium_for_blacklist,
                        unknown_contact_resolver=_get_or_create_unknown_contact,
                    )

                    if is_channel:
                        await publish(
                            "app:comms:teams_channel_message",
                            events_map[thread](
                                contact=contact,
                                content=content,
                                channel_id=event.get("channel_id", ""),
                                team_id=event.get("team_id", ""),
                                message_id=message_id,
                                is_reply=event.get("is_reply", False),
                                parent_message_id=event.get("parent_message_id"),
                                thread_id=event.get("thread_id", ""),
                                post_subject=event.get("post_subject"),
                                attachments=attachments,
                                participants=participants_list,
                            ).to_json(),
                        )
                    else:
                        await publish(
                            "app:comms:teams_message",
                            events_map[thread](
                                contact=contact,
                                content=content,
                                chat_id=event.get("chat_id", ""),
                                message_id=message_id,
                                chat_type=event.get("chat_type"),
                                chat_topic=event.get("chat_topic"),
                                attachments=attachments,
                                participants=participants_list,
                            ).to_json(),
                        )

                    if attachments:
                        schedule(add_unify_message_attachments(attachments))

                    if is_new_unknown:
                        await publish(
                            "app:comms:unknown_contact_created",
                            UnknownContactCreated(
                                contact=contact,
                                medium=medium_for_blacklist,
                                message_preview=content[:100] if content else "",
                            ).to_json(),
                        )

                    ack_now()
                    return

                if thread == "ms_teams_bot":
                    tenant_id = event.get("tenant_id", "")
                    conversation_id = event.get("conversation_id", "")
                    conversation_type = event.get("conversation_type", "personal")
                    channel_id = event.get("channel_id", "")
                    team_id = event.get("team_id", "")
                    thread_id = event.get("thread_id", "")
                    service_url = event.get("service_url", "")
                    bot_app_id = event.get("bot_app_id", "")
                    message_id = event.get("message_id", "")
                    sender_display_name = event.get("sender_display_name", "")
                    attachments = event.get("attachments") or []
                    routing_metadata = event.get("routing_metadata") or {}
                    sender_is_owner = bool(event.get("sender_is_owner"))

                    # A 1:1 chat is a DM; group chats and channels are shared
                    # conversations that route through the channel medium/tool
                    # (mirrors Slack's DM vs channel split).
                    is_channel = conversation_type in ("groupChat", "channel")

                    dedup_key = message_id or event.get("event_id", "")
                    if dedup_key and _already_seen_ms_teams_bot(dedup_key):
                        LOGGER.debug(
                            f"{DEFAULT_ICON} Skipping duplicate Teams bot activity {dedup_key}",
                        )
                        ack_now()
                        return

                    # Teams activities carry no durable contact key (no email;
                    # roster/Graph lookup is deferred), so a per-message display
                    # name is all we have. When Orchestra resolved the sender to
                    # the assistant's own owner, attribute the message to the
                    # durable boss contact rather than matching/minting by name
                    # — otherwise every inbound spawns a fresh contact and the
                    # boss's own DMs never resolve to contact 1. Otherwise match
                    # the AAD display name against known contacts (a stable name
                    # re-matches on later messages), then fall back to a
                    # respondable contact (the bot only receives 1:1 / @mention
                    # activities, i.e. clear intent to converse).
                    contact = None
                    if sender_is_owner:
                        contact = next(
                            (
                                c
                                for c in contacts
                                if c.get("contact_id")
                                == SESSION_DETAILS.boss_contact_id
                            ),
                            None,
                        )
                    if contact is None:
                        contact = _match_contact_by_name(sender_display_name, contacts)
                    if contact is None:
                        contact = _create_ms_teams_bot_contact(sender_display_name)

                    if contact is None:
                        LOGGER.error(
                            f"{DEFAULT_ICON} Failed to resolve contact for Teams bot "
                            f"from: {sender_display_name!r}",
                        )
                        ack_now()
                        return

                    if is_channel:
                        await publish(
                            "app:comms:ms_teams_bot_channel_message",
                            MsTeamsBotChannelMessageReceived(
                                contact=contact,
                                content=content,
                                tenant_id=tenant_id,
                                conversation_id=conversation_id,
                                conversation_type=conversation_type,
                                channel_id=channel_id,
                                team_id=team_id,
                                thread_id=thread_id,
                                service_url=service_url,
                                bot_app_id=bot_app_id,
                                message_id=message_id,
                                attachments=attachments,
                                routing_metadata=routing_metadata,
                            ).to_json(),
                        )
                    else:
                        await publish(
                            "app:comms:ms_teams_bot_message",
                            MsTeamsBotMessageReceived(
                                contact=contact,
                                content=content,
                                tenant_id=tenant_id,
                                conversation_id=conversation_id,
                                conversation_type=conversation_type,
                                channel_id=channel_id,
                                service_url=service_url,
                                bot_app_id=bot_app_id,
                                message_id=message_id,
                                is_channel=is_channel,
                                attachments=attachments,
                                routing_metadata=routing_metadata,
                            ).to_json(),
                        )

                    if attachments:
                        schedule(add_unify_message_attachments(attachments))

                    ack_now()
                    return

                contact_detail = event["from_number"].strip()
                medium_for_blacklist = Medium.SMS_MESSAGE

                if _is_blacklisted(medium_for_blacklist, contact_detail):
                    LOGGER.debug(
                        f"{DEFAULT_ICON} Ignoring blacklisted SMS from: {contact_detail}",
                    )
                    ack_now()
                    return

                contact = next(
                    (c for c in contacts if c["phone_number"] == contact_detail),
                    None,
                )
                is_new_unknown = False
                if contact is None:
                    contact = _lookup_known_contact(
                        medium_for_blacklist,
                        contact_detail,
                    )
                if contact is None:
                    contact = _get_or_create_unknown_contact(
                        medium_for_blacklist,
                        contact_detail,
                    )
                    is_new_unknown = contact is not None

                if contact is None:
                    LOGGER.error(
                        f"{DEFAULT_ICON} Failed to resolve contact for SMS from: {contact_detail}",
                    )
                    ack_now()
                    return

                await publish(
                    f"app:comms:{thread}_message",
                    events_map[thread](
                        content=content,
                        contact=contact,
                    ).to_json(),
                )

                if is_new_unknown:
                    await publish(
                        "app:comms:unknown_contact_created",
                        UnknownContactCreated(
                            contact=contact,
                            medium=medium_for_blacklist,
                            message_preview=content[:100] if content else "",
                        ).to_json(),
                    )

                ack_now()
                return

            elif thread == "log_pre_hire_chats":
                try:
                    assistant_id = event.get("assistant_id", "")
                    body = event.get("body", []) or []

                    published = 0
                    for item in body:
                        try:
                            msg_content = item.get("msg", "")
                            if not isinstance(msg_content, str):
                                msg_content = str(msg_content)

                            await publish(
                                "app:comms:pre_hire",
                                PreHireMessage(
                                    content=msg_content,
                                    role=item.get("role"),
                                    exchange_id=UNASSIGNED,
                                ).to_json(),
                            )
                            published += 1
                        except Exception as inner_exc:
                            LOGGER.debug(
                                f"{DEFAULT_ICON} Skipping malformed pre-hire item: {inner_exc}",
                            )

                    LOGGER.debug(
                        f"{DEFAULT_ICON} Logged {published} pre-hire chat message(s) for assistant {assistant_id}",
                    )
                    ack_now()
                except Exception as exc:
                    LOGGER.error(
                        f"{DEFAULT_ICON} Error processing pre-hire logs: {exc}",
                    )
                    nack_now()
                return

            if thread == "recording_ready":
                await publish(
                    "app:comms:recording_ready",
                    RecordingReady(
                        conference_name=event.get("conference_name", ""),
                        recording_url=event.get("recording_url", ""),
                        call_session_id=event.get("call_session_id"),
                        provider_call_sid=event.get("provider_call_sid"),
                        room_name=event.get("room_name") or event.get("livekit_room"),
                    ).to_json(),
                )
                ack_now()
                return

            if "call" in thread or "meet" in thread:
                contacts = [*event.get("contacts", []), _get_local_contact()]
                await publish_blocking(
                    "app:comms:backup_contacts",
                    BackupContactsEvent(contacts=contacts).to_json(),
                )

                if thread == "unify_meet":
                    # Every meet dispatch carries the call session's roster;
                    # the primary contact is the first human on it.
                    participants = event.get("participants") or []
                    call_session_id = event.get("call_session_id")
                    if not call_session_id or not participants:
                        LOGGER.error(
                            f"{DEFAULT_ICON} Dropping unify_meet envelope without "
                            f"call session/roster (session={call_session_id!r}, "
                            f"participants={len(participants)})",
                        )
                        ack_now()
                        return
                    primary = None
                    for member in participants:
                        if (
                            isinstance(member, dict)
                            and member.get("kind") == "human"
                            and member.get("contact_id") is not None
                        ):
                            cid = int(member["contact_id"])
                            primary = next(
                                (c for c in contacts if c.get("contact_id") == cid),
                                None,
                            )
                            if primary is None:
                                primary = {
                                    "contact_id": cid,
                                    "first_name": member.get("display_name") or "",
                                    "surname": "",
                                    "is_system": True,
                                }
                            break
                    if primary is None:
                        LOGGER.error(
                            f"{DEFAULT_ICON} Dropping unify_meet envelope: roster "
                            "has no human with a contact_id",
                        )
                        ack_now()
                        return
                    call_event = UnifyMeetReceived(
                        contact=primary,
                        room_name=event.get("livekit_room"),
                        opening_config=event.get("opening_config"),
                        call_session_id=call_session_id,
                        participants=participants,
                    )
                    event_topic = "app:comms:unify_meet_received"
                elif thread == "whatsapp_call":
                    number = event.get("caller_number", event.get("user_number"))
                    if number and number.startswith("whatsapp:"):
                        number = number[len("whatsapp:") :]

                    if _is_blacklisted(Medium.WHATSAPP_MESSAGE, number):
                        LOGGER.debug(
                            f"{DEFAULT_ICON} Ignoring blacklisted WhatsApp call from: {number}",
                        )
                        ack_now()
                        return

                    contact = next(
                        (c for c in contacts if c.get("whatsapp_number") == number),
                        None,
                    )
                    is_new_unknown = False
                    if contact is None:
                        contact = _lookup_known_contact(
                            Medium.WHATSAPP_CALL,
                            number,
                        )
                    if contact is None:
                        contact = _get_or_create_unknown_contact(
                            Medium.WHATSAPP_CALL,
                            number,
                        )
                        is_new_unknown = contact is not None

                    if contact is None:
                        LOGGER.error(
                            f"{DEFAULT_ICON} Failed to resolve contact for WhatsApp call from: {number}",
                        )
                        ack_now()
                        return

                    call_event = WhatsAppCallReceived(
                        contact=contact,
                        conference_name=event.get("conference_name", ""),
                        room_name=event.get("livekit_room"),
                        call_session_id=event.get("call_session_id"),
                        provider_call_sid=event.get("provider_call_sid"),
                    )
                    event_topic = "app:comms:whatsapp_call_received"

                    if is_new_unknown:
                        await publish(
                            "app:comms:unknown_contact_created",
                            UnknownContactCreated(
                                contact=contact,
                                medium=Medium.WHATSAPP_CALL,
                                message_preview="Incoming WhatsApp call",
                            ).to_json(),
                        )
                elif thread == "call":
                    number = event.get("caller_number", event.get("user_number"))

                    if _is_blacklisted(Medium.PHONE_CALL, number):
                        LOGGER.debug(
                            f"{DEFAULT_ICON} Ignoring blacklisted call from: {number}",
                        )
                        ack_now()
                        return

                    contact = next(
                        (c for c in contacts if c["phone_number"] == number),
                        None,
                    )
                    is_new_unknown = False
                    if contact is None:
                        contact = _lookup_known_contact(
                            Medium.PHONE_CALL,
                            number,
                        )
                    if contact is None:
                        contact = _get_or_create_unknown_contact(
                            Medium.PHONE_CALL,
                            number,
                        )
                        is_new_unknown = contact is not None

                    if contact is None:
                        LOGGER.error(
                            f"{DEFAULT_ICON} Failed to resolve contact for call from: {number}",
                        )
                        ack_now()
                        return

                    call_event = PhoneCallReceived(
                        contact=contact,
                        conference_name=event.get("conference_name", ""),
                        room_name=event.get("livekit_room"),
                        call_session_id=event.get("call_session_id"),
                        provider_call_sid=event.get("provider_call_sid"),
                    )
                    event_topic = "app:comms:call_received"

                    if is_new_unknown:
                        await publish(
                            "app:comms:unknown_contact_created",
                            UnknownContactCreated(
                                contact=contact,
                                medium=Medium.PHONE_CALL,
                                message_preview="Incoming phone call",
                            ).to_json(),
                        )
                elif thread == "whatsapp_call_sent":
                    number = event.get("user_number")
                    if number and number.startswith("whatsapp:"):
                        number = number[len("whatsapp:") :]
                    contact = next(
                        (c for c in contacts if c.get("whatsapp_number") == number),
                        None,
                    )
                    if contact is None:
                        contact = event.get("contact")
                    if contact is None:
                        contact = next(
                            (c for c in contacts if c.get("phone_number") == number),
                            None,
                        )
                    if contact is None:
                        contact = next(c for c in contacts if c["contact_id"] == 1)
                    call_event = WhatsAppCallSent(contact=contact)
                    event_topic = "app:comms:whatsapp_call_sent"
                elif thread == "whatsapp_call_answered":
                    number = event.get("user_number")
                    if number and number.startswith("whatsapp:"):
                        number = number[len("whatsapp:") :]
                    contact = next(
                        (c for c in contacts if c.get("whatsapp_number") == number),
                        None,
                    )
                    if contact is None:
                        contact = next(
                            (c for c in contacts if c.get("phone_number") == number),
                            None,
                        )
                    if contact is None:
                        contact = next(c for c in contacts if c["contact_id"] == 1)
                    call_event = WhatsAppCallAnswered(contact=contact)
                    event_topic = "app:comms:whatsapp_call_answered"
                elif thread == "whatsapp_call_not_answered":
                    number = event.get("user_number")
                    if number and number.startswith("whatsapp:"):
                        number = number[len("whatsapp:") :]
                    call_status = event.get("call_status", "no-answer")
                    contact = next(
                        (c for c in contacts if c.get("whatsapp_number") == number),
                        None,
                    )
                    if contact is None:
                        contact = next(
                            (c for c in contacts if c.get("phone_number") == number),
                            None,
                        )
                    if contact is None:
                        contact = next(c for c in contacts if c["contact_id"] == 1)
                    call_event = WhatsAppCallNotAnswered(
                        contact=contact,
                        reason=event.get("call_status", "no-answer"),
                    )
                    event_topic = "app:comms:whatsapp_call_not_answered"
                elif thread == "call_not_answered":
                    number = event.get("user_number")
                    contact = next(
                        (c for c in contacts if c["phone_number"] == number),
                        None,
                    )
                    if contact is None:
                        contact = next(c for c in contacts if c["contact_id"] == 1)
                    call_event = PhoneCallNotAnswered(
                        contact=contact,
                        reason=event.get("call_status", "no-answer"),
                    )
                    event_topic = "app:comms:call_not_answered"
                elif thread == "call_answered":
                    number = event.get("user_number")
                    contact = next(
                        (c for c in contacts if c["phone_number"] == number),
                        None,
                    )
                    if contact is None:
                        contact = next(c for c in contacts if c["contact_id"] == 1)
                    call_event = PhoneCallAnswered(contact=contact)
                    event_topic = "app:comms:call_answered"
                else:
                    LOGGER.warning(
                        f"{DEFAULT_ICON} Unhandled call/meet thread: {thread}",
                    )
                    ack_now()
                    return

                await publish_blocking(event_topic, call_event.to_json())
                ack_now()
                return

            if thread in (
                "unify_message_outbound",
                "unify_message_reaction_outbound",
                "chat_message",
                "chat_reaction",
                "system_error",
                "assistant_desktop_ready",
                "comms_activity",
                "action_event",
            ):
                ack_now()
                return

            LOGGER.error(f"{DEFAULT_ICON} Unknown event type: {thread}")
            ack_now()
        except Exception as exc:
            LOGGER.error(f"{DEFAULT_ICON} Error processing message: {exc}")
            publish_system_error(
                "An internal error occurred while processing a message. "
                "The assistant may not have received your last message.",
                error_type="message_failed",
            )
            ack_now()

    def handle_message(
        self,
        message: Any,
        subscription_id: str = "",
    ):
        """
        Handle incoming messages from PubSub subscriptions.

        NOTE: This method is called from a GCP PubSub thread pool thread,
        NOT from the asyncio event loop. It decodes the Pub/Sub payload and
        schedules the shared async envelope dispatcher on the main loop.
        """
        topic = subscription_id.removesuffix("-sub")
        try:
            payload = json.loads(message.data.decode("utf-8"))
            thread = payload["thread"]
            LOGGER.debug(
                f"{DEFAULT_ICON} Received message from {thread}: {message.data.decode('utf-8')}",
            )
            future = asyncio.run_coroutine_threadsafe(
                self.dispatch_envelope_payload(
                    payload,
                    source_topic=topic,
                    ack=message.ack,
                    nack=message.nack,
                ),
                self.loop,
            )
            future.add_done_callback(self._log_dispatch_future)
            if "call" in thread or "meet" in thread:
                future.result()
        except Exception as e:
            LOGGER.error(f"{DEFAULT_ICON} Error processing message: {e}")
            publish_system_error(
                "An internal error occurred while processing a message. "
                "The assistant may not have received your last message.",
                error_type="message_failed",
            )
            message.ack()

    async def _start_inbound_subscription(self) -> None:
        """Start inbound envelope delivery for the current assistant.

        Resolution order:

        1. If ``ingress_transport_factory`` was provided, it is called
           now (after ``_poll_for_assignment`` has set ``agent_id``) to
           materialize a transport. Factory may return ``None`` to opt
           out, in which case the legacy path is used.
        2. Otherwise, if ``ingress_transport`` was provided directly at
           construction time, it is used.
        3. Otherwise the legacy inline Pub/Sub subscriber is started
           via ``subscribe_to_topic``.

        All three paths produce the same observable behaviour against
        ``event_broker``.
        """
        transport = self.ingress_transport
        if self.ingress_transport_factory is not None:
            transport = self.ingress_transport_factory()
            # Cache the factory-materialized transport so _stop can reach it.
            self.ingress_transport = transport
        if transport is not None:
            await transport.start(self.dispatch_envelope_payload)
            return
        self.subscribe_to_topic(_get_subscription_id(), max_messages=10)

    async def _stop_inbound_subscription(self) -> None:
        """Tear down inbound envelope delivery on shutdown.

        Stops the injected transport (if present) and cancels any
        streaming-pull futures created by the legacy inline path.
        Idempotent against both paths.
        """
        if self.ingress_transport is not None:
            try:
                await self.ingress_transport.stop()
            except Exception as exc:
                LOGGER.warning(
                    f"{ICONS['lifecycle']} ingress_transport.stop raised: {exc}",
                )
        for future in self.subscribers.values():
            try:
                future.cancel()
            except Exception as exc:
                LOGGER.warning(
                    f"{ICONS['lifecycle']} subscriber future.cancel raised: {exc}",
                )

    def subscribe_to_topic(self, subscription_id: str, max_messages: int | None = None):
        """Subscribe to a specific PubSub topic and process messages."""
        if pubsub_v1 is None:
            LOGGER.error(
                f"{ICONS['subscription']} Google Pub/Sub client is unavailable; "
                "hosted subscriptions are disabled in this environment.",
            )
            return
        if not SETTINGS.GCP_PROJECT_ID:
            LOGGER.error(
                f"{ICONS['subscription']} GCP_PROJECT_ID is not set — "
                f"cannot subscribe to Pub/Sub. Set the GCP_PROJECT_ID environment variable.",
            )
            return
        try:
            # Let GCP libraries handle authentication automatically
            if self.credentials:
                subscriber = pubsub_v1.SubscriberClient(credentials=self.credentials)
            else:
                subscriber = pubsub_v1.SubscriberClient()
            subscription_path = subscriber.subscription_path(
                SETTINGS.GCP_PROJECT_ID,
                subscription_id,
            )

            LOGGER.debug(
                f"{ICONS['subscription']} Starting subscription to {subscription_path} (max_messages={max_messages})",
            )

            flow_control = (
                pubsub_v1.types.FlowControl(max_messages=max_messages)
                if max_messages
                else pubsub_v1.types.FlowControl()
            )

            callback = partial(self.handle_message, subscription_id=subscription_id)
            streaming_pull_future = subscriber.subscribe(
                subscription_path,
                callback=callback,
                flow_control=flow_control,
            )

            # Store the future for cleanup
            self.subscribers[subscription_id] = streaming_pull_future
            LOGGER.info(
                f"{ICONS['subscription']} Subscription active: {subscription_path} "
                f"(max_messages={max_messages})",
            )

        except Exception as e:
            LOGGER.error(
                f"{ICONS['subscription']} Error setting up subscription {subscription_id}: {e}",
            )

    async def _poll_for_assignment(self):
        """Wait for cluster-owned AssistantSession assignment.

        The session controller writes a session reference onto the real Job.
        Unity watches for that reference, reads the AssistantSession plus its
        bootstrap Secret, and emits the same StartupEvent path the existing
        ConversationManager already handles.
        """
        job_name = SETTINGS.conversation.JOB_NAME

        if not job_name:
            LOGGER.error(
                f"{DEFAULT_ICON} Cannot poll for assignment: "
                f"JOB_NAME not configured",
            )
            return

        LOGGER.debug(
            f"{DEFAULT_ICON} Waiting for AssistantSession assignment on {job_name}",
        )

        attempt = 0
        while True:
            attempt += 1
            try:
                LOGGER.info(
                    f"{DEFAULT_ICON} Assignment poll attempt {attempt} for {job_name}",
                )
                session_name = await asyncio.to_thread(
                    wait_for_assistant_session_name,
                    job_name,
                )
                LOGGER.info(
                    f"{DEFAULT_ICON} Assignment session discovered for {job_name}: "
                    f"{session_name}",
                )
                job_assignment = await asyncio.to_thread(
                    read_job_assignment_record,
                    job_name,
                )
                if job_assignment.session_name != session_name:
                    LOGGER.info(
                        f"{DEFAULT_ICON} Ignoring assignment on {job_name}: "
                        f"job now points at {job_assignment.session_name or 'no-session'} "
                        f"instead of {session_name}",
                    )
                    await asyncio.sleep(5)
                    continue
                if not job_assignment.binding_id:
                    LOGGER.info(
                        f"{DEFAULT_ICON} Waiting for binding identity on {job_name} "
                        f"before bootstrapping {session_name}",
                    )
                    await asyncio.sleep(5)
                    continue
                session = await asyncio.to_thread(read_assistant_session, session_name)
                session_spec = session.get("spec") or {}
                session_status = session.get("status") or {}
                session_binding_id = str(
                    ((session_status.get("binding") or {}).get("id") or ""),
                )
                activation_id = str(session_spec.get("activationId", "") or "")
                secret_name = str(session_spec.get("startupSecretRef", "") or "")
                if not secret_name:
                    raise RuntimeError(
                        f"AssistantSession {session_name} missing startupSecretRef",
                    )
                if not activation_id:
                    LOGGER.info(
                        f"{DEFAULT_ICON} Waiting for activation ownership on "
                        f"{session_name} before bootstrapping {job_name}",
                    )
                    await asyncio.sleep(5)
                    continue
                if not session_binding_id:
                    LOGGER.info(
                        f"{DEFAULT_ICON} Waiting for current binding on "
                        f"{session_name} before bootstrapping {job_name}",
                    )
                    await asyncio.sleep(5)
                    continue
                if job_assignment.binding_id != session_binding_id:
                    LOGGER.info(
                        "%s Ignoring stale assignment on %r: binding mismatch",
                        DEFAULT_ICON,
                        job_name,
                    )
                    await asyncio.sleep(5)
                    continue
                LOGGER.info(
                    "%s Assignment session loaded: phase=%r secret_present=%s "
                    "binding_present=%s",
                    DEFAULT_ICON,
                    session_status.get("phase") or "",
                    bool(secret_name),
                    bool(session_binding_id),
                )

                secret_record = await asyncio.to_thread(
                    read_session_bootstrap_secret_record,
                    secret_name,
                )
                event = secret_record.payload
                if not event:
                    raise RuntimeError(
                        f"AssistantSession bootstrap secret {secret_name} is empty",
                    )
                if secret_record.owner_session_name != session_name:
                    LOGGER.info(
                        f"{DEFAULT_ICON} Ignoring stale bootstrap Secret "
                        f"{secret_record.name} on {job_name}: owner session "
                        f"{secret_record.owner_session_name or 'missing'} != {session_name}",
                    )
                    await asyncio.sleep(5)
                    continue
                if secret_record.owner_activation_id != activation_id:
                    LOGGER.info(
                        f"{DEFAULT_ICON} Ignoring stale bootstrap Secret "
                        f"{secret_record.name} on {job_name}: owner activation "
                        f"{secret_record.owner_activation_id or 'missing'} != {activation_id}",
                    )
                    await asyncio.sleep(5)
                    continue
                expected_assistant_id = str(session_spec.get("assistantId", "") or "")
                event_assistant_id = str(event.get("assistant_id", "") or "")
                if (
                    expected_assistant_id
                    and event_assistant_id
                    and event_assistant_id != expected_assistant_id
                ):
                    LOGGER.info(
                        f"{DEFAULT_ICON} Ignoring bootstrap Secret {secret_record.name} "
                        f"on {job_name}: payload assistant {event_assistant_id} != "
                        f"session assistant {expected_assistant_id}",
                    )
                    await asyncio.sleep(5)
                    continue
                LOGGER.info(
                    f"{DEFAULT_ICON} Bootstrap secret read for {job_name}: "
                    f"assistant_id={event.get('assistant_id')} medium={event.get('medium')}",
                )

                LOGGER.debug(
                    f"{DEFAULT_ICON} Assignment detected for assistant "
                    f"{event.get('assistant_id')} via {session_name} on {job_name}",
                )

                SESSION_DETAILS.assistant.agent_id = int(event["assistant_id"])
                await self._start_inbound_subscription()
                LOGGER.info(
                    f"{DEFAULT_ICON} Assistant inbound subscription established for "
                    f"{job_name}: {_get_subscription_id()}",
                )

                details = {
                    "api_key": event["api_key"],
                    "binding_id": session_binding_id,
                    "medium": event.get("medium", "startup"),
                    "assistant_id": event["assistant_id"],
                    "user_id": event["user_id"],
                    "assistant_first_name": event["assistant_first_name"],
                    "assistant_surname": event["assistant_surname"],
                    "assistant_age": event["assistant_age"],
                    "assistant_nationality": event["assistant_nationality"],
                    "assistant_timezone": event.get("assistant_timezone", ""),
                    "assistant_about": event["assistant_about"],
                    "assistant_job_title": event.get("assistant_job_title", ""),
                    "assistant_number": event["assistant_number"],
                    "assistant_email": event["assistant_email"],
                    "assistant_email_provider": event.get(
                        "assistant_email_provider",
                        "google_workspace",
                    ),
                    "self_contact_id": _required_contact_id(
                        event,
                        "self_contact_id",
                    ),
                    "boss_contact_id": _required_contact_id(
                        event,
                        "boss_contact_id",
                    ),
                    "assistant_whatsapp_number": event.get(
                        "assistant_whatsapp_number",
                        "",
                    ),
                    "assistant_discord_bot_id": event.get(
                        "assistant_discord_bot_id",
                        "",
                    ),
                    "assistant_slack_bot_user_id": event.get(
                        "assistant_slack_bot_user_id",
                        "",
                    ),
                    "assistant_slack_team_id": event.get(
                        "assistant_slack_team_id",
                        "",
                    ),
                    "assistant_has_ms_teams_bot": event.get(
                        "assistant_has_ms_teams_bot",
                        SESSION_DETAILS.assistant.has_ms_teams_bot,
                    ),
                    "user_first_name": event["user_first_name"],
                    "user_surname": event["user_surname"],
                    "user_number": event["user_number"],
                    "user_email": event["user_email"],
                    "user_whatsapp_number": event.get("user_whatsapp_number", ""),
                    "voice_provider": event["voice_provider"],
                    "voice_id": event["voice_id"],
                    "default_model": event.get("default_model", ""),
                    "default_reasoning_effort": event.get(
                        "default_reasoning_effort",
                        "",
                    ),
                    "slow_brain_model": event.get("slow_brain_model", ""),
                    "slow_brain_reasoning_effort": event.get(
                        "slow_brain_reasoning_effort",
                        "",
                    ),
                    "desktop_mode": event.get("desktop_mode", "ubuntu"),
                    "user_desktops": event.get("user_desktops") or [],
                    "org_id": event.get("org_id"),
                    "org_name": event.get("org_name", ""),
                    "team_ids": event.get("team_ids") or [],
                    "team_summaries": event.get("team_summaries") or [],
                    "is_coordinator": event.get("is_coordinator", False),
                    "wake_reasons": event.get("wake_reasons") or [],
                }

                await self.event_broker.publish(
                    "app:comms:startup",
                    StartupEvent(**details).to_json(),
                )
                LOGGER.info(
                    f"{DEFAULT_ICON} StartupEvent published for assistant "
                    f"{event.get('assistant_id')} on {job_name}",
                )
                await asyncio.to_thread(mark_job_container_ready, job_name)
                LOGGER.info(
                    f"{DEFAULT_ICON} Container-ready signalled for {job_name}",
                )
                return
            except Exception as e:
                LOGGER.exception(
                    f"{DEFAULT_ICON} AssistantSession discovery failed for {job_name} "
                    f"on attempt {attempt}: {e}",
                )
                await asyncio.sleep(5)

    async def start(self):
        """Start all subscriptions and maintain connection to event manager."""
        if SESSION_DETAILS.assistant.agent_id is None:
            asyncio.create_task(self._poll_for_assignment())
            asyncio.create_task(self.send_pings())
        else:
            await self._start_inbound_subscription()

        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            LOGGER.debug(f"{ICONS['lifecycle']} Shutting down...")
            await self._stop_inbound_subscription()

    async def send_pings(self):
        """Send periodic pings to keep the event manager alive while waiting for startup."""
        LOGGER.debug(
            f"{ICONS['subscription']} Starting ping mechanism for idle container...",
        )
        while True:
            try:
                # Send ping to event manager (direct await since we're in async context)
                await self.event_broker.publish(
                    "app:comms:ping",
                    Ping(kind="keepalive").to_json(),
                )

                # Wait 30 seconds before next ping (pre-startup keepalive)
                await asyncio.sleep(30)

                # Check if we've received a startup message (indicated by assistant_id changed)
                if SESSION_DETAILS.assistant.agent_id is not None:
                    LOGGER.debug(
                        f"{ICONS['subscription']} Startup received, stopping ping mechanism",
                    )
                    break

            except Exception as e:
                LOGGER.error(f"{ICONS['subscription']} Error in ping mechanism: {e}")
                await asyncio.sleep(30)  # Continue trying


async def main():
    """Main entry point for the communication manager application."""
    from unify.conversation_manager.event_broker import get_event_broker

    event_broker = get_event_broker()
    manager = CommsManager(event_broker)
    await manager.start()


if __name__ == "__main__":
    asyncio.run(main(), debug=SETTINGS.UNITY_ASYNCIO_DEBUG)
