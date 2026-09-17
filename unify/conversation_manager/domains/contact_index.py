"""
ContactIndex: Conversation state management for ConversationManager.

All messages are stored in a single shared global deque. Per-contact views
are derived on demand. Contact information (name, email, response_policy,
etc.) is fetched from ContactManager, which is the single source of truth.
"""

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from unify.common.prompt_helpers import now as prompt_now
from unify.conversation_manager.cm_types import Medium

if TYPE_CHECKING:
    from unify.contact_manager.base import BaseContactManager


class CommsMessage:
    """Base class for actual communications with contacts.

    Message types representing real user<->assistant communications inherit
    from this class. Use isinstance(msg, CommsMessage) to distinguish actual
    communications from internal orchestration messages (like GuidanceMessage).
    """


@dataclass
class UnifyMessage(CommsMessage):
    """A message from the in-app chat, optionally with attachments.

    Each attachment is a dict with keys: filename, filepath, content_type,
    size_bytes.
    """

    name: str
    content: str
    timestamp: datetime
    role: str  # "user" or "assistant"
    attachments: list[dict] = field(default_factory=list)


@dataclass
class GuidanceMessage:
    """Internal orchestration message (not an actual communication).

    Carries guidance injected into a thread by the assistant's own components.
    These should NOT appear in transcripts shown to external systems or used
    for communication context.
    """

    name: str
    content: str
    timestamp: datetime


@dataclass
class GlobalThreadEntry:
    """An entry in the shared global thread.

    Wraps a message with its contact associations and medium, enabling
    per-contact views to be derived from the single deque.
    """

    message: UnifyMessage | GuidanceMessage
    medium: Medium
    # The contacts this entry belongs to. Values are reserved for a
    # per-contact role annotation and are None for chat messages.
    contact_roles: dict[int, str | None]


@dataclass
class ConversationState:
    """Per-contact conversation metadata (not message storage).

    Messages live in the shared global deque on ContactIndex.
    """

    contact_id: int


class ContactIndex:
    """
    Manages conversation state for active contacts.

    All messages are stored in a single shared global deque. Per-contact
    views are derived on demand via helper methods.

    Contact information (name, email, response_policy, etc.) is ALWAYS
    fetched from ContactManager - the single source of truth with DataStore-backed
    caching.

    Fallback Mechanism:
    -------------------
    Before ContactManager is initialized, inbound messages may arrive with contact
    data. These contacts are cached in `_fallback_contacts` so get_contact() can
    return them. Once ContactManager is set, the fallback cache is cleared and all
    lookups go through ContactManager.
    """

    DEFAULT_GLOBAL_THREAD_SIZE = 100

    def __init__(self, global_thread_size: int = DEFAULT_GLOBAL_THREAD_SIZE):
        self.active_conversations: dict[int, ConversationState] = {}
        self.global_thread: deque[GlobalThreadEntry] = deque(
            maxlen=global_thread_size,
        )
        self._contact_manager: "BaseContactManager | None" = None
        # Fallback cache for contacts before ContactManager is initialized
        self._fallback_contacts: dict[int, dict] = {}

    def set_contact_manager(self, contact_manager: "BaseContactManager") -> None:
        """Set the ContactManager to use as the source of truth for contact data.

        Note: We do NOT clear the fallback cache here. Contacts cached from inbounds
        that arrived before initialization should remain available until they can be
        looked up in ContactManager. The fallback cache is checked first in get_contact().
        """
        self._contact_manager = contact_manager

    @property
    def is_contact_manager_initialized(self) -> bool:
        """Check if ContactManager has been set."""
        return self._contact_manager is not None

    @property
    def contact_manager(self) -> "BaseContactManager":
        """Get the ContactManager. Raises if not set."""
        if self._contact_manager is None:
            raise RuntimeError("ContactManager not set on ContactIndex")
        return self._contact_manager

    def set_fallback_contacts(self, contacts: list[dict]) -> None:
        """
        Cache contacts from inbound messages.

        This is called when inbound messages arrive with contact data. These
        contacts are checked first in get_contact() before ContactManager,
        ensuring contacts from recent inbounds are always available even if
        ContactManager hasn't synced them yet.

        Args:
            contacts: List of contact dicts from inbound message events.
        """
        for contact in contacts:
            contact_id = contact.get("contact_id")
            if contact_id is not None:
                self._fallback_contacts[contact_id] = contact

    def clear_conversations(self):
        """Clear all active conversations for test isolation."""
        self.active_conversations.clear()
        self.global_thread.clear()

    def get_conversation_state(self, contact_id: int) -> ConversationState | None:
        """Get conversation state for a contact, or None if no active conversation."""
        return self.active_conversations.get(contact_id)

    def get_or_create_conversation(self, contact_id: int) -> ConversationState:
        """Get or create conversation state for a contact."""
        if contact_id not in self.active_conversations:
            self.active_conversations[contact_id] = ConversationState(
                contact_id=contact_id,
            )
        return self.active_conversations[contact_id]

    def get_contact(self, contact_id: int | None = None) -> dict | None:
        """
        Get contact information from fallback cache or ContactManager.

        Checks the local fallback cache first (populated from inbound message
        events). If not found, falls back to ContactManager.

        Args:
            contact_id: Contact ID.

        Returns:
            Contact dict or None if not found.
        """
        if contact_id is None:
            return None
        if self._contact_manager is None:
            return self._fallback_contacts.get(contact_id)
        try:
            result = self._contact_manager.get_contact_info(contact_id)
            return result.get(contact_id)
        except Exception:
            return None

    # =========================================================================
    # Message query helpers — derive views from the shared global deque
    # =========================================================================

    def get_messages_for_contact(self, contact_id: int) -> list:
        """Get messages for a contact.

        Args:
            contact_id: The contact to filter for.

        Returns:
            List of messages (in chronological order) for this contact.
        """
        return [
            entry.message
            for entry in self.global_thread
            if contact_id in entry.contact_roles
        ]

    def get_active_contact_ids(self) -> set[int]:
        """Return the set of contact_ids present in the global thread."""
        ids: set[int] = set()
        for entry in self.global_thread:
            ids.update(entry.contact_roles.keys())
        return ids

    def get_messages_grouped_by_contact(
        self,
    ) -> dict[int, list[GlobalThreadEntry]]:
        """Group all global thread entries by contact_id.

        Returns a dict mapping contact_id to a list of GlobalThreadEntry
        in chronological order. An entry appears under every contact_id
        in its contact_roles.
        """
        groups: dict[int, list[GlobalThreadEntry]] = {}
        for entry in self.global_thread:
            for cid in entry.contact_roles:
                if cid not in groups:
                    groups[cid] = []
                groups[cid].append(entry)
        return groups

    # =========================================================================
    # Message push
    # =========================================================================

    def build_message(
        self,
        contact_id: int,
        sender_name: str,
        message_content: str | None = None,
        attachments: list[dict] | None = None,
        timestamp: datetime | None = None,
        role: str = "user",
    ) -> "GlobalThreadEntry":
        """
        Build a GlobalThreadEntry without appending it to the global thread.

        Accepts the same arguments as push_message. Also ensures that
        conversation state exists for the contact.
        """
        if not timestamp:
            timestamp = prompt_now(as_string=False)

        # Ensure conversation state exists for this contact
        self.get_or_create_conversation(contact_id)

        # Determine display name (for rendering to brain)
        name = sender_name if role == "user" else "You" if role == "assistant" else role

        # Non-comms roles (e.g., "guidance") get a GuidanceMessage
        if role not in ("user", "assistant"):
            message = GuidanceMessage(
                name=name,
                content=message_content or "",
                timestamp=timestamp,
            )
        else:
            message = UnifyMessage(
                name=name,
                content=message_content or "",
                timestamp=timestamp,
                role=role,
                attachments=attachments or [],
            )

        return GlobalThreadEntry(
            message=message,
            medium=Medium.UNIFY_MESSAGE,
            contact_roles={contact_id: None},
        )

    def push_message(
        self,
        contact_id: int,
        sender_name: str,
        message_content: str | None = None,
        attachments: list[dict] | None = None,
        timestamp: datetime | None = None,
        role: str = "user",
    ) -> GlobalThreadEntry:
        """Build a message, append it to the shared global thread and return it."""
        entry = self.build_message(
            contact_id=contact_id,
            sender_name=sender_name,
            message_content=message_content,
            attachments=attachments,
            timestamp=timestamp,
            role=role,
        )
        self.global_thread.append(entry)
        return entry

    def prepend_entries(self, entries: list) -> None:
        """Prepend entries to the front of the global thread.

        Used by hydration to insert historical messages before any messages
        that arrived during initialization. Respects the deque maxlen by
        keeping the most recent entries when the combined size exceeds it.
        """
        if not entries:
            return
        existing = list(self.global_thread)
        self.global_thread.clear()
        # extend respects maxlen, dropping oldest (leftmost) if over capacity
        self.global_thread.extend(entries + existing)
