"""
ChatHistory: the one conversation between the user and the assistant.

Messages live in an in-memory list and are mirrored to one store table,
``Chat/Messages``, so the conversation survives a restart. The table is
declared in ``ChatHistory.Config.required_contexts`` and provisioned through
``ContextRegistry`` like any manager's table.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime

from unify import db
from unify.common.context_registry import ContextRegistry, TableContext
from unify.common.prompt_helpers import now as prompt_now

CHAT_TABLE = "Chat/Messages"


@dataclass
class ChatMessage:
    """One chat message, from the user or the assistant.

    ``attachments`` are workspace paths of files that travelled with the
    message. ``row_id`` is the store row backing the message, ``None`` while
    the message is only in memory.
    """

    role: str  # "user" or "assistant"
    content: str
    timestamp: datetime
    attachments: list[str] = field(default_factory=list)
    row_id: int | None = None


class ChatHistory:
    """The chat as an ordered list of messages, persisted one row per message.

    Until ``bind`` provisions the table (which needs the runtime's store
    context), appended messages stay in memory; ``bind`` writes them through
    and ``load`` prepends whatever earlier sessions left in the table.
    """

    DEFAULT_MAX_MESSAGES = 100

    class Config:
        required_contexts = [
            TableContext(
                name=CHAT_TABLE,
                description="Every message exchanged in the in-app chat.",
                fields={
                    "role": "str",
                    "content": "str",
                    "timestamp": "datetime",
                    "attachments": "list",
                },
            ),
        ]

    def __init__(self, max_messages: int = DEFAULT_MAX_MESSAGES):
        self.max_messages = max_messages
        self.messages: list[ChatMessage] = []
        self._ctx: str | None = None
        # ``append`` runs on the event loop while ``load`` may splice from a
        # worker thread; both mutate ``messages`` under this lock.
        self._lock = threading.Lock()

    @property
    def is_bound(self) -> bool:
        return self._ctx is not None

    def bind(self) -> str:
        """Provision the table and write through any messages held in memory."""
        self._ctx = ContextRegistry.get_context(self, CHAT_TABLE)
        with self._lock:
            pending = [m for m in self.messages if m.row_id is None]
        for message in pending:
            self._persist(message)
        return self._ctx

    def load(self) -> int:
        """Prepend the messages earlier sessions stored; return how many."""
        rows = db.get_logs(
            context=self._ctx,
            limit=self.max_messages,
            sorting={"timestamp": "descending"},
        )
        with self._lock:
            known = {m.row_id for m in self.messages if m.row_id is not None}
            restored = [
                self._from_row(row) for row in reversed(rows) if row.id not in known
            ]
            self.messages = (restored + self.messages)[-self.max_messages :]
        return len(restored)

    def append(
        self,
        *,
        role: str,
        content: str,
        attachments: list[str] | None = None,
        timestamp: datetime | None = None,
    ) -> ChatMessage:
        """Record a message, writing it to the store when the table is bound."""
        message = ChatMessage(
            role=role,
            content=content or "",
            timestamp=timestamp or prompt_now(as_string=False),
            attachments=list(attachments or []),
        )
        with self._lock:
            self.messages.append(message)
            del self.messages[: -self.max_messages]
        if self._ctx is not None:
            self._persist(message)
        return message

    def recent(self, max_messages: int | None = None) -> list[ChatMessage]:
        with self._lock:
            messages = list(self.messages)
        if max_messages is None:
            return messages
        return messages[-max_messages:]

    def clear(self) -> None:
        """Forget the in-memory messages (the store is untouched)."""
        with self._lock:
            self.messages.clear()

    def _persist(self, message: ChatMessage) -> None:
        row = db.log(
            context=self._ctx,
            role=message.role,
            content=message.content,
            timestamp=message.timestamp.isoformat(),
            attachments=list(message.attachments),
        )
        message.row_id = row.id

    @staticmethod
    def _from_row(row: db.Log) -> ChatMessage:
        entries = row.entries
        timestamp = entries.get("timestamp")
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        return ChatMessage(
            role=entries["role"],
            content=entries.get("content") or "",
            timestamp=timestamp,
            attachments=list(entries.get("attachments") or []),
            row_id=row.id,
        )
