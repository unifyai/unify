"""
Event Publisher for the ConversationManager sandbox.

Converts sandbox commands into inbound CM events and publishes them to the
in-memory event broker using the same ``app:comms:*`` topics as production.

The ``meet`` / ``end_meet`` commands spawn the production voice agent subprocess
over LiveKit instead of injecting synthetic call events.
"""

from __future__ import annotations

import mimetypes
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from droid.conversation_manager.events import (
    Event,
    SMSReceived,
    UnifyMeetEnded,
    UnifyMessageReceived,
)

if TYPE_CHECKING:
    from sandboxes.conversation_manager.live_voice import LiveVoiceSession


def build_unify_attachment_meta(attachments: list[Path] | None) -> list[dict]:
    """Stage attachment files under ``Downloads`` and build their metadata.

    Returns the per-attachment dicts (id, filename, file URL, content type,
    size) carried on an inbound ``unify_message`` event. Used both when
    publishing synthetic chat events and when delivering the same payload
    through the real ingress transport in the flow-test harness.
    """
    if not attachments:
        return []
    downloads_dir = Path("Downloads")
    downloads_dir.mkdir(parents=True, exist_ok=True)
    attachment_meta: list[dict] = []
    for src in attachments:
        dest = downloads_dir / src.name
        shutil.copy2(src, dest)
        content_type = mimetypes.guess_type(src.name)[0] or "application/octet-stream"
        attachment_meta.append(
            {
                "id": str(uuid.uuid4()),
                "filename": src.name,
                "url": dest.resolve().as_uri(),
                "content_type": content_type,
                "size_bytes": src.stat().st_size,
            },
        )
    return attachment_meta


def get_user_contact(cm=None) -> dict:
    """Build a boss/user contact dict for sandbox events.

    When a ConversationManager instance is supplied the real user info loaded
    from Orchestra is used (name, number, email).  Falls back to env-var
    overrides and finally to neutral placeholder values so the sandbox always
    has a valid contact dict even before Orchestra data is available.
    """
    if cm is not None:
        first_name = getattr(cm, "user_first_name", None) or os.getenv(
            "USER_FIRST_NAME",
            "",
        )
        surname = getattr(cm, "user_surname", None) or os.getenv("USER_SURNAME", "")
        phone = getattr(cm, "user_number", None) or os.getenv(
            "USER_NUMBER",
            "+15550001234",
        )
        email = getattr(cm, "user_email", None) or os.getenv(
            "USER_EMAIL",
            "user@example.com",
        )
        user_id = getattr(cm, "user_id", None)
        boss_contact_id = getattr(cm, "boss_contact_id", None)
        return {
            "contact_id": boss_contact_id or 1,
            "user_id": user_id,
            "first_name": first_name or "User",
            "surname": surname,
            "phone_number": phone,
            "email_address": email,
            "is_system": True,
        }
    return {
        "contact_id": 1,
        "first_name": os.getenv("USER_FIRST_NAME", "User"),
        "surname": os.getenv("USER_SURNAME", ""),
        "phone_number": os.getenv("USER_NUMBER", "+15550001234"),
        "email_address": os.getenv("USER_EMAIL", "user@example.com"),
    }


@dataclass
class EventPublisher:
    cm: object
    state: object
    args: object | None = None

    async def publish_event(self, event: Event) -> None:
        """Publish any Event instance to the broker using its ``topic`` ClassVar."""
        topic = getattr(type(event), "topic", None)
        if not topic:
            raise ValueError(
                f"Event {type(event).__name__} has no topic ClassVar set",
            )
        contact = get_user_contact()
        try:
            self.cm.contact_index.set_fallback_contacts([contact])
        except Exception:
            pass
        self.state.last_event_published_at = time.monotonic()
        await self.cm.event_broker.publish(topic, event.to_json())

    async def publish_unify_message(
        self,
        message: str,
        attachments: list[Path] | None = None,
    ) -> None:
        contact = get_user_contact()
        await self.publish_event(
            UnifyMessageReceived(
                contact=contact,
                content=message,
                attachments=build_unify_attachment_meta(attachments),
            ),
        )

    async def publish_sms(self, message: str) -> None:
        contact = get_user_contact()
        await self.publish_event(SMSReceived(contact=contact, content=message))

    async def publish_meet_end(self) -> None:
        self.state.in_meet = False
        contact = get_user_contact()
        await self.publish_event(UnifyMeetEnded(contact=contact))

    # ── Live voice ────────────────────────────────────────────────────────

    async def _start_live_session(self) -> "list[str]":
        """Spawn a LiveKit voice agent and return status lines."""
        from sandboxes.conversation_manager.live_voice import start_session

        contact = get_user_contact(self.cm)
        boss = get_user_contact(self.cm)
        try:
            self.cm.contact_index.set_fallback_contacts([contact])
        except Exception:
            pass

        session = await start_session(
            cm=self.cm,
            contact=contact,
            boss=boss,
        )
        self.state.live_voice_session = session
        self.state.in_meet = True
        self.state.last_event_published_at = time.monotonic()

        browser_line = (
            "  Playground opened in your browser (auto-connecting)."
            if getattr(session, "browser_opened", False)
            else "  (Could not open browser — open the URL below manually.)"
        )

        waited = float(getattr(session, "ready_wait_seconds", 0.0) or 0.0)
        timeout = float(getattr(session, "ready_timeout_seconds", 0.0) or 0.0)
        source = str(getattr(session, "ready_source", "") or "").strip()
        if bool(getattr(session, "ready", False)):
            readiness_line = (
                f"✅ Voice agent ready ({waited:.1f}s; signal: {source}). "
                "You can speak immediately."
            )
        elif bool(getattr(session, "agent_joined_room", False)):
            readiness_line = (
                f"⏳ Agent joined room but CM is still waiting for "
                f"`UnifyMeetStarted` ({waited:.1f}s / {timeout:.1f}s). "
                "Wait for the greeting before speaking."
            )
        else:
            readiness_line = (
                f"⏳ Voice agent still booting ({waited:.1f}s / {timeout:.1f}s). "
                "Audio starts once initialization completes."
            )

        playground_url = getattr(session, "playground_url", "") or ""

        return [
            "",
            "🎙️  LiveKit voice session started!",
            "",
            readiness_line,
            "",
            browser_line,
            f"  URL: {playground_url}",
            "",
            f"  Room:      {session.room_name}",
            f"  Agent log: {session.log_file}",
            "",
            "Speak through your browser mic; type `end_meet` here when done.",
            "",
        ]

    async def start_live_meet(self) -> "list[str]":
        return await self._start_live_session()

    async def end_live_session(self) -> "list[str]":
        """Stop the live voice session and clean up LiveKit + subprocess resources."""
        from sandboxes.conversation_manager.live_voice import stop_session

        session: LiveVoiceSession | None = getattr(
            self.state,
            "live_voice_session",
            None,
        )
        if session is None:
            self.state.in_meet = False
            return ["⚠️ No live voice session to end."]

        await stop_session(cm=self.cm, session=session)
        self.state.live_voice_session = None
        self.state.in_meet = False
        self.state.last_event_published_at = time.monotonic()

        return ["🎙️ Live voice session ended. Room and subprocess cleaned up."]
